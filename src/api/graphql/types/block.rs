#[allow(unused_imports)]
use crate::api::graphql::{
    context::ApiContext,
    scalars::DateTime,
    // Import the string_to_ibc_status function from the transaction module
    types::transaction::string_to_ibc_status,
    types::{Event, Transaction},
};
use async_graphql::{Context, Object, Result};
use sqlx::Row;

#[derive(Debug, Clone)]
pub struct Block {
    pub height: i32,
    pub created_at: DateTime,
    pub raw_json: Option<serde_json::Value>,
}

#[Object]
impl Block {
    async fn height(&self) -> i32 {
        self.height
    }

    #[graphql(name = "createdAt")]
    async fn created_at(&self) -> &DateTime {
        &self.created_at
    }

    #[graphql(name = "transactionsCount")]
    async fn transactions_count(&self, ctx: &Context<'_>) -> Result<i32> {
        let db = &ctx.data_unchecked::<ApiContext>().db;
        let result = sqlx::query_as::<_, (i32,)>(
            "SELECT num_transactions FROM explorer_block_details WHERE height = $1",
        )
        .bind(i64::from(self.height))
        .fetch_one(db)
        .await?;
        Ok(result.0)
    }

    async fn transactions(&self, ctx: &Context<'_>) -> Result<Vec<Transaction>> {
        let db = &ctx.data_unchecked::<ApiContext>().db;
        let rows = sqlx::query(
            r"
        SELECT
            tx_hash,
            block_height,
            timestamp,
            fee_amount::TEXT as fee_amount_str,
            chain_id,
            raw_data,
            raw_json,
            ibc_client_id,
            COALESCE(ibc_status, 'unknown') as ibc_status
        FROM
            explorer_transactions
        WHERE
            block_height = $1
        ORDER BY
            timestamp ASC
        ",
        )
        .bind(i64::from(self.height))
        .fetch_all(db)
        .await?;

        let mut transactions = Vec::with_capacity(rows.len());

        for row in rows {
            let tx_hash: Vec<u8> = row.get("tx_hash");
            let _block_height: i64 = row.get("block_height");
            let _timestamp: chrono::DateTime<chrono::Utc> = row.get("timestamp");
            let _fee_amount_str: String = row.get("fee_amount_str");
            let raw_data: String = row.get("raw_data");
            let raw_json_str: String = row.get("raw_json");
            let client_id: Option<String> = row.get("ibc_client_id");
            let ibc_status_str: String = row.get("ibc_status");
            let ibc_status = string_to_ibc_status(Some(&ibc_status_str));

            if !raw_json_str.is_empty() {
                // First parse the JSON for metadata extraction, but use the original string for storage
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&raw_json_str) {
                    let hash = hex::encode_upper(&tx_hash);
                    transactions.push(Transaction {
                        hash: hash.clone(),
                        anchor: String::new(),
                        binding_sig: String::new(),
                        index: extract_index_from_json(&json).unwrap_or(0),
                        raw: raw_data.clone(),
                        block: self.clone(),
                        body: crate::api::graphql::types::extract_transaction_body(&json),
                        raw_events: extract_events_from_json(&json),
                        // Store the original string to preserve DB ordering
                        raw_json: serde_json::Value::String(raw_json_str.clone()),
                        client_id,
                        ibc_status,
                    });
                }
            }
        }

        Ok(transactions)
    }
    #[graphql(name = "rawEvents")]
    #[allow(clippy::unused_async)]
    async fn raw_events(&self) -> Result<Vec<Event>> {
        let events = if let Some(json) = &self.raw_json {
            extract_events_from_block_json(json)
        } else {
            Vec::new()
        };
        Ok(events)
    }

    #[graphql(name = "rawJson")]
    #[allow(clippy::unused_async)]
    async fn raw_json(&self) -> Result<Option<String>> {
        if let Some(json_value) = &self.raw_json {
            if let Some(raw_str) = json_value.as_str() {
                Ok(Some(raw_str.to_string()))
            } else {
                Ok(Some(serde_json::to_string(json_value)?))
            }
        } else {
            Ok(None)
        }
    }

    #[graphql(name = "chainId")]
    async fn chain_id(&self, ctx: &Context<'_>) -> Result<Option<String>> {
        let db = &ctx.data_unchecked::<ApiContext>().db;
        let chain_id = sqlx::query_scalar::<_, Option<String>>(
            "SELECT chain_id FROM explorer_block_details WHERE height = $1",
        )
        .bind(i64::from(self.height))
        .fetch_one(db)
        .await?;
        Ok(chain_id)
    }
}

use async_graphql::SimpleObject;

#[derive(SimpleObject)]
#[allow(clippy::module_name_repetitions)]
pub struct DbBlock {
    pub height: i64,
    pub root_hex: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub num_transactions: i32,
    pub total_fees: Option<String>,
    pub validator_identity_key: Option<String>,
    pub previous_block_hash_hex: Option<String>,
    pub block_hash_hex: Option<String>,
    pub chain_id: Option<String>,
}

impl DbBlock {
    /// Gets a block by its height
    ///
    /// # Errors
    /// Returns an error if the database query fails
    pub async fn get_by_height(ctx: &Context<'_>, height: i64) -> Result<Option<Self>> {
        let db = &ctx.data_unchecked::<ApiContext>().db;
        let row_result = sqlx::query(
            r"
            SELECT
                height,
                root,
                timestamp,
                num_transactions,
                COALESCE(total_fees::TEXT, '0') as total_fees,
                validator_identity_key,
                previous_block_hash,
                block_hash,
                chain_id
            FROM
                explorer_block_details
            WHERE
                height = $1
            ",
        )
        .bind(height)
        .fetch_optional(db)
        .await?;
        if let Some(row) = row_result {
            let root: Vec<u8> = row.get("root");
            let previous_block_hash: Option<Vec<u8>> = row.get("previous_block_hash");
            let block_hash: Option<Vec<u8>> = row.get("block_hash");
            Ok(Some(Self {
                height: row.get("height"),
                root_hex: hex::encode_upper(&root),
                timestamp: row.get("timestamp"),
                num_transactions: row.get("num_transactions"),
                total_fees: row.get("total_fees"),
                validator_identity_key: row.get("validator_identity_key"),
                previous_block_hash_hex: previous_block_hash.map(|hash| hex::encode_upper(&hash)),
                block_hash_hex: block_hash.map(|hash| hex::encode_upper(&hash)),
                chain_id: row.get("chain_id"),
            }))
        } else {
            Ok(None)
        }
    }

    /// Gets all blocks with pagination
    ///
    /// # Errors
    /// Returns an error if the database query fails
    pub async fn get_all(
        ctx: &Context<'_>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<Self>> {
        let db = &ctx.data_unchecked::<ApiContext>().db;
        let limit = limit.unwrap_or(10);
        let offset = offset.unwrap_or(0);
        let rows = sqlx::query(
            r"
            SELECT
                height,
                root,
                timestamp,
                num_transactions,
                COALESCE(total_fees::TEXT, '0') as total_fees,
                validator_identity_key,
                previous_block_hash,
                block_hash,
                chain_id
            FROM
                explorer_block_details
            ORDER BY
                height DESC
            LIMIT $1 OFFSET $2
            ",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(db)
        .await?;
        let mut blocks = Vec::with_capacity(rows.len());
        for row in rows {
            let root: Vec<u8> = row.get("root");
            let previous_block_hash: Option<Vec<u8>> = row.get("previous_block_hash");
            let block_hash: Option<Vec<u8>> = row.get("block_hash");
            blocks.push(Self {
                height: row.get("height"),
                root_hex: hex::encode_upper(&root),
                timestamp: row.get("timestamp"),
                num_transactions: row.get("num_transactions"),
                total_fees: row.get("total_fees"),
                validator_identity_key: row.get("validator_identity_key"),
                previous_block_hash_hex: previous_block_hash.map(|hash| hex::encode_upper(&hash)),
                block_hash_hex: block_hash.map(|hash| hex::encode_upper(&hash)),
                chain_id: row.get("chain_id"),
            });
        }
        Ok(blocks)
    }

    /// Gets the latest block
    ///
    /// # Errors
    /// Returns an error if the database query fails
    pub async fn get_latest(ctx: &Context<'_>) -> Result<Option<Self>> {
        let db = &ctx.data_unchecked::<ApiContext>().db;
        let row_result = sqlx::query(
            r"
            SELECT
                height,
                root,
                timestamp,
                num_transactions,
                COALESCE(total_fees::TEXT, '0') as total_fees,
                validator_identity_key,
                previous_block_hash,
                block_hash,
                chain_id
            FROM
                explorer_block_details
            ORDER BY
                height DESC
            LIMIT 1
            ",
        )
        .fetch_optional(db)
        .await?;
        if let Some(row) = row_result {
            let root: Vec<u8> = row.get("root");
            let previous_block_hash: Option<Vec<u8>> = row.get("previous_block_hash");
            let block_hash: Option<Vec<u8>> = row.get("block_hash");
            Ok(Some(Self {
                height: row.get("height"),
                root_hex: hex::encode_upper(&root),
                timestamp: row.get("timestamp"),
                num_transactions: row.get("num_transactions"),
                total_fees: row.get("total_fees"),
                validator_identity_key: row.get("validator_identity_key"),
                previous_block_hash_hex: previous_block_hash.map(|hash| hex::encode_upper(&hash)),
                block_hash_hex: block_hash.map(|hash| hex::encode_upper(&hash)),
                chain_id: row.get("chain_id"),
            }))
        } else {
            Ok(None)
        }
    }
}

impl Block {
    #[must_use]
    pub fn new(
        height: i32,
        created_at: chrono::DateTime<chrono::Utc>,
        raw_json: Option<serde_json::Value>,
    ) -> Self {
        Self {
            height,
            created_at: DateTime(created_at),
            raw_json,
        }
    }
}

fn extract_index_from_json(json: &serde_json::Value) -> Option<i32> {
    json.get("index")
        .and_then(|i| i.as_str())
        .and_then(|i| i.parse::<i32>().ok())
}

fn extract_events_from_json(json: &serde_json::Value) -> Vec<Event> {
    let mut events = Vec::new();
    if let Some(events_array) = json.get("events").and_then(|e| e.as_array()) {
        for event_json in events_array {
            if let Some(event_type) = event_json.get("type").and_then(|t| t.as_str()) {
                let event_value = serde_json::to_string(event_json).unwrap_or_default();
                events.push(Event {
                    type_: event_type.to_string(),
                    value: event_value,
                });
            }
        }
    }
    events
}

fn extract_events_from_block_json(json: &serde_json::Value) -> Vec<Event> {
    let mut events = Vec::new();
    if let Some(block) = json.get("block") {
        if let Some(events_array) = block.get("events").and_then(|e| e.as_array()) {
            for event_json in events_array {
                if let Some(event_type) = event_json.get("type").and_then(|t| t.as_str()) {
                    let event_value = serde_json::to_string(event_json).unwrap_or_default();
                    events.push(Event {
                        type_: event_type.to_string(),
                        value: event_value,
                    });
                }
            }
        }
    }
    events
}
