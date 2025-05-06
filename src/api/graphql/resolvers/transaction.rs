use crate::api::graphql::{
    context::ApiContext,
    types::{
        string_to_ibc_status, Block, CollectionLimit, Event, RangeDirection, Transaction,
        TransactionCollection, TransactionFilter, TransactionsSelector,
    },
};
use async_graphql::Result;
use sqlx::Row;

/// Resolves a transaction by its hash
///
/// # Errors
/// Returns an error if database queries fail
#[allow(clippy::module_name_repetitions)]
pub async fn resolve_transaction(
    ctx: &async_graphql::Context<'_>,
    hash: String,
) -> Result<Option<Transaction>> {
    let db = &ctx.data_unchecked::<ApiContext>().db;

    let Ok(hash_bytes) = hex::decode(hash.trim_start_matches("0x")) else {
        return Ok(None);
    };

    let row = sqlx::query(
        r"
        SELECT
            t.tx_hash,
            t.block_height,
            t.timestamp,
            t.fee_amount::TEXT as fee_amount_str,
            t.chain_id,
            t.raw_data,
            t.raw_json,
            b.timestamp as block_timestamp,
            t.ibc_client_id,
            COALESCE(t.ibc_status, 'unknown') as ibc_status
        FROM
            explorer_transactions t
        JOIN
            explorer_block_details b ON t.block_height = b.height
        WHERE
            t.tx_hash = $1
        ",
    )
    .bind(hash_bytes.as_slice())
    .fetch_optional(db)
    .await?;

    if let Some(r) = row {
        let tx_hash: Vec<u8> = r.get("tx_hash");
        let block_height: i64 = r.get("block_height");
        let timestamp: chrono::DateTime<chrono::Utc> = r.get("block_timestamp");
        let _fee_amount_str: String = r.get("fee_amount_str");
        let _chain_id: Option<String> = r.get("chain_id");
        let raw_data: String = r.get("raw_data");
        let raw_json_str: String = r.get("raw_json");
        let client_id: Option<String> = r.get("ibc_client_id");
        let ibc_status_str: String = r.get("ibc_status");
        let ibc_status = string_to_ibc_status(Some(&ibc_status_str));

        let json_value = match serde_json::from_str::<serde_json::Value>(&raw_json_str) {
            Ok(value) => value,
            Err(_) => serde_json::json!({}),
        };

        let hash = hex::encode_upper(&tx_hash);

        Ok(Some(Transaction {
            hash,
            anchor: String::new(),
            binding_sig: String::new(),
            index: extract_index_from_json(&json_value).unwrap_or(0),
            raw: raw_data.clone(),
            block: Block::new(
                i32::try_from(block_height).unwrap_or_default(),
                timestamp,
                None,
            ),
            body: crate::api::graphql::types::extract_transaction_body(&json_value),
            raw_events: extract_events_from_json(&json_value),
            raw_json: json_value,
            client_id,
            ibc_status,
        }))
    } else {
        Ok(None)
    }
}

/// Resolves transactions based on the provided selector
///
/// # Errors
/// Returns an error if database queries fail
#[allow(clippy::module_name_repetitions)]
pub async fn resolve_transactions(
    ctx: &async_graphql::Context<'_>,
    selector: TransactionsSelector,
) -> Result<Vec<Transaction>> {
    let db = &ctx.data_unchecked::<ApiContext>().db;

    // Updated base query to include ibc_client_id and ibc_status
    let base_query = r"
        SELECT
            t.tx_hash,
            t.block_height,
            t.timestamp,
            t.fee_amount::TEXT as fee_amount_str,
            t.chain_id,
            t.raw_data,
            t.raw_json,
            b.timestamp as block_timestamp,
            t.ibc_client_id,
            COALESCE(t.ibc_status, 'unknown') as ibc_status
        FROM
            explorer_transactions t
        JOIN
            explorer_block_details b ON t.block_height = b.height
    ";

    let (query, _param_count) = build_transactions_query(&selector, base_query);

    let rows = if let Some(range) = &selector.range {
        let Ok(hash_bytes) = hex::decode(range.from_tx_hash.trim_start_matches("0x")) else {
            return Ok(vec![]);
        };

        // Handle client_id filter if present
        if let Some(client_id) = &selector.client_id {
            sqlx::query(&query)
                .bind(client_id)
                .bind(&hash_bytes)
                .bind(i64::from(range.limit))
                .fetch_all(db)
                .await?
        } else {
            sqlx::query(&query)
                .bind(&hash_bytes)
                .bind(i64::from(range.limit))
                .fetch_all(db)
                .await?
        }
    } else if let Some(latest) = &selector.latest {
        // Handle client_id filter if present
        if let Some(client_id) = &selector.client_id {
            sqlx::query(&query)
                .bind(client_id)
                .bind(i64::from(latest.limit))
                .fetch_all(db)
                .await?
        } else {
            sqlx::query(&query)
                .bind(i64::from(latest.limit))
                .fetch_all(db)
                .await?
        }
    } else if let Some(client_id) = &selector.client_id {
        sqlx::query(&query).bind(client_id).fetch_all(db).await?
    } else {
        sqlx::query(&query).fetch_all(db).await?
    };

    let mut transactions = process_transaction_rows(rows)?;

    if let Some(range) = &selector.range {
        if range.direction == RangeDirection::Previous {
            transactions.reverse();
        }
    }

    Ok(transactions)
}

/// Resolves transactions with pagination and optional filtering
///
/// # Errors
/// Returns an error if database queries fail
///
/// # Panics
/// This function may panic if the `hash_bytes_storage` is accessed while None,
/// which shouldn't occur due to the logic flow that only accesses the storage when it's initialized.
pub async fn resolve_transactions_collection(
    ctx: &async_graphql::Context<'_>,
    limit: CollectionLimit,
    filter: Option<TransactionFilter>,
) -> Result<TransactionCollection> {
    let db = &ctx.data_unchecked::<ApiContext>().db;

    // Create storage for our potential hash bytes
    let mut hash_bytes_storage: Option<Vec<u8>> = None;

    let mut count_query = String::from("SELECT COUNT(*) FROM explorer_transactions");
    let mut where_clauses = Vec::new();
    let mut param_count = 0;

    // Build WHERE clauses
    if let Some(filter) = &filter {
        if let Some(hash) = &filter.hash {
            if let Ok(hash_bytes) = hex::decode(hash.trim_start_matches("0x")) {
                hash_bytes_storage = Some(hash_bytes);
                param_count += 1;
                where_clauses.push(format!("tx_hash = ${param_count}"));
            } else {
                return Ok(TransactionCollection {
                    items: vec![],
                    total: 0,
                });
            }
        }

        // Add client_id filter
        if filter.client_id.is_some() {
            param_count += 1;
            where_clauses.push(format!("ibc_client_id = ${param_count}"));
        }
    }

    // Apply WHERE clauses to query if any exist
    if !where_clauses.is_empty() {
        count_query.push_str(" WHERE ");
        count_query.push_str(&where_clauses.join(" AND "));
    }

    // Build count query
    let mut count_query_builder = sqlx::query_scalar::<_, i64>(&count_query);

    // Bind parameters to count query
    if let Some(filter) = &filter {
        if let Some(hash_bytes) = &hash_bytes_storage {
            count_query_builder = count_query_builder.bind(hash_bytes.as_slice());
        }

        if let Some(client_id) = &filter.client_id {
            count_query_builder = count_query_builder.bind(client_id);
        }
    }

    let total_count = count_query_builder.fetch_one(db).await?;

    // Updated base query to include ibc_client_id and ibc_status
    let base_query = r"
        SELECT
            t.tx_hash,
            t.block_height,
            t.timestamp,
            t.fee_amount::TEXT as fee_amount_str,
            t.chain_id,
            t.raw_data,
            t.raw_json,
            b.timestamp as block_timestamp,
            t.ibc_client_id,
            COALESCE(t.ibc_status, 'unknown') as ibc_status
        FROM
            explorer_transactions t
        JOIN
            explorer_block_details b ON t.block_height = b.height
    ";

    let mut query = String::from(base_query);

    // Apply WHERE clauses to query if any exist
    if !where_clauses.is_empty() {
        query.push_str(" WHERE ");
        query.push_str(&where_clauses.join(" AND "));
    }

    query.push_str(" ORDER BY t.timestamp DESC, t.tx_hash ASC");

    let length = limit.length.unwrap_or(10);
    let offset = limit.offset.unwrap_or(0);

    query.push_str(&format!(" LIMIT {length} OFFSET {offset}"));

    // Build data query
    let mut query_builder = sqlx::query(&query);

    // Bind parameters to data query
    if let Some(filter) = &filter {
        if let Some(hash_bytes) = &hash_bytes_storage {
            query_builder = query_builder.bind(hash_bytes.as_slice());
        }

        if let Some(client_id) = &filter.client_id {
            query_builder = query_builder.bind(client_id);
        }
    }

    let rows = query_builder.fetch_all(db).await?;

    let transactions = process_transaction_rows(rows)?;

    Ok(TransactionCollection {
        items: transactions,
        total: i32::try_from(total_count).unwrap_or(0),
    })
}

#[allow(clippy::unnecessary_wraps)]
fn process_transaction_rows(rows: Vec<sqlx::postgres::PgRow>) -> Result<Vec<Transaction>> {
    let mut transactions = Vec::with_capacity(rows.len());

    for row in rows {
        let tx_hash: Vec<u8> = row.get("tx_hash");
        let block_height: i64 = row.get("block_height");
        let timestamp: chrono::DateTime<chrono::Utc> = row.get("block_timestamp");
        let raw_data: String = row.get("raw_data");
        let raw_json_str: String = row.get("raw_json");
        let client_id: Option<String> = row.get("ibc_client_id");
        let ibc_status_str: String = row.get("ibc_status");
        let ibc_status = string_to_ibc_status(Some(&ibc_status_str));

        if !raw_json_str.is_empty() {
            let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&raw_json_str) else {
                tracing::warn!(
                    "Failed to parse JSON for transaction: {}",
                    hex::encode_upper(&tx_hash)
                );
                continue;
            };

            let hash = hex::encode_upper(&tx_hash);

            transactions.push(Transaction {
                hash,
                anchor: String::new(),
                binding_sig: String::new(),
                index: extract_index_from_json(&json_value).unwrap_or(0),
                raw: raw_data.clone(),
                block: Block::new(
                    i32::try_from(block_height).unwrap_or_default(),
                    timestamp,
                    None,
                ),
                body: crate::api::graphql::types::extract_transaction_body(&json_value),
                raw_events: extract_events_from_json(&json_value),
                raw_json: json_value,
                client_id,
                ibc_status,
            });
        }
    }

    Ok(transactions)
}

fn extract_index_from_json(json: &serde_json::Value) -> Option<i32> {
    json.get("index")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i32>().ok())
}

fn extract_events_from_json(json: &serde_json::Value) -> Vec<Event> {
    let mut events = Vec::new();

    if let Some(array) = json.get("events").and_then(|e| e.as_array()) {
        for e in array {
            if let Some(typ) = e.get("type").and_then(|t| t.as_str()) {
                events.push(Event {
                    type_: typ.to_string(),
                    value: serde_json::to_string(e).unwrap_or_default(),
                });
            }
        }
    }

    events
}

fn build_transactions_query(selector: &TransactionsSelector, base: &str) -> (String, usize) {
    let mut query = String::from(base);
    let mut param_count: usize = 0;
    let mut where_clauses = Vec::new();

    // Add client_id filter if present
    if let Some(_client_id) = &selector.client_id {
        param_count += 1;
        where_clauses.push(format!("t.ibc_client_id = ${param_count}"));
    }

    if let Some(range) = &selector.range {
        param_count = if selector.client_id.is_some() { 2 } else { 1 };

        let ref_query = "(SELECT timestamp FROM explorer_transactions WHERE tx_hash = $";
        let param_pos = if selector.client_id.is_some() { 2 } else { 1 };
        let formatted_ref_query = format!("{ref_query}{param_pos})");

        if !where_clauses.is_empty() {
            where_clauses.push("AND".to_string());
        }

        match range.direction {
            RangeDirection::Next => {
                where_clauses.push(format!("((t.timestamp < {formatted_ref_query})"));
                where_clauses.push(format!(
                    "OR (t.timestamp = {formatted_ref_query} AND t.tx_hash > ${param_pos}))"
                ));
            }
            RangeDirection::Previous => {
                where_clauses.push(format!("((t.timestamp > {formatted_ref_query})"));
                where_clauses.push(format!(
                    "OR (t.timestamp = {formatted_ref_query} AND t.tx_hash < ${param_pos}))"
                ));
            }
        }
    }

    if !where_clauses.is_empty() {
        query.push_str(" WHERE ");
        query.push_str(&where_clauses.join(" "));
    }

    if let Some(range) = &selector.range {
        match range.direction {
            RangeDirection::Next => {
                query.push_str(" ORDER BY t.timestamp DESC, t.tx_hash ASC");
            }
            RangeDirection::Previous => {
                query.push_str(" ORDER BY t.timestamp ASC, t.tx_hash DESC");
            }
        }

        param_count += 1;
        query.push_str(&format!(" LIMIT ${param_count}"));
    } else if selector.latest.is_some() {
        query.push_str(" ORDER BY t.timestamp DESC, t.tx_hash ASC");

        if selector.client_id.is_some() {
            param_count += 1;
            query.push_str(&format!(" LIMIT ${param_count}"));
        } else {
            param_count = 1;
            query.push_str(" LIMIT $1");
        }
    } else {
        query.push_str(" ORDER BY t.timestamp DESC, t.tx_hash ASC LIMIT 10");
    }

    (query, param_count)
}
