use anyhow::Result;
use cometindex::{
    async_trait,
    index::{EventBatch, EventBatchContext},
    sqlx, AppView, ContextualizedEvent, PgTransaction,
};
use serde_json::Value;
use sqlx::{
    postgres::PgPool,
    types::chrono::{DateTime, Utc},
};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;

use crate::app_views::utils::block::Metadata as BlockMetadata;
use crate::app_views::utils::transaction::Metadata as TransactionMetadata;
use crate::app_views::utils::validator::ValidatorParams;
use crate::app_views::utils::{block, ibc, transaction, validator};
use crate::parsing::encode_to_base64;

#[derive(Debug)]
pub struct Explorer {
    source_pool: Option<Arc<PgPool>>,
    chain_id: Option<String>,
}

impl Default for Explorer {
    fn default() -> Self {
        Self::new()
    }
}

impl Explorer {
    #[must_use]
    pub fn new() -> Self {
        let chain_id = Self::read_chain_id_from_genesis();

        if let Some(id) = &chain_id {
            tracing::info!("Initialized Explorer with chain_id = {}", id);
        } else {
            tracing::warn!("Failed to read chain ID from genesis.json, will use 'unknown'");
        }

        Self {
            source_pool: None,
            chain_id,
        }
    }

    #[must_use]
    pub fn with_source_pool(mut self, pool: Arc<PgPool>) -> Self {
        self.source_pool = Some(pool);
        self
    }

    fn read_chain_id_from_genesis() -> Option<String> {
        let file = match File::open("genesis.json") {
            Ok(f) => f,
            Err(e) => {
                tracing::error!("Failed to open genesis.json: {}", e);
                return None;
            }
        };

        let mut contents = String::new();
        if let Err(e) = file.take(10_000_000).read_to_string(&mut contents) {
            tracing::error!("Failed to read genesis.json: {}", e);
            return None;
        }

        // Parse genesis JSON as a generic Value
        let genesis: Result<Value, _> = serde_json::from_str(&contents);
        if let Err(e) = genesis {
            tracing::error!("Failed to parse genesis.json: {}", e);
            return None;
        }

        let genesis = genesis.unwrap();

        let chain_id = genesis["chain_id"].as_str().map(String::from);

        if chain_id.is_none() {
            let app_chain_id = genesis["app_state"]["genesisContent"]["chainId"]
                .as_str()
                .map(String::from);

            if app_chain_id.is_none() {
                tracing::error!("Could not find chain_id in genesis.json");
            }

            app_chain_id
        } else {
            chain_id
        }
    }

    fn get_chain_id(&self) -> &str {
        self.chain_id.as_deref().unwrap_or("unknown")
    }

    /// Initialize validators from genesis.json
    #[allow(clippy::too_many_lines)]
    async fn initialize_validators_from_genesis(
        &self,
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<(), anyhow::Error> {
        let validator_count: i64 = match sqlx::query_scalar("SELECT COUNT(*) FROM validators")
            .fetch_one(dbtx.as_mut())
            .await
        {
            Ok(count) => count,
            Err(e) => {
                tracing::error!("Failed to check validator count: {}", e);
                return Err(anyhow::anyhow!("Failed to check validator count: {}", e));
            }
        };

        if validator_count > 0 {
            tracing::info!(
                "Validators already initialized (found {}), skipping genesis initialization",
                validator_count
            );
            return Ok(());
        }

        let file = match File::open("genesis.json") {
            Ok(f) => f,
            Err(e) => {
                tracing::error!("Failed to open genesis.json: {}", e);
                return Err(anyhow::anyhow!("Failed to open genesis.json: {}", e));
            }
        };

        let mut contents = String::new();
        if let Err(e) = file.take(10_000_000).read_to_string(&mut contents) {
            tracing::error!("Failed to read genesis.json: {}", e);
            return Err(anyhow::anyhow!("Failed to read genesis.json: {}", e));
        }

        let genesis: serde_json::Value = match serde_json::from_str(&contents) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!("Failed to parse genesis.json: {}", e);
                return Err(anyhow::anyhow!("Failed to parse genesis.json: {}", e));
            }
        };

        let Some(genesis_time) = genesis["genesis_time"].as_str() else {
            tracing::error!("Missing genesis_time in genesis.json");
            return Err(anyhow::anyhow!("Missing genesis_time in genesis.json"));
        };

        let timestamp = match chrono::DateTime::parse_from_rfc3339(genesis_time) {
            Ok(t) => t.with_timezone(&Utc),
            Err(e) => {
                tracing::error!("Failed to parse genesis time: {}", e);
                return Err(anyhow::anyhow!("Failed to parse genesis time: {}", e));
            }
        };

        let Some(validators) = genesis
            .get("app_state")
            .and_then(|app_state| app_state.get("genesisContent"))
            .and_then(|content| content.get("stakeContent"))
            .and_then(|stake| stake.get("validators"))
            .and_then(|vals| vals.as_array())
        else {
            tracing::warn!("No validators found in genesis.json structure");
            return Ok(());
        };

        if validators.is_empty() {
            tracing::warn!("Empty validators array in genesis.json");
            return Ok(());
        }

        tracing::info!("Found {} validators in genesis.json", validators.len());

        let mut successful_count = 0;
        let mut failed_count = 0;

        for (i, validator_data) in validators.iter().enumerate() {
            let validator_name = validator_data["name"].as_str().unwrap_or("unknown");

            if validator_data
                .get("identityKey")
                .and_then(|key| key.get("ik"))
                .and_then(|ik| ik.as_str())
                .is_none()
            {
                failed_count += 1;
                tracing::error!(
                    "Validator #{} missing required identityKey.ik field, skipping",
                    i
                );
                continue;
            }

            let state = "VALIDATOR_STATE_ENUM_ACTIVE";

            let bonding_state = validator_data
                .get("bondingState")
                .and_then(|s| s.get("state"))
                .and_then(|s| s.as_str());

            match validator::Validator::from_event(
                validator_data,
                1,
                timestamp,
                state,
                bonding_state.unwrap_or(""),
                0,
                0.0,
            ) {
                Ok(validator) => match validator.insert_or_update(dbtx).await {
                    Ok(()) => {
                        if let Some(funding_streams) = validator_data.get("fundingStreams") {
                            if let Err(e) =
                                validator::ValidatorFundingStream::process_funding_streams(
                                    &validator.identity_key,
                                    funding_streams,
                                    timestamp,
                                    dbtx,
                                )
                                .await
                            {
                                tracing::error!("Failed to process funding streams for genesis validator #{} ({}): {}", i, validator_name, e);
                            }
                        }

                        successful_count += 1;
                        tracing::info!(
                            "Successfully inserted genesis validator #{}: {}",
                            i,
                            validator_name
                        );
                    }
                    Err(e) => {
                        failed_count += 1;
                        tracing::error!(
                            "Failed to insert genesis validator #{} ({}): {}",
                            i,
                            validator_name,
                            e
                        );
                    }
                },
                Err(e) => {
                    failed_count += 1;
                    tracing::error!(
                        "Failed to parse genesis validator #{} ({}): {}",
                        i,
                        validator_name,
                        e
                    );
                }
            }
        }

        tracing::info!(
            "Genesis validator initialization complete: {} successful, {} failed",
            successful_count,
            failed_count
        );

        Ok(())
    }

    /// Record signed blocks for all active validators for a given height
    async fn record_validator_blocks_for_height(
        &self,
        dbtx: &mut PgTransaction<'_>,
        height: u64,
        timestamp: DateTime<Utc>,
    ) -> Result<(), anyhow::Error> {
        let block_exists: i64 = match sqlx::query_scalar(
            "SELECT COUNT(*) FROM explorer_block_details WHERE height = $1",
        )
        .bind(i64::try_from(height).unwrap_or(i64::MAX))
        .fetch_one(dbtx.as_mut())
        .await
        {
            Ok(count) => count,
            Err(e) => {
                tracing::error!(
                    "Failed to check if block exists at height {}: {}",
                    height,
                    e
                );
                return Ok(());
            }
        };

        if block_exists == 0 {
            tracing::debug!(
                "Skipping validator block records for non-existent block at height {}",
                height
            );
            return Ok(());
        }

        let validator_count: i64 = match sqlx::query_scalar("SELECT COUNT(*) FROM validators")
            .fetch_one(dbtx.as_mut())
            .await
        {
            Ok(count) => count,
            Err(e) => {
                tracing::error!("Failed to check validator count: {}", e);
                return Ok(());
            }
        };

        if validator_count == 0 {
            tracing::debug!("No validators in database, skipping block participation records");
            return Ok(());
        }

        let active_state: Option<String> = match sqlx::query_scalar(
            "SELECT DISTINCT state FROM validators WHERE state LIKE '%ACTIVE%' LIMIT 1",
        )
        .fetch_optional(dbtx.as_mut())
        .await
        {
            Ok(state) => state,
            Err(e) => {
                tracing::error!("Failed to determine active validator state: {}", e);
                return Ok(());
            }
        };

        if active_state.is_none() {
            tracing::debug!(
                "No active validator state found, skipping block participation records"
            );
            return Ok(());
        }

        let active_validators: Vec<String> = match sqlx::query_scalar(&format!(
            "SELECT identity_key FROM validators WHERE state = '{}'",
            active_state.unwrap()
        ))
        .fetch_all(dbtx.as_mut())
        .await
        {
            Ok(validators) => validators,
            Err(e) => {
                tracing::error!("Failed to retrieve active validators: {}", e);
                return Ok(());
            }
        };

        if active_validators.is_empty() {
            tracing::debug!("No active validators found, skipping block participation records");
            return Ok(());
        }

        tracing::debug!(
            "Recording signed blocks for {} active validators at height {}",
            active_validators.len(),
            height
        );

        let validator_records: Vec<(String, i64, DateTime<Utc>, bool)> = active_validators
            .into_iter()
            .map(|identity_key| {
                (
                    identity_key,
                    i64::try_from(height).unwrap_or(i64::MAX),
                    timestamp,
                    true,
                )
            })
            .collect();

        let _ = validator::Validator::record_validator_blocks_bulk(&validator_records, dbtx).await;

        Ok(())
    }
}

#[async_trait]
impl AppView for Explorer {
    fn name(&self) -> String {
        "explorer".to_string()
    }

    #[allow(clippy::too_many_lines)]
    async fn init_chain(&self, dbtx: &mut PgTransaction, _: &Value) -> Result<(), anyhow::Error> {
        tracing::info!(
            "Initializing Explorer with chain_id = {}",
            self.get_chain_id()
        );

        sqlx::query(
            r"
            CREATE TABLE IF NOT EXISTS explorer_block_details (
                height BIGINT PRIMARY KEY,
                root BYTEA NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                num_transactions INT NOT NULL DEFAULT 0,
                total_fees NUMERIC(39, 0) DEFAULT 0,
                validator_identity_key TEXT,
                previous_block_hash BYTEA,
                block_hash BYTEA,
                chain_id TEXT,
                raw_json JSONB
            )
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE INDEX IF NOT EXISTS idx_explorer_block_details_timestamp
            ON explorer_block_details(timestamp DESC)
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE INDEX IF NOT EXISTS idx_explorer_block_details_validator
            ON explorer_block_details(validator_identity_key)
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE TABLE IF NOT EXISTS explorer_transactions (
                tx_hash BYTEA PRIMARY KEY,
                block_height BIGINT NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                fee_amount NUMERIC(39, 0) DEFAULT 0,
                chain_id TEXT,
                raw_data TEXT,
                raw_json JSONB,
                -- IBC fields
                ibc_channel_id TEXT,
                ibc_client_id TEXT,
                ibc_status TEXT,
                ibc_direction TEXT,
                ibc_sequence TEXT,
                -- Validator field
                validator_identity_key TEXT,
                FOREIGN KEY (block_height) REFERENCES explorer_block_details(height)
                    DEFERRABLE INITIALLY DEFERRED
            )
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE INDEX IF NOT EXISTS idx_explorer_transactions_block_height
            ON explorer_transactions(block_height)
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE INDEX IF NOT EXISTS idx_explorer_transactions_timestamp
            ON explorer_transactions(timestamp DESC)
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE INDEX IF NOT EXISTS idx_explorer_transactions_validator_identity_key
            ON explorer_transactions(validator_identity_key)
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE OR REPLACE VIEW explorer_recent_blocks AS
            SELECT
                height,
                timestamp,
                num_transactions,
                total_fees,
                validator_identity_key,
                chain_id,
                raw_json
            FROM
                explorer_block_details
            ORDER BY
                height DESC
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE OR REPLACE VIEW explorer_transaction_summary AS
            SELECT
                t.tx_hash,
                t.block_height,
                t.timestamp,
                t.fee_amount,
                t.chain_id,
                t.raw_json,
                t.ibc_channel_id,
                t.ibc_client_id,
                t.ibc_status,
                t.ibc_direction,
                t.ibc_sequence,
                t.validator_identity_key
            FROM
                explorer_transactions t
            ORDER BY
                t.timestamp DESC
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE TABLE IF NOT EXISTS ibc_clients (
                client_id TEXT PRIMARY KEY,
                status TEXT DEFAULT 'Unknown',
                channel_id TEXT,
                counterparty_channel_id TEXT,
                last_active_height BIGINT,
                last_active_time TIMESTAMP WITH TIME ZONE
            )
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE TABLE IF NOT EXISTS ibc_connections (
                connection_id TEXT PRIMARY KEY,
                client_id TEXT NOT NULL REFERENCES ibc_clients(client_id),
                counterparty_connection_id TEXT,
                state TEXT DEFAULT 'unknown'
            )
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE TABLE IF NOT EXISTS ibc_channels (
                channel_id TEXT PRIMARY KEY,
                client_id TEXT REFERENCES ibc_clients(client_id),
                connection_id TEXT REFERENCES ibc_connections(connection_id),
                counterparty_channel_id TEXT
            )
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
    CREATE TABLE IF NOT EXISTS asset_prices (
        asset_id BYTEA PRIMARY KEY,
        price_usd DOUBLE PRECISION NOT NULL DEFAULT 0.0,
        last_updated TIMESTAMP WITH TIME ZONE NOT NULL,
        symbol TEXT
    )
    ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
CREATE TABLE IF NOT EXISTS ibc_transfers (
    id SERIAL PRIMARY KEY,
    client_id TEXT NOT NULL REFERENCES ibc_clients(client_id),
    channel_id TEXT NOT NULL,
    direction TEXT NOT NULL,
    amount NUMERIC NOT NULL DEFAULT 0,
    asset_id BYTEA,
    usd_amount DOUBLE PRECISION, -- New field for storing USD amount
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    tx_hash BYTEA,
    status TEXT
)
",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE INDEX IF NOT EXISTS idx_ibc_transfers_client_id
            ON ibc_transfers(client_id)
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE INDEX IF NOT EXISTS idx_ibc_transfers_timestamp
            ON ibc_transfers(timestamp DESC)
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE INDEX IF NOT EXISTS idx_ibc_transfers_direction
            ON ibc_transfers(direction)
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE INDEX IF NOT EXISTS idx_ibc_transfers_status
            ON ibc_transfers(status)
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE TABLE IF NOT EXISTS validator_staking_parameters (
                chain_id TEXT PRIMARY KEY,
                active_validator_limit BIGINT NOT NULL,
                min_validator_stake TEXT NOT NULL,
                total_staked TEXT NOT NULL,
                uptime_blocks_window BIGINT NOT NULL,
                uptime_min_required TEXT NOT NULL,
                slashing_penalty_downtime TEXT,
                slashing_penalty_misbehavior TEXT NOT NULL,
                unbonding_delay TEXT NOT NULL
            )
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE TABLE IF NOT EXISTS validators (
                identity_key TEXT PRIMARY KEY,
                decoded_address TEXT,
                name TEXT,
                website TEXT,
                description TEXT,
                consensus_key TEXT,
                governance_key TEXT,
                state TEXT DEFAULT 'unknown',
                bonding_state TEXT DEFAULT 'unknown',
                voting_power BIGINT DEFAULT 0,
                voting_power_percentage DOUBLE PRECISION DEFAULT 0,
                first_seen_height BIGINT,
                first_seen_time TIMESTAMPTZ,
                last_updated TIMESTAMPTZ
            )
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE TABLE IF NOT EXISTS validator_funding_streams (
                id SERIAL PRIMARY KEY,
                identity_key TEXT NOT NULL REFERENCES validators(identity_key),
                stream_type TEXT NOT NULL, -- 'toAddress' or 'toCommunityPool'
                recipient_address TEXT, -- Only for 'toAddress' type
                rate_bps INTEGER NOT NULL, -- Rate in basis points
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                UNIQUE(identity_key, stream_type, recipient_address)
            )
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE INDEX IF NOT EXISTS idx_validator_funding_streams_identity_key
            ON validator_funding_streams(identity_key)
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE TABLE IF NOT EXISTS validator_blocks (
                identity_key TEXT NOT NULL REFERENCES validators(identity_key),
                block_height BIGINT NOT NULL REFERENCES explorer_block_details(height),
                timestamp TIMESTAMPTZ NOT NULL,
                signed BOOLEAN NOT NULL DEFAULT TRUE,
                PRIMARY KEY (identity_key, block_height)
            )
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE INDEX IF NOT EXISTS idx_validators_state
            ON validators(state)
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE INDEX IF NOT EXISTS idx_validators_voting_power
            ON validators(voting_power DESC)
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE OR REPLACE VIEW validator_performance AS
            WITH uptime_window AS (
                SELECT uptime_blocks_window FROM validator_staking_parameters LIMIT 1
            ),
            block_stats AS (
                SELECT 
                    vb.identity_key,
                    COUNT(*) as total_blocks,
                    SUM(CASE WHEN vb.signed = TRUE THEN 1 ELSE 0 END) as signed_blocks,
                    SUM(CASE WHEN vb.signed = FALSE THEN 1 ELSE 0 END) as missed_blocks
                FROM validator_blocks vb
                CROSS JOIN uptime_window uw
                WHERE vb.block_height > (SELECT MAX(height) FROM explorer_block_details) - uw.uptime_blocks_window
                GROUP BY vb.identity_key
            ),
            total_blocks_available AS (
                SELECT
                    v.identity_key,
                    LEAST(
                        uw.uptime_blocks_window,
                        GREATEST(
                            (SELECT MAX(height) FROM explorer_block_details) - v.first_seen_height,
                            1
                        )
                    ) as blocks_in_window
                FROM validators v
                CROSS JOIN uptime_window uw
            ),
            commission_rates AS (
                SELECT
                    identity_key,
                    ROUND(SUM(rate_bps)::numeric / 100.0, 2) as commission_rate_percentage
                FROM validator_funding_streams
                GROUP BY identity_key
            )
            SELECT 
                v.identity_key,
                v.decoded_address,
                COALESCE(v.name, '') as name,
                v.website,
                v.description,
                v.state,
                COALESCE(v.bonding_state, '') as bonding_state,
                v.voting_power,
                v.voting_power_percentage,
                v.first_seen_height,
                v.first_seen_time,
                COALESCE(cr.commission_rate_percentage, 0.0) as commission_rate,
                COALESCE(bs.missed_blocks, 0) as missed_blocks,
                COALESCE(bs.signed_blocks, 0) as signed_blocks,
                COALESCE(bs.total_blocks, 0) as total_tracked_blocks,
                tb.blocks_in_window,
                CASE 
                    WHEN COALESCE(bs.total_blocks, 0) > 0 THEN
                        ROUND(
                            (COALESCE(bs.signed_blocks, 0)::numeric / 
                            bs.total_blocks::numeric) * 100.0,
                            2
                        )
                    ELSE NULL
                END as uptime_percentage
            FROM 
                validators v
            LEFT JOIN block_stats bs ON v.identity_key = bs.identity_key
            JOIN total_blocks_available tb ON v.identity_key = tb.identity_key
            LEFT JOIN commission_rates cr ON v.identity_key = cr.identity_key
            ORDER BY 
                v.voting_power DESC
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE TABLE IF NOT EXISTS ibc_stats (
                client_id TEXT PRIMARY KEY REFERENCES ibc_clients(client_id),
                shielded_volume BIGINT NOT NULL DEFAULT 0,
                shielded_tx_count BIGINT NOT NULL DEFAULT 0,
                unshielded_volume BIGINT NOT NULL DEFAULT 0,
                unshielded_tx_count BIGINT NOT NULL DEFAULT 0,
                pending_tx_count BIGINT NOT NULL DEFAULT 0,
                expired_tx_count BIGINT NOT NULL DEFAULT 0,
                last_updated TIMESTAMP WITH TIME ZONE
            )
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE INDEX IF NOT EXISTS idx_ibc_transactions_client_id ON explorer_transactions(ibc_client_id)
            "
        )
            .execute(dbtx.as_mut())
            .await?;

        sqlx::query(
            r"
            CREATE INDEX IF NOT EXISTS idx_ibc_transactions_channel_id ON explorer_transactions(ibc_channel_id)
            "
        )
            .execute(dbtx.as_mut())
            .await?;

        sqlx::query(
            r"
            CREATE INDEX IF NOT EXISTS idx_ibc_transactions_status ON explorer_transactions(ibc_status)
            ",
        )
            .execute(dbtx.as_mut())
            .await?;

        sqlx::query(
            r"
        CREATE OR REPLACE VIEW ibc_client_summary AS
        WITH volume_stats AS (
            SELECT
                t.client_id,
                SUM(CASE
                    WHEN t.direction = 'inbound' AND t.status = 'completed'
                    -- Add upper bound to prevent extreme values
                    THEN LEAST(COALESCE(t.usd_amount, 0), 1000000000)
                    ELSE 0
                END) as shielded_volume,
                SUM(CASE
                    WHEN t.direction = 'outbound' AND t.status = 'completed'
                    -- Add upper bound to prevent extreme values
                    THEN LEAST(COALESCE(t.usd_amount, 0), 1000000000)
                    ELSE 0
                END) as unshielded_volume
            FROM
                ibc_transfers t
            GROUP BY
                t.client_id
        ),
        tx_stats AS (
            SELECT
                t.ibc_client_id as client_id,
                -- Shielded: Inbound token transfers with status completed
                COUNT(DISTINCT CASE WHEN t.ibc_direction = 'inbound' AND t.ibc_status = 'completed' THEN t.tx_hash ELSE NULL END) as shielded_tx_count,
                -- Unshielded: Outbound token transfers with status completed
                COUNT(DISTINCT CASE WHEN t.ibc_direction = 'outbound' AND t.ibc_status = 'completed' THEN t.tx_hash ELSE NULL END) as unshielded_tx_count,
                -- Total: ALL transactions (regardless of status)
                COUNT(DISTINCT t.tx_hash) as completed_tx_count,
                -- Pending: All transactions with status pending
                COUNT(DISTINCT CASE WHEN t.ibc_status = 'pending' THEN t.tx_hash ELSE NULL END) as pending_tx_count,
                -- Expired: All transactions with status expired
                COUNT(DISTINCT CASE WHEN t.ibc_status = 'expired' THEN t.tx_hash ELSE NULL END) as expired_tx_count,
                -- Last transaction timestamp for this client
                MAX(t.timestamp) as last_updated
            FROM
                explorer_transactions t
            WHERE
                t.ibc_client_id IS NOT NULL
            GROUP BY
                t.ibc_client_id
        )
        SELECT
            c.client_id,
            c.status,
            c.channel_id,
            c.counterparty_channel_id,
            COALESCE(v.shielded_volume, 0) as shielded_volume,
            COALESCE(t.shielded_tx_count, 0) as shielded_tx_count,
            COALESCE(v.unshielded_volume, 0) as unshielded_volume,
            COALESCE(t.unshielded_tx_count, 0) as unshielded_tx_count,
            (COALESCE(v.shielded_volume, 0) + COALESCE(v.unshielded_volume, 0)) as total_volume,
            COALESCE(t.completed_tx_count, 0) as total_tx_count,
            COALESCE(t.pending_tx_count, 0) as pending_tx_count,
            COALESCE(t.expired_tx_count, 0) as expired_tx_count,
            t.last_updated
        FROM
            ibc_clients c
        LEFT JOIN
            volume_stats v ON c.client_id = v.client_id
        LEFT JOIN
            tx_stats t ON c.client_id = t.client_id
        ORDER BY
            total_volume DESC
        ",
        )
            .execute(dbtx.as_mut())
            .await?;

        sqlx::query(
            r"
        CREATE OR REPLACE VIEW ibc_client_summary_24h AS
        WITH volume_stats AS (
            SELECT
                t.client_id,
                SUM(CASE
                    WHEN t.direction = 'inbound' AND t.status = 'completed'
                    THEN LEAST(COALESCE(t.usd_amount, 0), 1000000000)
                    ELSE 0
                END) as shielded_volume,
                SUM(CASE
                    WHEN t.direction = 'outbound' AND t.status = 'completed'
                    THEN LEAST(COALESCE(t.usd_amount, 0), 1000000000)
                    ELSE 0
                END) as unshielded_volume
            FROM
                ibc_transfers t
            WHERE
                t.timestamp > NOW() - INTERVAL '24 hours'
            GROUP BY
                t.client_id
        ),
        tx_stats AS (
            SELECT
                t.ibc_client_id as client_id,
                -- Shielded: Inbound token transfers with status completed
                COUNT(DISTINCT CASE WHEN t.ibc_direction = 'inbound' AND t.ibc_status = 'completed' THEN t.tx_hash ELSE NULL END) as shielded_tx_count,
                -- Unshielded: Outbound token transfers with status completed
                COUNT(DISTINCT CASE WHEN t.ibc_direction = 'outbound' AND t.ibc_status = 'completed' THEN t.tx_hash ELSE NULL END) as unshielded_tx_count,
                -- Total: ALL transactions (regardless of status)
                COUNT(DISTINCT t.tx_hash) as completed_tx_count,
                -- Pending: All transactions with status pending
                COUNT(DISTINCT CASE WHEN t.ibc_status = 'pending' THEN t.tx_hash ELSE NULL END) as pending_tx_count,
                -- Expired: All transactions with status expired
                COUNT(DISTINCT CASE WHEN t.ibc_status = 'expired' THEN t.tx_hash ELSE NULL END) as expired_tx_count,
                -- Last transaction timestamp for this client in the 24h period
                MAX(t.timestamp) as last_updated
            FROM
                explorer_transactions t
            WHERE
                t.ibc_client_id IS NOT NULL
                AND t.timestamp > NOW() - INTERVAL '24 hours'
            GROUP BY
                t.ibc_client_id
        )
        SELECT
            c.client_id,
            c.status,
            c.channel_id,
            c.counterparty_channel_id,
            COALESCE(v.shielded_volume, 0) as shielded_volume,
            COALESCE(t.shielded_tx_count, 0) as shielded_tx_count,
            COALESCE(v.unshielded_volume, 0) as unshielded_volume,
            COALESCE(t.unshielded_tx_count, 0) as unshielded_tx_count,
            (COALESCE(v.shielded_volume, 0) + COALESCE(v.unshielded_volume, 0)) as total_volume,
            COALESCE(t.completed_tx_count, 0) as total_tx_count,
            COALESCE(t.pending_tx_count, 0) as pending_tx_count,
            COALESCE(t.expired_tx_count, 0) as expired_tx_count,
            t.last_updated
        FROM
            ibc_clients c
        LEFT JOIN
            volume_stats v ON c.client_id = v.client_id
        LEFT JOIN
            tx_stats t ON c.client_id = t.client_id
        ORDER BY
            total_volume DESC
        ",
        )
            .execute(dbtx.as_mut())
            .await?;

        sqlx::query(
            r"
        CREATE OR REPLACE VIEW ibc_client_summary_30d AS
        WITH volume_stats AS (
            SELECT
                t.client_id,
                SUM(CASE
                    WHEN t.direction = 'inbound' AND t.status = 'completed'
                    THEN LEAST(COALESCE(t.usd_amount, 0), 1000000000)
                    ELSE 0
                END) as shielded_volume,
                SUM(CASE
                    WHEN t.direction = 'outbound' AND t.status = 'completed'
                    THEN LEAST(COALESCE(t.usd_amount, 0), 1000000000)
                    ELSE 0
                END) as unshielded_volume
            FROM
                ibc_transfers t
            WHERE
                t.timestamp > NOW() - INTERVAL '30 days'
            GROUP BY
                t.client_id
        ),
        tx_stats AS (
            SELECT
                t.ibc_client_id as client_id,
                -- Shielded: Inbound token transfers with status completed
                COUNT(DISTINCT CASE WHEN t.ibc_direction = 'inbound' AND t.ibc_status = 'completed' THEN t.tx_hash ELSE NULL END) as shielded_tx_count,
                -- Unshielded: Outbound token transfers with status completed
                COUNT(DISTINCT CASE WHEN t.ibc_direction = 'outbound' AND t.ibc_status = 'completed' THEN t.tx_hash ELSE NULL END) as unshielded_tx_count,
                -- Total: ALL transactions (regardless of status)
                COUNT(DISTINCT t.tx_hash) as completed_tx_count,
                -- Pending: All transactions with status pending
                COUNT(DISTINCT CASE WHEN t.ibc_status = 'pending' THEN t.tx_hash ELSE NULL END) as pending_tx_count,
                -- Expired: All transactions with status expired
                COUNT(DISTINCT CASE WHEN t.ibc_status = 'expired' THEN t.tx_hash ELSE NULL END) as expired_tx_count,
                -- Last transaction timestamp for this client in the 30d period
                MAX(t.timestamp) as last_updated
            FROM
                explorer_transactions t
            WHERE
                t.ibc_client_id IS NOT NULL
                AND t.timestamp > NOW() - INTERVAL '30 days'
            GROUP BY
                t.ibc_client_id
        )
        SELECT
            c.client_id,
            c.status,
            c.channel_id,
            c.counterparty_channel_id,
            COALESCE(v.shielded_volume, 0) as shielded_volume,
            COALESCE(t.shielded_tx_count, 0) as shielded_tx_count,
            COALESCE(v.unshielded_volume, 0) as unshielded_volume,
            COALESCE(t.unshielded_tx_count, 0) as unshielded_tx_count,
            (COALESCE(v.shielded_volume, 0) + COALESCE(v.unshielded_volume, 0)) as total_volume,
            COALESCE(t.completed_tx_count, 0) as total_tx_count,
            COALESCE(t.pending_tx_count, 0) as pending_tx_count,
            COALESCE(t.expired_tx_count, 0) as expired_tx_count,
            t.last_updated
        FROM
            ibc_clients c
        LEFT JOIN
            volume_stats v ON c.client_id = v.client_id
        LEFT JOIN
            tx_stats t ON c.client_id = t.client_id
        ORDER BY
            total_volume DESC
        ",
        )
            .execute(dbtx.as_mut())
            .await?;

        sqlx::query(
            r"
            CREATE OR REPLACE VIEW ibc_client_stats_with_periods AS
            -- All-time stats
            SELECT
                c.client_id,
                'all_time' AS period,
                c.status,
                c.channel_id,
                c.counterparty_channel_id,
                COALESCE(v.shielded_volume, 0) as shielded_volume,
                COALESCE(t.shielded_tx_count, 0) as shielded_tx_count,
                COALESCE(v.unshielded_volume, 0) as unshielded_volume,
                COALESCE(t.unshielded_tx_count, 0) as unshielded_tx_count,
                (COALESCE(v.shielded_volume, 0) + COALESCE(v.unshielded_volume, 0)) as total_volume,
                COALESCE(t.completed_tx_count, 0) as total_tx_count,
                COALESCE(t.pending_tx_count, 0) as pending_tx_count,
                COALESCE(t.expired_tx_count, 0) as expired_tx_count,
                t.last_updated
            FROM
                ibc_clients c
            LEFT JOIN (
                SELECT
                    t.client_id,
                    SUM(CASE WHEN t.direction = 'inbound' AND t.status = 'completed' THEN COALESCE(t.usd_amount, 0) ELSE 0 END) as shielded_volume,
                    SUM(CASE WHEN t.direction = 'outbound' AND t.status = 'completed' THEN COALESCE(t.usd_amount, 0) ELSE 0 END) as unshielded_volume
                FROM
                    ibc_transfers t
                GROUP BY
                    t.client_id
            ) v ON c.client_id = v.client_id
            LEFT JOIN (
                SELECT
                    t.ibc_client_id as client_id,
                    -- Shielded: Inbound token transfers with status completed
                    COUNT(DISTINCT CASE WHEN t.ibc_direction = 'inbound' AND t.ibc_status = 'completed' THEN t.tx_hash ELSE NULL END) as shielded_tx_count,
                    -- Unshielded: Outbound token transfers with status completed
                    COUNT(DISTINCT CASE WHEN t.ibc_direction = 'outbound' AND t.ibc_status = 'completed' THEN t.tx_hash ELSE NULL END) as unshielded_tx_count,
                    -- Total: ALL transactions with status completed (all directions)
                    COUNT(DISTINCT CASE WHEN t.ibc_status = 'completed' THEN t.tx_hash ELSE NULL END) as completed_tx_count,
                    -- Pending: All transactions with status pending
                    COUNT(DISTINCT CASE WHEN t.ibc_status = 'pending' THEN t.tx_hash ELSE NULL END) as pending_tx_count,
                    -- Expired: All transactions with status expired
                    COUNT(DISTINCT CASE WHEN t.ibc_status = 'expired' THEN t.tx_hash ELSE NULL END) as expired_tx_count,
                    -- Last transaction timestamp for this client
                    MAX(t.timestamp) as last_updated
                FROM
                    explorer_transactions t
                WHERE
                    t.ibc_client_id IS NOT NULL
                GROUP BY
                    t.ibc_client_id
            ) t ON c.client_id = t.client_id

            UNION ALL

            -- 24h stats
            SELECT
                c.client_id,
                '24h' AS period,
                c.status,
                c.channel_id,
                c.counterparty_channel_id,
                COALESCE(v.shielded_volume, 0) as shielded_volume,
                COALESCE(t.shielded_tx_count, 0) as shielded_tx_count,
                COALESCE(v.unshielded_volume, 0) as unshielded_volume,
                COALESCE(t.unshielded_tx_count, 0) as unshielded_tx_count,
                (COALESCE(v.shielded_volume, 0) + COALESCE(v.unshielded_volume, 0)) as total_volume,
                COALESCE(t.completed_tx_count, 0) as total_tx_count,
                COALESCE(t.pending_tx_count, 0) as pending_tx_count,
                COALESCE(t.expired_tx_count, 0) as expired_tx_count,
                t.last_updated
            FROM
                ibc_clients c
            LEFT JOIN (
                SELECT
                    t.client_id,
                    SUM(CASE WHEN t.direction = 'inbound' AND t.status = 'completed' THEN COALESCE(t.usd_amount, 0) ELSE 0 END) as shielded_volume,
                    SUM(CASE WHEN t.direction = 'outbound' AND t.status = 'completed' THEN COALESCE(t.usd_amount, 0) ELSE 0 END) as unshielded_volume
                FROM
                    ibc_transfers t
                WHERE
                    t.timestamp > NOW() - INTERVAL '24 hours'
                GROUP BY
                    t.client_id
            ) v ON c.client_id = v.client_id
            LEFT JOIN (
                SELECT
                    t.ibc_client_id as client_id,
                    -- Shielded: Inbound token transfers with status completed
                    COUNT(DISTINCT CASE WHEN t.ibc_direction = 'inbound' AND t.ibc_status = 'completed' THEN t.tx_hash ELSE NULL END) as shielded_tx_count,
                    -- Unshielded: Outbound token transfers with status completed
                    COUNT(DISTINCT CASE WHEN t.ibc_direction = 'outbound' AND t.ibc_status = 'completed' THEN t.tx_hash ELSE NULL END) as unshielded_tx_count,
                    -- Total: ALL transactions with status completed (all directions)
                    COUNT(DISTINCT CASE WHEN t.ibc_status = 'completed' THEN t.tx_hash ELSE NULL END) as completed_tx_count,
                    -- Pending: All transactions with status pending
                    COUNT(DISTINCT CASE WHEN t.ibc_status = 'pending' THEN t.tx_hash ELSE NULL END) as pending_tx_count,
                    -- Expired: All transactions with status expired
                    COUNT(DISTINCT CASE WHEN t.ibc_status = 'expired' THEN t.tx_hash ELSE NULL END) as expired_tx_count,
                    -- Last transaction timestamp for this client
                    MAX(t.timestamp) as last_updated
                FROM
                    explorer_transactions t
                WHERE
                    t.ibc_client_id IS NOT NULL
                    AND t.timestamp > NOW() - INTERVAL '24 hours'
                GROUP BY
                    t.ibc_client_id
            ) t ON c.client_id = t.client_id

            UNION ALL

            -- 30d stats
            SELECT
                c.client_id,
                '30d' AS period,
                c.status,
                c.channel_id,
                c.counterparty_channel_id,
                COALESCE(v.shielded_volume, 0) as shielded_volume,
                COALESCE(t.shielded_tx_count, 0) as shielded_tx_count,
                COALESCE(v.unshielded_volume, 0) as unshielded_volume,
                COALESCE(t.unshielded_tx_count, 0) as unshielded_tx_count,
                (COALESCE(v.shielded_volume, 0) + COALESCE(v.unshielded_volume, 0)) as total_volume,
                COALESCE(t.completed_tx_count, 0) as total_tx_count,
                COALESCE(t.pending_tx_count, 0) as pending_tx_count,
                COALESCE(t.expired_tx_count, 0) as expired_tx_count,
                t.last_updated
            FROM
                ibc_clients c
            LEFT JOIN (
                SELECT
                    t.client_id,
                    SUM(CASE WHEN t.direction = 'inbound' AND t.status = 'completed' THEN COALESCE(t.usd_amount, 0) ELSE 0 END) as shielded_volume,
                    SUM(CASE WHEN t.direction = 'outbound' AND t.status = 'completed' THEN COALESCE(t.usd_amount, 0) ELSE 0 END) as unshielded_volume
                FROM
                    ibc_transfers t
                WHERE
                    t.timestamp > NOW() - INTERVAL '30 days'
                GROUP BY
                    t.client_id
            ) v ON c.client_id = v.client_id
            LEFT JOIN (
                SELECT
                    t.ibc_client_id as client_id,
                    -- Shielded: Inbound token transfers with status completed
                    COUNT(DISTINCT CASE WHEN t.ibc_direction = 'inbound' AND t.ibc_status = 'completed' THEN t.tx_hash ELSE NULL END) as shielded_tx_count,
                    -- Unshielded: Outbound token transfers with status completed
                    COUNT(DISTINCT CASE WHEN t.ibc_direction = 'outbound' AND t.ibc_status = 'completed' THEN t.tx_hash ELSE NULL END) as unshielded_tx_count,
                    -- Total: ALL transactions with status completed (all directions)
                    COUNT(DISTINCT CASE WHEN t.ibc_status = 'completed' THEN t.tx_hash ELSE NULL END) as completed_tx_count,
                    -- Pending: All transactions with status pending
                    COUNT(DISTINCT CASE WHEN t.ibc_status = 'pending' THEN t.tx_hash ELSE NULL END) as pending_tx_count,
                    -- Expired: All transactions with status expired
                    COUNT(DISTINCT CASE WHEN t.ibc_status = 'expired' THEN t.tx_hash ELSE NULL END) as expired_tx_count,
                    -- Last transaction timestamp for this client
                    MAX(t.timestamp) as last_updated
                FROM
                    explorer_transactions t
                WHERE
                    t.ibc_client_id IS NOT NULL
                    AND t.timestamp > NOW() - INTERVAL '30 days'
                GROUP BY
                    t.ibc_client_id
            ) t ON c.client_id = t.client_id
            ",
        )
            .execute(dbtx.as_mut())
            .await?;

        tracing::info!("Reading genesis file to initialize validator staking parameters");

        match ValidatorParams::from_genesis_json() {
            Ok(validator_params) => {
                tracing::info!(
                    "Initializing validator staking parameters for chain_id = {}",
                    validator_params.chain_id
                );

                if let Err(e) = validator_params.initialize_table(dbtx).await {
                    tracing::error!("Failed to initialize validator staking parameters: {}", e);
                }
            }
            Err(e) => {
                tracing::error!("Failed to extract validator staking parameters: {}", e);
                return Err(e);
            }
        }

        tracing::info!("Reading genesis file to initialize validators");
        if let Err(e) = self.initialize_validators_from_genesis(dbtx).await {
            tracing::error!("Failed to initialize validators from genesis: {}", e);
        }

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn index_batch(
        &self,
        dbtx: &mut PgTransaction,
        batch: EventBatch,
        ctx: EventBatchContext,
    ) -> Result<(), anyhow::Error> {
        let mut block_data_to_process = Vec::new();
        let mut transactions_to_process = Vec::new();

        let block_results = block::process_block_events(&batch).await?;

        tracing::info!("Processed {} blocks from batch", block_results.len());

        for (height, root, ts, tx_count, _, raw_json, block_txs) in block_results {
            let formatted_block_json = block::create_block_json(
                height,
                self.get_chain_id(),
                ts,
                &block::collect_block_transactions(&raw_json, ts),
                &block::collect_block_events(&raw_json),
            );

            block_data_to_process.push((height, root, ts, tx_count, formatted_block_json));

            for (tx_hash, tx_bytes, tx_index, tx_events) in block_txs {
                transactions_to_process.push((tx_hash, tx_bytes, tx_index, height, ts, tx_events));
            }
        }

        let mut height_to_timestamp: HashMap<u64, DateTime<Utc>> = HashMap::new();
        for (height, _, ts, _, _) in &block_data_to_process {
            height_to_timestamp.insert(*height, *ts);
        }
        for (_, _, _, height, ts, _) in &transactions_to_process {
            height_to_timestamp.insert(*height, *ts);
        }

        for (height, root, ts, tx_count, formatted_json) in block_data_to_process {
            let meta = BlockMetadata {
                height,
                root,
                timestamp: ts,
                tx_count,
                chain_id: self.get_chain_id(),
                raw_json: formatted_json,
            };

            block::insert(dbtx, meta).await?;

            self.record_validator_blocks_for_height(dbtx, height, ts)
                .await?;
        }

        for (tx_hash, tx_bytes, tx_index, height, timestamp, tx_events) in &transactions_to_process
        {
            let formatted_tx_json = transaction::create_transaction_json(
                *tx_hash, tx_bytes, *height, *timestamp, *tx_index, tx_events,
            );

            let fee_amount =
                transaction::extract_fee_amount(&formatted_tx_json["transaction_view"]);

            let chain_id = self
                .chain_id
                .clone()
                .unwrap_or_else(|| "unknown".to_string());

            let tx_bytes_base64 = encode_to_base64(tx_bytes);

            let meta = TransactionMetadata {
                tx_hash: *tx_hash,
                height: *height,
                timestamp: *timestamp,
                fee_amount,
                chain_id: &chain_id,
                tx_bytes_base64,
                decoded_tx_json: formatted_tx_json,
            };

            if let Err(e) = transaction::insert(dbtx, meta).await {
                let tx_hash_hex = crate::parsing::encode_to_hex(*tx_hash);

                let is_fk_error = match e.as_database_error() {
                    Some(dbe) => {
                        if let Some(pg_err) =
                            dbe.try_downcast_ref::<sqlx::postgres::PgDatabaseError>()
                        {
                            pg_err.code() == "23503"
                        } else {
                            false
                        }
                    }
                    None => false,
                };

                if is_fk_error {
                    tracing::warn!(
                        "Block {} not found for transaction {}. Foreign key constraint failed.",
                        height,
                        tx_hash_hex
                    );
                } else {
                    tracing::error!("Error inserting transaction {}: {:?}", tx_hash_hex, e);
                }
            }
        }

        for block_events in batch.events_by_block() {
            let height = block_events.height();
            let events: Vec<ContextualizedEvent> = block_events.events().collect();

            let timestamp = *height_to_timestamp.get(&height).unwrap_or(&Utc::now());

            if !events.is_empty() {
                if let Err(e) = ibc::process_events(dbtx, &events, height, timestamp).await {
                    tracing::error!("Error processing IBC events for block {}: {:?}", height, e);
                }

                if let Err(e) =
                    validator::ValidatorParams::process_events(dbtx, &events, height, timestamp)
                        .await
                {
                    tracing::error!(
                        "Error processing validator parameter events for block {}: {:?}",
                        height,
                        e
                    );
                }
                if let Err(e) =
                    validator::Validator::process_events(dbtx, &events, height, timestamp).await
                {
                    tracing::error!(
                        "Error processing validator events for block {}: {:?}",
                        height,
                        e
                    );
                }
            }
        }

        if ctx.is_last() {
            if let Err(e) = ibc::update_old_pending_transactions(dbtx).await {
                tracing::error!("Error updating old pending transactions: {:?}", e);
            }
        }

        Ok(())
    }
}
