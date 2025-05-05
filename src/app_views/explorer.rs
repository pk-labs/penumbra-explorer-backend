use anyhow::Result;
use cometindex::{
    async_trait,
    index::{EventBatch, EventBatchContext},
    sqlx, AppView, ContextualizedEvent, PgTransaction,
};
use serde_json::{json, Value};
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
use crate::app_views::utils::{block, ibc, transaction};
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

    /// Attempts to read the `chain_id` from the genesis.json file
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

        let genesis: Result<serde_json::Value, _> = serde_json::from_str(&contents);
        if let Err(e) = genesis {
            tracing::error!("Failed to parse genesis.json: {}", e);
            return None;
        }

        let genesis = genesis.unwrap();
        let chain_id = genesis["chain_id"].as_str().map(String::from);

        if chain_id.is_none() {
            tracing::error!("Could not find chain_id in genesis.json");
        }

        chain_id
    }

    /// Returns the chain ID, using "unknown" if not available
    fn get_chain_id(&self) -> &str {
        self.chain_id.as_deref().unwrap_or("unknown")
    }
}

#[async_trait]
impl AppView for Explorer {
    fn name(&self) -> String {
        "explorer".to_string()
    }

    #[allow(clippy::too_many_lines)]
    async fn init_chain(
        &self,
        dbtx: &mut PgTransaction,
        _: &serde_json::Value,
    ) -> Result<(), anyhow::Error> {
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
                raw_json TEXT
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
                raw_json TEXT,
                -- IBC fields
                ibc_channel_id TEXT,
                ibc_client_id TEXT,
                ibc_status TEXT,
                ibc_direction TEXT,
                ibc_sequence TEXT,
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
                t.ibc_sequence
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
                last_active_height BIGINT,
                last_active_time TIMESTAMP WITH TIME ZONE
            )
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE TABLE IF NOT EXISTS ibc_channels (
                channel_id TEXT PRIMARY KEY,
                client_id TEXT NOT NULL REFERENCES ibc_clients(client_id),
                connection_id TEXT,
                counterparty_channel_id TEXT
            )
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        // Create asset_prices table BEFORE ibc_transfers to ensure it exists
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
"
        )
            .execute(dbtx.as_mut())
            .await?;

        // Create indices for efficient querying
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

        // Keep the legacy tables for backward compatibility
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

        // Create a view that shows all time stats calculated from ibc_transfers
        sqlx::query(
            r"
            CREATE OR REPLACE VIEW ibc_client_summary AS
            WITH client_stats AS (
                SELECT
                    t.client_id,
                    SUM(CASE WHEN t.direction = 'inbound' THEN 
                        CASE 
                            WHEN t.asset_id IS NOT NULL AND p.price_usd IS NOT NULL 
                            THEN t.amount * p.price_usd 
                            ELSE t.amount 
                        END 
                        ELSE 0 
                    END) as shielded_volume,
                    COUNT(CASE WHEN t.direction = 'inbound' THEN 1 ELSE NULL END) as shielded_tx_count,
                    SUM(CASE WHEN t.direction = 'outbound' THEN 
                        CASE 
                            WHEN t.asset_id IS NOT NULL AND p.price_usd IS NOT NULL 
                            THEN t.amount * p.price_usd 
                            ELSE t.amount 
                        END 
                        ELSE 0 
                    END) as unshielded_volume,
                    COUNT(CASE WHEN t.direction = 'outbound' THEN 1 ELSE NULL END) as unshielded_tx_count,
                    COUNT(CASE WHEN t.status = 'pending' THEN 1 ELSE NULL END) as pending_tx_count,
                    COUNT(CASE WHEN t.status = 'expired' THEN 1 ELSE NULL END) as expired_tx_count,
                    MAX(t.timestamp) as last_updated
                FROM
                    ibc_transfers t
                LEFT JOIN
                    asset_prices p ON t.asset_id = p.asset_id
                GROUP BY
                    t.client_id
            )
            SELECT
                c.client_id,
                COALESCE(s.shielded_volume, 0) as shielded_volume,
                COALESCE(s.shielded_tx_count, 0) as shielded_tx_count,
                COALESCE(s.unshielded_volume, 0) as unshielded_volume,
                COALESCE(s.unshielded_tx_count, 0) as unshielded_tx_count,
                (COALESCE(s.shielded_volume, 0) + COALESCE(s.unshielded_volume, 0)) as total_volume,
                (COALESCE(s.shielded_tx_count, 0) + COALESCE(s.unshielded_tx_count, 0)) as total_tx_count,
                COALESCE(s.pending_tx_count, 0) as pending_tx_count,
                COALESCE(s.expired_tx_count, 0) as expired_tx_count,
                s.last_updated
            FROM
                ibc_clients c
            LEFT JOIN
                client_stats s ON c.client_id = s.client_id
            ORDER BY
                total_volume DESC
            ",
        )
            .execute(dbtx.as_mut())
            .await?;

        sqlx::query(
            r"
            CREATE OR REPLACE VIEW ibc_client_summary_24h AS
            WITH client_stats AS (
                SELECT
                    t.client_id,
                    SUM(CASE WHEN t.direction = 'inbound' THEN 
                        CASE 
                            WHEN t.asset_id IS NOT NULL AND p.price_usd IS NOT NULL 
                            THEN t.amount * p.price_usd 
                            ELSE t.amount 
                        END 
                        ELSE 0 
                    END) as shielded_volume,
                    COUNT(CASE WHEN t.direction = 'inbound' THEN 1 ELSE NULL END) as shielded_tx_count,
                    SUM(CASE WHEN t.direction = 'outbound' THEN 
                        CASE 
                            WHEN t.asset_id IS NOT NULL AND p.price_usd IS NOT NULL 
                            THEN t.amount * p.price_usd 
                            ELSE t.amount 
                        END 
                        ELSE 0 
                    END) as unshielded_volume,
                    COUNT(CASE WHEN t.direction = 'outbound' THEN 1 ELSE NULL END) as unshielded_tx_count,
                    COUNT(CASE WHEN t.status = 'pending' THEN 1 ELSE NULL END) as pending_tx_count,
                    COUNT(CASE WHEN t.status = 'expired' THEN 1 ELSE NULL END) as expired_tx_count,
                    MAX(t.timestamp) as last_updated
                FROM
                    ibc_transfers t
                LEFT JOIN
                    asset_prices p ON t.asset_id = p.asset_id
                WHERE
                    t.timestamp > NOW() - INTERVAL '24 hours'
                GROUP BY
                    t.client_id
            )
            SELECT
                c.client_id,
                COALESCE(s.shielded_volume, 0) as shielded_volume,
                COALESCE(s.shielded_tx_count, 0) as shielded_tx_count,
                COALESCE(s.unshielded_volume, 0) as unshielded_volume,
                COALESCE(s.unshielded_tx_count, 0) as unshielded_tx_count,
                (COALESCE(s.shielded_volume, 0) + COALESCE(s.unshielded_volume, 0)) as total_volume,
                (COALESCE(s.shielded_tx_count, 0) + COALESCE(s.unshielded_tx_count, 0)) as total_tx_count,
                COALESCE(s.pending_tx_count, 0) as pending_tx_count,
                COALESCE(s.expired_tx_count, 0) as expired_tx_count,
                s.last_updated
            FROM
                ibc_clients c
            LEFT JOIN
                client_stats s ON c.client_id = s.client_id
            ORDER BY
                total_volume DESC
            ",
        )
            .execute(dbtx.as_mut())
            .await?;

        sqlx::query(
            r"
            CREATE OR REPLACE VIEW ibc_client_summary_30d AS
            WITH client_stats AS (
                SELECT
                    t.client_id,
                    SUM(CASE WHEN t.direction = 'inbound' THEN 
                        CASE 
                            WHEN t.asset_id IS NOT NULL AND p.price_usd IS NOT NULL 
                            THEN t.amount * p.price_usd 
                            ELSE t.amount 
                        END 
                        ELSE 0 
                    END) as shielded_volume,
                    COUNT(CASE WHEN t.direction = 'inbound' THEN 1 ELSE NULL END) as shielded_tx_count,
                    SUM(CASE WHEN t.direction = 'outbound' THEN 
                        CASE 
                            WHEN t.asset_id IS NOT NULL AND p.price_usd IS NOT NULL 
                            THEN t.amount * p.price_usd 
                            ELSE t.amount 
                        END 
                        ELSE 0 
                    END) as unshielded_volume,
                    COUNT(CASE WHEN t.direction = 'outbound' THEN 1 ELSE NULL END) as unshielded_tx_count,
                    COUNT(CASE WHEN t.status = 'pending' THEN 1 ELSE NULL END) as pending_tx_count,
                    COUNT(CASE WHEN t.status = 'expired' THEN 1 ELSE NULL END) as expired_tx_count,
                    MAX(t.timestamp) as last_updated
                FROM
                    ibc_transfers t
                LEFT JOIN
                    asset_prices p ON t.asset_id = p.asset_id
                WHERE
                    t.timestamp > NOW() - INTERVAL '30 days'
                GROUP BY
                    t.client_id
            )
            SELECT
                c.client_id,
                COALESCE(s.shielded_volume, 0) as shielded_volume,
                COALESCE(s.shielded_tx_count, 0) as shielded_tx_count,
                COALESCE(s.unshielded_volume, 0) as unshielded_volume,
                COALESCE(s.unshielded_tx_count, 0) as unshielded_tx_count,
                (COALESCE(s.shielded_volume, 0) + COALESCE(s.unshielded_volume, 0)) as total_volume,
                (COALESCE(s.shielded_tx_count, 0) + COALESCE(s.unshielded_tx_count, 0)) as total_tx_count,
                COALESCE(s.pending_tx_count, 0) as pending_tx_count,
                COALESCE(s.expired_tx_count, 0) as expired_tx_count,
                s.last_updated
            FROM
                ibc_clients c
            LEFT JOIN
                client_stats s ON c.client_id = s.client_id
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
                COALESCE(s.shielded_volume, 0) as shielded_volume,
                COALESCE(s.shielded_tx_count, 0) as shielded_tx_count,
                COALESCE(s.unshielded_volume, 0) as unshielded_volume,
                COALESCE(s.unshielded_tx_count, 0) as unshielded_tx_count,
                (COALESCE(s.shielded_volume, 0) + COALESCE(s.unshielded_volume, 0)) as total_volume,
                (COALESCE(s.shielded_tx_count, 0) + COALESCE(s.unshielded_tx_count, 0)) as total_tx_count,
                COALESCE(s.pending_tx_count, 0) as pending_tx_count,
                COALESCE(s.expired_tx_count, 0) as expired_tx_count,
                s.last_updated
            FROM
                ibc_clients c
            LEFT JOIN (
                SELECT
                    t.client_id,
                    SUM(CASE WHEN t.direction = 'inbound' THEN 
                        CASE 
                            WHEN t.asset_id IS NOT NULL AND p.price_usd IS NOT NULL 
                            THEN t.amount * p.price_usd 
                            ELSE t.amount 
                        END 
                        ELSE 0 
                    END) as shielded_volume,
                    COUNT(CASE WHEN t.direction = 'inbound' THEN 1 ELSE NULL END) as shielded_tx_count,
                    SUM(CASE WHEN t.direction = 'outbound' THEN 
                        CASE 
                            WHEN t.asset_id IS NOT NULL AND p.price_usd IS NOT NULL 
                            THEN t.amount * p.price_usd 
                            ELSE t.amount 
                        END 
                        ELSE 0 
                    END) as unshielded_volume,
                    COUNT(CASE WHEN t.direction = 'outbound' THEN 1 ELSE NULL END) as unshielded_tx_count,
                    COUNT(CASE WHEN t.status = 'pending' THEN 1 ELSE NULL END) as pending_tx_count,
                    COUNT(CASE WHEN t.status = 'expired' THEN 1 ELSE NULL END) as expired_tx_count,
                    MAX(t.timestamp) as last_updated
                FROM
                    ibc_transfers t
                LEFT JOIN
                    asset_prices p ON t.asset_id = p.asset_id
                GROUP BY
                    t.client_id
            ) s ON c.client_id = s.client_id

            UNION ALL

            -- 24h stats
            SELECT
                c.client_id,
                '24h' AS period,
                COALESCE(s.shielded_volume, 0) as shielded_volume,
                COALESCE(s.shielded_tx_count, 0) as shielded_tx_count,
                COALESCE(s.unshielded_volume, 0) as unshielded_volume,
                COALESCE(s.unshielded_tx_count, 0) as unshielded_tx_count,
                (COALESCE(s.shielded_volume, 0) + COALESCE(s.unshielded_volume, 0)) as total_volume,
                (COALESCE(s.shielded_tx_count, 0) + COALESCE(s.unshielded_tx_count, 0)) as total_tx_count,
                COALESCE(s.pending_tx_count, 0) as pending_tx_count,
                COALESCE(s.expired_tx_count, 0) as expired_tx_count,
                s.last_updated
            FROM
                ibc_clients c
            LEFT JOIN (
                SELECT
                    t.client_id,
                    SUM(CASE WHEN t.direction = 'inbound' THEN 
                        CASE 
                            WHEN t.asset_id IS NOT NULL AND p.price_usd IS NOT NULL 
                            THEN t.amount * p.price_usd 
                            ELSE t.amount 
                        END 
                        ELSE 0 
                    END) as shielded_volume,
                    COUNT(CASE WHEN t.direction = 'inbound' THEN 1 ELSE NULL END) as shielded_tx_count,
                    SUM(CASE WHEN t.direction = 'outbound' THEN 
                        CASE 
                            WHEN t.asset_id IS NOT NULL AND p.price_usd IS NOT NULL 
                            THEN t.amount * p.price_usd 
                            ELSE t.amount 
                        END 
                        ELSE 0 
                    END) as unshielded_volume,
                    COUNT(CASE WHEN t.direction = 'outbound' THEN 1 ELSE NULL END) as unshielded_tx_count,
                    COUNT(CASE WHEN t.status = 'pending' THEN 1 ELSE NULL END) as pending_tx_count,
                    COUNT(CASE WHEN t.status = 'expired' THEN 1 ELSE NULL END) as expired_tx_count,
                    MAX(t.timestamp) as last_updated
                FROM
                    ibc_transfers t
                LEFT JOIN
                    asset_prices p ON t.asset_id = p.asset_id
                WHERE
                    t.timestamp > NOW() - INTERVAL '24 hours'
                GROUP BY
                    t.client_id
            ) s ON c.client_id = s.client_id

            UNION ALL

            -- 30d stats
            SELECT
                c.client_id,
                '30d' AS period,
                COALESCE(s.shielded_volume, 0) as shielded_volume,
                COALESCE(s.shielded_tx_count, 0) as shielded_tx_count,
                COALESCE(s.unshielded_volume, 0) as unshielded_volume,
                COALESCE(s.unshielded_tx_count, 0) as unshielded_tx_count,
                (COALESCE(s.shielded_volume, 0) + COALESCE(s.unshielded_volume, 0)) as total_volume,
                (COALESCE(s.shielded_tx_count, 0) + COALESCE(s.unshielded_tx_count, 0)) as total_tx_count,
                COALESCE(s.pending_tx_count, 0) as pending_tx_count,
                COALESCE(s.expired_tx_count, 0) as expired_tx_count,
                s.last_updated
            FROM
                ibc_clients c
            LEFT JOIN (
                SELECT
                    t.client_id,
                    SUM(CASE WHEN t.direction = 'inbound' THEN 
                        CASE 
                            WHEN t.asset_id IS NOT NULL AND p.price_usd IS NOT NULL 
                            THEN t.amount * p.price_usd 
                            ELSE t.amount 
                        END 
                        ELSE 0 
                    END) as shielded_volume,
                    COUNT(CASE WHEN t.direction = 'inbound' THEN 1 ELSE NULL END) as shielded_tx_count,
                    SUM(CASE WHEN t.direction = 'outbound' THEN 
                        CASE 
                            WHEN t.asset_id IS NOT NULL AND p.price_usd IS NOT NULL 
                            THEN t.amount * p.price_usd 
                            ELSE t.amount 
                        END 
                        ELSE 0 
                    END) as unshielded_volume,
                    COUNT(CASE WHEN t.direction = 'outbound' THEN 1 ELSE NULL END) as unshielded_tx_count,
                    COUNT(CASE WHEN t.status = 'pending' THEN 1 ELSE NULL END) as pending_tx_count,
                    COUNT(CASE WHEN t.status = 'expired' THEN 1 ELSE NULL END) as expired_tx_count,
                    MAX(t.timestamp) as last_updated
                FROM
                    ibc_transfers t
                LEFT JOIN
                    asset_prices p ON t.asset_id = p.asset_id
                WHERE
                    t.timestamp > NOW() - INTERVAL '30 days'
                GROUP BY
                    t.client_id
            ) s ON c.client_id = s.client_id
            ",
        )
            .execute(dbtx.as_mut())
            .await?;

        // Asset_prices table already created earlier, skipping...

        // Insert default prices for common assets
        // USDC price (1.0)
        sqlx::query(
            r"
    INSERT INTO asset_prices (asset_id, price_usd, last_updated, symbol)
    VALUES ($1, 1.0, NOW(), 'USDC')
    ON CONFLICT (asset_id) DO NOTHING
    ",
        )
            .bind(hex::decode("75736463").unwrap_or_default()) // 'usdc' in hex
            .execute(dbtx.as_mut())
            .await?;
            
        // Default prices for other common assets
        // USDT (1.0)
        sqlx::query(
            r"
    INSERT INTO asset_prices (asset_id, price_usd, last_updated, symbol)
    VALUES ($1, 1.0, NOW(), 'USDT')
    ON CONFLICT (asset_id) DO NOTHING
    ",
        )
            .bind(hex::decode("75736474").unwrap_or_default()) // 'usdt' in hex
            .execute(dbtx.as_mut())
            .await?;
            
        // Penumbra native token (approximate initial price)
        sqlx::query(
            r"
    INSERT INTO asset_prices (asset_id, price_usd, last_updated, symbol)
    VALUES ($1, 0.1, NOW(), 'PNB')
    ON CONFLICT (asset_id) DO NOTHING
    ",
        )
            .bind(hex::decode("706e62").unwrap_or_default()) // 'pnb' in hex
            .execute(dbtx.as_mut())
            .await?;
            
        // ATOM (approximate default price)
        sqlx::query(
            r"
    INSERT INTO asset_prices (asset_id, price_usd, last_updated, symbol)
    VALUES ($1, 7.5, NOW(), 'ATOM')
    ON CONFLICT (asset_id) DO NOTHING
    ",
        )
            .bind(hex::decode("61746f6d").unwrap_or_default()) // 'atom' in hex
            .execute(dbtx.as_mut())
            .await?;

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
        }

        let mut height_to_timestamp: HashMap<u64, DateTime<Utc>> = HashMap::new();
        for (_, _, _, height, ts, _) in &transactions_to_process {
            height_to_timestamp.insert(*height, *ts);
        }

        for (tx_hash, tx_bytes, tx_index, height, timestamp, tx_events) in &transactions_to_process
        {
            let formatted_tx_json = transaction::create_transaction_json(
                *tx_hash, tx_bytes, *height, *timestamp, *tx_index, tx_events,
            );

            let parsed_json: Value =
                serde_json::from_str(&formatted_tx_json).unwrap_or_else(|_| json!({}));
            let fee_amount = transaction::extract_fee_amount(&parsed_json["transaction_view"]);

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
