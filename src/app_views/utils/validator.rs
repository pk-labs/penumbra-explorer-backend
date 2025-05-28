use crate::parsing::identity_key_to_validator_address;
use anyhow::Result;
use base64::{engine::general_purpose, Engine as _};
use cometindex::ContextualizedEvent;
use serde_json::Value;
use sqlx::types::chrono::{DateTime, Utc};
use sqlx::PgTransaction;
use std::collections::HashSet;
use std::fs::File;
use std::io::Read;
use tracing::{debug, error, info};

/// Performance optimization: Cache for validator existence checks within a transaction
#[derive(Debug, Default)]
struct ValidatorExistenceCache {
    existing_validators: HashSet<String>,
    cache_loaded: bool,
}

impl ValidatorExistenceCache {
    /// Load existing validators into cache (called once per transaction)
    async fn ensure_loaded(&mut self, dbtx: &mut PgTransaction<'_>) -> Result<()> {
        if self.cache_loaded {
            return Ok(());
        }

        let validators: Vec<String> = sqlx::query_scalar("SELECT identity_key FROM validators")
            .fetch_all(dbtx.as_mut())
            .await
            .unwrap_or_default();

        self.existing_validators = validators.into_iter().collect();
        self.cache_loaded = true;
        debug!(
            "Loaded {} existing validators into cache",
            self.existing_validators.len()
        );

        Ok(())
    }

    /// Check if validator exists (uses cache after first load)
    async fn validator_exists(
        &mut self,
        identity_key: &str,
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<bool> {
        self.ensure_loaded(dbtx).await?;
        Ok(self.existing_validators.contains(identity_key))
    }

    /// Add validator to cache when created
    fn add_validator(&mut self, identity_key: &str) {
        if self.cache_loaded {
            self.existing_validators.insert(identity_key.to_string());
        }
    }
}

/// Performance optimization: Batch voting power changes for single recalculation
#[derive(Debug, Default)]
struct VotingPowerBatch {
    changes: Vec<(String, i64)>,
}

impl VotingPowerBatch {
    /// Add a voting power change to the batch
    fn add_change(&mut self, identity_key: String, voting_power: i64) {
        self.changes.push((identity_key, voting_power));
    }

    /// Apply all batched voting power changes with single percentage recalculation
    async fn apply_all(
        &mut self,
        dbtx: &mut PgTransaction<'_>,
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        if self.changes.is_empty() {
            return Ok(());
        }

        debug!(
            "Applying {} batched voting power changes",
            self.changes.len()
        );

        for (identity_key, voting_power) in &self.changes {
            if let Err(e) = Validator::update_voting_power(
                identity_key,
                *voting_power,
                0.0, // Temporary percentage, will be recalculated
                dbtx,
                timestamp,
            )
            .await
            {
                error!("Failed to update voting power for {}: {}", identity_key, e);
            }
        }

        match Validator::calculate_total_voting_power(dbtx).await {
            Ok(total) => {
                if total > 0 {
                    if let Err(e) =
                        Validator::update_all_voting_power_active_percentages(dbtx).await
                    {
                        error!("Failed to update voting power active percentages: {}", e);
                    }

                    if let Err(e) = Validator::update_total_staked(dbtx).await {
                        error!("Failed to update total_staked parameter: {}", e);
                    }
                }
            }
            Err(e) => {
                error!("Failed to calculate total voting power: {}", e);
            }
        }

        self.changes.clear();

        Ok(())
    }
}

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct ValidatorParams {
    pub chain_id: String,
    pub active_validator_limit: i64,
    pub min_validator_stake: String,
    pub total_staked: String,
    pub uptime_blocks_window: i64,
    pub uptime_min_required: String,
    pub slashing_penalty_downtime: String,
    pub slashing_penalty_misbehavior: String,
    pub unbonding_delay: String,
}

/// Represents a validator funding stream
#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct ValidatorFundingStream {
    pub identity_key: String,
    pub stream_type: String,
    pub recipient_address: Option<String>,
    pub rate_bps: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Represents a validator entity
#[derive(Debug)]
pub struct Validator {
    pub identity_key: String,
    pub decoded_address: Option<String>,
    pub name: Option<String>,
    pub website: Option<String>,
    pub description: Option<String>,
    pub consensus_key: Option<String>,
    pub governance_key: Option<String>,
    pub state: String,
    pub bonding_state: Option<String>,
    pub voting_power: i64,
    pub voting_power_active_percentage: f64,
    pub first_seen_height: Option<i64>,
    pub first_seen_time: DateTime<Utc>,
    pub last_updated: DateTime<Utc>,
}

impl Validator {
    /// Helper function to find an attribute value in an event
    #[must_use]
    pub fn find_attribute_value<'a>(
        event: &'a ContextualizedEvent<'_>,
        key: &str,
    ) -> Option<&'a str> {
        for attr in &event.event.attributes {
            if let Ok(attr_key) = attr.key_str() {
                if attr_key == key {
                    if let Ok(attr_value) = attr.value_str() {
                        return Some(attr_value);
                    }
                }
            }
        }
        None
    }

    /// Extract validator identity key from event based on event type
    pub fn extract_identity_key_from_event(event: &ContextualizedEvent<'_>) -> Option<String> {
        match event.event.kind.as_str() {
            "penumbra.core.component.stake.v1.EventDelegate"
            | "penumbra.core.component.stake.v1.EventUndelegate"
            | "penumbra.core.component.stake.v1.EventValidatorBondingStateChange"
            | "penumbra.core.component.stake.v1.EventValidatorMissedBlock"
            | "penumbra.core.component.stake.v1.EventValidatorStateChange"
            | "penumbra.core.component.stake.v1.EventValidatorVotingPowerChange"
            | "penumbra.core.component.stake.v1.EventRateDataChange"
            | "penumbra.core.component.stake.v1.EventSlashingPenaltyApplied"
            | "penumbra.core.component.stake.v1.EventTombstoneValidator" => {
                if let Some(identity_key_json) = Self::find_attribute_value(event, "identityKey") {
                    match serde_json::from_str::<Value>(identity_key_json) {
                        Ok(identity_data) => identity_data["ik"].as_str().map(String::from),
                        Err(_) => None,
                    }
                } else {
                    None
                }
            }
            "penumbra.core.component.stake.v1.EventValidatorDefinitionUpload" => {
                if let Some(validator_json) = Self::find_attribute_value(event, "validator") {
                    match serde_json::from_str::<Value>(validator_json) {
                        Ok(validator_data) => validator_data["identityKey"]["ik"]
                            .as_str()
                            .map(String::from),
                        Err(_) => None,
                    }
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Link a transaction hash to a validator identity key
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn link_transaction_to_validator(
        tx_hash: &[u8],
        identity_key: &str,
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<()> {
        debug!(
            "Linking transaction {:?} to validator {}",
            crate::parsing::encode_to_hex(tx_hash.try_into().unwrap_or([0u8; 32])),
            identity_key
        );

        match sqlx::query(
            r"
            UPDATE explorer_transactions 
            SET validator_identity_key = $1
            WHERE tx_hash = $2
            ",
        )
        .bind(identity_key)
        .bind(tx_hash)
        .execute(dbtx.as_mut())
        .await
        {
            Ok(result) => {
                if result.rows_affected() > 0 {
                    debug!(
                        "Successfully linked transaction to validator {}",
                        identity_key
                    );
                } else {
                    debug!(
                        "Transaction not found in explorer_transactions table for validator {}",
                        identity_key
                    );
                }
                Ok(())
            }
            Err(e) => {
                error!(
                    "Failed to link transaction to validator {}: {}",
                    identity_key, e
                );
                Ok(())
            }
        }
    }

    /// Parse a validator definition from an event
    ///
    /// # Errors
    ///
    /// Returns an error if required fields are missing from the event data.
    ///
    /// # Panics
    ///
    /// Panics if `DateTime::from_timestamp(0, 0)` fails, which should never happen.
    pub fn from_event(
        event_json: &Value,
        height: u64,
        timestamp: DateTime<Utc>,
        default_state: &str,
        default_bonding_state: &str,
        voting_power: i64,
        voting_power_active_percentage: f64,
    ) -> Result<Self> {
        let identity_key = event_json["identityKey"]["ik"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing or invalid identity key"))?
            .to_string();

        let decoded_address = identity_key_to_validator_address(&identity_key).ok();

        let name = event_json["name"].as_str().map(String::from);
        let website = event_json["website"].as_str().map(String::from);
        let description = event_json["description"].as_str().map(String::from);
        let consensus_key = event_json["consensusKey"].as_str().map(String::from);

        let governance_key = event_json
            .get("governanceKey")
            .and_then(|gk| gk.get("gk"))
            .and_then(|gk| gk.as_str())
            .map(String::from);

        let (first_seen_height, first_seen_time) = if default_state.contains("ACTIVE")
            && height == 1
        {
            debug!(
                "Creating genesis ACTIVE validator {} at height {}, timestamp {}",
                identity_key, height, timestamp
            );
            (Some(i64::try_from(height).unwrap_or(i64::MAX)), timestamp)
        } else if default_state.contains("DEFINED") {
            debug!("Creating DEFINED validator {} at height {}, timestamp {} - height will be set when ACTIVE",
                  identity_key, height, timestamp);
            (None, timestamp)
        } else if default_state.contains("ACTIVE") {
            debug!("Creating event ACTIVE validator {} at height {} - using current timestamp as fallback",
                  identity_key, height);
            (Some(i64::try_from(height).unwrap_or(i64::MAX)), timestamp)
        } else {
            debug!(
                "Creating validator {} with state {} - height will be NULL until ACTIVE",
                identity_key, default_state
            );
            (None, DateTime::<Utc>::from_timestamp(0, 0).unwrap())
        };

        debug!(
            "Created validator from event: identity_key={}, name={:?}, consensus_key={:?}",
            identity_key, name, consensus_key
        );

        let bonding_state = if default_bonding_state.is_empty() {
            Some("BONDING_STATE_ENUM_UNSPECIFIED".to_string())
        } else {
            Some(default_bonding_state.to_string())
        };

        Ok(Self {
            identity_key,
            decoded_address,
            name,
            website,
            description,
            consensus_key,
            governance_key,
            state: default_state.to_string(),
            bonding_state,
            voting_power,
            voting_power_active_percentage,
            first_seen_height,
            first_seen_time,
            last_updated: timestamp,
        })
    }

    /// Insert only - fails if validator already exists (for `ensure_validator_exists`)
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails or if the validator already exists.
    pub async fn insert_only(&self, dbtx: &mut PgTransaction<'_>) -> Result<()> {
        sqlx::query(
            r"
            INSERT INTO validators (
                identity_key,
                decoded_address,
                name,
                website,
                description,
                consensus_key,
                governance_key,
                state,
                bonding_state,
                voting_power,
                voting_power_active_percentage,
                first_seen_height,
                first_seen_time,
                last_updated
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
            )
            ",
        )
        .bind(&self.identity_key)
        .bind(&self.decoded_address)
        .bind(&self.name)
        .bind(&self.website)
        .bind(&self.description)
        .bind(&self.consensus_key)
        .bind(&self.governance_key)
        .bind(&self.state)
        .bind(&self.bonding_state)
        .bind(self.voting_power)
        .bind(self.voting_power_active_percentage)
        .bind(self.first_seen_height)
        .bind(self.first_seen_time)
        .bind(self.last_updated)
        .execute(dbtx.as_mut())
        .await?;

        Ok(())
    }

    /// Insert or update a validator in the database
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn insert_or_update(&self, dbtx: &mut PgTransaction<'_>) -> Result<()> {
        sqlx::query(
            r"
            INSERT INTO validators (
                identity_key,
                decoded_address,
                name,
                website,
                description,
                consensus_key,
                governance_key,
                state,
                bonding_state,
                voting_power,
                voting_power_active_percentage,
                first_seen_height,
                first_seen_time,
                last_updated
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
            )
            ON CONFLICT (identity_key) DO UPDATE SET
                decoded_address = COALESCE(validators.decoded_address, EXCLUDED.decoded_address),
                name = EXCLUDED.name,
                website = EXCLUDED.website,
                description = EXCLUDED.description,
                consensus_key = EXCLUDED.consensus_key,
                governance_key = EXCLUDED.governance_key,
                state = EXCLUDED.state,
                bonding_state = EXCLUDED.bonding_state,
                voting_power = EXCLUDED.voting_power,
                voting_power_active_percentage = EXCLUDED.voting_power_active_percentage,
                last_updated = GREATEST(validators.last_updated, EXCLUDED.last_updated)
            ",
        )
        .bind(&self.identity_key)
        .bind(&self.decoded_address)
        .bind(&self.name)
        .bind(&self.website)
        .bind(&self.description)
        .bind(&self.consensus_key)
        .bind(&self.governance_key)
        .bind(&self.state)
        .bind(&self.bonding_state)
        .bind(self.voting_power)
        .bind(self.voting_power_active_percentage)
        .bind(self.first_seen_height)
        .bind(self.first_seen_time)
        .bind(self.last_updated)
        .execute(dbtx.as_mut())
        .await?;

        Ok(())
    }

    /// Update only metadata (name, website, description, etc.) without changing state or voting power
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn update_metadata_only(&self, dbtx: &mut PgTransaction<'_>) -> Result<()> {
        sqlx::query(
            r"
            UPDATE validators 
            SET 
                name = $2,
                website = $3,
                description = $4,
                consensus_key = $5,
                governance_key = $6,
                last_updated = $7
            WHERE 
                identity_key = $1
            ",
        )
        .bind(&self.identity_key)
        .bind(&self.name)
        .bind(&self.website)
        .bind(&self.description)
        .bind(&self.consensus_key)
        .bind(&self.governance_key)
        .bind(self.last_updated)
        .execute(dbtx.as_mut())
        .await?;

        Ok(())
    }

    /// Update validator state
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn update_state(
        identity_key: &str,
        state: &str,
        dbtx: &mut PgTransaction<'_>,
        timestamp: DateTime<Utc>,
        height: u64,
    ) -> Result<()> {
        if state.contains("DEFINED") {
            let validator_info: Option<(Option<String>,)> =
                sqlx::query_as("SELECT state FROM validators WHERE identity_key = $1")
                    .bind(identity_key)
                    .fetch_optional(dbtx.as_mut())
                    .await?;

            match validator_info {
                Some((current_state,)) => {
                    if !current_state.map_or(false, |s| s.contains("DEFINED")) {
                        debug!(
                            "Validator {} transitioned to DEFINED state - setting first_seen_time",
                            identity_key
                        );

                        sqlx::query(
                            r"
                            UPDATE validators 
                            SET 
                                state = $1,
                                last_updated = $2,
                                first_seen_time = $2
                            WHERE 
                                identity_key = $3
                            ",
                        )
                        .bind(state)
                        .bind(timestamp)
                        .bind(identity_key)
                        .execute(dbtx.as_mut())
                        .await?;

                        return Ok(());
                    }
                }
                None => {
                    debug!(
                        "Validator {} not found in database when updating to DEFINED state",
                        identity_key
                    );
                }
            }
        } else if state.contains("ACTIVE") {
            let validator_info: Option<(Option<String>, Option<i64>)> = sqlx::query_as(
                "SELECT state, first_seen_height FROM validators WHERE identity_key = $1",
            )
            .bind(identity_key)
            .fetch_optional(dbtx.as_mut())
            .await?;

            match validator_info {
                Some((_current_state, current_height)) => {
                    if current_height.is_none() {
                        debug!("Validator {} transitioned to ACTIVE state - setting first_seen_height for uptime tracking", identity_key);

                        sqlx::query(
                            r"
                            UPDATE validators 
                            SET 
                                state = $1,
                                last_updated = $2,
                                first_seen_height = $3
                            WHERE 
                                identity_key = $4
                            ",
                        )
                        .bind(state)
                        .bind(timestamp)
                        .bind(i64::try_from(height).unwrap_or(i64::MAX))
                        .bind(identity_key)
                        .execute(dbtx.as_mut())
                        .await?;

                        return Ok(());
                    }
                }
                None => {
                    debug!(
                        "Validator {} not found in database when updating to ACTIVE state",
                        identity_key
                    );
                }
            }
        }

        sqlx::query(
            r"
            UPDATE validators 
            SET 
                state = $1,
                last_updated = $2
            WHERE 
                identity_key = $3
            ",
        )
        .bind(state)
        .bind(timestamp)
        .bind(identity_key)
        .execute(dbtx.as_mut())
        .await?;

        Ok(())
    }

    /// Update validator bonding state
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn update_bonding_state(
        identity_key: &str,
        bonding_state: &str,
        dbtx: &mut PgTransaction<'_>,
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        debug!(
            "Updating bonding state for validator {} to {}",
            identity_key, bonding_state
        );

        sqlx::query(
            r"
            UPDATE validators 
            SET 
                bonding_state = $1,
                last_updated = $2
            WHERE 
                identity_key = $3
            ",
        )
        .bind(bonding_state)
        .bind(timestamp)
        .bind(identity_key)
        .execute(dbtx.as_mut())
        .await?;

        Ok(())
    }

    /// Update validator voting power
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn update_voting_power(
        identity_key: &str,
        voting_power: i64,
        voting_power_active_percentage: f64,
        dbtx: &mut PgTransaction<'_>,
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            r"
            UPDATE validators 
            SET 
                voting_power = $1,
                voting_power_active_percentage = $2,
                last_updated = $3
            WHERE 
                identity_key = $4
            ",
        )
        .bind(voting_power)
        .bind(voting_power_active_percentage)
        .bind(timestamp)
        .bind(identity_key)
        .execute(dbtx.as_mut())
        .await?;

        Ok(())
    }

    /// Performance optimization: Bulk record block participation for multiple validators
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn record_validator_blocks_bulk(
        validator_records: &[(String, i64, DateTime<Utc>, bool)],
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<()> {
        if validator_records.is_empty() {
            return Ok(());
        }

        debug!(
            "Bulk recording {} validator block records",
            validator_records.len()
        );

        let mut values_clauses = Vec::new();

        for i in 0..validator_records.len() {
            let param_base = i * 4;
            values_clauses.push(format!(
                "(${}, ${}, ${}, ${})",
                param_base + 1,
                param_base + 2,
                param_base + 3,
                param_base + 4
            ));
        }

        let query = format!(
            r"
            INSERT INTO validator_blocks (identity_key, block_height, timestamp, signed)
            VALUES {}
            ON CONFLICT (identity_key, block_height) DO UPDATE SET
                signed = EXCLUDED.signed,
                timestamp = EXCLUDED.timestamp
            ",
            values_clauses.join(", ")
        );

        let mut sqlx_query = sqlx::query(&query);
        for (identity_key, block_height, timestamp, signed) in validator_records {
            sqlx_query = sqlx_query
                .bind(identity_key)
                .bind(block_height)
                .bind(timestamp)
                .bind(signed);
        }

        match sqlx_query.execute(dbtx.as_mut()).await {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("Failed to bulk record validator blocks: {}", e);
                Ok(())
            }
        }
    }

    /// Record block participation for a validator
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn record_validator_block(
        identity_key: &str,
        block_height: i64,
        timestamp: DateTime<Utc>,
        signed: bool,
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<()> {
        let validator_exists: i64 =
            match sqlx::query_scalar("SELECT COUNT(*) FROM validators WHERE identity_key = $1")
                .bind(identity_key)
                .fetch_one(dbtx.as_mut())
                .await
            {
                Ok(count) => count,
                Err(e) => {
                    error!("Failed to check if validator exists: {}", e);
                    return Ok(());
                }
            };

        if validator_exists == 0 {
            debug!(
                "Skipping validator_block record for non-existent validator: {}",
                identity_key
            );
            return Ok(());
        }

        let block_exists: i64 = match sqlx::query_scalar(
            "SELECT COUNT(*) FROM explorer_block_details WHERE height = $1",
        )
        .bind(block_height)
        .fetch_one(dbtx.as_mut())
        .await
        {
            Ok(count) => count,
            Err(e) => {
                error!("Failed to check if block exists: {}", e);
                return Ok(());
            }
        };

        if block_exists == 0 {
            debug!(
                "Skipping validator_block record for non-existent block height: {}",
                block_height
            );
            return Ok(());
        }

        match sqlx::query(
            r"
            INSERT INTO validator_blocks (identity_key, block_height, timestamp, signed)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (identity_key, block_height) DO UPDATE SET
                signed = EXCLUDED.signed,
                timestamp = EXCLUDED.timestamp
            ",
        )
        .bind(identity_key)
        .bind(block_height)
        .bind(timestamp)
        .bind(signed)
        .execute(dbtx.as_mut())
        .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                error!(
                    "Failed to record validator block: validator={}, block={}, error={}",
                    identity_key, block_height, e
                );
                Ok(())
            }
        }
    }

    /// Calculate total voting power across ACTIVE validators only
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn calculate_total_voting_power(dbtx: &mut PgTransaction<'_>) -> Result<i64> {
        let active_state: Option<String> = sqlx::query_scalar(
            "SELECT DISTINCT state FROM validators WHERE state LIKE '%ACTIVE%' LIMIT 1",
        )
        .fetch_optional(dbtx.as_mut())
        .await?;

        let result = match active_state {
            Some(state) => {
                sqlx::query_scalar::<_, i64>(
                    &format!("SELECT COALESCE(SUM(voting_power)::BIGINT, 0) FROM validators WHERE state = '{state}'")
                )
                .fetch_one(dbtx.as_mut())
                .await?
            },
            None => 0,
        };

        Ok(result)
    }

    /// Update voting power active percentages for ACTIVE validators only (set others to 0%)
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn update_all_voting_power_active_percentages(
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<()> {
        let total_voting_power = Self::calculate_total_voting_power(dbtx).await?;

        if total_voting_power == 0 {
            return Ok(());
        }

        let active_state: Option<String> = sqlx::query_scalar(
            "SELECT DISTINCT state FROM validators WHERE state LIKE '%ACTIVE%' LIMIT 1",
        )
        .fetch_optional(dbtx.as_mut())
        .await?;

        if let Some(state) = active_state {
            let query = format!(
                r"
                UPDATE validators
                SET 
                    voting_power_active_percentage = ROUND(((voting_power::float8 / $1::float8) * 100.0)::numeric, 2)
                WHERE
                    state = '{state}'
                "
            );

            sqlx::query(&query)
                .bind(total_voting_power)
                .execute(dbtx.as_mut())
                .await?;

            let clear_inactive_query = format!(
                r"
                UPDATE validators
                SET 
                    voting_power_active_percentage = 0.0
                WHERE
                    state != '{state}'
                "
            );

            sqlx::query(&clear_inactive_query)
                .execute(dbtx.as_mut())
                .await?;
        }

        Ok(())
    }

    /// Update `total_staked` in `validator_staking_parameters` with sum of ACTIVE validators only
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn update_total_staked(dbtx: &mut PgTransaction<'_>) -> Result<()> {
        let total_active_voting_power = Self::calculate_total_voting_power(dbtx).await?;

        let formatted_total = format!("{total_active_voting_power} UM");

        let chain_id: Option<String> =
            sqlx::query_scalar("SELECT chain_id FROM validator_staking_parameters LIMIT 1")
                .fetch_optional(dbtx.as_mut())
                .await?;

        if let Some(chain_id) = chain_id {
            sqlx::query(
                r"
                UPDATE validator_staking_parameters
                SET total_staked = $1
                WHERE chain_id = $2
                ",
            )
            .bind(&formatted_total)
            .bind(&chain_id)
            .execute(dbtx.as_mut())
            .await?;
        }

        Ok(())
    }

    /// Ensure validator exists in database, creating it if necessary
    /// IMPORTANT: This function only creates new validators, never modifies existing ones
    #[allow(clippy::too_many_arguments)]
    async fn ensure_validator_exists(
        identity_key: &str,
        height: u64,
        timestamp: DateTime<Utc>,
        dbtx: &mut PgTransaction<'_>,
        state: Option<&str>,
        bonding_state: Option<&str>,
        voting_power: Option<i64>,
        cache: &mut ValidatorExistenceCache,
    ) -> Result<()> {
        let validator_exists = match cache.validator_exists(identity_key, dbtx).await {
            Ok(exists) => exists,
            Err(e) => {
                error!("Failed to check if validator exists: {}", e);
                return Err(anyhow::anyhow!("Failed to check validator existence"));
            }
        };

        if validator_exists {
            debug!(
                "Validator {} already exists, skipping creation",
                identity_key
            );
        } else {
            debug!("Creating new validator from event: {}", identity_key);

            let validator_state = match state {
                Some(s) if !s.is_empty() => {
                    debug!(
                        "Creating new validator {} with explicit state: {}",
                        identity_key, s
                    );
                    s
                }
                _ => {
                    debug!("Creating new validator {} with UNSPECIFIED state (discovered through non-state event)", identity_key);
                    "VALIDATOR_STATE_ENUM_UNSPECIFIED"
                }
            };

            match Self::from_event(
                &serde_json::json!({
                    "identityKey": {"ik": identity_key}
                }),
                height,
                timestamp,
                validator_state,
                bonding_state.unwrap_or(""),
                voting_power.unwrap_or(0),
                0.0,
            ) {
                Ok(validator) => {
                    if let Err(e) = validator.insert_only(dbtx).await {
                        error!("Failed to insert new validator: {}", e);
                        return Err(anyhow::anyhow!("Failed to insert validator"));
                    }

                    cache.add_validator(identity_key);
                }
                Err(e) => {
                    error!("Failed to create validator: {}", e);
                    return Err(anyhow::anyhow!("Failed to create validator"));
                }
            }
        }

        Ok(())
    }

    /// Process validator-related events
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    #[allow(clippy::too_many_lines)]
    pub async fn process_events(
        dbtx: &mut PgTransaction<'_>,
        events: &[ContextualizedEvent<'_>],
        height: u64,
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        debug!(
            "Processing validator events for block {} with timestamp {}",
            height, timestamp
        );
        let mut existence_cache = ValidatorExistenceCache::default();
        let mut voting_power_batch = VotingPowerBatch::default();

        let mut tx_validator_mappings: Vec<([u8; 32], String)> = Vec::new();

        for event in events {
            if let Some(identity_key) = Self::extract_identity_key_from_event(event) {
                if let Some(tx_hash_bytes) = event.tx_hash() {
                    let tx_hash_array = tx_hash_bytes;
                    tx_validator_mappings.push((tx_hash_array, identity_key.clone()));
                    debug!(
                        "Added transaction mapping: {} -> {}",
                        crate::parsing::encode_to_hex(tx_hash_array),
                        identity_key
                    );
                }
            }
        }

        for event in events {
            if event.event.kind.as_str()
                == "penumbra.core.component.stake.v1.EventValidatorDefinitionUpload"
            {
                match Self::find_attribute_value(event, "validator") {
                    Some(validator_json) => {
                        debug!("Processing validator definition: {}", validator_json);

                        match serde_json::from_str::<Value>(validator_json) {
                            Ok(validator_data) => {
                                let Some(identity_key) =
                                    validator_data["identityKey"]["ik"].as_str()
                                else {
                                    error!("EventValidatorDefinitionUpload missing identity key");
                                    continue;
                                };

                                let validator_exists = match existence_cache
                                    .validator_exists(identity_key, dbtx)
                                    .await
                                {
                                    Ok(exists) => exists,
                                    Err(e) => {
                                        error!("Failed to check if validator exists: {}", e);
                                        continue;
                                    }
                                };

                                if validator_exists {
                                    debug!(
                                        "Updating metadata for existing validator: {}",
                                        identity_key
                                    );

                                    match Self::from_event(
                                        &validator_data,
                                        height,
                                        timestamp,
                                        "dummy",
                                        "",
                                        0,
                                        0.0,
                                    ) {
                                        Ok(validator) => {
                                            if let Err(e) =
                                                validator.update_metadata_only(dbtx).await
                                            {
                                                error!(
                                                    "Failed to update validator metadata: {}",
                                                    e
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            error!("Failed to create validator for metadata update: {}", e);
                                        }
                                    }
                                } else {
                                    debug!(
                                        "Creating new validator from definition: {}",
                                        identity_key
                                    );

                                    let state = validator_data
                                        .get("state")
                                        .and_then(|s| s.get("state"))
                                        .and_then(|s| s.as_str());

                                    let bonding_state = validator_data
                                        .get("bondingState")
                                        .and_then(|s| s.get("state"))
                                        .and_then(|s| s.as_str());

                                    let default_state = match state {
                                        Some(s) if !s.is_empty() => s,
                                        _ => "VALIDATOR_STATE_ENUM_UNSPECIFIED",
                                    };

                                    match Self::from_event(
                                        &validator_data,
                                        height,
                                        timestamp,
                                        default_state,
                                        bonding_state.unwrap_or(""),
                                        0,
                                        0.0,
                                    ) {
                                        Ok(validator) => {
                                            if let Err(e) = validator.insert_only(dbtx).await {
                                                debug!("Validator {} was created concurrently, skipping: {}", identity_key, e);
                                            } else {
                                                existence_cache.add_validator(identity_key);
                                            }
                                        }
                                        Err(e) => {
                                            error!(
                                                "Failed to parse new validator definition: {}",
                                                e
                                            );
                                        }
                                    }
                                }

                                if let Some(funding_streams) = validator_data.get("fundingStreams")
                                {
                                    if let Err(e) = ValidatorFundingStream::process_funding_streams(
                                        identity_key,
                                        funding_streams,
                                        timestamp,
                                        dbtx,
                                    )
                                    .await
                                    {
                                        error!("Failed to process funding streams for validator {}: {}", identity_key, e);
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    "Failed to parse validator JSON: {} - {}",
                                    validator_json, e
                                );
                            }
                        }
                    }
                    None => {
                        error!("EventValidatorDefinitionUpload missing validator attribute");
                    }
                }
            }
        }

        for event in events {
            match event.event.kind.as_str() {
                "penumbra.core.component.stake.v1.EventValidatorStateChange" => {
                    match (
                        Self::find_attribute_value(event, "identityKey"),
                        Self::find_attribute_value(event, "state"),
                    ) {
                        (Some(identity_key_json), Some(state_json)) => {
                            debug!(
                                "Processing validator state change: {}, {}",
                                identity_key_json, state_json
                            );

                            let identity_key_result =
                                serde_json::from_str::<Value>(identity_key_json);
                            let state_result = serde_json::from_str::<Value>(state_json);

                            match (identity_key_result, state_result) {
                                (Ok(identity_data), Ok(state_data)) => {
                                    if let Some(identity_key) = identity_data["ik"].as_str() {
                                        if let Some(state) = state_data["state"].as_str() {
                                            if let Err(e) = Self::ensure_validator_exists(
                                                identity_key,
                                                height,
                                                timestamp,
                                                dbtx,
                                                Some(state),
                                                None,
                                                None,
                                                &mut existence_cache,
                                            )
                                            .await
                                            {
                                                error!("Failed to ensure validator exists: {}", e);
                                                continue;
                                            }

                                            if let Err(e) = Self::update_state(
                                                identity_key,
                                                state,
                                                dbtx,
                                                timestamp,
                                                height,
                                            )
                                            .await
                                            {
                                                error!("Failed to update validator state: {}", e);
                                            }
                                        }
                                    }
                                }
                                _ => {
                                    error!("Failed to parse validator state change data");
                                }
                            }
                        }
                        _ => {
                            error!("EventValidatorStateChange missing required attributes");
                        }
                    }
                }
                "penumbra.core.component.stake.v1.EventValidatorBondingStateChange" => {
                    match (
                        Self::find_attribute_value(event, "identityKey"),
                        Self::find_attribute_value(event, "bondingState"),
                    ) {
                        (Some(identity_key_json), Some(bonding_state_json)) => {
                            debug!(
                                "Processing validator bonding state change: {}, {}",
                                identity_key_json, bonding_state_json
                            );

                            let identity_key_result =
                                serde_json::from_str::<Value>(identity_key_json);
                            let bonding_state_result =
                                serde_json::from_str::<Value>(bonding_state_json);

                            match (identity_key_result, bonding_state_result) {
                                (Ok(identity_data), Ok(bonding_state_data)) => {
                                    if let Some(identity_key) = identity_data["ik"].as_str() {
                                        if let Some(bonding_state) =
                                            bonding_state_data["state"].as_str()
                                        {
                                            if let Err(e) = Self::ensure_validator_exists(
                                                identity_key,
                                                height,
                                                timestamp,
                                                dbtx,
                                                None,
                                                Some(bonding_state),
                                                None,
                                                &mut existence_cache,
                                            )
                                            .await
                                            {
                                                error!("Failed to ensure validator exists: {}", e);
                                                continue;
                                            }

                                            if let Err(e) = Self::update_bonding_state(
                                                identity_key,
                                                bonding_state,
                                                dbtx,
                                                timestamp,
                                            )
                                            .await
                                            {
                                                error!(
                                                    "Failed to update validator bonding state: {}",
                                                    e
                                                );
                                            }
                                        }
                                    }
                                }
                                _ => {
                                    error!("Failed to parse validator bonding state change data");
                                }
                            }
                        }
                        _ => {
                            error!("EventValidatorBondingStateChange missing required attributes");
                        }
                    }
                }
                "penumbra.core.component.stake.v1.EventValidatorVotingPowerChange" => {
                    match (
                        Self::find_attribute_value(event, "identityKey"),
                        Self::find_attribute_value(event, "votingPower"),
                    ) {
                        (Some(identity_key_json), Some(voting_power_json)) => {
                            debug!(
                                "Processing validator voting power change: {}, {}",
                                identity_key_json, voting_power_json
                            );

                            let identity_key_result =
                                serde_json::from_str::<Value>(identity_key_json);
                            let voting_power_result =
                                serde_json::from_str::<Value>(voting_power_json);

                            match (identity_key_result, voting_power_result) {
                                (Ok(identity_data), Ok(voting_power_data)) => {
                                    if let Some(identity_key) = identity_data["ik"].as_str() {
                                        if let Some(voting_power_str) =
                                            voting_power_data["lo"].as_str()
                                        {
                                            match voting_power_str.parse::<i64>() {
                                                Ok(raw_voting_power) => {
                                                    let voting_power = raw_voting_power / 1_000_000;

                                                    if let Err(e) = Self::ensure_validator_exists(
                                                        identity_key,
                                                        height,
                                                        timestamp,
                                                        dbtx,
                                                        None,
                                                        None,
                                                        Some(voting_power),
                                                        &mut existence_cache,
                                                    )
                                                    .await
                                                    {
                                                        error!(
                                                            "Failed to ensure validator exists: {}",
                                                            e
                                                        );
                                                        continue;
                                                    }

                                                    voting_power_batch.add_change(
                                                        identity_key.to_string(),
                                                        voting_power,
                                                    );
                                                }
                                                Err(e) => {
                                                    error!(
                                                        "Failed to parse voting power '{}': {}",
                                                        voting_power_str, e
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                                _ => {
                                    error!("Failed to parse validator voting power change data");
                                }
                            }
                        }
                        _ => {
                            error!("EventValidatorVotingPowerChange missing required attributes");
                        }
                    }
                }
                "penumbra.core.component.stake.v1.EventValidatorMissedBlock" => {
                    if let Some(identity_key_json) =
                        Self::find_attribute_value(event, "identityKey")
                    {
                        debug!("Processing validator missed block: {}", identity_key_json);

                        match serde_json::from_str::<Value>(identity_key_json) {
                            Ok(identity_data) => {
                                if let Some(identity_key) = identity_data["ik"].as_str() {
                                    if let Err(e) = Self::ensure_validator_exists(
                                        identity_key,
                                        height,
                                        timestamp,
                                        dbtx,
                                        None,
                                        None,
                                        None,
                                        &mut existence_cache,
                                    )
                                    .await
                                    {
                                        error!("Failed to ensure validator exists: {}", e);
                                        continue;
                                    }

                                    if let Err(e) = Self::record_validator_block(
                                        identity_key,
                                        i64::try_from(height).unwrap_or(i64::MAX),
                                        timestamp,
                                        false,
                                        dbtx,
                                    )
                                    .await
                                    {
                                        error!("Failed to record missed block: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    "Failed to parse validator identity key '{}': {}",
                                    identity_key_json, e
                                );
                            }
                        }
                    }
                }
                "penumbra.core.component.stake.v1.EventDelegate" => {
                    if let Some(identity_key_json) =
                        Self::find_attribute_value(event, "identityKey")
                    {
                        debug!("Processing delegate event: {}", identity_key_json);

                        match serde_json::from_str::<Value>(identity_key_json) {
                            Ok(identity_data) => {
                                if let Some(identity_key) = identity_data["ik"].as_str() {
                                    if let Err(e) = Self::ensure_validator_exists(
                                        identity_key,
                                        height,
                                        timestamp,
                                        dbtx,
                                        None,
                                        None,
                                        None,
                                        &mut existence_cache,
                                    )
                                    .await
                                    {
                                        error!("Failed to ensure validator exists for delegate event: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    "Failed to parse validator identity key '{}': {}",
                                    identity_key_json, e
                                );
                            }
                        }
                    }
                }
                "penumbra.core.component.stake.v1.EventUndelegate" => {
                    if let Some(identity_key_json) =
                        Self::find_attribute_value(event, "identityKey")
                    {
                        debug!("Processing undelegate event: {}", identity_key_json);

                        match serde_json::from_str::<Value>(identity_key_json) {
                            Ok(identity_data) => {
                                if let Some(identity_key) = identity_data["ik"].as_str() {
                                    if let Err(e) = Self::ensure_validator_exists(
                                        identity_key,
                                        height,
                                        timestamp,
                                        dbtx,
                                        None,
                                        None,
                                        None,
                                        &mut existence_cache,
                                    )
                                    .await
                                    {
                                        error!("Failed to ensure validator exists for undelegate event: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    "Failed to parse validator identity key '{}': {}",
                                    identity_key_json, e
                                );
                            }
                        }
                    }
                }
                "penumbra.core.component.stake.v1.EventRateDataChange" => {
                    if let Some(identity_key_json) =
                        Self::find_attribute_value(event, "identityKey")
                    {
                        debug!("Processing rate data change event: {}", identity_key_json);

                        match serde_json::from_str::<Value>(identity_key_json) {
                            Ok(identity_data) => {
                                if let Some(identity_key) = identity_data["ik"].as_str() {
                                    if let Err(e) = Self::ensure_validator_exists(
                                        identity_key,
                                        height,
                                        timestamp,
                                        dbtx,
                                        None,
                                        None,
                                        None,
                                        &mut existence_cache,
                                    )
                                    .await
                                    {
                                        error!("Failed to ensure validator exists for rate data change event: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    "Failed to parse validator identity key '{}': {}",
                                    identity_key_json, e
                                );
                            }
                        }
                    }
                }
                "penumbra.core.component.stake.v1.EventSlashingPenaltyApplied" => {
                    if let Some(identity_key_json) =
                        Self::find_attribute_value(event, "identityKey")
                    {
                        debug!(
                            "Processing slashing penalty applied event: {}",
                            identity_key_json
                        );

                        match serde_json::from_str::<Value>(identity_key_json) {
                            Ok(identity_data) => {
                                if let Some(identity_key) = identity_data["ik"].as_str() {
                                    if let Err(e) = Self::ensure_validator_exists(
                                        identity_key,
                                        height,
                                        timestamp,
                                        dbtx,
                                        None,
                                        None,
                                        None,
                                        &mut existence_cache,
                                    )
                                    .await
                                    {
                                        error!("Failed to ensure validator exists for slashing penalty event: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    "Failed to parse validator identity key '{}': {}",
                                    identity_key_json, e
                                );
                            }
                        }
                    }
                }
                "penumbra.core.component.stake.v1.EventTombstoneValidator" => {
                    if let Some(identity_key_json) =
                        Self::find_attribute_value(event, "identityKey")
                    {
                        debug!(
                            "Processing tombstone validator event: {}",
                            identity_key_json
                        );

                        match serde_json::from_str::<Value>(identity_key_json) {
                            Ok(identity_data) => {
                                if let Some(identity_key) = identity_data["ik"].as_str() {
                                    if let Err(e) = Self::ensure_validator_exists(
                                        identity_key,
                                        height,
                                        timestamp,
                                        dbtx,
                                        None,
                                        None,
                                        None,
                                        &mut existence_cache,
                                    )
                                    .await
                                    {
                                        error!("Failed to ensure validator exists for tombstone event: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    "Failed to parse validator identity key '{}': {}",
                                    identity_key_json, e
                                );
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        if let Err(e) = voting_power_batch.apply_all(dbtx, timestamp).await {
            error!("Failed to apply batched voting power changes: {}", e);
        }

        debug!(
            "Processing {} transaction-to-validator mappings",
            tx_validator_mappings.len()
        );
        for (tx_hash, identity_key) in tx_validator_mappings {
            if let Err(e) = Self::link_transaction_to_validator(&tx_hash, &identity_key, dbtx).await
            {
                error!(
                    "Failed to link transaction to validator {}: {}",
                    identity_key, e
                );
            }
        }

        Ok(())
    }
}

impl ValidatorFundingStream {
    /// Create a new funding stream
    #[must_use]
    pub fn new(
        identity_key: String,
        stream_type: String,
        recipient_address: Option<String>,
        rate_bps: i32,
        timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            identity_key,
            stream_type,
            recipient_address,
            rate_bps,
            created_at: timestamp,
            updated_at: timestamp,
        }
    }

    /// Insert or update a funding stream in the database
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn insert_or_update(&self, dbtx: &mut PgTransaction<'_>) -> Result<()> {
        sqlx::query(
            r"
            INSERT INTO validator_funding_streams (
                identity_key,
                stream_type,
                recipient_address,
                rate_bps,
                created_at,
                updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6
            )
            ON CONFLICT (identity_key, stream_type, recipient_address) DO UPDATE SET
                rate_bps = EXCLUDED.rate_bps,
                updated_at = EXCLUDED.updated_at
            ",
        )
        .bind(&self.identity_key)
        .bind(&self.stream_type)
        .bind(&self.recipient_address)
        .bind(self.rate_bps)
        .bind(self.created_at)
        .bind(self.updated_at)
        .execute(dbtx.as_mut())
        .await?;

        Ok(())
    }

    /// Process funding streams from validator data
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn process_funding_streams(
        identity_key: &str,
        funding_streams_json: &Value,
        timestamp: DateTime<Utc>,
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<()> {
        if let Err(e) = sqlx::query("DELETE FROM validator_funding_streams WHERE identity_key = $1")
            .bind(identity_key)
            .execute(dbtx.as_mut())
            .await
        {
            error!(
                "Failed to delete old funding streams for validator {}: {}",
                identity_key, e
            );
        }

        debug!(
            "Deleted old funding streams for validator {}, inserting new ones",
            identity_key
        );

        if let Some(funding_streams) = funding_streams_json.as_array() {
            for stream in funding_streams {
                if let Some(to_address) = stream.get("toAddress") {
                    if let Some(address) = to_address.get("address").and_then(|a| a.as_str()) {
                        if let Some(rate_bps) = to_address
                            .get("rateBps")
                            .and_then(serde_json::Value::as_i64)
                        {
                            let funding_stream = Self::new(
                                identity_key.to_string(),
                                "toAddress".to_string(),
                                Some(address.to_string()),
                                i32::try_from(rate_bps).unwrap_or(i32::MAX),
                                timestamp,
                            );

                            if let Err(e) = funding_stream.insert_or_update(dbtx).await {
                                error!(
                                    "Failed to insert funding stream for validator {}: {}",
                                    identity_key, e
                                );
                            }
                        }
                    }
                }

                if let Some(to_community_pool) = stream.get("toCommunityPool") {
                    if let Some(rate_bps) = to_community_pool
                        .get("rateBps")
                        .and_then(serde_json::Value::as_i64)
                    {
                        let funding_stream = Self::new(
                            identity_key.to_string(),
                            "toCommunityPool".to_string(),
                            None,
                            i32::try_from(rate_bps).unwrap_or(i32::MAX),
                            timestamp,
                        );

                        if let Err(e) = funding_stream.insert_or_update(dbtx).await {
                            error!("Failed to insert community pool funding stream for validator {}: {}", identity_key, e);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Calculate total commission rate for a validator
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn calculate_total_commission_rate(
        identity_key: &str,
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<f64> {
        let total_rate_bps: Option<i64> = sqlx::query_scalar(
            "SELECT SUM(rate_bps) FROM validator_funding_streams WHERE identity_key = $1",
        )
        .bind(identity_key)
        .fetch_optional(dbtx.as_mut())
        .await?;

        let total_percentage =
            f64::from(i32::try_from(total_rate_bps.unwrap_or(0)).unwrap_or(0)) / 100.0;

        Ok(total_percentage)
    }
}

impl ValidatorParams {
    ///
    /// # Errors
    ///
    /// Returns an error if the genesis.json file cannot be read or parsed.
    #[allow(clippy::too_many_lines)]
    pub fn from_genesis_json() -> Result<Self> {
        let file = File::open("genesis.json").map_err(|e| {
            tracing::error!("Failed to open genesis.json: {}", e);
            anyhow::anyhow!("Failed to open genesis.json: {}", e)
        })?;

        let mut contents = String::new();
        file.take(10_000_000)
            .read_to_string(&mut contents)
            .map_err(|e| {
                tracing::error!("Failed to read genesis.json: {}", e);
                anyhow::anyhow!("Failed to read genesis.json: {}", e)
            })?;

        let genesis: Value = serde_json::from_str(&contents).map_err(|e| {
            tracing::error!("Failed to parse genesis.json: {}", e);
            anyhow::anyhow!("Failed to parse genesis.json: {}", e)
        })?;

        let chain_id = if let Some(id) = genesis.get("chain_id").and_then(|v| v.as_str()) {
            id.to_string()
        } else if let Some(id) = genesis
            .get("app_state")
            .and_then(|app| app.get("genesisContent"))
            .and_then(|content| content.get("chainId"))
            .and_then(|id| id.as_str())
        {
            id.to_string()
        } else {
            tracing::error!("Failed to find chain_id in genesis.json");
            return Err(anyhow::anyhow!("Missing chain_id in genesis.json"));
        };

        tracing::info!("Found chain_id in genesis.json: {}", chain_id);

        let Some(stake_params) = genesis
            .get("app_state")
            .and_then(|app| app.get("genesisContent"))
            .and_then(|content| content.get("stakeContent"))
            .and_then(|stake| stake.get("stakeParams"))
        else {
            tracing::error!("Failed to find stakeParams in genesis.json");
            return Err(anyhow::anyhow!("Missing stakeParams in genesis.json"));
        };

        let Some(Ok(active_validator_limit)) = stake_params
            .get("activeValidatorLimit")
            .and_then(|limit| limit.as_str())
            .map(str::parse::<i64>)
        else {
            tracing::error!("Failed to parse activeValidatorLimit in genesis.json");
            return Err(anyhow::anyhow!(
                "Missing or invalid activeValidatorLimit in genesis.json"
            ));
        };

        let min_validator_stake = if let Some(Ok(raw_val)) = stake_params
            .get("minValidatorStake")
            .and_then(|stake| stake.get("lo"))
            .and_then(|lo| lo.as_str())
            .map(str::parse::<i64>)
        {
            format!("{} UM", raw_val / 1_000_000)
        } else {
            tracing::error!("Failed to parse minValidatorStake.lo in genesis.json");
            return Err(anyhow::anyhow!(
                "Missing or invalid minValidatorStake.lo in genesis.json"
            ));
        };

        let total_staked = String::new();

        let Some(Ok(uptime_blocks_window)) = stake_params
            .get("signedBlocksWindowLen")
            .and_then(|window| window.as_str())
            .map(str::parse::<i64>)
        else {
            tracing::error!("Failed to parse signedBlocksWindowLen in genesis.json");
            return Err(anyhow::anyhow!(
                "Missing or invalid signedBlocksWindowLen in genesis.json"
            ));
        };

        let uptime_min_required = if let Some(Ok(missed_max)) = stake_params
            .get("missedBlocksMaximum")
            .and_then(|max| max.as_str())
            .map(str::parse::<i64>)
        {
            let min_percent = 100.0
                * f64::from(i32::try_from(uptime_blocks_window - missed_max).unwrap_or(0))
                / f64::from(i32::try_from(uptime_blocks_window).unwrap_or(1));
            format!("{min_percent:.2}%")
        } else {
            tracing::error!("Failed to parse missedBlocksMaximum in genesis.json");
            return Err(anyhow::anyhow!(
                "Missing or invalid missedBlocksMaximum in genesis.json"
            ));
        };

        let slashing_penalty_downtime = if let Some(Ok(penalty)) = stake_params
            .get("slashingPenaltyDowntime")
            .and_then(|penalty| penalty.as_str())
            .map(str::parse::<i64>)
        {
            let penalty_float = f64::from(i32::try_from(penalty).unwrap_or(0)) / 1_000_000.0;
            format!("{penalty_float:.2}%")
        } else {
            tracing::warn!("slashingPenaltyDowntime not found in genesis.json");
            String::new()
        };

        let slashing_penalty_misbehavior = if let Some(Ok(penalty)) = stake_params
            .get("slashingPenaltyMisbehavior")
            .and_then(|penalty| penalty.as_str())
            .map(str::parse::<i64>)
        {
            let penalty_float = f64::from(i32::try_from(penalty).unwrap_or(0)) / 1_000_000.0;
            format!("{penalty_float:.2}%")
        } else {
            tracing::error!("Failed to parse slashingPenaltyMisbehavior in genesis.json");
            return Err(anyhow::anyhow!(
                "Missing or invalid slashingPenaltyMisbehavior in genesis.json"
            ));
        };

        let Some(delay) = stake_params
            .get("unbondingDelay")
            .and_then(|delay| delay.as_str())
        else {
            tracing::error!("Failed to find unbondingDelay in genesis.json");
            return Err(anyhow::anyhow!("Missing unbondingDelay in genesis.json"));
        };
        let unbonding_delay = format!("{delay} blocks");

        Ok(Self {
            chain_id,
            active_validator_limit,
            min_validator_stake,
            total_staked,
            uptime_blocks_window,
            uptime_min_required,
            slashing_penalty_downtime,
            slashing_penalty_misbehavior,
            unbonding_delay,
        })
    }

    /// Initialize the validator staking parameters table
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn initialize_table(&self, dbtx: &mut PgTransaction<'_>) -> Result<()> {
        sqlx::query(
            r"
            INSERT INTO validator_staking_parameters (
                chain_id, 
                active_validator_limit, 
                min_validator_stake, 
                total_staked, 
                uptime_blocks_window, 
                uptime_min_required,
                slashing_penalty_downtime,
                slashing_penalty_misbehavior,
                unbonding_delay
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (chain_id) DO UPDATE SET
                active_validator_limit = EXCLUDED.active_validator_limit,
                min_validator_stake = EXCLUDED.min_validator_stake,
                total_staked = EXCLUDED.total_staked,
                uptime_blocks_window = EXCLUDED.uptime_blocks_window,
                uptime_min_required = EXCLUDED.uptime_min_required,
                slashing_penalty_downtime = EXCLUDED.slashing_penalty_downtime,
                slashing_penalty_misbehavior = EXCLUDED.slashing_penalty_misbehavior,
                unbonding_delay = EXCLUDED.unbonding_delay
            ",
        )
        .bind(&self.chain_id)
        .bind(self.active_validator_limit)
        .bind(&self.min_validator_stake)
        .bind(&self.total_staked)
        .bind(self.uptime_blocks_window)
        .bind(&self.uptime_min_required)
        .bind(&self.slashing_penalty_downtime)
        .bind(&self.slashing_penalty_misbehavior)
        .bind(&self.unbonding_delay)
        .execute(dbtx.as_mut())
        .await?;

        Ok(())
    }

    /// Helper function to find an attribute value in an event
    #[must_use]
    pub fn find_attribute_value<'a>(
        event: &'a ContextualizedEvent<'_>,
        key: &str,
    ) -> Option<&'a str> {
        for attr in &event.event.attributes {
            if let Ok(attr_key) = attr.key_str() {
                if attr_key == key {
                    if let Ok(attr_value) = attr.value_str() {
                        return Some(attr_value);
                    }
                }
            }
        }
        None
    }

    /// Process validator parameter changes from `EventAppParametersChange` events
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    #[allow(clippy::too_many_lines)]
    pub async fn process_events(
        dbtx: &mut PgTransaction<'_>,
        events: &[ContextualizedEvent<'_>],
        height: u64,
        _timestamp: DateTime<Utc>,
    ) -> Result<()> {
        let table_exists: i64 = match sqlx::query_scalar(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'validator_staking_parameters'"
        )
        .fetch_one(dbtx.as_mut())
        .await {
            Ok(count) => count,
            Err(e) => {
                error!("Failed to check if validator_staking_parameters table exists: {}", e);
                return Ok(());
            }
        };

        if table_exists == 0 {
            debug!(
                "validator_staking_parameters table does not exist yet, skipping parameters update"
            );
            return Ok(());
        }

        for event in events {
            if event.event.kind == "penumbra.core.app.v1.EventAppParametersChange" {
                debug!("Found EventAppParametersChange event at height {}", height);

                match Self::find_attribute_value(event, "newParameters") {
                    Some(params_json) => {
                        debug!("Processing parameter changes: {}", params_json);

                        let params: Value = match serde_json::from_str(params_json) {
                            Ok(p) => p,
                            Err(e) => {
                                error!("Failed to parse parameter JSON: {}", e);
                                continue;
                            }
                        };

                        let Some(id) = params.get("chainId").and_then(|id| id.as_str()) else {
                            error!("Could not find chainId in EventAppParametersChange");
                            continue;
                        };
                        let chain_id = id.to_string();

                        if let Some(stake_params) = params.get("stakeParams") {
                            let mut updates = Vec::new();
                            let mut bindings = Vec::new();

                            let has_stake_params =
                                stake_params.get("activeValidatorLimit").is_some()
                                    || stake_params.get("minValidatorStake").is_some()
                                    || stake_params.get("missedBlocksMaximum").is_some()
                                    || stake_params.get("signedBlocksWindowLen").is_some()
                                    || stake_params.get("slashingPenaltyDowntime").is_some()
                                    || stake_params.get("slashingPenaltyMisbehavior").is_some()
                                    || stake_params.get("unbondingDelay").is_some();

                            if !has_stake_params {
                                debug!("No stake parameters in EventAppParametersChange, skipping");
                                continue;
                            }

                            if let Some(val) = stake_params
                                .get("activeValidatorLimit")
                                .and_then(|v| v.as_str())
                            {
                                match val.parse::<i64>() {
                                    Ok(limit) => {
                                        updates.push("active_validator_limit = $1");
                                        bindings.push(limit.to_string());
                                    }
                                    Err(e) => {
                                        error!(
                                            "Failed to parse activeValidatorLimit '{}': {}",
                                            val, e
                                        );
                                    }
                                }
                            }

                            if let Some(stake) = stake_params.get("minValidatorStake") {
                                if let Some(lo) = stake.get("lo").and_then(|v| v.as_str()) {
                                    match lo.parse::<i64>() {
                                        Ok(raw_val) => {
                                            let formatted = format!("{} UM", raw_val / 1_000_000);
                                            updates.push("min_validator_stake = $2");
                                            bindings.push(formatted);
                                        }
                                        Err(e) => {
                                            error!(
                                                "Failed to parse minValidatorStake.lo '{}': {}",
                                                lo, e
                                            );
                                        }
                                    }
                                }
                            }

                            if let Some(val) = stake_params
                                .get("signedBlocksWindowLen")
                                .and_then(|v| v.as_str())
                            {
                                match val.parse::<i64>() {
                                    Ok(window) => {
                                        updates.push("uptime_blocks_window = $3");
                                        bindings.push(window.to_string());

                                        if let Some(missed) = stake_params
                                            .get("missedBlocksMaximum")
                                            .and_then(|v| v.as_str())
                                        {
                                            match missed.parse::<i64>() {
                                                Ok(missed_max) => {
                                                    let min_percent = 100.0
                                                        * f64::from(
                                                            i32::try_from(window - missed_max)
                                                                .unwrap_or(0),
                                                        )
                                                        / f64::from(
                                                            i32::try_from(window).unwrap_or(1),
                                                        );
                                                    let formatted = format!("{min_percent:.2}%");
                                                    updates.push("uptime_min_required = $4");
                                                    bindings.push(formatted);
                                                }
                                                Err(e) => {
                                                    error!("Failed to parse missedBlocksMaximum '{}': {}", missed, e);
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!(
                                            "Failed to parse signedBlocksWindowLen '{}': {}",
                                            val, e
                                        );
                                    }
                                }
                            }

                            if let Some(val) = stake_params
                                .get("slashingPenaltyDowntime")
                                .and_then(|v| v.as_str())
                            {
                                match val.parse::<i64>() {
                                    Ok(penalty) => {
                                        let penalty_float =
                                            f64::from(i32::try_from(penalty).unwrap_or(0))
                                                / 1_000_000.0;
                                        let formatted = format!("{penalty_float:.2}%");
                                        debug!(
                                            "Parsed slashingPenaltyDowntime '{}' as '{}'",
                                            val, formatted
                                        );
                                        updates.push("slashing_penalty_downtime = $5");
                                        bindings.push(formatted);
                                    }
                                    Err(e) => {
                                        error!(
                                            "Failed to parse slashingPenaltyDowntime '{}': {}",
                                            val, e
                                        );
                                    }
                                }
                            }

                            if let Some(val) = stake_params
                                .get("slashingPenaltyMisbehavior")
                                .and_then(|v| v.as_str())
                            {
                                match val.parse::<i64>() {
                                    Ok(penalty) => {
                                        let penalty_float =
                                            f64::from(i32::try_from(penalty).unwrap_or(0))
                                                / 1_000_000.0;
                                        let formatted = format!("{penalty_float:.2}%");
                                        updates.push("slashing_penalty_misbehavior = $6");
                                        bindings.push(formatted);
                                    }
                                    Err(e) => {
                                        error!(
                                            "Failed to parse slashingPenaltyMisbehavior '{}': {}",
                                            val, e
                                        );
                                    }
                                }
                            }

                            if let Some(val) =
                                stake_params.get("unbondingDelay").and_then(|v| v.as_str())
                            {
                                let formatted = format!("{val} blocks");
                                updates.push("unbonding_delay = $7");
                                bindings.push(formatted);
                            }

                            if !updates.is_empty() {
                                let chain_exists: i64 = match sqlx::query_scalar(
                                    "SELECT COUNT(*) FROM validator_staking_parameters WHERE chain_id = $1"
                                )
                                .bind(&chain_id)
                                .fetch_one(dbtx.as_mut())
                                .await {
                                    Ok(count) => count,
                                    Err(e) => {
                                        error!("Failed to check if chain_id exists: {}", e);
                                        continue;
                                    }
                                };

                                if chain_exists == 0 {
                                    info!("Chain ID '{}' does not exist in validator_staking_parameters, skipping update", chain_id);
                                    continue;
                                }

                                let update_str = updates.join(", ");
                                let query = format!(
                                    "UPDATE validator_staking_parameters SET {update_str} WHERE chain_id = $8"
                                );

                                info!(
                                    "Updating validator staking parameters for chain_id = {}",
                                    chain_id
                                );

                                let mut q = sqlx::query(&query);

                                for (i, val) in bindings.iter().enumerate() {
                                    match i {
                                        0 => {
                                            if updates.contains(&"active_validator_limit = $1") {
                                                match val.parse::<i64>() {
                                                    Ok(v) => q = q.bind(v),
                                                    Err(e) => {
                                                        error!("Failed to bind active_validator_limit '{}': {}", val, e);
                                                        continue;
                                                    }
                                                }
                                            }
                                        }
                                        1 => {
                                            if updates.contains(&"min_validator_stake = $2") {
                                                q = q.bind(val);
                                            }
                                        }
                                        2 => {
                                            if updates.contains(&"uptime_blocks_window = $3") {
                                                match val.parse::<i64>() {
                                                    Ok(v) => q = q.bind(v),
                                                    Err(e) => {
                                                        error!("Failed to bind uptime_blocks_window '{}': {}", val, e);
                                                        continue;
                                                    }
                                                }
                                            }
                                        }
                                        3 => {
                                            if updates.contains(&"uptime_min_required = $4") {
                                                q = q.bind(val);
                                            }
                                        }
                                        4 => {
                                            if updates.contains(&"slashing_penalty_downtime = $5") {
                                                q = q.bind(val);
                                            }
                                        }
                                        5 => {
                                            if updates
                                                .contains(&"slashing_penalty_misbehavior = $6")
                                            {
                                                q = q.bind(val);
                                            }
                                        }
                                        6 => {
                                            if updates.contains(&"unbonding_delay = $7") {
                                                q = q.bind(val);
                                            }
                                        }
                                        _ => {}
                                    }
                                }

                                q = q.bind(&chain_id);

                                match q.execute(dbtx.as_mut()).await {
                                    Ok(result) => {
                                        info!(
                                            "Updated {} validator staking parameters for chain_id = {}",
                                            result.rows_affected(), chain_id
                                        );
                                    }
                                    Err(e) => {
                                        error!(
                                            "Failed to update validator staking parameters: {}",
                                            e
                                        );
                                    }
                                }
                            }
                        }
                    }
                    None => {
                        error!("EventAppParametersChange missing newParameters attribute");
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct ChainParameters {
    pub chain_id: String,
    pub current_block_height: i64,
    pub current_block_time: DateTime<Utc>,
    pub current_epoch: i64,
    pub epoch_duration: i64,
    pub next_epoch_in: i64,
    pub last_updated: DateTime<Utc>,
}

impl ChainParameters {
    /// Simple update of current block info and read latest epoch from epochs table
    ///
    /// # Errors
    ///
    /// Returns an error if database operations fail
    pub async fn update_basic_chain_info(
        dbtx: &mut PgTransaction<'_>,
        chain_id: &str,
        block_height: u64,
        block_timestamp: DateTime<Utc>,
    ) -> Result<()> {
        let height_i64 = i64::try_from(block_height).unwrap_or(i64::MAX);

        // Get the latest epoch from epochs table
        let latest_epoch: Option<i64> = sqlx::query_scalar(
            "SELECT epoch_index FROM epochs WHERE chain_id = $1 ORDER BY epoch_index DESC LIMIT 1",
        )
        .bind(chain_id)
        .fetch_optional(dbtx.as_mut())
        .await?;

        let current_epoch = latest_epoch.unwrap_or(0);

        // Get existing epoch_duration if available
        let existing_epoch_duration: Option<i64> = sqlx::query_scalar(
            "SELECT epoch_duration FROM validator_chain_parameters WHERE chain_id = $1",
        )
        .bind(chain_id)
        .fetch_optional(dbtx.as_mut())
        .await?;

        let epoch_duration = existing_epoch_duration.unwrap_or(0);

        let next_epoch_in = if epoch_duration > 0 && current_epoch > 0 {
            let current_epoch_start: Option<i64> = sqlx::query_scalar(
                "SELECT start_height FROM epochs WHERE epoch_index = $1 AND chain_id = $2",
            )
            .bind(current_epoch)
            .bind(chain_id)
            .fetch_optional(dbtx.as_mut())
            .await?;

            if let Some(start_height) = current_epoch_start {
                let next_epoch_start = start_height + epoch_duration;
                std::cmp::max(0, next_epoch_start - height_i64)
            } else {
                0
            }
        } else {
            0
        };

        sqlx::query(
            r"
            INSERT INTO validator_chain_parameters (
                chain_id,
                current_block_height,
                current_block_time,
                current_epoch,
                epoch_duration,
                next_epoch_in,
                last_updated
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (chain_id) DO UPDATE SET
                current_block_height = EXCLUDED.current_block_height,
                current_block_time = EXCLUDED.current_block_time,
                current_epoch = EXCLUDED.current_epoch,
                next_epoch_in = EXCLUDED.next_epoch_in,
                last_updated = EXCLUDED.last_updated
            ",
        )
        .bind(chain_id)
        .bind(height_i64)
        .bind(block_timestamp)
        .bind(current_epoch)
        .bind(epoch_duration)
        .bind(next_epoch_in)
        .bind(block_timestamp)
        .execute(dbtx.as_mut())
        .await?;

        Ok(())
    }

    /// Process events to update chain parameters
    ///
    /// # Errors
    ///
    /// Returns an error if database operations fail
    pub async fn process_events(
        dbtx: &mut PgTransaction<'_>,
        events: &[ContextualizedEvent<'_>],
        _height: u64,
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        for event in events {
            if event.event.kind == "penumbra.core.app.v1.EventAppParametersChange" {
                match ValidatorParams::find_attribute_value(event, "newParameters") {
                    Some(params_json) => match serde_json::from_str::<Value>(params_json) {
                        Ok(params) => {
                            if let Some(chain_id) = params.get("chainId").and_then(|id| id.as_str())
                            {
                                if let Some(sct_params) = params.get("sctParams") {
                                    if let Some(epoch_duration_str) =
                                        sct_params.get("epochDuration").and_then(|d| d.as_str())
                                    {
                                        match epoch_duration_str.parse::<i64>() {
                                            Ok(epoch_duration) => {
                                                tracing::info!(
                                                        "Updating epoch duration for chain {} to {} blocks",
                                                        chain_id,
                                                        epoch_duration
                                                    );

                                                sqlx::query(
                                                        "UPDATE validator_chain_parameters SET epoch_duration = $1, last_updated = $2 WHERE chain_id = $3"
                                                    )
                                                    .bind(epoch_duration)
                                                    .bind(timestamp)
                                                    .bind(chain_id)
                                                    .execute(dbtx.as_mut())
                                                    .await?;
                                            }
                                            Err(e) => {
                                                tracing::error!(
                                                    "Failed to parse epochDuration '{}': {}",
                                                    epoch_duration_str,
                                                    e
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!("Failed to parse EventAppParametersChange JSON: {}", e);
                        }
                    },
                    None => {
                        tracing::error!("EventAppParametersChange missing newParameters attribute");
                    }
                }
            }
        }

        Ok(())
    }

    /// Update current block info with latest height and timestamp
    ///
    /// # Errors
    ///
    /// Returns an error if database operations fail
    ///
    /// # Panics
    ///
    /// Panics if the `height_to_timestamp` map is not empty but has no maximum entry
    pub async fn update_current_block_info(
        dbtx: &mut PgTransaction<'_>,
        chain_id: &str,
        height_to_timestamp: &std::collections::HashMap<u64, DateTime<Utc>>,
    ) -> Result<()> {
        if height_to_timestamp.is_empty() {
            return Ok(());
        }

        let (latest_height, latest_timestamp) = height_to_timestamp
            .iter()
            .max_by_key(|(height, _)| *height)
            .map(|(h, t)| (*h, *t))
            .unwrap();

        let latest_epoch: Option<i64> = sqlx::query_scalar(
            "SELECT epoch_index FROM epochs WHERE chain_id = $1 ORDER BY epoch_index DESC LIMIT 1",
        )
        .bind(chain_id)
        .fetch_optional(dbtx.as_mut())
        .await?;

        let current_epoch = latest_epoch.unwrap_or(0);

        let latest_epoch_start_height: Option<i64> = sqlx::query_scalar(
            "SELECT start_height FROM epochs WHERE epoch_index = $1 AND chain_id = $2",
        )
        .bind(current_epoch)
        .bind(chain_id)
        .fetch_optional(dbtx.as_mut())
        .await?;

        let epoch_duration: i64 = sqlx::query_scalar(
            "SELECT epoch_duration FROM validator_chain_parameters WHERE chain_id = $1",
        )
        .bind(chain_id)
        .fetch_optional(dbtx.as_mut())
        .await?
        .unwrap_or(34560);

        let next_epoch_in = if let Some(epoch_start) = latest_epoch_start_height {
            let next_epoch_start = epoch_start + epoch_duration;
            std::cmp::max(
                0,
                next_epoch_start - i64::try_from(latest_height).unwrap_or(i64::MAX),
            )
        } else {
            epoch_duration
        };

        sqlx::query(
            r"
            INSERT INTO validator_chain_parameters (
                chain_id,
                current_block_height,
                current_block_time,
                current_epoch,
                epoch_duration,
                next_epoch_in,
                last_updated
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (chain_id) DO UPDATE SET
                current_block_height = EXCLUDED.current_block_height,
                current_block_time = EXCLUDED.current_block_time,
                current_epoch = EXCLUDED.current_epoch,
                next_epoch_in = EXCLUDED.next_epoch_in,
                last_updated = EXCLUDED.last_updated
            ",
        )
        .bind(chain_id)
        .bind(i64::try_from(latest_height).unwrap_or(i64::MAX))
        .bind(latest_timestamp)
        .bind(current_epoch)
        .bind(epoch_duration)
        .bind(next_epoch_in)
        .bind(latest_timestamp)
        .execute(dbtx.as_mut())
        .await?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct Epoch {
    pub epoch_index: i64,
    pub chain_id: String,
    pub start_height: i64,
    pub start_time: DateTime<Utc>,
    pub epoch_root: Vec<u8>,
}

impl Epoch {
    /// Process events to extract epoch information
    ///
    /// # Errors
    ///
    /// Returns an error if database operations fail
    pub async fn process_events(
        dbtx: &mut PgTransaction<'_>,
        events: &[ContextualizedEvent<'_>],
        height: u64,
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        for event in events {
            if event.event.kind == "penumbra.core.component.sct.v1.EventEpochRoot" {
                tracing::info!("Found EventEpochRoot at height {}", height);
                let index_str = ValidatorParams::find_attribute_value(event, "index");
                let root_str = ValidatorParams::find_attribute_value(event, "root");

                match (index_str, root_str) {
                    (Some(index_json), Some(root_json)) => {
                        match (
                            serde_json::from_str::<Value>(index_json),
                            serde_json::from_str::<Value>(root_json),
                        ) {
                            (Ok(index_val), Ok(root_val)) => {
                                if let (Some(epoch_index), Some(root_inner)) = (
                                    index_val.as_str().and_then(|s| s.parse::<i64>().ok()),
                                    root_val.get("inner").and_then(|r| r.as_str()),
                                ) {
                                    match general_purpose::STANDARD.decode(root_inner) {
                                        Ok(epoch_root) => {
                                            let chain_id = sqlx::query_scalar::<_, String>(
                                                "SELECT chain_id FROM explorer_block_details WHERE height = $1 LIMIT 1"
                                            )
                                            .bind(i64::try_from(height).unwrap_or(i64::MAX))
                                            .fetch_optional(dbtx.as_mut())
                                            .await?
                                            .unwrap_or_else(|| "unknown".to_string());

                                            tracing::info!(
                                                "Processing epoch {} at height {} for chain {}",
                                                epoch_index,
                                                height,
                                                chain_id
                                            );

                                            sqlx::query(
                                                r"
                                                INSERT INTO epochs (
                                                    epoch_index,
                                                    chain_id,
                                                    start_height,
                                                    start_time,
                                                    epoch_root
                                                ) VALUES ($1, $2, $3, $4, $5)
                                                ON CONFLICT (epoch_index) DO UPDATE SET
                                                    chain_id = EXCLUDED.chain_id,
                                                    start_height = EXCLUDED.start_height,
                                                    start_time = EXCLUDED.start_time,
                                                    epoch_root = EXCLUDED.epoch_root
                                                ",
                                            )
                                            .bind(epoch_index)
                                            .bind(&chain_id)
                                            .bind(i64::try_from(height).unwrap_or(i64::MAX))
                                            .bind(timestamp)
                                            .bind(&epoch_root)
                                            .execute(dbtx.as_mut())
                                            .await?;
                                        }
                                        Err(e) => {
                                            tracing::error!(
                                                "Failed to decode epoch root base64 '{}': {}",
                                                root_inner,
                                                e
                                            );
                                        }
                                    }
                                }
                            }
                            _ => {
                                tracing::error!("Failed to parse EventEpochRoot JSON data");
                            }
                        }
                    }
                    _ => {
                        tracing::error!("EventEpochRoot missing required attributes");
                    }
                }
            }
        }

        Ok(())
    }
}
