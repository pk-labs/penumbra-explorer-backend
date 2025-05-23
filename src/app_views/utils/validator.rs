use anyhow::Result;
use cometindex::ContextualizedEvent;
use serde_json::Value;
use sqlx::PgTransaction;
use std::fs::File;
use std::io::Read;
use std::collections::HashSet;
use tracing::{debug, info, error};
use sqlx::types::chrono::{DateTime, Utc};

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
        
        let validators: Vec<String> = sqlx::query_scalar(
            "SELECT identity_key FROM validators"
        )
        .fetch_all(dbtx.as_mut())
        .await
        .unwrap_or_default();
        
        self.existing_validators = validators.into_iter().collect();
        self.cache_loaded = true;
        debug!("Loaded {} existing validators into cache", self.existing_validators.len());
        
        Ok(())
    }
    
    /// Check if validator exists (uses cache after first load)
    async fn validator_exists(&mut self, identity_key: &str, dbtx: &mut PgTransaction<'_>) -> Result<bool> {
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
    async fn apply_all(&mut self, dbtx: &mut PgTransaction<'_>, timestamp: DateTime<Utc>) -> Result<()> {
        if self.changes.is_empty() {
            return Ok(());
        }
        
        debug!("Applying {} batched voting power changes", self.changes.len());
        
        for (identity_key, voting_power) in &self.changes {
            if let Err(e) = Validator::update_voting_power(
                identity_key, 
                *voting_power, 
                0.0, // Temporary percentage, will be recalculated
                dbtx, 
                timestamp
            ).await {
                error!("Failed to update voting power for {}: {}", identity_key, e);
            }
        }
        
        match Validator::calculate_total_voting_power(dbtx).await {
            Ok(total) => {
                if total > 0 {
                    if let Err(e) = Validator::update_all_voting_power_percentages(dbtx).await {
                        error!("Failed to update voting power percentages: {}", e);
                    }
                    
                    if let Err(e) = Validator::update_total_staked(dbtx).await {
                        error!("Failed to update total_staked parameter: {}", e);
                    }
                }
            },
            Err(e) => {
                error!("Failed to calculate total voting power: {}", e);
            }
        }
        
        self.changes.clear();
        
        Ok(())
    }
}

#[derive(Debug)]
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
    pub name: Option<String>,
    pub website: Option<String>,
    pub description: Option<String>,
    pub consensus_key: Option<String>,
    pub governance_key: Option<String>,
    pub state: String,
    pub bonding_state: Option<String>,
    pub voting_power: i64,
    pub voting_power_percentage: f64,
    pub first_seen_height: Option<i64>,
    pub first_seen_time: DateTime<Utc>,
    pub last_updated: DateTime<Utc>,
}

impl Validator {
    /// Helper function to find an attribute value in an event
    pub fn find_attribute_value<'a>(event: &'a ContextualizedEvent<'_>, key: &str) -> Option<&'a str> {
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
            "penumbra.core.component.stake.v1.EventDelegate" |
            "penumbra.core.component.stake.v1.EventUndelegate" |
            "penumbra.core.component.stake.v1.EventValidatorBondingStateChange" |
            "penumbra.core.component.stake.v1.EventValidatorMissedBlock" |
            "penumbra.core.component.stake.v1.EventValidatorStateChange" |
            "penumbra.core.component.stake.v1.EventValidatorVotingPowerChange" |
            "penumbra.core.component.stake.v1.EventRateDataChange" |
            "penumbra.core.component.stake.v1.EventSlashingPenaltyApplied" |
            "penumbra.core.component.stake.v1.EventTombstoneValidator" => {
                if let Some(identity_key_json) = Self::find_attribute_value(event, "identityKey") {
                    match serde_json::from_str::<Value>(identity_key_json) {
                        Ok(identity_data) => {
                            identity_data["ik"].as_str().map(String::from)
                        },
                        Err(_) => None
                    }
                } else {
                    None
                }
            },
            "penumbra.core.component.stake.v1.EventValidatorDefinitionUpload" => {
                if let Some(validator_json) = Self::find_attribute_value(event, "validator") {
                    match serde_json::from_str::<Value>(validator_json) {
                        Ok(validator_data) => {
                            validator_data["identityKey"]["ik"].as_str().map(String::from)
                        },
                        Err(_) => None
                    }
                } else {
                    None
                }
            },
            _ => None
        }
    }

    /// Link a transaction hash to a validator identity key
    pub async fn link_transaction_to_validator(
        tx_hash: &[u8],
        identity_key: &str,
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<()> {
        debug!("Linking transaction {:?} to validator {}", 
               crate::parsing::encode_to_hex(tx_hash.try_into().unwrap_or([0u8; 32])), 
               identity_key);
        
        match sqlx::query(
            r"
            UPDATE explorer_transactions 
            SET validator_identity_key = $1
            WHERE tx_hash = $2
            "
        )
        .bind(identity_key)
        .bind(tx_hash)
        .execute(dbtx.as_mut())
        .await {
            Ok(result) => {
                if result.rows_affected() > 0 {
                    debug!("Successfully linked transaction to validator {}", identity_key);
                } else {
                    debug!("Transaction not found in explorer_transactions table for validator {}", identity_key);
                }
                Ok(())
            },
            Err(e) => {
                error!("Failed to link transaction to validator {}: {}", identity_key, e);
                Ok(())
            }
        }
    }
    
    /// Parse a validator definition from an event
    pub fn from_event(
        event_json: &Value, 
        height: u64,
        timestamp: DateTime<Utc>,
        default_state: &str,
        default_bonding_state: &str,
        voting_power: i64,
        voting_power_percentage: f64,
    ) -> Result<Self> {
        let identity_key = event_json["identityKey"]["ik"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing or invalid identity key"))?
            .to_string();
        
        let name = event_json["name"].as_str().map(String::from);
        let website = event_json["website"].as_str().map(String::from);
        let description = event_json["description"].as_str().map(String::from);
        let consensus_key = event_json["consensusKey"].as_str().map(String::from);
        
        let governance_key = event_json.get("governanceKey")
            .and_then(|gk| gk.get("gk"))
            .and_then(|gk| gk.as_str())
            .map(String::from);

        let (first_seen_height, first_seen_time) = if default_state.contains("ACTIVE") && height == 1 {
            debug!("Creating genesis ACTIVE validator {} at height {}, timestamp {}",
                  identity_key, height, timestamp);
            (Some(height as i64), timestamp)
        } else if default_state.contains("DEFINED") {
            debug!("Creating DEFINED validator {} at height {}, timestamp {} - height will be set when ACTIVE",
                  identity_key, height, timestamp);
            (None, timestamp)
        } else if default_state.contains("ACTIVE") {
            debug!("Creating event ACTIVE validator {} at height {} - time should have been set when DEFINED",
                  identity_key, height);
            (Some(height as i64), DateTime::<Utc>::from_timestamp(0, 0).unwrap()) // Time placeholder
        } else {
            debug!("Creating validator {} with state {} - height will be NULL until ACTIVE",
                  identity_key, default_state);
            (None, DateTime::<Utc>::from_timestamp(0, 0).unwrap())
        };
        
        debug!("Created validator from event: identity_key={}, name={:?}, consensus_key={:?}", 
              identity_key, name, consensus_key);
        
        let bonding_state = if default_bonding_state.is_empty() {
            None
        } else {
            Some(default_bonding_state.to_string())
        };
        
        Ok(Self {
            identity_key,
            name,
            website,
            description,
            consensus_key,
            governance_key,
            state: default_state.to_string(),
            bonding_state,
            voting_power,
            voting_power_percentage,
            first_seen_height,
            first_seen_time,
            last_updated: timestamp,
        })
    }
    
    /// Insert only - fails if validator already exists (for ensure_validator_exists)
    pub async fn insert_only(&self, dbtx: &mut PgTransaction<'_>) -> Result<()> {
        sqlx::query(
            r"
            INSERT INTO validators (
                identity_key,
                name,
                website,
                description,
                consensus_key,
                governance_key,
                state,
                bonding_state,
                voting_power,
                voting_power_percentage,
                first_seen_height,
                first_seen_time,
                last_updated
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
            )
            ",
        )
        .bind(&self.identity_key)
        .bind(&self.name)
        .bind(&self.website)
        .bind(&self.description)
        .bind(&self.consensus_key)
        .bind(&self.governance_key)
        .bind(&self.state)
        .bind(&self.bonding_state)
        .bind(self.voting_power)
        .bind(self.voting_power_percentage)
        .bind(self.first_seen_height)
        .bind(self.first_seen_time)
        .bind(self.last_updated)
        .execute(dbtx.as_mut())
        .await?;
        
        Ok(())
    }
    
    /// Insert or update a validator in the database
    pub async fn insert_or_update(&self, dbtx: &mut PgTransaction<'_>) -> Result<()> {
        sqlx::query(
            r"
            INSERT INTO validators (
                identity_key,
                name,
                website,
                description,
                consensus_key,
                governance_key,
                state,
                bonding_state,
                voting_power,
                voting_power_percentage,
                first_seen_height,
                first_seen_time,
                last_updated
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
            )
            ON CONFLICT (identity_key) DO UPDATE SET
                name = EXCLUDED.name,
                website = EXCLUDED.website,
                description = EXCLUDED.description,
                consensus_key = EXCLUDED.consensus_key,
                governance_key = EXCLUDED.governance_key,
                state = EXCLUDED.state,
                bonding_state = EXCLUDED.bonding_state,
                voting_power = EXCLUDED.voting_power,
                voting_power_percentage = EXCLUDED.voting_power_percentage,
                last_updated = GREATEST(validators.last_updated, EXCLUDED.last_updated)
            ",
        )
        .bind(&self.identity_key)
        .bind(&self.name)
        .bind(&self.website)
        .bind(&self.description)
        .bind(&self.consensus_key)
        .bind(&self.governance_key)
        .bind(&self.state)
        .bind(&self.bonding_state)
        .bind(self.voting_power)
        .bind(self.voting_power_percentage)
        .bind(self.first_seen_height)
        .bind(self.first_seen_time)
        .bind(self.last_updated)
        .execute(dbtx.as_mut())
        .await?;
        
        Ok(())
    }
    
    /// Update only metadata (name, website, description, etc.) without changing state or voting power
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
    pub async fn update_state(
        identity_key: &str,
        state: &str,
        dbtx: &mut PgTransaction<'_>,
        timestamp: DateTime<Utc>,
        height: u64,
    ) -> Result<()> {
        if state.contains("DEFINED") {
            let validator_info: Option<(Option<String>,)> = sqlx::query_as(
                "SELECT state FROM validators WHERE identity_key = $1"
            )
            .bind(identity_key)
            .fetch_optional(dbtx.as_mut())
            .await?;
            
            match validator_info {
                Some((current_state,)) => {
                    if !current_state.map_or(false, |s| s.contains("DEFINED")) {
                        debug!("Validator {} transitioned to DEFINED state - setting first_seen_time", identity_key);
                        
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
                },
                None => {
                    debug!("Validator {} not found in database when updating to DEFINED state", identity_key);
                }
            }
        } else if state.contains("ACTIVE") {
            let validator_info: Option<(Option<String>, Option<i64>)> = sqlx::query_as(
                "SELECT state, first_seen_height FROM validators WHERE identity_key = $1"
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
                        .bind(height as i64)
                        .bind(identity_key)
                        .execute(dbtx.as_mut())
                        .await?;
                        
                        return Ok(());
                    }
                },
                None => {
                    debug!("Validator {} not found in database when updating to ACTIVE state", identity_key);
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
    pub async fn update_bonding_state(
        identity_key: &str,
        bonding_state: &str,
        dbtx: &mut PgTransaction<'_>,
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        // Only proceed if bonding_state is not empty
        if bonding_state.is_empty() {
            debug!("Empty bonding state received for validator {}, skipping update", identity_key);
            return Ok(());
        }
        
        debug!("Updating bonding state for validator {} to {}", identity_key, bonding_state);
        
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
    pub async fn update_voting_power(
        identity_key: &str,
        voting_power: i64,
        voting_power_percentage: f64,
        dbtx: &mut PgTransaction<'_>,
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            r"
            UPDATE validators 
            SET 
                voting_power = $1,
                voting_power_percentage = $2,
                last_updated = $3
            WHERE 
                identity_key = $4
            ",
        )
        .bind(voting_power)
        .bind(voting_power_percentage)
        .bind(timestamp)
        .bind(identity_key)
        .execute(dbtx.as_mut())
        .await?;
        
        Ok(())
    }
    
    /// Performance optimization: Bulk record block participation for multiple validators
    pub async fn record_validator_blocks_bulk(
        validator_records: &[(String, i64, DateTime<Utc>, bool)],
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<()> {
        if validator_records.is_empty() {
            return Ok(());
        }
        
        debug!("Bulk recording {} validator block records", validator_records.len());
        
        let mut values_clauses = Vec::new();
        
        for i in 0..validator_records.len() {
            let param_base = i * 4;
            values_clauses.push(format!("(${}, ${}, ${}, ${})", 
                param_base + 1, param_base + 2, param_base + 3, param_base + 4));
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
            sqlx_query = sqlx_query.bind(identity_key)
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
    pub async fn record_validator_block(
        identity_key: &str,
        block_height: i64,
        timestamp: DateTime<Utc>,
        signed: bool,
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<()> {
        // First check that both the validator and block exist
        let validator_exists: i64 = match sqlx::query_scalar(
            "SELECT COUNT(*) FROM validators WHERE identity_key = $1"
        )
        .bind(identity_key)
        .fetch_one(dbtx.as_mut())
        .await {
            Ok(count) => count,
            Err(e) => {
                error!("Failed to check if validator exists: {}", e);
                return Ok(());  // Return Ok to avoid aborting the transaction
            }
        };
        
        if validator_exists == 0 {
            debug!("Skipping validator_block record for non-existent validator: {}", identity_key);
            return Ok(());
        }
        
        let block_exists: i64 = match sqlx::query_scalar(
            "SELECT COUNT(*) FROM explorer_block_details WHERE height = $1"
        )
        .bind(block_height)
        .fetch_one(dbtx.as_mut())
        .await {
            Ok(count) => count,
            Err(e) => {
                error!("Failed to check if block exists: {}", e);
                return Ok(());  // Return Ok to avoid aborting the transaction
            }
        };
        
        if block_exists == 0 {
            debug!("Skipping validator_block record for non-existent block height: {}", block_height);
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
        .await {
            Ok(_) => Ok(()),
            Err(e) => {
                // Log error but don't propagate it to avoid aborting the transaction
                error!("Failed to record validator block: validator={}, block={}, error={}", 
                      identity_key, block_height, e);
                Ok(())
            }
        }
    }
    
    /// Calculate total voting power across ACTIVE validators only
    pub async fn calculate_total_voting_power(dbtx: &mut PgTransaction<'_>) -> Result<i64> {
        // Query the active state value first to avoid hardcoding it
        let active_state: Option<String> = sqlx::query_scalar(
            "SELECT DISTINCT state FROM validators WHERE state LIKE '%ACTIVE%' LIMIT 1"
        )
        .fetch_optional(dbtx.as_mut())
        .await?;
        
        // Use the queried active state or default to a query that returns 0
        let result = match active_state {
            Some(state) => {
                // Cast SUM to BIGINT to match our expected i64 type
                sqlx::query_scalar::<_, i64>(
                    &format!("SELECT COALESCE(SUM(voting_power)::BIGINT, 0) FROM validators WHERE state = '{}'", state)
                )
                .fetch_one(dbtx.as_mut())
                .await?
            },
            None => 0, // If no active validators found, return 0
        };
        
        Ok(result)
    }
    
    /// Update voting power percentages for ACTIVE validators only (set others to 0%)
    pub async fn update_all_voting_power_percentages(dbtx: &mut PgTransaction<'_>) -> Result<()> {
        // First get the total active voting power
        let total_voting_power = Self::calculate_total_voting_power(dbtx).await?;
        
        if total_voting_power == 0 {
            return Ok(());  // Skip if no total voting power
        }
        
        // Query the active state value first to avoid hardcoding it
        let active_state: Option<String> = sqlx::query_scalar(
            "SELECT DISTINCT state FROM validators WHERE state LIKE '%ACTIVE%' LIMIT 1"
        )
        .fetch_optional(dbtx.as_mut())
        .await?;
        
        // Only update if we found an active state
        if let Some(state) = active_state {
            // Update percentages for ACTIVE validators only
            let query = format!(
                r"
                UPDATE validators
                SET 
                    voting_power_percentage = (voting_power::float8 / $1::float8) * 100.0
                WHERE
                    state = '{}'
                ",
                state
            );
            
            sqlx::query(&query)
                .bind(total_voting_power)
                .execute(dbtx.as_mut())
                .await?;
            
            // Set percentage to 0 for all non-active validators
            let clear_inactive_query = format!(
                r"
                UPDATE validators
                SET 
                    voting_power_percentage = 0.0
                WHERE
                    state != '{}'
                ",
                state
            );
            
            sqlx::query(&clear_inactive_query)
                .execute(dbtx.as_mut())
                .await?;
        }
        
        Ok(())
    }
    
    /// Update total_staked in validator_staking_parameters with sum of ACTIVE validators only
    pub async fn update_total_staked(dbtx: &mut PgTransaction<'_>) -> Result<()> {
        // Calculate total voting power of ACTIVE validators only
        let total_active_voting_power = Self::calculate_total_voting_power(dbtx).await?;
        
        // Format total staked in UM format (already converted from microunits)
        let formatted_total = format!("{} UM", total_active_voting_power);
        
        // Get the chain_id 
        let chain_id: Option<String> = sqlx::query_scalar(
            "SELECT chain_id FROM validator_staking_parameters LIMIT 1"
        )
        .fetch_optional(dbtx.as_mut())
        .await?;
        
        // Only update if we have a chain_id
        if let Some(chain_id) = chain_id {
            sqlx::query(
                r"
                UPDATE validator_staking_parameters
                SET total_staked = $1
                WHERE chain_id = $2
                "
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
        // Check if validator exists using cache
        let validator_exists = match cache.validator_exists(identity_key, dbtx).await {
            Ok(exists) => exists,
            Err(e) => {
                error!("Failed to check if validator exists: {}", e);
                return Err(anyhow::anyhow!("Failed to check validator existence"));
            }
        };
        
        // Only create validator if it doesn't exist - never overwrite existing validators
        if !validator_exists {
            debug!("Creating new validator from event: {}", identity_key);
            
            // Determine state for new validator:
            // - If state is explicitly provided (from state change events), use it
            // - Otherwise, use UNSPECIFIED for validators discovered through non-state events
            let validator_state = match state {
                Some(s) if !s.is_empty() => {
                    debug!("Creating new validator {} with explicit state: {}", identity_key, s);
                    s
                },
                _ => {
                    debug!("Creating new validator {} with UNSPECIFIED state (discovered through non-state event)", identity_key);
                    "VALIDATOR_STATE_ENUM_UNSPECIFIED"
                }
            };
            
            // Create minimal validator with available data from the event
            match Self::from_event(
                &serde_json::json!({
                    "identityKey": {"ik": identity_key}
                }),
                height,
                timestamp,
                validator_state,
                bonding_state.unwrap_or(""), // Use bonding state from event or empty
                voting_power.unwrap_or(0), // Use voting power from event or 0
                0.0, // Percentage will be calculated later
            ) {
                Ok(validator) => {
                    // Use INSERT only (not insert_or_update) to avoid overwriting existing validators
                    if let Err(e) = validator.insert_only(dbtx).await {
                        error!("Failed to insert new validator: {}", e);
                        return Err(anyhow::anyhow!("Failed to insert validator"));
                    }
                    
                    // Add to cache after successful insertion
                    cache.add_validator(identity_key);
                },
                Err(e) => {
                    error!("Failed to create validator: {}", e);
                    return Err(anyhow::anyhow!("Failed to create validator"));
                }
            }
        } else {
            debug!("Validator {} already exists, skipping creation", identity_key);
        }
        
        Ok(())
    }

    /// Process validator-related events
    pub async fn process_events(
        dbtx: &mut PgTransaction<'_>,
        events: &[ContextualizedEvent<'_>],
        height: u64,
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        // Performance optimizations: cache and batch for this transaction
        let mut existence_cache = ValidatorExistenceCache::default();
        let mut voting_power_batch = VotingPowerBatch::default();
        
        // Collect transaction hash to validator identity key mappings
        let mut tx_validator_mappings: Vec<([u8; 32], String)> = Vec::new();
        
        // First pass: collect transaction-to-validator mappings from all validator events
        for event in events {
            // Check if this is a validator-related event and extract identity key
            if let Some(identity_key) = Self::extract_identity_key_from_event(event) {
                // If the event has a transaction hash, add mapping
                if let Some(tx_hash_bytes) = event.tx_hash() {
                    let tx_hash_array: [u8; 32] = tx_hash_bytes.try_into().unwrap_or([0u8; 32]);
                    tx_validator_mappings.push((tx_hash_array, identity_key.clone()));
                    debug!("Added transaction mapping: {} -> {}", 
                           crate::parsing::encode_to_hex(tx_hash_array), identity_key);
                }
            }
        }
        
        // Second pass: process validator definitions
        for event in events {
            if event.event.kind.as_str() == "penumbra.core.component.stake.v1.EventValidatorDefinitionUpload" {
                match Self::find_attribute_value(event, "validator") {
                    Some(validator_json) => {
                        debug!("Processing validator definition: {}", validator_json);
                        
                        match serde_json::from_str::<Value>(validator_json) {
                            Ok(validator_data) => {
                                // Extract identity key to check if validator exists
                                let identity_key = match validator_data["identityKey"]["ik"].as_str() {
                                    Some(key) => key,
                                    None => {
                                        error!("EventValidatorDefinitionUpload missing identity key");
                                        continue;
                                    }
                                };
                                
                                // Check if validator already exists using cache
                                let validator_exists = match existence_cache.validator_exists(identity_key, dbtx).await {
                                    Ok(exists) => exists,
                                    Err(e) => {
                                        error!("Failed to check if validator exists: {}", e);
                                        continue;
                                    }
                                };
                                
                                if validator_exists {
                                    // Validator already exists - only update metadata, never change state
                                    debug!("Updating metadata for existing validator: {}", identity_key);
                                    
                                    // Create validator object with metadata (dummy values for state/voting power)
                                    match Self::from_event(
                                        &validator_data,
                                        height,
                                        timestamp,
                                        "dummy", // This won't be used since we're only updating metadata
                                        "",      // This won't be used
                                        0,        // This won't be used
                                        0.0,      // This won't be used
                                    ) {
                                        Ok(validator) => {
                                            // Only update metadata, preserve existing state and voting power
                                            if let Err(e) = validator.update_metadata_only(dbtx).await {
                                                error!("Failed to update validator metadata: {}", e);
                                            }
                                        }
                                        Err(e) => {
                                            error!("Failed to create validator for metadata update: {}", e);
                                        }
                                    }
                                } else {
                                    // New validator - create with proper initial state
                                    debug!("Creating new validator from definition: {}", identity_key);
                                    
                                    // Extract actual state from event if present
                                    let state = validator_data.get("state")
                                        .and_then(|s| s.get("state"))
                                        .and_then(|s| s.as_str());
                                    
                                    // Extract bonding state from event if present
                                    let bonding_state = validator_data.get("bondingState")
                                        .and_then(|s| s.get("state"))
                                        .and_then(|s| s.as_str());
                                    
                                    // For new validators from definitions, use UNSPECIFIED only if no explicit state
                                    let default_state = match state {
                                        Some(s) if !s.is_empty() => s,
                                        _ => "VALIDATOR_STATE_ENUM_UNSPECIFIED", // Only for new validators without explicit state
                                    };
                                    
                                    match Self::from_event(
                                        &validator_data,
                                        height,
                                        timestamp,
                                        default_state,
                                        bonding_state.unwrap_or(""),
                                        0,         // initial voting power
                                        0.0,       // initial voting power percentage
                                    ) {
                                        Ok(validator) => {
                                            // Use insert_only to ensure we don't overwrite if it was created in the meantime
                                            if let Err(e) = validator.insert_only(dbtx).await {
                                                debug!("Validator {} was created concurrently, skipping: {}", identity_key, e);
                                            } else {
                                                // Add to cache after successful insertion
                                                existence_cache.add_validator(identity_key);
                                            }
                                        }
                                        Err(e) => {
                                            error!("Failed to parse new validator definition: {}", e);
                                        }
                                    }
                                }
                                
                                // Process funding streams if they exist (for both new and existing validators)
                                if let Some(funding_streams) = validator_data.get("fundingStreams") {
                                    if let Err(e) = ValidatorFundingStream::process_funding_streams(
                                        identity_key,
                                        funding_streams,
                                        timestamp,
                                        dbtx
                                    ).await {
                                        error!("Failed to process funding streams for validator {}: {}", identity_key, e);
                                    }
                                }
                            },
                            Err(e) => {
                                error!("Failed to parse validator JSON: {} - {}", validator_json, e);
                                // Continue processing other validators
                            }
                        }
                    },
                    None => {
                        error!("EventValidatorDefinitionUpload missing validator attribute");
                        // Continue processing other events
                    }
                }
            }
        }
        
        // Third pass: process validator states and voting power
        for event in events {
            match event.event.kind.as_str() {
                "penumbra.core.component.stake.v1.EventValidatorStateChange" => {
                    match (
                        Self::find_attribute_value(event, "identityKey"),
                        Self::find_attribute_value(event, "state"),
                    ) {
                        (Some(identity_key_json), Some(state_json)) => {
                            debug!("Processing validator state change: {}, {}", identity_key_json, state_json);
                            
                            let identity_key_result = serde_json::from_str::<Value>(identity_key_json);
                            let state_result = serde_json::from_str::<Value>(state_json);
                            
                            match (identity_key_result, state_result) {
                                (Ok(identity_data), Ok(state_data)) => {
                                    if let Some(identity_key) = identity_data["ik"].as_str() {
                                        if let Some(state) = state_data["state"].as_str() {
                                            // Ensure validator exists, creating if necessary
                                            if let Err(e) = Self::ensure_validator_exists(
                                                identity_key, 
                                                height, 
                                                timestamp, 
                                                dbtx, 
                                                Some(state), 
                                                None, 
                                                None,
                                                &mut existence_cache
                                            ).await {
                                                error!("Failed to ensure validator exists: {}", e);
                                                continue;
                                            }
                                            
                                            if let Err(e) = Self::update_state(identity_key, state, dbtx, timestamp, height).await {
                                                error!("Failed to update validator state: {}", e);
                                                // Continue processing other events
                                            }
                                        }
                                    }
                                },
                                _ => {
                                    error!("Failed to parse validator state change data");
                                    // Continue processing other events
                                }
                            }
                        },
                        _ => {
                            error!("EventValidatorStateChange missing required attributes");
                            // Continue processing other events
                        }
                    }
                },
                "penumbra.core.component.stake.v1.EventValidatorBondingStateChange" => {
                    match (
                        Self::find_attribute_value(event, "identityKey"),
                        Self::find_attribute_value(event, "bondingState"),
                    ) {
                        (Some(identity_key_json), Some(bonding_state_json)) => {
                            debug!("Processing validator bonding state change: {}, {}", identity_key_json, bonding_state_json);
                            
                            let identity_key_result = serde_json::from_str::<Value>(identity_key_json);
                            let bonding_state_result = serde_json::from_str::<Value>(bonding_state_json);
                            
                            match (identity_key_result, bonding_state_result) {
                                (Ok(identity_data), Ok(bonding_state_data)) => {
                                    if let Some(identity_key) = identity_data["ik"].as_str() {
                                        if let Some(bonding_state) = bonding_state_data["state"].as_str() {
                                            // Ensure validator exists, creating if necessary
                                            if let Err(e) = Self::ensure_validator_exists(
                                                identity_key, 
                                                height, 
                                                timestamp, 
                                                dbtx, 
                                                None, 
                                                Some(bonding_state), 
                                                None,
                                                &mut existence_cache
                                            ).await {
                                                error!("Failed to ensure validator exists: {}", e);
                                                continue;
                                            }
                                            
                                            if let Err(e) = Self::update_bonding_state(identity_key, bonding_state, dbtx, timestamp).await {
                                                error!("Failed to update validator bonding state: {}", e);
                                                // Continue processing other events
                                            }
                                        }
                                    }
                                },
                                _ => {
                                    error!("Failed to parse validator bonding state change data");
                                    // Continue processing other events
                                }
                            }
                        },
                        _ => {
                            error!("EventValidatorBondingStateChange missing required attributes");
                            // Continue processing other events
                        }
                    }
                },
                "penumbra.core.component.stake.v1.EventValidatorVotingPowerChange" => {
                    match (
                        Self::find_attribute_value(event, "identityKey"),
                        Self::find_attribute_value(event, "votingPower"),
                    ) {
                        (Some(identity_key_json), Some(voting_power_json)) => {
                            debug!("Processing validator voting power change: {}, {}", identity_key_json, voting_power_json);
                            
                            let identity_key_result = serde_json::from_str::<Value>(identity_key_json);
                            let voting_power_result = serde_json::from_str::<Value>(voting_power_json);
                            
                            match (identity_key_result, voting_power_result) {
                                (Ok(identity_data), Ok(voting_power_data)) => {
                                    if let Some(identity_key) = identity_data["ik"].as_str() {
                                        if let Some(voting_power_str) = voting_power_data["lo"].as_str() {
                                            match voting_power_str.parse::<i64>() {
                                                Ok(raw_voting_power) => {
                                                    // Raw voting power is in microunits (UM)
                                                    // Convert to human-readable format by dividing by 1,000,000
                                                    let voting_power = raw_voting_power / 1_000_000;
                                                    
                                                    // Ensure validator exists, creating if necessary
                                                    if let Err(e) = Self::ensure_validator_exists(
                                                        identity_key, 
                                                        height, 
                                                        timestamp, 
                                                        dbtx, 
                                                        None, 
                                                        None, 
                                                        Some(voting_power),
                                                        &mut existence_cache
                                                    ).await {
                                                        error!("Failed to ensure validator exists: {}", e);
                                                        continue;
                                                    }
                                                    
                                                    // Add to batch instead of immediate processing
                                                    voting_power_batch.add_change(identity_key.to_string(), voting_power);
                                                },
                                                Err(e) => {
                                                    error!("Failed to parse voting power '{}': {}", voting_power_str, e);
                                                    // Continue processing other events
                                                }
                                            }
                                        }
                                    }
                                },
                                _ => {
                                    error!("Failed to parse validator voting power change data");
                                    // Continue processing other events
                                }
                            }
                        },
                        _ => {
                            error!("EventValidatorVotingPowerChange missing required attributes");
                            // Continue processing other events
                        }
                    }
                },
                "penumbra.core.component.stake.v1.EventValidatorMissedBlock" => {
                    if let Some(identity_key_json) = Self::find_attribute_value(event, "identityKey") {
                        debug!("Processing validator missed block: {}", identity_key_json);
                        
                        match serde_json::from_str::<Value>(identity_key_json) {
                            Ok(identity_data) => {
                                if let Some(identity_key) = identity_data["ik"].as_str() {
                                    // Ensure validator exists, creating if necessary
                                    if let Err(e) = Self::ensure_validator_exists(
                                        identity_key, 
                                        height, 
                                        timestamp, 
                                        dbtx, 
                                        None, 
                                        None, 
                                        None,
                                        &mut existence_cache
                                    ).await {
                                        error!("Failed to ensure validator exists: {}", e);
                                        continue;
                                    }
                                    
                                    // Record missed block
                                    if let Err(e) = Self::record_validator_block(
                                        identity_key, 
                                        height as i64, 
                                        timestamp, 
                                        false, 
                                        dbtx
                                    ).await {
                                        error!("Failed to record missed block: {}", e);
                                        // Don't propagate error to avoid aborting the transaction
                                    }
                                }
                            },
                            Err(e) => {
                                error!("Failed to parse validator identity key '{}': {}", identity_key_json, e);
                                // Continue processing other events
                            }
                        }
                    }
                },
                "penumbra.core.component.stake.v1.EventDelegate" => {
                    if let Some(identity_key_json) = Self::find_attribute_value(event, "identityKey") {
                        debug!("Processing delegate event: {}", identity_key_json);
                        
                        match serde_json::from_str::<Value>(identity_key_json) {
                            Ok(identity_data) => {
                                if let Some(identity_key) = identity_data["ik"].as_str() {
                                    // Ensure validator exists, creating if necessary
                                    if let Err(e) = Self::ensure_validator_exists(
                                        identity_key, 
                                        height, 
                                        timestamp, 
                                        dbtx, 
                                        None, 
                                        None, 
                                        None,
                                        &mut existence_cache
                                    ).await {
                                        error!("Failed to ensure validator exists for delegate event: {}", e);
                                        // Continue processing other events
                                    }
                                }
                            },
                            Err(e) => {
                                error!("Failed to parse validator identity key '{}': {}", identity_key_json, e);
                                // Continue processing other events
                            }
                        }
                    }
                },
                "penumbra.core.component.stake.v1.EventUndelegate" => {
                    if let Some(identity_key_json) = Self::find_attribute_value(event, "identityKey") {
                        debug!("Processing undelegate event: {}", identity_key_json);
                        
                        match serde_json::from_str::<Value>(identity_key_json) {
                            Ok(identity_data) => {
                                if let Some(identity_key) = identity_data["ik"].as_str() {
                                    // Ensure validator exists, creating if necessary
                                    if let Err(e) = Self::ensure_validator_exists(
                                        identity_key, 
                                        height, 
                                        timestamp, 
                                        dbtx, 
                                        None, 
                                        None, 
                                        None,
                                        &mut existence_cache
                                    ).await {
                                        error!("Failed to ensure validator exists for undelegate event: {}", e);
                                        // Continue processing other events
                                    }
                                }
                            },
                            Err(e) => {
                                error!("Failed to parse validator identity key '{}': {}", identity_key_json, e);
                                // Continue processing other events
                            }
                        }
                    }
                },
                "penumbra.core.component.stake.v1.EventRateDataChange" => {
                    if let Some(identity_key_json) = Self::find_attribute_value(event, "identityKey") {
                        debug!("Processing rate data change event: {}", identity_key_json);
                        
                        match serde_json::from_str::<Value>(identity_key_json) {
                            Ok(identity_data) => {
                                if let Some(identity_key) = identity_data["ik"].as_str() {
                                    // Ensure validator exists, creating if necessary
                                    if let Err(e) = Self::ensure_validator_exists(
                                        identity_key, 
                                        height, 
                                        timestamp, 
                                        dbtx, 
                                        None, 
                                        None, 
                                        None,
                                        &mut existence_cache
                                    ).await {
                                        error!("Failed to ensure validator exists for rate data change event: {}", e);
                                        // Continue processing other events
                                    }
                                }
                            },
                            Err(e) => {
                                error!("Failed to parse validator identity key '{}': {}", identity_key_json, e);
                                // Continue processing other events
                            }
                        }
                    }
                },
                "penumbra.core.component.stake.v1.EventSlashingPenaltyApplied" => {
                    if let Some(identity_key_json) = Self::find_attribute_value(event, "identityKey") {
                        debug!("Processing slashing penalty applied event: {}", identity_key_json);
                        
                        match serde_json::from_str::<Value>(identity_key_json) {
                            Ok(identity_data) => {
                                if let Some(identity_key) = identity_data["ik"].as_str() {
                                    // Ensure validator exists, creating if necessary
                                    if let Err(e) = Self::ensure_validator_exists(
                                        identity_key, 
                                        height, 
                                        timestamp, 
                                        dbtx, 
                                        None, 
                                        None, 
                                        None,
                                        &mut existence_cache
                                    ).await {
                                        error!("Failed to ensure validator exists for slashing penalty event: {}", e);
                                        // Continue processing other events
                                    }
                                }
                            },
                            Err(e) => {
                                error!("Failed to parse validator identity key '{}': {}", identity_key_json, e);
                                // Continue processing other events
                            }
                        }
                    }
                },
                "penumbra.core.component.stake.v1.EventTombstoneValidator" => {
                    if let Some(identity_key_json) = Self::find_attribute_value(event, "identityKey") {
                        debug!("Processing tombstone validator event: {}", identity_key_json);
                        
                        match serde_json::from_str::<Value>(identity_key_json) {
                            Ok(identity_data) => {
                                if let Some(identity_key) = identity_data["ik"].as_str() {
                                    // Ensure validator exists, creating if necessary
                                    if let Err(e) = Self::ensure_validator_exists(
                                        identity_key, 
                                        height, 
                                        timestamp, 
                                        dbtx, 
                                        None, 
                                        None, 
                                        None,
                                        &mut existence_cache
                                    ).await {
                                        error!("Failed to ensure validator exists for tombstone event: {}", e);
                                        // Continue processing other events
                                    }
                                }
                            },
                            Err(e) => {
                                error!("Failed to parse validator identity key '{}': {}", identity_key_json, e);
                                // Continue processing other events
                            }
                        }
                    }
                },
                _ => {} // Ignore other event types
            }
        }
        
        // Apply batched voting power changes at the end (single recalculation)
        if let Err(e) = voting_power_batch.apply_all(dbtx, timestamp).await {
            error!("Failed to apply batched voting power changes: {}", e);
        }
        
        // Final step: link transactions to validators
        debug!("Processing {} transaction-to-validator mappings", tx_validator_mappings.len());
        for (tx_hash, identity_key) in tx_validator_mappings {
            if let Err(e) = Self::link_transaction_to_validator(&tx_hash, &identity_key, dbtx).await {
                error!("Failed to link transaction to validator {}: {}", identity_key, e);
                // Continue processing other mappings
            }
        }
        
        Ok(())
    }
    
}

impl ValidatorFundingStream {
    /// Create a new funding stream
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
    pub async fn process_funding_streams(
        identity_key: &str,
        funding_streams_json: &Value,
        timestamp: DateTime<Utc>,
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<()> {
        if let Some(funding_streams) = funding_streams_json.as_array() {
            for stream in funding_streams {
                // Handle toAddress type
                if let Some(to_address) = stream.get("toAddress") {
                    if let Some(address) = to_address.get("address").and_then(|a| a.as_str()) {
                        if let Some(rate_bps) = to_address.get("rateBps").and_then(|r| r.as_i64()) {
                            let funding_stream = Self::new(
                                identity_key.to_string(),
                                "toAddress".to_string(),
                                Some(address.to_string()),
                                rate_bps as i32,
                                timestamp,
                            );
                            
                            if let Err(e) = funding_stream.insert_or_update(dbtx).await {
                                error!("Failed to insert funding stream for validator {}: {}", identity_key, e);
                            }
                        }
                    }
                }
                
                // Handle toCommunityPool type
                if let Some(to_community_pool) = stream.get("toCommunityPool") {
                    if let Some(rate_bps) = to_community_pool.get("rateBps").and_then(|r| r.as_i64()) {
                        let funding_stream = Self::new(
                            identity_key.to_string(),
                            "toCommunityPool".to_string(),
                            None,
                            rate_bps as i32,
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
    pub async fn calculate_total_commission_rate(
        identity_key: &str,
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<f64> {
        let total_rate_bps: Option<i64> = sqlx::query_scalar(
            "SELECT SUM(rate_bps) FROM validator_funding_streams WHERE identity_key = $1"
        )
        .bind(identity_key)
        .fetch_optional(dbtx.as_mut())
        .await?;
        
        // Convert basis points to percentage (divide by 100)
        let total_percentage = total_rate_bps.unwrap_or(0) as f64 / 100.0;
        
        Ok(total_percentage)
    }
}

impl ValidatorParams {
    pub fn from_genesis_json() -> Result<Self> {
        let file = File::open("genesis.json")
            .map_err(|e| {
                tracing::error!("Failed to open genesis.json: {}", e);
                anyhow::anyhow!("Failed to open genesis.json: {}", e)
            })?;

        let mut contents = String::new();
        file.take(10_000_000).read_to_string(&mut contents)
            .map_err(|e| {
                tracing::error!("Failed to read genesis.json: {}", e);
                anyhow::anyhow!("Failed to read genesis.json: {}", e)
            })?;

        let genesis: Value = serde_json::from_str(&contents)
            .map_err(|e| {
                tracing::error!("Failed to parse genesis.json: {}", e);
                anyhow::anyhow!("Failed to parse genesis.json: {}", e)
            })?;
        
        // Try to get chain_id from different possible locations
        let chain_id = match genesis.get("chain_id").and_then(|v| v.as_str()) {
            Some(id) => id.to_string(),
            None => {
                // Try app_state.genesisContent.chainId
                match genesis.get("app_state")
                    .and_then(|app| app.get("genesisContent"))
                    .and_then(|content| content.get("chainId"))
                    .and_then(|id| id.as_str()) {
                        Some(id) => id.to_string(),
                        None => {
                            tracing::error!("Failed to find chain_id in genesis.json");
                            return Err(anyhow::anyhow!("Missing chain_id in genesis.json"));
                        }
                    }
            }
        };
        
        tracing::info!("Found chain_id in genesis.json: {}", chain_id);
        
        // Get stake params from the expected location
        let stake_params = match genesis.get("app_state")
            .and_then(|app| app.get("genesisContent"))
            .and_then(|content| content.get("stakeContent"))
            .and_then(|stake| stake.get("stakeParams")) {
                Some(params) => params,
                None => {
                    tracing::error!("Failed to find stakeParams in genesis.json");
                    return Err(anyhow::anyhow!("Missing stakeParams in genesis.json"));
                }
            };
        
        // Parse active validator limit
        let active_validator_limit = match stake_params.get("activeValidatorLimit")
            .and_then(|limit| limit.as_str())
            .map(|s| s.parse::<i64>()) {
                Some(Ok(limit)) => limit,
                _ => {
                    tracing::error!("Failed to parse activeValidatorLimit in genesis.json");
                    return Err(anyhow::anyhow!("Missing or invalid activeValidatorLimit in genesis.json"));
                }
            };
        
        // Parse minimum validator stake
        let min_validator_stake = match stake_params.get("minValidatorStake")
            .and_then(|stake| stake.get("lo"))
            .and_then(|lo| lo.as_str())
            .map(|s| s.parse::<i64>()) {
                Some(Ok(raw_val)) => format!("{} UM", raw_val / 1_000_000),
                _ => {
                    tracing::error!("Failed to parse minValidatorStake.lo in genesis.json");
                    return Err(anyhow::anyhow!("Missing or invalid minValidatorStake.lo in genesis.json"));
                }
            };

        // Total staked should be calculated from validators in genesis
        // This requires more complex logic, so we will return empty until it's updated by events
        let total_staked = "".to_string(); // Explicitly set to empty to indicate missing data
        
        // Parse uptime blocks window
        let uptime_blocks_window = match stake_params.get("signedBlocksWindowLen")
            .and_then(|window| window.as_str())
            .map(|s| s.parse::<i64>()) {
                Some(Ok(window)) => window,
                _ => {
                    tracing::error!("Failed to parse signedBlocksWindowLen in genesis.json");
                    return Err(anyhow::anyhow!("Missing or invalid signedBlocksWindowLen in genesis.json"));
                }
            };
        
        // Calculate uptime minimum required percentage
        let uptime_min_required = match stake_params.get("missedBlocksMaximum")
            .and_then(|max| max.as_str())
            .map(|s| s.parse::<i64>()) {
                Some(Ok(missed_max)) => {
                    let min_percent = 100.0 * (uptime_blocks_window - missed_max) as f64 / uptime_blocks_window as f64;
                    format!("{:.2}%", min_percent)
                },
                _ => {
                    tracing::error!("Failed to parse missedBlocksMaximum in genesis.json");
                    return Err(anyhow::anyhow!("Missing or invalid missedBlocksMaximum in genesis.json"));
                }
            };
        
        // Parse slashing penalty downtime
        let slashing_penalty_downtime = match stake_params.get("slashingPenaltyDowntime")
            .and_then(|penalty| penalty.as_str())
            .map(|s| s.parse::<i64>()) {
                Some(Ok(penalty)) => {
                    format!("{:.2}%", penalty as f64 / 1_000_000.0)
                },
                _ => {
                    // We'll use an empty string to indicate this is not provided
                    // This makes it explicit that the value is missing rather than defaulting to 0
                    tracing::warn!("slashingPenaltyDowntime not found in genesis.json");
                    "".to_string()
                }
            };
        
        // Parse slashing penalty misbehavior
        let slashing_penalty_misbehavior = match stake_params.get("slashingPenaltyMisbehavior")
            .and_then(|penalty| penalty.as_str())
            .map(|s| s.parse::<i64>()) {
                Some(Ok(penalty)) => {
                    format!("{:.2}%", penalty as f64 / 1_000_000.0)
                },
                _ => {
                    tracing::error!("Failed to parse slashingPenaltyMisbehavior in genesis.json");
                    return Err(anyhow::anyhow!("Missing or invalid slashingPenaltyMisbehavior in genesis.json"));
                }
            };
        
        // Parse unbonding delay
        let unbonding_delay = match stake_params.get("unbondingDelay")
            .and_then(|delay| delay.as_str()) {
                Some(delay) => format!("{} blocks", delay),
                None => {
                    tracing::error!("Failed to find unbondingDelay in genesis.json");
                    return Err(anyhow::anyhow!("Missing unbondingDelay in genesis.json"));
                }
            };
        
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
    pub fn find_attribute_value<'a>(event: &'a ContextualizedEvent<'_>, key: &str) -> Option<&'a str> {
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
    
    /// Process validator parameter changes from EventAppParametersChange events
    pub async fn process_events(
        dbtx: &mut PgTransaction<'_>,
        events: &[ContextualizedEvent<'_>],
        height: u64,
        _timestamp: DateTime<Utc>,
    ) -> Result<()> {
        // Check if validator_staking_parameters table exists
        let table_exists: i64 = match sqlx::query_scalar(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'validator_staking_parameters'"
        )
        .fetch_one(dbtx.as_mut())
        .await {
            Ok(count) => count,
            Err(e) => {
                error!("Failed to check if validator_staking_parameters table exists: {}", e);
                return Ok(());  // Return Ok to avoid aborting the transaction
            }
        };
        
        if table_exists == 0 {
            debug!("validator_staking_parameters table does not exist yet, skipping parameters update");
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
                        
                        let chain_id = match params.get("chainId").and_then(|id| id.as_str()) {
                            Some(id) => id.to_string(),
                            None => {
                                error!("Could not find chainId in EventAppParametersChange");
                                continue;
                            }
                        };
                        
                        if let Some(stake_params) = params.get("stakeParams") {
                            let mut updates = Vec::new();
                            let mut bindings = Vec::new();
                            
                            let has_stake_params = stake_params.get("activeValidatorLimit").is_some() ||
                                stake_params.get("minValidatorStake").is_some() ||
                                stake_params.get("missedBlocksMaximum").is_some() ||
                                stake_params.get("signedBlocksWindowLen").is_some() ||
                                stake_params.get("slashingPenaltyDowntime").is_some() ||
                                stake_params.get("slashingPenaltyMisbehavior").is_some() ||
                                stake_params.get("unbondingDelay").is_some();
                                
                            if !has_stake_params {
                                debug!("No stake parameters in EventAppParametersChange, skipping");
                                continue;
                            }
                            
                            // Extract active validator limit
                            if let Some(val) = stake_params.get("activeValidatorLimit").and_then(|v| v.as_str()) {
                                match val.parse::<i64>() {
                                    Ok(limit) => {
                                        updates.push("active_validator_limit = $1");
                                        bindings.push(limit.to_string());
                                    },
                                    Err(e) => {
                                        error!("Failed to parse activeValidatorLimit '{}': {}", val, e);
                                    }
                                }
                            }
                            
                            // Extract minimum validator stake
                            if let Some(stake) = stake_params.get("minValidatorStake") {
                                if let Some(lo) = stake.get("lo").and_then(|v| v.as_str()) {
                                    match lo.parse::<i64>() {
                                        Ok(raw_val) => {
                                            let formatted = format!("{} UM", raw_val / 1_000_000);
                                            updates.push("min_validator_stake = $2");
                                            bindings.push(formatted);
                                        },
                                        Err(e) => {
                                            error!("Failed to parse minValidatorStake.lo '{}': {}", lo, e);
                                        }
                                    }
                                }
                            }
                            
                            // Extract signed blocks window length and missed blocks maximum
                            if let Some(val) = stake_params.get("signedBlocksWindowLen").and_then(|v| v.as_str()) {
                                match val.parse::<i64>() {
                                    Ok(window) => {
                                        updates.push("uptime_blocks_window = $3");
                                        bindings.push(window.to_string());
                                        
                                        if let Some(missed) = stake_params.get("missedBlocksMaximum").and_then(|v| v.as_str()) {
                                            match missed.parse::<i64>() {
                                                Ok(missed_max) => {
                                                    let min_percent = 100.0 * (window - missed_max) as f64 / window as f64;
                                                    let formatted = format!("{:.2}%", min_percent);
                                                    updates.push("uptime_min_required = $4");
                                                    bindings.push(formatted);
                                                },
                                                Err(e) => {
                                                    error!("Failed to parse missedBlocksMaximum '{}': {}", missed, e);
                                                }
                                            }
                                        }
                                    },
                                    Err(e) => {
                                        error!("Failed to parse signedBlocksWindowLen '{}': {}", val, e);
                                    }
                                }
                            }
                            
                            // Extract slashing penalty downtime
                            if let Some(val) = stake_params.get("slashingPenaltyDowntime").and_then(|v| v.as_str()) {
                                match val.parse::<i64>() {
                                    Ok(penalty) => {
                                        let formatted = format!("{:.2}%", penalty as f64 / 1_000_000.0);
                                        debug!("Parsed slashingPenaltyDowntime '{}' as '{}'", val, formatted);
                                        updates.push("slashing_penalty_downtime = $5");
                                        bindings.push(formatted);
                                    },
                                    Err(e) => {
                                        error!("Failed to parse slashingPenaltyDowntime '{}': {}", val, e);
                                    }
                                }
                            }
                            
                            // Extract slashing penalty misbehavior
                            if let Some(val) = stake_params.get("slashingPenaltyMisbehavior").and_then(|v| v.as_str()) {
                                match val.parse::<i64>() {
                                    Ok(penalty) => {
                                        let formatted = format!("{:.2}%", penalty as f64 / 1_000_000.0);
                                        updates.push("slashing_penalty_misbehavior = $6");
                                        bindings.push(formatted);
                                    },
                                    Err(e) => {
                                        error!("Failed to parse slashingPenaltyMisbehavior '{}': {}", val, e);
                                    }
                                }
                            }
                            
                            // Extract unbonding delay
                            if let Some(val) = stake_params.get("unbondingDelay").and_then(|v| v.as_str()) {
                                let formatted = format!("{} blocks", val);
                                updates.push("unbonding_delay = $7");
                                bindings.push(formatted);
                            }
                            
                            // Only proceed if we have updates to make
                            if !updates.is_empty() {
                                // Check if the chain_id exists in the table
                                let chain_exists: i64 = match sqlx::query_scalar(
                                    "SELECT COUNT(*) FROM validator_staking_parameters WHERE chain_id = $1"
                                )
                                .bind(&chain_id)
                                .fetch_one(dbtx.as_mut())
                                .await {
                                    Ok(count) => count,
                                    Err(e) => {
                                        error!("Failed to check if chain_id exists: {}", e);
                                        continue;  // Skip this update
                                    }
                                };
                                
                                if chain_exists == 0 {
                                    info!("Chain ID '{}' does not exist in validator_staking_parameters, skipping update", chain_id);
                                    continue;  // Skip this update
                                }
                                
                                let update_str = updates.join(", ");
                                let query = format!(
                                    "UPDATE validator_staking_parameters SET {} WHERE chain_id = $8",
                                    update_str
                                );
                                
                                info!("Updating validator staking parameters for chain_id = {}", chain_id);
                                
                                let mut q = sqlx::query(&query);
                                
                                // Add parameter bindings in order
                                for (i, val) in bindings.iter().enumerate() {
                                    match i {
                                        0 => if updates.contains(&"active_validator_limit = $1") {
                                            match val.parse::<i64>() {
                                                Ok(v) => q = q.bind(v),
                                                Err(e) => {
                                                    error!("Failed to bind active_validator_limit '{}': {}", val, e);
                                                    continue;
                                                }
                                            }
                                        },
                                        1 => if updates.contains(&"min_validator_stake = $2") {
                                            q = q.bind(val);
                                        },
                                        2 => if updates.contains(&"uptime_blocks_window = $3") {
                                            match val.parse::<i64>() {
                                                Ok(v) => q = q.bind(v),
                                                Err(e) => {
                                                    error!("Failed to bind uptime_blocks_window '{}': {}", val, e);
                                                    continue;
                                                }
                                            }
                                        },
                                        3 => if updates.contains(&"uptime_min_required = $4") {
                                            q = q.bind(val);
                                        },
                                        4 => if updates.contains(&"slashing_penalty_downtime = $5") {
                                            q = q.bind(val);
                                        },
                                        5 => if updates.contains(&"slashing_penalty_misbehavior = $6") {
                                            q = q.bind(val);
                                        },
                                        6 => if updates.contains(&"unbonding_delay = $7") {
                                            q = q.bind(val);
                                        },
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
                                    },
                                    Err(e) => {
                                        error!("Failed to update validator staking parameters: {}", e);
                                    }
                                }
                            }
                        }
                    },
                    None => {
                        error!("EventAppParametersChange missing newParameters attribute");
                    }
                }
            }
        }
        
        Ok(())
    }
}
