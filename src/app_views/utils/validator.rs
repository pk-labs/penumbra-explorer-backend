use anyhow::Result;
use cometindex::ContextualizedEvent;
use serde_json::Value;
use sqlx::PgTransaction;
use std::fs::File;
use std::io::Read;
use tracing::{debug, info, error};
use sqlx::types::chrono::{DateTime, Utc};
// We don't need base64 imports here
// use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};

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

/// Represents a validator entity
#[derive(Debug)]
pub struct Validator {
    pub identity_key: String,     // Base64 encoded identifier ("ik" field)
    pub name: Option<String>,     // Name of the validator
    pub website: Option<String>,  // Website URL
    pub description: Option<String>, // Description text
    pub consensus_key: Option<String>,    // Consensus public key
    pub governance_key: Option<String>, // Governance key
    pub state: String,            // Current validator state (ACTIVE, INACTIVE, etc.)
    pub bonding_state: Option<String>,    // Current bonding state (BONDED, UNBONDING, UNBONDED)
    pub voting_power: i64,        // Current voting power in raw units
    pub voting_power_percentage: f64, // Percentage of total voting power
    pub first_seen_height: i64,   // Block height when validator was first seen
    pub first_seen_time: DateTime<Utc>, // Timestamp when validator was first seen
    pub last_updated: DateTime<Utc>, // Last update timestamp
    pub address: Option<String>,  // Address extracted from funding streams (if available)
    pub commission_rate: Option<i32>, // Commission rate in basis points (if available)
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
        // Extract identity key (the only required field)
        let identity_key = event_json["identityKey"]["ik"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing or invalid identity key"))?
            .to_string();
        
        // Extract all other fields as optional
        let name = event_json["name"].as_str().map(String::from);
        let website = event_json["website"].as_str().map(String::from);
        let description = event_json["description"].as_str().map(String::from);
        let consensus_key = event_json["consensusKey"].as_str().map(String::from);
        
        // Extract governance key (optional)
        let governance_key = event_json.get("governanceKey")
            .and_then(|gk| gk.get("gk"))
            .and_then(|gk| gk.as_str())
            .map(String::from);
        
        // Extract address and commission rate from funding streams
        let mut address = None;
        let mut commission_rate = None;
        
        if let Some(funding_streams) = event_json["fundingStreams"].as_array() {
            for stream in funding_streams {
                // Look for toAddress funding stream
                if let Some(to_address) = stream.get("toAddress") {
                    address = to_address.get("address").and_then(|a| a.as_str()).map(String::from);
                    commission_rate = to_address.get("rateBps").and_then(|r| r.as_i64()).map(|r| r as i32);
                    break; // Take the first one
                }
            }
        }
        
        // Only set first_seen values if the state is DEFINED, otherwise leave as placeholders
        // For validators in events (not genesis), these will be updated when the state transitions to DEFINED
        let (first_seen_height, first_seen_time) = if default_state.contains("DEFINED") {
            // For validators that are already in DEFINED state (like in genesis), use the supplied height/time
            debug!("Creating validator {} already in DEFINED state at height {}, timestamp {}", 
                  identity_key, height, timestamp);
            (height as i64, timestamp)
        } else {
            // For validators that aren't yet DEFINED, use placeholders that will be updated
            // when the validator transitions to DEFINED state
            debug!("Creating validator {} with non-DEFINED state ({}), using placeholder first_seen values", 
                  identity_key, default_state);
            // Use max value as a clear indicator these are placeholders
            (i64::MAX, DateTime::<Utc>::from_timestamp(0, 0).unwrap())
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
            address,
            commission_rate,
        })
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
                last_updated,
                address,
                commission_rate
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
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
                last_updated = GREATEST(validators.last_updated, EXCLUDED.last_updated),
                address = COALESCE(validators.address, EXCLUDED.address),
                commission_rate = COALESCE(validators.commission_rate, EXCLUDED.commission_rate)
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
        .bind(&self.address)
        .bind(self.commission_rate)
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
        // Special handling for DEFINED state - this is when we consider a validator to be "officially" created
        if state.contains("DEFINED") {
            // Check if this validator already exists and if it already has DEFINED state
            let validator_info: Option<(Option<String>,)> = sqlx::query_as(
                "SELECT state FROM validators WHERE identity_key = $1"
            )
            .bind(identity_key)
            .fetch_optional(dbtx.as_mut())
            .await?;
            
            match validator_info {
                // If validator exists
                Some((current_state,)) => {
                    // Check if validator doesn't have DEFINED state yet
                    if !current_state.map_or(false, |s| s.contains("DEFINED")) {
                        // This is the validator's transition to DEFINED state
                        // Set first_seen_time to CURRENT timestamp (when the DEFINED event occurred)
                        // This is the official "creation date" even if the validator was in the DB earlier
                        debug!("Validator {} transitioned to DEFINED state at height {}, timestamp {}", 
                               identity_key, height, timestamp);
                        
                        sqlx::query(
                            r"
                            UPDATE validators 
                            SET 
                                state = $1,
                                last_updated = $2,
                                -- Set creation/first_seen timestamp to DEFINED event timestamp
                                first_seen_time = $2,
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
                // If validator doesn't exist, it will be created elsewhere
                None => {
                    debug!("Validator {} not found in database when updating to state {}", identity_key, state);
                }
            }
        }
        
        // Standard update without changing first_seen_time for non-DEFINED states
        // or for validators that are already in DEFINED state
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
    
    /// Calculate total voting power across all validators
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
                sqlx::query_scalar::<_, i64>(
                    &format!("SELECT COALESCE(SUM(voting_power), 0) FROM validators WHERE state = '{}'", state)
                )
                .fetch_one(dbtx.as_mut())
                .await?
            },
            None => 0, // If no active validators found, return 0
        };
        
        Ok(result)
    }
    
    /// Update voting power percentages for all validators
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
            // Update percentages for all validators with the active state
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
        }
        
        Ok(())
    }
    
    /// Update total_staked in validator_staking_parameters 
    pub async fn update_total_staked(dbtx: &mut PgTransaction<'_>, total_voting_power: i64) -> Result<()> {
        // Format total staked in UM format (dividing by 1,000,000)
        let formatted_total = format!("{} UM", total_voting_power / 1_000_000);
        
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
    async fn ensure_validator_exists(
        identity_key: &str,
        height: u64,
        timestamp: DateTime<Utc>,
        dbtx: &mut PgTransaction<'_>,
        state: Option<&str>,
        bonding_state: Option<&str>,
        voting_power: Option<i64>,
    ) -> Result<()> {
        // Check if validator exists
        let validator_exists: i64 = match sqlx::query_scalar(
            "SELECT COUNT(*) FROM validators WHERE identity_key = $1"
        )
        .bind(identity_key)
        .fetch_one(dbtx.as_mut())
        .await {
            Ok(count) => count,
            Err(e) => {
                error!("Failed to check if validator exists: {}", e);
                return Err(anyhow::anyhow!("Failed to check validator existence"));
            }
        };
        
        // If validator doesn't exist, create a minimal record
        if validator_exists == 0 {
            debug!("Creating new validator from event: {}", identity_key);
            
            // Create minimal validator with available data from the event
            match Self::from_event(
                &serde_json::json!({
                    "identityKey": {"ik": identity_key}
                }),
                height,
                timestamp,
                state.unwrap_or(""), // Use state from event or empty
                bonding_state.unwrap_or(""), // Use bonding state from event or empty
                voting_power.unwrap_or(0), // Use voting power from event or 0
                0.0, // Percentage will be calculated later
            ) {
                Ok(validator) => {
                    if let Err(e) = validator.insert_or_update(dbtx).await {
                        error!("Failed to insert new validator: {}", e);
                        return Err(anyhow::anyhow!("Failed to insert validator"));
                    }
                },
                Err(e) => {
                    error!("Failed to create validator: {}", e);
                    return Err(anyhow::anyhow!("Failed to create validator"));
                }
            }
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

        // We'll calculate total voting power in the voting power change events section
        
        // First pass: process validator definitions
        for event in events {
            if event.event.kind.as_str() == "penumbra.core.component.stake.v1.EventValidatorDefinitionUpload" {
                match Self::find_attribute_value(event, "validator") {
                    Some(validator_json) => {
                        debug!("Processing validator definition: {}", validator_json);
                        
                        match serde_json::from_str::<Value>(validator_json) {
                            Ok(validator_data) => {
                                // Extract actual state from event if present
                                let state = validator_data.get("state")
                                    .and_then(|s| s.get("state"))
                                    .and_then(|s| s.as_str());
                                
                                // Extract bonding state from event if present
                                let bonding_state = validator_data.get("bondingState")
                                    .and_then(|s| s.get("state"))
                                    .and_then(|s| s.as_str());
                                
                                // Create validator object with state values only if they're present in event
                                // Otherwise, they'll be updated through state change events
                                match Self::from_event(
                                    &validator_data,
                                    height,
                                    timestamp,
                                    state.unwrap_or(""),  // Empty string if not present, will be updated by events
                                    bonding_state.unwrap_or(""),  // Empty string if not present, will be updated by events
                                    0,         // initial voting power
                                    0.0,       // initial voting power percentage
                                ) {
                                    Ok(validator) => {
                                        if let Err(e) = validator.insert_or_update(dbtx).await {
                                            error!("Failed to insert/update validator: {}", e);
                                            // Continue processing other validators
                                        }
                                    }
                                    Err(e) => {
                                        error!("Failed to parse validator definition: {}", e);
                                        // Continue processing other validators
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
        
        // Second pass: process validator states and voting power
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
                                                None
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
                                                None
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
                                                Ok(voting_power) => {
                                                    // Raw voting power is in microunits (UM)
                                                    // We store the raw value for calculations but will display divided by 1,000,000
                                                    
                                                    // Ensure validator exists, creating if necessary
                                                    if let Err(e) = Self::ensure_validator_exists(
                                                        identity_key, 
                                                        height, 
                                                        timestamp, 
                                                        dbtx, 
                                                        None, 
                                                        None, 
                                                        Some(voting_power)
                                                    ).await {
                                                        error!("Failed to ensure validator exists: {}", e);
                                                        continue;
                                                    }
                                                    
                                                    // Update this validator's voting power first
                                                    if let Err(e) = Self::update_voting_power(
                                                        identity_key, 
                                                        voting_power, 
                                                        0.0, // Temporary percentage, will update
                                                        dbtx, 
                                                        timestamp
                                                    ).await {
                                                        error!("Failed to update validator voting power: {}", e);
                                                        continue; // Skip to next validator
                                                    }
                                                    
                                                    // After updating all validator powers, calculate the new total
                                                    match Self::calculate_total_voting_power(dbtx).await {
                                                        Ok(total) => {
                                                            // Now update percentages for all validators
                                                            if total > 0 {
                                                                // Update all validators' percentages
                                                                if let Err(e) = Self::update_all_voting_power_percentages(dbtx).await {
                                                                    error!("Failed to update voting power percentages: {}", e);
                                                                }
                                                                
                                                                // Also update the total_staked in the validator_staking_parameters table
                                                                if let Err(e) = Self::update_total_staked(dbtx, total).await {
                                                                    error!("Failed to update total_staked parameter: {}", e);
                                                                }
                                                            }
                                                        },
                                                        Err(e) => {
                                                            error!("Failed to calculate total voting power: {}", e);
                                                        }
                                                    }
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
                                        None
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
                                        None
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
                                        None
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
                                        None
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
                                        None
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
                                        None
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
        
        Ok(())
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
                    // The value is in basis points (1/100 of a percent)
                    format!("{:.2}%", penalty as f64 / 100.0)
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
                                        // The value is in basis points (1/100 of a percent)
                                        let formatted = format!("{:.2}%", penalty as f64 / 100.0);
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
                                
                                // Add the chain_id as the final parameter
                                q = q.bind(&chain_id);
                                
                                // Execute the update
                                match q.execute(dbtx.as_mut()).await {
                                    Ok(result) => {
                                        info!(
                                            "Updated {} validator staking parameters for chain_id = {}",
                                            result.rows_affected(), chain_id
                                        );
                                    },
                                    Err(e) => {
                                        error!("Failed to update validator staking parameters: {}", e);
                                        // Continue processing other events
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
