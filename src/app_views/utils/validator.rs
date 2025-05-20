use anyhow::Result;
use cometindex::ContextualizedEvent;
use serde_json::Value;
use sqlx::PgTransaction;
use std::fs::File;
use std::io::Read;
use tracing::{debug, info, error};
use sqlx::types::chrono::{DateTime, Utc};

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
        
        let chain_id = genesis["chain_id"].as_str()
            .ok_or_else(|| {
                tracing::error!("Failed to find chain_id in genesis.json");
                anyhow::anyhow!("Missing chain_id in genesis.json")
            })?
            .to_string();
        
        let stake_params = &genesis["app_state"]["genesisContent"]["stakeContent"]["stakeParams"];
        
        let active_validator_limit = stake_params["activeValidatorLimit"].as_str()
            .ok_or_else(|| {
                tracing::error!("Failed to find activeValidatorLimit in genesis.json");
                anyhow::anyhow!("Missing activeValidatorLimit in genesis.json")
            })?
            .parse::<i64>()
            .map_err(|_| anyhow::anyhow!("Failed to parse activeValidatorLimit as a number"))?;
        
        let min_validator_stake = {
            let raw_val = stake_params["minValidatorStake"]["lo"].as_str()
                .ok_or_else(|| {
                    tracing::error!("Failed to find minValidatorStake.lo in genesis.json");
                    anyhow::anyhow!("Missing minValidatorStake.lo in genesis.json")
                })?
                .parse::<i64>()
                .map_err(|_| anyhow::anyhow!("Failed to parse minValidatorStake.lo as a number"))?;
            
            format!("{} UM", raw_val / 1_000_000)
        };

        let total_staked = "".to_string();
        
        let uptime_blocks_window = stake_params["signedBlocksWindowLen"].as_str()
            .ok_or_else(|| {
                tracing::error!("Failed to find signedBlocksWindowLen in genesis.json");
                anyhow::anyhow!("Missing signedBlocksWindowLen in genesis.json")
            })?
            .parse::<i64>()
            .map_err(|_| anyhow::anyhow!("Failed to parse signedBlocksWindowLen as a number"))?;
        
        let uptime_min_required = {
            let missed_max = stake_params["missedBlocksMaximum"].as_str()
                .ok_or_else(|| {
                    tracing::error!("Failed to find missedBlocksMaximum in genesis.json");
                    anyhow::anyhow!("Missing missedBlocksMaximum in genesis.json")
                })?
                .parse::<i64>()
                .map_err(|_| anyhow::anyhow!("Failed to parse missedBlocksMaximum as a number"))?;
            
            let min_percent = 100.0 * (uptime_blocks_window - missed_max) as f64 / uptime_blocks_window as f64;
            format!("{:.2}%", min_percent)
        };
        

        let slashing_penalty_downtime = "".to_string();
        
        let slashing_penalty_misbehavior = {
            let ppm = stake_params["slashingPenaltyMisbehavior"].as_str()
                .ok_or_else(|| {
                    tracing::error!("Failed to find slashingPenaltyMisbehavior in genesis.json");
                    anyhow::anyhow!("Missing slashingPenaltyMisbehavior in genesis.json")
                })?
                .parse::<i64>()
                .map_err(|_| anyhow::anyhow!("Failed to parse slashingPenaltyMisbehavior as a number"))?;
            
            format!("{:.2}%", ppm as f64 / 1_000_000.0)
        };
        
        let unbonding_delay = {
            let delay = stake_params["unbondingDelay"].as_str()
                .ok_or_else(|| {
                    tracing::error!("Failed to find unbondingDelay in genesis.json");
                    anyhow::anyhow!("Missing unbondingDelay in genesis.json")
                })?;
            
            format!("{} blocks", delay)
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
    fn find_attribute_value<'a>(event: &'a ContextualizedEvent<'_>, key: &str) -> Option<&'a str> {
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
        for event in events {
            if event.event.kind == "penumbra.core.app.v1.EventAppParametersChange" {
                debug!("Found EventAppParametersChange event at height {}", height);
                
                if let Some(params_json) = Self::find_attribute_value(event, "newParameters") {
                    debug!("Processing parameter changes: {}", params_json);
                    
                    let params: Value = match serde_json::from_str(params_json) {
                        Ok(p) => p,
                        Err(e) => {
                            error!("Failed to parse parameter JSON: {}", e);
                            continue;
                        }
                    };
                    
                    let chain_id = match params["chainId"].as_str() {
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
                        if let Some(val) = stake_params["activeValidatorLimit"].as_str() {
                            if let Ok(limit) = val.parse::<i64>() {
                                updates.push("active_validator_limit = $1");
                                bindings.push(limit.to_string());
                            }
                        }
                        
                        if let Some(stake) = stake_params.get("minValidatorStake") {
                            if let Some(lo) = stake["lo"].as_str() {
                                if let Ok(raw_val) = lo.parse::<i64>() {
                                    let formatted = format!("{} UM", raw_val / 1_000_000);
                                    updates.push("min_validator_stake = $2");
                                    bindings.push(formatted);
                                }
                            }
                        }
                        
                        if let Some(val) = stake_params["signedBlocksWindowLen"].as_str() {
                            if let Ok(window) = val.parse::<i64>() {
                                updates.push("uptime_blocks_window = $3");
                                bindings.push(window.to_string());
                                
                                if let Some(missed) = stake_params["missedBlocksMaximum"].as_str() {
                                    if let Ok(missed_max) = missed.parse::<i64>() {
                                        let min_percent = 100.0 * (window - missed_max) as f64 / window as f64;
                                        let formatted = format!("{:.2}%", min_percent);
                                        updates.push("uptime_min_required = $4");
                                        bindings.push(formatted);
                                    }
                                }
                            }
                        }
                        
                        if let Some(val) = stake_params["slashingPenaltyDowntime"].as_str() {
                            if let Ok(penalty) = val.parse::<i64>() {
                                // The value is in basis points (1/100 of a percent)
                                // 1 basis point = 0.01%
                                // So 1 = 0.01%, 10000 = 100%
                                let formatted = format!("{:.2}%", penalty as f64 / 100.0);
                                
                                debug!("Parsed slashingPenaltyDowntime '{}' as '{}'", val, formatted);
                                
                                updates.push("slashing_penalty_downtime = $5");
                                bindings.push(formatted);
                            }
                        }
                        
                        if let Some(val) = stake_params["slashingPenaltyMisbehavior"].as_str() {
                            if let Ok(penalty) = val.parse::<i64>() {
                                let formatted = format!("{:.2}%", penalty as f64 / 1_000_000.0);
                                updates.push("slashing_penalty_misbehavior = $6");
                                bindings.push(formatted);
                            }
                        }
                        
                        if let Some(val) = stake_params["unbondingDelay"].as_str() {
                            let formatted = format!("{} blocks", val);
                            updates.push("unbonding_delay = $7");
                            bindings.push(formatted);
                        }
                        
                        if !updates.is_empty() {
                            let update_str = updates.join(", ");
                            let query = format!(
                                "UPDATE validator_staking_parameters SET {} WHERE chain_id = '{}'",
                                update_str, chain_id
                            );
                            
                            info!("Updating validator staking parameters for chain_id = {}", chain_id);
                            
                            let mut q = sqlx::query(&query);
                            
                            for (i, val) in bindings.iter().enumerate() {
                                match i {
                                    0 => if updates.contains(&"active_validator_limit = $1") {
                                        q = q.bind(val.parse::<i64>().unwrap());
                                    },
                                    1 => if updates.contains(&"min_validator_stake = $2") {
                                        q = q.bind(val);
                                    },
                                    2 => if updates.contains(&"uptime_blocks_window = $3") {
                                        q = q.bind(val.parse::<i64>().unwrap());
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
                            
                            let result = q.execute(dbtx.as_mut()).await?;
                            
                            info!(
                                "Updated {} validator staking parameters for chain_id = {}",
                                result.rows_affected(), chain_id
                            );
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
}
