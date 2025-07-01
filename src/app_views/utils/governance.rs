use anyhow::Result;
use cometindex::ContextualizedEvent;
use serde_json::Value;
use sqlx::types::chrono::{DateTime, Utc};
use sqlx::types::BigDecimal;
use sqlx::PgTransaction;
use std::str::FromStr;
use tracing::{debug, error, info};

/// Governance parameters structure
#[derive(Debug, Clone)]
pub struct GovernanceParameters {
    pub chain_id: String,
    pub deposit_amount: BigDecimal,
    pub passing_threshold: f64,
    pub slashing_threshold: f64,
    pub valid_quorum: f64,
    pub proposal_voting_blocks: i64,
    pub updated_height: i64,
    pub updated_at: DateTime<Utc>,
    pub raw_params: Value,
}

/// Governance proposal structure
#[derive(Debug, Clone)]
pub struct GovernanceProposal {
    pub proposal_id: i64,
    pub title: String,
    pub description: String,
    pub kind: String,
    pub state: String,
    pub outcome: Option<String>,
    pub deposit_amount: BigDecimal,
    pub start_block_height: i64,
    pub end_block_height: i64,
    pub start_timestamp: DateTime<Utc>,
    pub end_timestamp: Option<DateTime<Utc>>,
    pub quorum: BigDecimal,
    pub payload: Value,
}

impl GovernanceProposal {
    /// Extract proposal ID, assigning 0 for proposals without ID
    fn extract_proposal_id(submit_data: &Value) -> i64 {
        if let Some(id_str) = submit_data["proposal"]["id"].as_str() {
            id_str.parse::<i64>().unwrap_or(0)
        } else {
            // No ID field = first proposal on blockchain = assign ID 0
            0
        }
    }
    
    /// Determine proposal kind from proposal content
    fn determine_proposal_kind(proposal: &Value) -> String {
        if proposal.get("parameterChange").is_some() {
            "Parameter Change".to_string()
        } else if proposal.get("communityPoolSpend").is_some() {
            "Community Pool Spend".to_string()
        } else if proposal.get("upgradeProposal").is_some() {
            "Upgrade Plan".to_string()
        } else if proposal.get("freezeIbcClient").is_some() {
            "Freeze IBC Client".to_string()
        } else if proposal.get("unfreezeIbcClient").is_some() {
            "Unfreeze IBC Client".to_string()
        } else if proposal.get("emergency").is_some() {
            "Emergency".to_string()
        } else {
            "Signaling".to_string()
        }
    }
    
    /// Calculate quorum based on total validator voting power and governance parameters
    /// This is the voting power threshold that must be reached for a proposal to pass
    async fn calculate_quorum(
        chain_id: &str,
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<BigDecimal> {
        debug!("Starting quorum calculation for chain_id: {}", chain_id);
        
        // Get TOTAL voting power from ALL validators (not just active)
        let total_voting_power: Option<BigDecimal> = sqlx::query_scalar(
            "SELECT SUM(voting_power) FROM validators"
        )
        .fetch_optional(dbtx.as_mut())
        .await?;
        
        debug!("Total voting power query result: {:?}", total_voting_power);
        
        if total_voting_power.is_none() {
            error!("No validators found in database");
            return Err(anyhow::anyhow!("No validators found in database"));
        }
        
        let total_power = total_voting_power.unwrap();
        debug!("Total voting power: {}", total_power);
        
        if total_power == BigDecimal::from(0) {
            error!("Total voting power is zero");
            return Err(anyhow::anyhow!("Total voting power is zero"));
        }
        
        // Get quorum percentage from governance_parameters
        let quorum_percentage: Option<BigDecimal> = sqlx::query_scalar(
            "SELECT valid_quorum FROM governance_parameters WHERE chain_id = $1"
        )
        .bind(chain_id)
        .fetch_optional(dbtx.as_mut())
        .await?;
        
        debug!("Quorum percentage query result: {:?}", quorum_percentage);
        
        if quorum_percentage.is_none() {
            error!("No governance parameters found for chain_id: {}", chain_id);
            return Err(anyhow::anyhow!("No governance parameters found for chain_id: {}", chain_id));
        }
        
        let quorum_percentage = quorum_percentage.unwrap();
        debug!("Valid quorum percentage: {}%", quorum_percentage);
        
        // Calculate required quorum: total_voting_power * (valid_quorum / 100)
        let hundred = BigDecimal::from(100);
        let required_quorum = &total_power * &quorum_percentage / &hundred;
        
        info!(
            "Calculated quorum: {} voting power required ({}% of {} total)", 
            required_quorum, quorum_percentage, total_power
        );
        
        Ok(required_quorum)
    }
    
    /// Create proposal from EventProposalSubmit
    pub async fn from_proposal_submit_event(
        event: &ContextualizedEvent<'_>,
        _height: u64,
        timestamp: DateTime<Utc>,
        chain_id: &str,
        dbtx: &mut PgTransaction<'_>,
    ) -> Option<Self> {
        // Extract submit data
        let submit_str = event.event.attributes
            .iter()
            .find(|attr| attr.key_str().ok() == Some("submit"))
            .and_then(|attr| attr.value_str().ok())?;
        
        let submit_data: Value = serde_json::from_str(submit_str).ok()?;
        
        // Extract start and end heights from attributes
        let start_height_str = event.event.attributes
            .iter()
            .find(|attr| attr.key_str().ok() == Some("startHeight"))
            .and_then(|attr| attr.value_str().ok())?;
        
        let end_height_str = event.event.attributes
            .iter()
            .find(|attr| attr.key_str().ok() == Some("endHeight"))
            .and_then(|attr| attr.value_str().ok())?;
        
        // Parse heights (remove quotes)
        let start_height = start_height_str.trim_matches('"').parse::<i64>().ok()?;
        let end_height = end_height_str.trim_matches('"').parse::<i64>().ok()?;
        
        // Extract proposal details
        let proposal = submit_data.get("proposal")?;
        let proposal_id = Self::extract_proposal_id(&submit_data);
        let title = proposal["title"].as_str()?.to_string();
        let description = proposal["description"].as_str()?.to_string();
        let kind = Self::determine_proposal_kind(proposal);
        
        // Extract deposit amount (divide by 1M)
        let deposit_str = submit_data["depositAmount"]["lo"].as_str()?;
        let deposit_base = BigDecimal::from_str(deposit_str).ok()?;
        let deposit_amount = deposit_base / BigDecimal::from(1_000_000);
        
        // Calculate quorum
        let quorum = match Self::calculate_quorum(chain_id, dbtx).await {
            Ok(q) => q,
            Err(e) => {
                error!("Failed to calculate quorum for proposal {}: {}", proposal_id, e);
                BigDecimal::from(0)
            }
        };
        
        Some(Self {
            proposal_id,
            title,
            description,
            kind,
            state: "Voting".to_string(),
            outcome: None,
            deposit_amount,
            start_block_height: start_height,
            end_block_height: end_height,
            start_timestamp: timestamp,
            end_timestamp: None, // Will be set when we reach end_block_height
            quorum,
            payload: proposal.clone(),
        })
    }
    
    /// Insert proposal into database
    pub async fn insert(&self, dbtx: &mut PgTransaction<'_>) -> Result<()> {
        sqlx::query(
            r"
            INSERT INTO governance_proposals (
                proposal_id, title, description, kind, state, outcome,
                deposit_amount, start_block_height, end_block_height,
                start_timestamp, end_timestamp, quorum, payload, created_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $14)
            ON CONFLICT (proposal_id) DO UPDATE SET
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                kind = EXCLUDED.kind,
                state = EXCLUDED.state,
                outcome = EXCLUDED.outcome,
                deposit_amount = EXCLUDED.deposit_amount,
                start_block_height = EXCLUDED.start_block_height,
                end_block_height = EXCLUDED.end_block_height,
                start_timestamp = EXCLUDED.start_timestamp,
                end_timestamp = EXCLUDED.end_timestamp,
                quorum = EXCLUDED.quorum,
                payload = EXCLUDED.payload,
                updated_at = EXCLUDED.updated_at
            ",
        )
        .bind(self.proposal_id)
        .bind(&self.title)
        .bind(&self.description)
        .bind(&self.kind)
        .bind(&self.state)
        .bind(&self.outcome)
        .bind(&self.deposit_amount)
        .bind(self.start_block_height)
        .bind(self.end_block_height)
        .bind(self.start_timestamp)
        .bind(self.end_timestamp)
        .bind(&self.quorum)
        .bind(&self.payload)
        .bind(self.start_timestamp)
        .execute(dbtx.as_mut())
        .await?;
        
        info!(
            "Inserted/updated proposal {}: {} ({})",
            self.proposal_id, self.title, self.state
        );
        
        Ok(())
    }
    
    /// Update proposal state
    pub async fn update_state(
        proposal_id: i64,
        state: &str,
        outcome: Option<&str>,
        timestamp: DateTime<Utc>,
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<()> {
        sqlx::query(
            r"
            UPDATE governance_proposals 
            SET state = $2, outcome = $3, updated_at = $4
            WHERE proposal_id = $1
            ",
        )
        .bind(proposal_id)
        .bind(state)
        .bind(outcome)
        .bind(timestamp)
        .execute(dbtx.as_mut())
        .await?;
        
        info!("Updated proposal {} state to: {} (outcome: {:?})", proposal_id, state, outcome);
        
        Ok(())
    }
    
    /// Update proposal state only (keep existing outcome)
    pub async fn update_state_only(
        proposal_id: i64,
        state: &str,
        timestamp: DateTime<Utc>,
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<()> {
        sqlx::query(
            r"
            UPDATE governance_proposals 
            SET state = $2, updated_at = $3
            WHERE proposal_id = $1
            ",
        )
        .bind(proposal_id)
        .bind(state)
        .bind(timestamp)
        .execute(dbtx.as_mut())
        .await?;
        
        info!("Updated proposal {} state to: {} (keeping existing outcome)", proposal_id, state);
        
        Ok(())
    }
    
    /// Update proposal end_timestamp when reaching end_block_height
    pub async fn update_end_timestamp(
        proposal_id: i64,
        end_timestamp: DateTime<Utc>,
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<()> {
        sqlx::query(
            r"
            UPDATE governance_proposals 
            SET end_timestamp = $2, updated_at = $2
            WHERE proposal_id = $1
            ",
        )
        .bind(proposal_id)
        .bind(end_timestamp)
        .execute(dbtx.as_mut())
        .await?;
        
        info!("Updated proposal {} end_timestamp", proposal_id);
        
        Ok(())
    }
}

impl GovernanceParameters {
    /// Read initial governance parameters from genesis.json
    /// This ensures governance parameters are available from the start for quorum calculation
    pub fn from_genesis_json() -> Result<Self> {
        use std::fs::File;
        use std::io::Read;
        
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
        } else if let Some(id) = genesis["app_state"]["genesisContent"]["chainId"].as_str() {
            id.to_string()
        } else {
            return Err(anyhow::anyhow!("Could not find chain_id in genesis.json"));
        };

        let gov_params = genesis["app_state"]["genesisContent"]["governanceContent"]["governanceParams"]
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("Missing governanceParams in genesis.json"))?;

        let deposit_amount_str = gov_params["proposalDepositAmount"]["lo"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing proposalDepositAmount.lo"))?;
        let deposit_amount_base = BigDecimal::from_str(deposit_amount_str)?;
        let deposit_amount = deposit_amount_base / BigDecimal::from(1_000_000);

        let passing_threshold = Self::parse_fraction_to_percentage(
            gov_params["proposalPassThreshold"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Missing proposalPassThreshold"))?
        )?;

        let slashing_threshold = Self::parse_fraction_to_percentage(
            gov_params["proposalSlashThreshold"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Missing proposalSlashThreshold"))?
        )?;

        let valid_quorum = Self::parse_fraction_to_percentage(
            gov_params["proposalValidQuorum"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Missing proposalValidQuorum"))?
        )?;

        let proposal_voting_blocks = gov_params["proposalVotingBlocks"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing proposalVotingBlocks"))?
            .parse::<i64>()?;

        info!(
            "Loaded governance parameters from genesis.json: deposit={} UM, pass={}%, slash={}%, quorum={}%, voting_blocks={}",
            deposit_amount, passing_threshold, slashing_threshold, valid_quorum, proposal_voting_blocks
        );

        Ok(Self {
            chain_id,
            deposit_amount,
            passing_threshold,
            slashing_threshold,
            valid_quorum,
            proposal_voting_blocks,
            updated_height: 0, // Genesis = height 0
            updated_at: chrono::Utc::now(),
            raw_params: Value::Object(gov_params.clone()),
        })
    }

    /// Parse fraction string like "67/100" to decimal percentage
    fn parse_fraction_to_percentage(fraction: &str) -> Result<f64> {
        let parts: Vec<&str> = fraction.split('/').collect();
        if parts.len() != 2 {
            return Err(anyhow::anyhow!("Invalid fraction format: {}", fraction));
        }
        
        let numerator: f64 = parts[0].parse()?;
        let denominator: f64 = parts[1].parse()?;
        
        if denominator == 0.0 {
            return Err(anyhow::anyhow!("Division by zero in fraction"));
        }
        
        let percentage = (numerator / denominator) * 100.0;
        Ok((percentage * 100.0).round() / 100.0)
    }
    
    /// Extract governance parameters from EventAppParametersChange
    /// Returns None if required fields are missing (don't crash the indexer)
    pub fn from_app_parameters_event(
        event: &ContextualizedEvent,
        chain_id: &str,
        height: u64,
        timestamp: DateTime<Utc>,
    ) -> Option<Self> {
        let new_params_str = event.event.attributes
            .iter()
            .find(|attr| attr.key_str().ok() == Some("newParameters"))
            .and_then(|attr| attr.value_str().ok())?;
        
        let new_params: Value = serde_json::from_str(new_params_str).ok()?;
        
        let gov_params = new_params.get("governanceParams")?;
        
        let deposit_amount_str = gov_params["proposalDepositAmount"]["lo"].as_str()?;
        let deposit_amount_base = BigDecimal::from_str(deposit_amount_str).ok()?;
        let deposit_amount = deposit_amount_base / BigDecimal::from(1_000_000);
        
        // Parse thresholds
        let passing_threshold = Self::parse_fraction_to_percentage(
            gov_params["proposalPassThreshold"].as_str()?
        ).ok()?;
        
        let slashing_threshold = Self::parse_fraction_to_percentage(
            gov_params["proposalSlashThreshold"].as_str()?
        ).ok()?;
        
        let valid_quorum = Self::parse_fraction_to_percentage(
            gov_params["proposalValidQuorum"].as_str()?
        ).ok()?;
        
        // Parse voting blocks
        let proposal_voting_blocks = gov_params["proposalVotingBlocks"]
            .as_str()?
            .parse::<i64>().ok()?;
        
        Some(Self {
            chain_id: chain_id.to_string(),
            deposit_amount,
            passing_threshold,
            slashing_threshold,
            valid_quorum,
            proposal_voting_blocks,
            updated_height: i64::try_from(height).unwrap_or(i64::MAX),
            updated_at: timestamp,
            raw_params: gov_params.clone(),
        })
    }
    
    /// Insert or update governance parameters in database
    pub async fn upsert(&self, dbtx: &mut PgTransaction<'_>) -> Result<()> {
        sqlx::query(
            r"
            INSERT INTO governance_parameters (
                chain_id,
                deposit_amount,
                passing_threshold,
                slashing_threshold,
                valid_quorum,
                proposal_voting_blocks,
                updated_height,
                updated_at,
                raw_params
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (chain_id) DO UPDATE SET
                deposit_amount = EXCLUDED.deposit_amount,
                passing_threshold = EXCLUDED.passing_threshold,
                slashing_threshold = EXCLUDED.slashing_threshold,
                valid_quorum = EXCLUDED.valid_quorum,
                proposal_voting_blocks = EXCLUDED.proposal_voting_blocks,
                updated_height = EXCLUDED.updated_height,
                updated_at = EXCLUDED.updated_at,
                raw_params = EXCLUDED.raw_params
            WHERE governance_parameters.updated_height < EXCLUDED.updated_height
            ",
        )
        .bind(&self.chain_id)
        .bind(&self.deposit_amount)
        .bind(BigDecimal::from_str(&format!("{:.2}", self.passing_threshold))?)
        .bind(BigDecimal::from_str(&format!("{:.2}", self.slashing_threshold))?)
        .bind(BigDecimal::from_str(&format!("{:.2}", self.valid_quorum))?)
        .bind(self.proposal_voting_blocks)
        .bind(self.updated_height)
        .bind(self.updated_at)
        .bind(&self.raw_params)
        .execute(dbtx.as_mut())
        .await?;
        
        info!(
            "Updated governance parameters at height {}: deposit={} UM, pass={}%, slash={}%, quorum={}%, voting_blocks={}",
            self.updated_height,
            self.deposit_amount,
            self.passing_threshold,
            self.slashing_threshold,
            self.valid_quorum,
            self.proposal_voting_blocks
        );
        
        Ok(())
    }
    
    /// Initialize governance parameters from genesis.json if not already present
    pub async fn initialize_from_genesis_if_needed(dbtx: &mut PgTransaction<'_>) -> Result<()> {
        // Check if governance parameters already exist
        let params_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM governance_parameters")
            .fetch_one(dbtx.as_mut())
            .await?;

        if params_count > 0 {
            info!("Governance parameters already initialized, skipping genesis initialization");
            return Ok(());
        }

        // Load from genesis.json and insert
        match Self::from_genesis_json() {
            Ok(params) => {
                params.upsert(dbtx).await?;
                info!("Successfully initialized governance parameters from genesis.json");
            }
            Err(e) => {
                error!("Failed to load governance parameters from genesis.json: {}", e);
                return Err(e);
            }
        }

        Ok(())
    }
}

/// Update end_timestamp for proposals that reach their end_block_height
async fn update_proposals_end_timestamp(
    height: u64,
    timestamp: DateTime<Utc>,
    dbtx: &mut PgTransaction<'_>,
) -> Result<()> {
    // Get all proposals that end at this height and don't have end_timestamp set yet
    let proposals: Vec<i64> = sqlx::query_scalar(
        r"
        SELECT proposal_id 
        FROM governance_proposals 
        WHERE end_block_height = $1 AND end_timestamp IS NULL
        ",
    )
    .bind(i64::try_from(height).unwrap_or(i64::MAX))
    .fetch_all(dbtx.as_mut())
    .await?;

    for proposal_id in proposals {
        GovernanceProposal::update_end_timestamp(proposal_id, timestamp, dbtx).await?;
        debug!("Set end_timestamp for proposal {} at height {}", proposal_id, height);
    }

    Ok(())
}

/// Process governance events
pub async fn process_events(
    dbtx: &mut PgTransaction<'_>,
    events: &[ContextualizedEvent<'_>],
    height: u64,
    timestamp: DateTime<Utc>,
    chain_id: &str,
) -> Result<()> {
    debug!("Processing governance events for block {}", height);
    
    // First, check if any proposals reach their end_block_height at this height
    if let Err(e) = update_proposals_end_timestamp(height, timestamp, dbtx).await {
        error!("Error updating proposal end timestamps for block {}: {}", height, e);
    }
    
    for event in events {
        match event.event.kind.as_str() {
            "penumbra.core.app.v1.EventAppParametersChange" => {
                debug!("Processing EventAppParametersChange");
                if let Err(e) = process_app_parameters_change(event, height, timestamp, chain_id, dbtx).await {
                    error!("Error processing EventAppParametersChange: {}", e);
                }
            }
            "penumbra.core.component.governance.v1.EventProposalSubmit" => {
                debug!("Processing EventProposalSubmit");
                if let Err(e) = process_proposal_submit(event, height, timestamp, chain_id, dbtx).await {
                    error!("Error processing EventProposalSubmit: {}", e);
                }
            }
            "penumbra.core.component.governance.v1.EventProposalPassed" => {
                debug!("Processing EventProposalPassed");
                if let Err(e) = process_proposal_passed(event, height, timestamp, chain_id, dbtx).await {
                    error!("Error processing EventProposalPassed: {}", e);
                }
            }
            "penumbra.core.component.governance.v1.EventProposalFailed" => {
                debug!("Processing EventProposalFailed");
                if let Err(e) = process_proposal_failed(event, height, timestamp, chain_id, dbtx).await {
                    error!("Error processing EventProposalFailed: {}", e);
                }
            }
            "penumbra.core.component.governance.v1.EventProposalSlashed" => {
                debug!("Processing EventProposalSlashed");
                if let Err(e) = process_proposal_slashed(event, height, timestamp, chain_id, dbtx).await {
                    error!("Error processing EventProposalSlashed: {}", e);
                }
            }
            "penumbra.core.component.governance.v1.EventProposalWithdraw" => {
                debug!("Processing EventProposalWithdraw");
                if let Err(e) = process_proposal_withdraw(event, height, timestamp, chain_id, dbtx).await {
                    error!("Error processing EventProposalWithdraw: {}", e);
                }
            }
            "penumbra.core.component.governance.v1.EventProposalDepositClaim" => {
                debug!("Processing EventProposalDepositClaim");
                if let Err(e) = process_proposal_deposit_claim(event, height, timestamp, chain_id, dbtx).await {
                    error!("Error processing EventProposalDepositClaim: {}", e);
                }
            }
            _ => {}
        }
    }
    
    Ok(())
}

/// Process EventAppParametersChange
async fn process_app_parameters_change(
    event: &ContextualizedEvent<'_>,
    height: u64,
    timestamp: DateTime<Utc>,
    chain_id: &str,
    dbtx: &mut PgTransaction<'_>,
) -> Result<()> {
    if let Some(params) = GovernanceParameters::from_app_parameters_event(event, chain_id, height, timestamp) {
        params.upsert(dbtx).await?;
        debug!("Successfully processed governance parameters at height {}", height);
    } else {
        debug!("Skipping EventAppParametersChange at height {} - missing required governance fields", height);
    }
    
    Ok(())
}

/// Process EventProposalSubmit
async fn process_proposal_submit(
    event: &ContextualizedEvent<'_>,
    height: u64,
    timestamp: DateTime<Utc>,
    chain_id: &str,
    dbtx: &mut PgTransaction<'_>,
) -> Result<()> {
    if let Some(proposal) = GovernanceProposal::from_proposal_submit_event(event, height, timestamp, chain_id, dbtx).await {
        proposal.insert(dbtx).await?;
        debug!("Successfully processed proposal submission at height {}", height);
    } else {
        debug!("Skipping EventProposalSubmit at height {} - missing required fields", height);
    }
    
    Ok(())
}

/// Extract proposal ID from event data, returns 0 if missing (first proposal)
fn extract_proposal_id_from_event_data(data: &Value, field_name: &str) -> i64 {
    if let Some(id_str) = data.get(field_name).and_then(|v| v.as_str()) {
        id_str.parse::<i64>().unwrap_or(0)
    } else {
        // No ID field = first proposal = ID 0
        0
    }
}

/// Process EventProposalPassed - handles both proposals with and without IDs
async fn process_proposal_passed(
    event: &ContextualizedEvent<'_>,
    _height: u64,
    timestamp: DateTime<Utc>,
    _chain_id: &str,
    dbtx: &mut PgTransaction<'_>,
) -> Result<()> {
    let proposal_str = event.event.attributes
        .iter()
        .find(|attr| attr.key_str().ok() == Some("proposal"))
        .and_then(|attr| attr.value_str().ok());
    
    if let Some(proposal_json) = proposal_str {
        if let Ok(proposal_data) = serde_json::from_str::<Value>(proposal_json) {
            let proposal_id = extract_proposal_id_from_event_data(&proposal_data, "id");
            GovernanceProposal::update_state(proposal_id, "Finished", Some("Passed"), timestamp, dbtx).await?;
            debug!("Proposal {} passed", proposal_id);
        }
    }
    
    Ok(())
}

/// Process EventProposalFailed - handles both proposals with and without IDs
async fn process_proposal_failed(
    event: &ContextualizedEvent<'_>,
    _height: u64,
    timestamp: DateTime<Utc>,
    _chain_id: &str,
    dbtx: &mut PgTransaction<'_>,
) -> Result<()> {
    let proposal_str = event.event.attributes
        .iter()
        .find(|attr| attr.key_str().ok() == Some("proposal"))
        .and_then(|attr| attr.value_str().ok());
    
    if let Some(proposal_json) = proposal_str {
        if let Ok(proposal_data) = serde_json::from_str::<Value>(proposal_json) {
            let proposal_id = extract_proposal_id_from_event_data(&proposal_data, "id");
            GovernanceProposal::update_state(proposal_id, "Finished", Some("Failed"), timestamp, dbtx).await?;
            debug!("Proposal {} failed", proposal_id);
        }
    }
    
    Ok(())
}

/// Process EventProposalSlashed - handles both proposals with and without IDs
async fn process_proposal_slashed(
    event: &ContextualizedEvent<'_>,
    _height: u64,
    timestamp: DateTime<Utc>,
    _chain_id: &str,
    dbtx: &mut PgTransaction<'_>,
) -> Result<()> {
    let proposal_str = event.event.attributes
        .iter()
        .find(|attr| attr.key_str().ok() == Some("proposal"))
        .and_then(|attr| attr.value_str().ok());
    
    if let Some(proposal_json) = proposal_str {
        if let Ok(proposal_data) = serde_json::from_str::<Value>(proposal_json) {
            let proposal_id = extract_proposal_id_from_event_data(&proposal_data, "id");
            GovernanceProposal::update_state(proposal_id, "Finished", Some("Slashed"), timestamp, dbtx).await?;
            debug!("Proposal {} slashed", proposal_id);
        }
    }
    
    Ok(())
}

/// Process EventProposalWithdraw
async fn process_proposal_withdraw(
    event: &ContextualizedEvent<'_>,
    height: u64,
    timestamp: DateTime<Utc>,
    _chain_id: &str,
    dbtx: &mut PgTransaction<'_>,
) -> Result<()> {
    let withdraw_str = event.event.attributes
        .iter()
        .find(|attr| attr.key_str().ok() == Some("withdraw"))
        .and_then(|attr| attr.value_str().ok());
    
    if let Some(withdraw_json) = withdraw_str {
        if let Ok(withdraw_data) = serde_json::from_str::<Value>(withdraw_json) {
            let proposal_id = extract_proposal_id_from_event_data(&withdraw_data, "proposal");
            GovernanceProposal::update_state_only(proposal_id, "Withdrawn", timestamp, dbtx).await?;
            debug!("Proposal {} withdrawn at height {}", proposal_id, height);
        }
    }
    
    Ok(())
}

/// Process EventProposalDepositClaim
async fn process_proposal_deposit_claim(
    event: &ContextualizedEvent<'_>,
    height: u64,
    timestamp: DateTime<Utc>,
    _chain_id: &str,
    dbtx: &mut PgTransaction<'_>,
) -> Result<()> {
    let claim_str = event.event.attributes
        .iter()
        .find(|attr| attr.key_str().ok() == Some("depositClaim"))
        .and_then(|attr| attr.value_str().ok());
    
    if let Some(claim_json) = claim_str {
        if let Ok(claim_data) = serde_json::from_str::<Value>(claim_json) {
            let proposal_id = extract_proposal_id_from_event_data(&claim_data, "proposal");
            GovernanceProposal::update_state_only(proposal_id, "Claimed", timestamp, dbtx).await?;
            debug!("Proposal {} deposit claimed at height {}", proposal_id, height);
        }
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_fraction_to_percentage() {
        assert_eq!(GovernanceParameters::parse_fraction_to_percentage("67/100").unwrap(), 67.00);
        assert_eq!(GovernanceParameters::parse_fraction_to_percentage("40/100").unwrap(), 40.00);
        assert_eq!(GovernanceParameters::parse_fraction_to_percentage("80/100").unwrap(), 80.00);
        assert_eq!(GovernanceParameters::parse_fraction_to_percentage("1/3").unwrap(), 33.33);
        assert_eq!(GovernanceParameters::parse_fraction_to_percentage("2/3").unwrap(), 66.67);
    }
}