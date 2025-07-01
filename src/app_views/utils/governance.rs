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
    pub validator_quorum: f64,
    pub proposal_voting_blocks: i64,
    pub updated_height: i64,
    pub updated_at: DateTime<Utc>,
    pub raw_params: Value,
}

impl GovernanceParameters {
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
        
        let validator_quorum = Self::parse_fraction_to_percentage(
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
            validator_quorum,
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
                validator_quorum,
                proposal_voting_blocks,
                updated_height,
                updated_at,
                raw_params
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (chain_id) DO UPDATE SET
                deposit_amount = EXCLUDED.deposit_amount,
                passing_threshold = EXCLUDED.passing_threshold,
                slashing_threshold = EXCLUDED.slashing_threshold,
                validator_quorum = EXCLUDED.validator_quorum,
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
        .bind(BigDecimal::from_str(&format!("{:.2}", self.validator_quorum))?)
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
            self.validator_quorum,
            self.proposal_voting_blocks
        );
        
        Ok(())
    }
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
    
    for event in events {
        match event.event.kind.as_str() {
            "penumbra.core.app.v1.EventAppParametersChange" => {
                debug!("Processing EventAppParametersChange");
                if let Err(e) = process_app_parameters_change(event, height, timestamp, chain_id, dbtx).await {
                    error!("Error processing EventAppParametersChange: {}", e);
                }
            }
            // We'll add more event types here later
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