use crate::api::graphql::context::ApiContext;
use crate::api::graphql::types::governance::GovernanceParameters;
use crate::api::graphql::scalars::Decimal;
use async_graphql::{Context, Result};
use sqlx::Row;
use tracing::error;

/// Get the current governance parameters for the chain
pub async fn resolve_governance_parameters(ctx: &Context<'_>) -> Result<Option<GovernanceParameters>> {
    let db = &ctx.data_unchecked::<ApiContext>().db;

    let row = sqlx::query(
        r"
        SELECT 
            valid_quorum,
            passing_threshold,
            slashing_threshold,
            deposit_amount,
            proposal_voting_blocks
        FROM governance_parameters
        ORDER BY updated_height DESC
        LIMIT 1
        "
    )
    .fetch_optional(db)
    .await?;

    match row {
        Some(row) => {
            let valid_quorum: sqlx::types::BigDecimal = row.get("valid_quorum");
            let passing_threshold: sqlx::types::BigDecimal = row.get("passing_threshold");
            let slashing_threshold: sqlx::types::BigDecimal = row.get("slashing_threshold");
            let deposit_amount: sqlx::types::BigDecimal = row.get("deposit_amount");
            let proposal_duration: i64 = row.get("proposal_voting_blocks");

            Ok(Some(GovernanceParameters {
                valid_quorum: Decimal(valid_quorum),
                passing_threshold: Decimal(passing_threshold),
                slashing_threshold: Decimal(slashing_threshold),
                deposit_amount: Decimal(deposit_amount),
                proposal_duration,
            }))
        }
        None => {
            error!("No governance parameters found in database");
            Ok(None)
        }
    }
}