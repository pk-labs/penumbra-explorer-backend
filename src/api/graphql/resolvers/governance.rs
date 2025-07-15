#![allow(clippy::missing_errors_doc)]
#![allow(clippy::single_match_else)]
#![allow(clippy::too_many_lines)]

use crate::api::graphql::context::ApiContext;
use crate::api::graphql::scalars::{DateTime, Decimal};
use crate::api::graphql::types::governance::{
    ActiveProposal, GovernanceParameters, PastProposal, PastProposalCollection, ProposalDetail,
    ProposalKind, ProposalOutcome, ProposalState, Vote, VoteForTransaction, VoteValue,
};
use crate::api::graphql::types::CollectionLimit;
use async_graphql::{Context, Result};
use sqlx::Row;
use tracing::error;

/// Get the current governance parameters for the chain
pub async fn resolve_governance_parameters(
    ctx: &Context<'_>,
) -> Result<Option<GovernanceParameters>> {
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
        ",
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

/// Get all past proposals (finished, withdrawn, claimed) with pagination
pub async fn resolve_past_proposals(
    ctx: &Context<'_>,
    limit: CollectionLimit,
) -> Result<PastProposalCollection> {
    let db = &ctx.data_unchecked::<ApiContext>().db;

    let where_clause = "WHERE state IN ('Finished', 'Withdrawn', 'Claimed')";

    let count_query = format!("SELECT COUNT(*) FROM governance_proposals {where_clause}");
    let total_count: i64 = sqlx::query_scalar(&count_query).fetch_one(db).await?;

    let length = limit.length.unwrap_or(10);
    let offset = limit.offset.unwrap_or(0);

    let rows = sqlx::query(
        r"
        SELECT 
            proposal_id,
            title,
            kind,
            state,
            outcome,
            total_votes,
            end_block_height,
            end_timestamp
        FROM governance_proposals
        WHERE state IN ('Finished', 'Withdrawn', 'Claimed')
        ORDER BY proposal_id DESC
        LIMIT $1 OFFSET $2
        ",
    )
    .bind(i64::from(length))
    .bind(i64::from(offset))
    .fetch_all(db)
    .await?;

    let mut proposals = Vec::new();
    for row in rows {
        let proposal_id: i64 = row.get("proposal_id");
        let title: String = row.get("title");
        let kind_str: String = row.get("kind");
        let state_str: String = row.get("state");
        let outcome_str: Option<String> = row.get("outcome");
        let total_votes: sqlx::types::BigDecimal = row.get("total_votes");
        let end_block_height: i64 = row.get("end_block_height");
        let end_timestamp: Option<sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>> =
            row.get("end_timestamp");

        let kind = ProposalKind::from_database_string(&kind_str);
        let state = ProposalState::from_database_string(&state_str);
        let outcome = ProposalOutcome::from_database_string(outcome_str.as_deref());
        let end_timestamp_wrapped = end_timestamp.map(DateTime);

        proposals.push(PastProposal {
            id: proposal_id,
            title,
            kind,
            state,
            outcome,
            total_votes: Decimal(total_votes),
            end_block_height,
            end_timestamp: end_timestamp_wrapped,
        });
    }

    Ok(PastProposalCollection {
        items: proposals,
        total: i32::try_from(total_count).unwrap_or(0),
    })
}

/// Get all active proposals (state = 'Voting')
pub async fn resolve_active_proposals(ctx: &Context<'_>) -> Result<Vec<ActiveProposal>> {
    let db = &ctx.data_unchecked::<ApiContext>().db;

    let rows = sqlx::query(
        r"
        SELECT 
            proposal_id,
            title,
            state,
            kind,
            end_block_height
        FROM governance_proposals
        WHERE state = 'Voting'
        ORDER BY proposal_id DESC
        ",
    )
    .fetch_all(db)
    .await?;

    let mut proposals = Vec::new();
    for row in rows {
        let proposal_id: i64 = row.get("proposal_id");
        let title: String = row.get("title");
        let state_str: String = row.get("state");
        let kind_str: String = row.get("kind");
        let end_block_height: i64 = row.get("end_block_height");

        let state = ProposalState::from_database_string(&state_str);
        let kind = ProposalKind::from_database_string(&kind_str);

        proposals.push(ActiveProposal {
            id: proposal_id,
            title,
            state,
            kind,
            end_block_height,
        });
    }

    Ok(proposals)
}

/// Get detailed proposal information by ID
pub async fn resolve_proposal_detail(ctx: &Context<'_>, id: i64) -> Result<Option<ProposalDetail>> {
    let db = &ctx.data_unchecked::<ApiContext>().db;

    let proposal_row = sqlx::query(
        r"
        SELECT 
            proposal_id,
            title,
            description,
            kind,
            state,
            outcome,
            deposit_amount,
            start_block_height,
            end_block_height,
            start_timestamp,
            end_timestamp,
            quorum,
            payload,
            total_votes,
            yes_votes,
            yes_votes_percentage,
            abstain_votes,
            abstain_votes_percentage,
            no_votes,
            no_votes_percentage
        FROM governance_proposals
        WHERE proposal_id = $1
        ",
    )
    .bind(id)
    .fetch_optional(db)
    .await?;

    let Some(proposal_row) = proposal_row else {
        return Ok(None);
    };

    let vote_rows = sqlx::query(
        r"
        SELECT 
            gv.validator_identity_key,
            gv.vote,
            gv.effective_voting_power,
            gv.voting_power_percentage,
            gv.voted_at,
            gv.tx_hash,
            v.name as validator_name,
            v.decoded_address
        FROM governance_votes gv
        LEFT JOIN validators v ON gv.validator_identity_key = v.identity_key
        WHERE gv.proposal_id = $1
        ORDER BY gv.effective_voting_power DESC
        ",
    )
    .bind(id)
    .fetch_all(db)
    .await?;

    let mut votes = Vec::new();
    for vote_row in vote_rows {
        let validator_identity_key: Option<String> = vote_row.get("validator_identity_key");
        let validator_name: Option<String> = vote_row.get("validator_name");
        let decoded_address: Option<String> = vote_row.get("decoded_address");
        let vote_str: String = vote_row.get("vote");
        let effective_voting_power: sqlx::types::BigDecimal =
            vote_row.get("effective_voting_power");
        let voting_power_percentage: sqlx::types::BigDecimal =
            vote_row.get("voting_power_percentage");
        let voted_at: sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc> =
            vote_row.get("voted_at");
        let tx_hash: Option<Vec<u8>> = vote_row.get("tx_hash");

        let (name, id) = if validator_identity_key.is_some() {
            let name = validator_name
                .or_else(|| decoded_address.clone())
                .unwrap_or_else(|| "Unknown Validator".to_string());
            let id = decoded_address;
            (name, id)
        } else {
            ("Delegator".to_string(), None)
        };

        let tx_hash_hex = tx_hash.map(hex::encode);

        votes.push(Vote {
            name,
            id,
            effective_voting_power: Decimal(effective_voting_power),
            voting_power_percentage: Decimal(voting_power_percentage),
            vote: VoteValue::from_database_string(&vote_str),
            voted_at: DateTime(voted_at),
            tx_hash: tx_hash_hex,
        });
    }

    let proposal_id: i64 = proposal_row.get("proposal_id");
    let title: String = proposal_row.get("title");
    let description: String = proposal_row
        .get::<String, _>("description")
        .trim()
        .to_string();
    let kind_str: String = proposal_row.get("kind");
    let state_str: String = proposal_row.get("state");
    let outcome_str: Option<String> = proposal_row.get("outcome");
    let deposit_amount: sqlx::types::BigDecimal = proposal_row.get("deposit_amount");
    let start_block_height: i64 = proposal_row.get("start_block_height");
    let end_block_height: i64 = proposal_row.get("end_block_height");
    let start_timestamp: sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc> =
        proposal_row.get("start_timestamp");
    let end_timestamp: Option<sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>> =
        proposal_row.get("end_timestamp");
    let quorum: sqlx::types::BigDecimal = proposal_row.get("quorum");
    let payload: serde_json::Value = proposal_row.get("payload");

    let total_votes: sqlx::types::BigDecimal = proposal_row.get("total_votes");
    let yes_votes: sqlx::types::BigDecimal = proposal_row.get("yes_votes");
    let yes_votes_percentage: sqlx::types::BigDecimal = proposal_row.get("yes_votes_percentage");
    let abstain_votes: sqlx::types::BigDecimal = proposal_row.get("abstain_votes");
    let abstain_votes_percentage: sqlx::types::BigDecimal =
        proposal_row.get("abstain_votes_percentage");
    let no_votes: sqlx::types::BigDecimal = proposal_row.get("no_votes");
    let no_votes_percentage: sqlx::types::BigDecimal = proposal_row.get("no_votes_percentage");

    let kind = ProposalKind::from_database_string(&kind_str);
    let state = ProposalState::from_database_string(&state_str);
    let outcome = ProposalOutcome::from_database_string(outcome_str.as_deref());

    Ok(Some(ProposalDetail {
        id: proposal_id,
        title,
        description,
        kind,
        deposit_amount: Decimal(deposit_amount),
        payload,
        voting_started_block_height: start_block_height,
        voting_started_timestamp: DateTime(start_timestamp),
        voting_ended_block_height: end_block_height,
        voting_ended_timestamp: end_timestamp.map(DateTime),
        state,
        outcome,
        total_votes: Decimal(total_votes),
        quorum: Decimal(quorum),
        yes_votes: Decimal(yes_votes),
        yes_votes_percentage: Decimal(yes_votes_percentage),
        abstain_votes: Decimal(abstain_votes),
        abstain_votes_percentage: Decimal(abstain_votes_percentage),
        no_votes: Decimal(no_votes),
        no_votes_percentage: Decimal(no_votes_percentage),
        votes,
    }))
}

/// Get vote information for a specific transaction hash
pub async fn resolve_vote_for_transaction(
    ctx: &Context<'_>,
    tx_hash: String,
) -> Result<Option<VoteForTransaction>> {
    let db = &ctx.data_unchecked::<ApiContext>().db;

    let Ok(tx_hash_bytes) = hex::decode(&tx_hash) else {
        return Ok(None);
    };

    let vote_row = sqlx::query(
        r"
        SELECT 
            gv.proposal_id,
            gv.validator_identity_key,
            gv.vote,
            gv.effective_voting_power,
            v.decoded_address
        FROM governance_votes gv
        LEFT JOIN validators v ON gv.validator_identity_key = v.identity_key
        WHERE gv.tx_hash = $1
        ",
    )
    .bind(&tx_hash_bytes)
    .fetch_optional(db)
    .await?;

    match vote_row {
        Some(row) => {
            let proposal_id: i64 = row.get("proposal_id");
            let validator_identity_key: Option<String> = row.get("validator_identity_key");
            let decoded_address: Option<String> = row.get("decoded_address");
            let vote_str: String = row.get("vote");
            let effective_voting_power: sqlx::types::BigDecimal = row.get("effective_voting_power");

            let id = if validator_identity_key.is_some() {
                decoded_address
            } else {
                None
            };

            Ok(Some(VoteForTransaction {
                id,
                vote: VoteValue::from_database_string(&vote_str),
                proposal: proposal_id,
                voting_power: Decimal(effective_voting_power),
            }))
        }
        None => Ok(None),
    }
}
