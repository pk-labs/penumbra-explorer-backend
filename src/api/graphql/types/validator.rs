use async_graphql::{Object, SimpleObject};
use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};
use crate::api::graphql::types::{ValidatorFilter, ValidatorStateFilter};

#[derive(Debug, Clone, SimpleObject)]
pub struct Validator {
    pub identity_key: String,
    pub decoded_address: Option<String>,
    pub name: Option<String>,
    pub state: String,
    pub bonding_state: Option<String>,
    pub voting_power: i64,
    pub voting_power_active_percentage: f64,
    pub uptime: Option<f64>,
    pub first_seen_time: Option<DateTime<Utc>>,
    pub commission: f64,
}

#[derive(Debug, Clone, SimpleObject)]
#[allow(clippy::module_name_repetitions)]
pub struct ValidatorSearchResult {
    pub identity_key: String,
    pub decoded_address: String,
    pub display_name: String,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct CommissionInfo {
    pub stream_type: String,
    pub recipient_address: Option<String>,
    pub rate_bps: i32,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct BlockParticipation {
    pub height: i64,
    pub signed: bool,
}

#[derive(Debug, Clone, SimpleObject)]
#[allow(clippy::module_name_repetitions)]
pub struct ValidatorDetails {
    pub id: String, // decoded_address
    pub identity_key: String,
    pub name: Option<String>,
    pub website: Option<String>,
    pub description: Option<String>,
    pub state: String,
    pub bonding_state: Option<String>,
    pub total_uptime: Option<f64>,
    pub uptime_block_window: i64,
    pub missed_blocks: i64,
    pub signed_blocks: i64,
    pub commission_percentage: f64,
    pub commission_streams: Vec<CommissionInfo>,
    pub voting_power: i64,
    pub voting_power_active_percentage: f64,
    pub active_since: Option<DateTime<Utc>>,
    pub last_300_blocks: Vec<BlockParticipation>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct StakingParameters {
    pub total_staked: String,
    pub active_validator_limit: i64,
    pub active_validator_count: i64,
    pub unbonding_delay: String,
    pub uptime_blocks_window: i64,
    pub uptime_min_required: String,
    pub slashing_penalty_downtime: String,
    pub slashing_penalty_misbehavior: String,
    pub min_validator_stake: String,
}

#[derive(Debug, Clone)]
#[allow(clippy::module_name_repetitions)]
pub struct ValidatorHomepageData {
    pub validators: Vec<Validator>,
    pub staking_parameters: StakingParameters,
}

#[Object]
impl ValidatorHomepageData {
    async fn validators(&self) -> &Vec<Validator> {
        &self.validators
    }

    async fn staking_parameters(&self) -> &StakingParameters {
        &self.staking_parameters
    }
}

impl ValidatorHomepageData {
    /// Fetches homepage data for validators
    /// 
    /// # Errors
    /// 
    /// Returns an error if database queries fail
    pub async fn fetch_homepage_data(
        ctx: &async_graphql::Context<'_>,
        filter: Option<ValidatorFilter>,
    ) -> async_graphql::Result<Self> {
        let pool = ctx.data::<PgPool>()?;

        let where_clause = match filter.as_ref().and_then(|f| f.state) {
            Some(ValidatorStateFilter::Active) => "WHERE state LIKE '%ACTIVE%'",
            Some(ValidatorStateFilter::Inactive) => "WHERE state NOT LIKE '%ACTIVE%'",
            Some(ValidatorStateFilter::All) | None => "",
        };

        let query = format!(
            r"
            SELECT 
                identity_key,
                decoded_address,
                name,
                state,
                bonding_state,
                voting_power,
                voting_power_active_percentage,
                uptime_percentage::FLOAT8 as uptime_percentage,
                first_seen_time,
                commission_rate::FLOAT8 as commission_rate
            FROM 
                validator_performance
            {where_clause}
            ORDER BY 
                voting_power DESC
            "
        );
        
        let validators: Vec<ValidatorRow> = sqlx::query_as(&query)
        .fetch_all(pool)
        .await?;

        let params = sqlx::query_as::<_, StakingParamsRow>(
            r"
            SELECT 
                total_staked,
                active_validator_limit,
                unbonding_delay,
                uptime_blocks_window,
                uptime_min_required,
                slashing_penalty_downtime,
                slashing_penalty_misbehavior,
                min_validator_stake
            FROM 
                validator_staking_parameters
            LIMIT 1
            "
        )
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| {
            async_graphql::Error::new("Staking parameters not found in database")
        })?;

        let active_count: i64 = sqlx::query_scalar(
            r"
            SELECT COUNT(*)
            FROM validators 
            WHERE state LIKE '%ACTIVE%'
            "
        )
        .fetch_one(pool)
        .await?;

        let validators = validators
            .into_iter()
            .map(|row| Validator {
                identity_key: row.identity_key,
                decoded_address: row.decoded_address,
                name: row.name,
                state: row.state,
                bonding_state: row.bonding_state,
                voting_power: row.voting_power,
                voting_power_active_percentage: row.voting_power_active_percentage,
                uptime: row.uptime_percentage,
                first_seen_time: row.first_seen_time,
                commission: row.commission_rate,
            })
            .collect();

        let staking_parameters = StakingParameters {
            total_staked: params.total_staked,
            active_validator_limit: params.active_validator_limit,
            active_validator_count: active_count,
            unbonding_delay: params.unbonding_delay,
            uptime_blocks_window: params.uptime_blocks_window,
            uptime_min_required: params.uptime_min_required,
            slashing_penalty_downtime: params.slashing_penalty_downtime.unwrap_or_default(),
            slashing_penalty_misbehavior: params.slashing_penalty_misbehavior,
            min_validator_stake: params.min_validator_stake,
        };

        Ok(Self {
            validators,
            staking_parameters,
        })
    }
}

#[derive(FromRow)]
struct ValidatorRow {
    identity_key: String,
    decoded_address: Option<String>,
    name: Option<String>,
    state: String,
    bonding_state: Option<String>,
    voting_power: i64,
    voting_power_active_percentage: f64,
    uptime_percentage: Option<f64>,
    first_seen_time: Option<DateTime<Utc>>,
    commission_rate: f64,
}

#[derive(FromRow)]
struct StakingParamsRow {
    total_staked: String,
    active_validator_limit: i64,
    unbonding_delay: String,
    uptime_blocks_window: i64,
    uptime_min_required: String,
    slashing_penalty_downtime: Option<String>,
    slashing_penalty_misbehavior: String,
    min_validator_stake: String,
}

impl ValidatorSearchResult {
    /// Searches for a validator by decoded address
    /// 
    /// # Errors
    /// 
    /// Returns an error if the database query fails
    pub async fn search_by_address(
        pool: &PgPool,
        search_address: &str,
    ) -> async_graphql::Result<Option<Self>> {
        let result: Option<(String, Option<String>, Option<String>)> = sqlx::query_as(
            r"
            SELECT 
                identity_key,
                decoded_address,
                name
            FROM 
                validators
            WHERE 
                decoded_address = $1
            LIMIT 1
            "
        )
        .bind(search_address)
        .fetch_optional(pool)
        .await?;

        match result {
            Some((identity_key, decoded_address, name)) => {
                if let Some(addr) = decoded_address {
                    Ok(Some(Self {
                        identity_key,
                        decoded_address: addr.clone(),
                        display_name: name.unwrap_or(addr),
                    }))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }
}

impl ValidatorDetails {
    /// Gets validator details by decoded address
    /// 
    /// # Errors
    /// 
    /// Returns an error if database queries fail
    #[allow(clippy::too_many_lines)]
    pub async fn get_by_address(
        pool: &PgPool,
        decoded_address: &str,
    ) -> async_graphql::Result<Option<Self>> {
        let validator_info: Option<ValidatorDetailsRow> = sqlx::query_as(
            r"
            SELECT 
                v.identity_key,
                v.decoded_address,
                v.name,
                v.website,
                v.description,
                v.state,
                v.bonding_state,
                vp.uptime_percentage::FLOAT8 as uptime_percentage,
                vp.blocks_in_window as uptime_block_window,
                vp.missed_blocks,
                vp.signed_blocks,
                vp.commission_rate::FLOAT8 as commission_rate,
                v.voting_power,
                v.voting_power_active_percentage,
                v.first_seen_time
            FROM 
                validator_performance vp
            JOIN
                validators v ON v.identity_key = vp.identity_key
            WHERE 
                v.decoded_address = $1
            LIMIT 1
            "
        )
        .bind(decoded_address)
        .fetch_optional(pool)
        .await?;

        let Some(info) = validator_info else {
            return Ok(None);
        };

        let commission_streams: Vec<CommissionStreamRow> = sqlx::query_as(
            r"
            SELECT 
                stream_type,
                recipient_address,
                rate_bps
            FROM 
                validator_funding_streams
            WHERE 
                identity_key = $1
            ORDER BY
                stream_type, recipient_address
            "
        )
        .bind(&info.identity_key)
        .fetch_all(pool)
        .await?;

        let current_height: i64 = sqlx::query_scalar(
            "SELECT MAX(height) FROM explorer_block_details"
        )
        .fetch_one(pool)
        .await?;

        let last_300_blocks: Vec<(i64, bool)> = sqlx::query_as(
            r"
            SELECT 
                block_height,
                signed
            FROM 
                validator_blocks
            WHERE 
                identity_key = $1
                AND block_height > $2
            ORDER BY 
                block_height DESC
            "
        )
        .bind(&info.identity_key)
        .bind(current_height - 300)
        .fetch_all(pool)
        .await?;

        let last_300_blocks_array: Vec<BlockParticipation> = last_300_blocks
            .into_iter()
            .map(|(height, signed)| BlockParticipation { height, signed })
            .collect();

        Ok(Some(Self {
            id: info.decoded_address.clone().unwrap_or_default(),
            identity_key: info.identity_key,
            name: info.name,
            website: info.website,
            description: info.description,
            state: info.state,
            bonding_state: info.bonding_state,
            total_uptime: info.uptime_percentage,
            uptime_block_window: info.uptime_block_window,
            missed_blocks: info.missed_blocks,
            signed_blocks: info.signed_blocks,
            commission_percentage: info.commission_rate,
            commission_streams: commission_streams
                .into_iter()
                .map(|cs| CommissionInfo {
                    stream_type: cs.stream_type,
                    recipient_address: cs.recipient_address,
                    rate_bps: cs.rate_bps,
                })
                .collect(),
            voting_power: info.voting_power,
            voting_power_active_percentage: info.voting_power_active_percentage,
            active_since: info.first_seen_time,
            last_300_blocks: last_300_blocks_array,
        }))
    }
}

#[derive(FromRow)]
struct ValidatorDetailsRow {
    identity_key: String,
    decoded_address: Option<String>,
    name: Option<String>,
    website: Option<String>,
    description: Option<String>,
    state: String,
    bonding_state: Option<String>,
    uptime_percentage: Option<f64>,
    uptime_block_window: i64,
    missed_blocks: i64,
    signed_blocks: i64,
    commission_rate: f64,
    voting_power: i64,
    voting_power_active_percentage: f64,
    first_seen_time: Option<DateTime<Utc>>,
}

#[derive(FromRow)]
struct CommissionStreamRow {
    stream_type: String,
    recipient_address: Option<String>,
    rate_bps: i32,
}