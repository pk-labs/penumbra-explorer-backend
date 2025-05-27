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
pub struct ValidatorSearchResult {
    pub identity_key: String,
    pub decoded_address: String,
    pub display_name: String,
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
            r#"
            SELECT 
                identity_key,
                decoded_address,
                name,
                state,
                bonding_state,
                voting_power,
                voting_power_percentage,
                uptime_percentage::FLOAT8 as uptime_percentage,
                first_seen_time,
                commission_rate::FLOAT8 as commission_rate
            FROM 
                validator_performance
            {}
            ORDER BY 
                voting_power DESC
            "#,
            where_clause
        );
        
        let validators: Vec<ValidatorRow> = sqlx::query_as(&query)
        .fetch_all(pool)
        .await?;

        let params = sqlx::query_as::<_, StakingParamsRow>(
            r#"
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
            "#
        )
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| {
            async_graphql::Error::new("Staking parameters not found in database")
        })?;

        // Count active validators
        let active_count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM validators 
            WHERE state LIKE '%ACTIVE%'
            "#
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
                voting_power_active_percentage: row.voting_power_percentage,
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

// Helper struct for database query
#[derive(FromRow)]
struct ValidatorRow {
    identity_key: String,
    decoded_address: Option<String>,
    name: Option<String>,
    state: String,
    bonding_state: Option<String>,
    voting_power: i64,
    voting_power_percentage: f64,
    uptime_percentage: Option<f64>,
    first_seen_time: Option<DateTime<Utc>>,
    commission_rate: f64,
}

// Helper struct for staking parameters query
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
    pub async fn search_by_address(
        pool: &PgPool,
        search_address: &str,
    ) -> async_graphql::Result<Option<Self>> {
        // Search for validator by decoded address
        let result: Option<(String, Option<String>, Option<String>)> = sqlx::query_as(
            r#"
            SELECT 
                identity_key,
                decoded_address,
                name
            FROM 
                validators
            WHERE 
                decoded_address = $1
            LIMIT 1
            "#
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