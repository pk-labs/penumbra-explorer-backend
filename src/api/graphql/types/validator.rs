use crate::api::graphql::types::{ValidatorFilter, ValidatorStateFilter};
use async_graphql::{Enum, Object, SimpleObject};
use chrono::{DateTime, Utc};
use sqlx::types::BigDecimal;
use sqlx::{FromRow, PgPool};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
#[allow(clippy::module_name_repetitions)]
pub enum ValidatorState {
    #[graphql(name = "VALIDATOR_STATE_ENUM_UNSPECIFIED")]
    Unspecified,
    #[graphql(name = "VALIDATOR_STATE_ENUM_ACTIVE")]
    Active,
    #[graphql(name = "VALIDATOR_STATE_ENUM_DEFINED")]
    Defined,
    #[graphql(name = "VALIDATOR_STATE_ENUM_DISABLED")]
    Disabled,
    #[graphql(name = "VALIDATOR_STATE_ENUM_INACTIVE")]
    Inactive,
    #[graphql(name = "VALIDATOR_STATE_ENUM_JAILED")]
    Jailed,
    #[graphql(name = "VALIDATOR_STATE_ENUM_TOMBSTONED")]
    Tombstoned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum BondingState {
    #[graphql(name = "BONDING_STATE_ENUM_UNSPECIFIED")]
    Unspecified,
    #[graphql(name = "BONDING_STATE_ENUM_BONDED")]
    Bonded,
    #[graphql(name = "BONDING_STATE_ENUM_UNBONDED")]
    Unbonded,
    #[graphql(name = "BONDING_STATE_ENUM_UNBONDING")]
    Unbonding,
}

impl ValidatorState {
    fn from_db_string(s: &str) -> Self {
        match s {
            s if s.contains("VALIDATOR_STATE_ENUM_ACTIVE") => Self::Active,
            s if s.contains("VALIDATOR_STATE_ENUM_DEFINED") => Self::Defined,
            s if s.contains("VALIDATOR_STATE_ENUM_DISABLED") => Self::Disabled,
            s if s.contains("VALIDATOR_STATE_ENUM_INACTIVE") => Self::Inactive,
            s if s.contains("VALIDATOR_STATE_ENUM_JAILED") => Self::Jailed,
            s if s.contains("VALIDATOR_STATE_ENUM_TOMBSTONED") => Self::Tombstoned,
            _ => Self::Unspecified,
        }
    }
}

impl BondingState {
    fn from_db_string(s: &str) -> Self {
        match s {
            s if s.contains("BONDING_STATE_ENUM_BONDED") => Self::Bonded,
            s if s.contains("BONDING_STATE_ENUM_UNBONDED") => Self::Unbonded,
            s if s.contains("BONDING_STATE_ENUM_UNBONDING") => Self::Unbonding,
            _ => Self::Unspecified,
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct Validator {
    pub id: String,
    pub name: Option<String>,
    pub state: ValidatorState,
    pub bonding_state: BondingState,
    pub voting_power: i64,
    pub voting_power_active_percentage: f64,
    pub uptime: Option<f64>,
    pub first_seen_time: Option<DateTime<Utc>>,
    pub commission: f64,
}

#[derive(Debug, Clone, SimpleObject)]
#[allow(clippy::module_name_repetitions)]
pub struct ValidatorSearchResult {
    pub id: String,
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
    pub id: String,
    pub name: Option<String>,
    pub website: Option<String>,
    pub description: Option<String>,
    pub state: ValidatorState,
    pub bonding_state: BondingState,
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
    pub total_staked: i64,
    pub active_validator_limit: i64,
    pub active_validator_count: i64,
    pub unbonding_delay: i64,
    pub uptime_blocks_window: i64,
    pub uptime_min_required: f64,
    pub slashing_penalty_downtime: f64,
    pub slashing_penalty_misbehavior: f64,
    pub min_validator_stake: i64,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct ChainParameters {
    pub chain_id: String,
    pub current_block_height: i64,
    pub current_block_time: DateTime<Utc>,
    pub current_epoch: i64,
    pub epoch_duration: i64,
    pub next_epoch_in: i64,
    pub last_updated: DateTime<Utc>,
}

#[derive(Debug, Clone)]
#[allow(clippy::module_name_repetitions)]
pub struct ValidatorHomepageData {
    pub validators: Vec<Validator>,
    pub staking_parameters: StakingParameters,
    pub chain_parameters: Option<ChainParameters>,
}

#[Object]
impl ValidatorHomepageData {
    async fn validators(&self) -> &Vec<Validator> {
        &self.validators
    }

    async fn staking_parameters(&self) -> &StakingParameters {
        &self.staking_parameters
    }

    async fn chain_parameters(&self) -> &Option<ChainParameters> {
        &self.chain_parameters
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

        let validators = Self::fetch_validators(pool, filter).await?;
        let staking_parameters = Self::fetch_staking_parameters(pool).await?;
        let chain_parameters = Self::fetch_chain_parameters(pool).await?;

        Ok(Self {
            validators,
            staking_parameters,
            chain_parameters,
        })
    }

    async fn fetch_validators(
        pool: &PgPool,
        filter: Option<ValidatorFilter>,
    ) -> async_graphql::Result<Vec<Validator>> {
        let where_clause = match filter.as_ref().and_then(|f| f.state) {
            Some(ValidatorStateFilter::Active) => "WHERE state = 'VALIDATOR_STATE_ENUM_ACTIVE'",
            Some(ValidatorStateFilter::Inactive) => "WHERE state != 'VALIDATOR_STATE_ENUM_ACTIVE'",
            Some(ValidatorStateFilter::All) | None => "",
        };

        let query = format!(
            r"
            SELECT 
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

        let validators: Vec<ValidatorRow> = sqlx::query_as(&query).fetch_all(pool).await?;

        Ok(validators
            .into_iter()
            .map(|row| Validator {
                id: row.decoded_address.unwrap_or_default(),
                name: row.name,
                state: ValidatorState::from_db_string(&row.state),
                bonding_state: row
                    .bonding_state
                    .as_deref()
                    .map_or(BondingState::Unspecified, BondingState::from_db_string),
                voting_power: row.voting_power,
                voting_power_active_percentage: row.voting_power_active_percentage,
                uptime: row.uptime_percentage,
                first_seen_time: row.first_seen_time,
                commission: row.commission_rate,
            })
            .collect())
    }

    async fn fetch_staking_parameters(pool: &PgPool) -> async_graphql::Result<StakingParameters> {
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
            ",
        )
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| async_graphql::Error::new("Staking parameters not found in database"))?;

        let active_count: i64 = sqlx::query_scalar(
            r"
            SELECT COUNT(*)
            FROM validators 
            WHERE state = 'VALIDATOR_STATE_ENUM_ACTIVE'
            ",
        )
        .fetch_one(pool)
        .await?;

        Ok(StakingParameters {
            total_staked: params.total_staked.to_string().parse::<i64>().unwrap_or(0),
            active_validator_limit: params.active_validator_limit,
            active_validator_count: active_count,
            unbonding_delay: params.unbonding_delay,
            uptime_blocks_window: params.uptime_blocks_window,
            uptime_min_required: params.uptime_min_required,
            slashing_penalty_downtime: params.slashing_penalty_downtime.unwrap_or(0.0),
            slashing_penalty_misbehavior: params.slashing_penalty_misbehavior,
            min_validator_stake: params
                .min_validator_stake
                .to_string()
                .parse::<i64>()
                .unwrap_or(0),
        })
    }

    async fn fetch_chain_parameters(
        pool: &PgPool,
    ) -> async_graphql::Result<Option<ChainParameters>> {
        let chain_params_row = sqlx::query_as::<_, ChainParametersRow>(
            r"
            SELECT 
                chain_id,
                current_block_height,
                current_block_time,
                current_epoch,
                epoch_duration,
                next_epoch_in,
                last_updated
            FROM 
                validator_chain_parameters
            LIMIT 1
            ",
        )
        .fetch_optional(pool)
        .await?;

        Ok(chain_params_row.map(|row| ChainParameters {
            chain_id: row.chain_id,
            current_block_height: row.current_block_height,
            current_block_time: row.current_block_time,
            current_epoch: row.current_epoch,
            epoch_duration: row.epoch_duration,
            next_epoch_in: row.next_epoch_in,
            last_updated: row.last_updated,
        }))
    }
}

#[derive(FromRow)]
struct ValidatorRow {
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
    total_staked: BigDecimal,
    active_validator_limit: i64,
    unbonding_delay: i64,
    uptime_blocks_window: i64,
    uptime_min_required: f64,
    slashing_penalty_downtime: Option<f64>,
    slashing_penalty_misbehavior: f64,
    min_validator_stake: BigDecimal,
}

#[derive(FromRow)]
struct ChainParametersRow {
    chain_id: String,
    current_block_height: i64,
    current_block_time: DateTime<Utc>,
    current_epoch: i64,
    epoch_duration: i64,
    next_epoch_in: i64,
    last_updated: DateTime<Utc>,
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
        let result: Option<(Option<String>, Option<String>)> = sqlx::query_as(
            r"
            SELECT 
                decoded_address,
                name
            FROM 
                validators
            WHERE 
                decoded_address = $1
            LIMIT 1
            ",
        )
        .bind(search_address)
        .fetch_optional(pool)
        .await?;

        match result {
            Some((decoded_address, name)) => {
                if let Some(addr) = decoded_address {
                    Ok(Some(Self {
                        id: addr.clone(),
                        display_name: name.unwrap_or(addr),
                    }))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    /// Searches for a validator by name
    ///
    /// # Errors
    ///
    /// Returns an error if the database query fails
    pub async fn search_by_name(
        pool: &PgPool,
        search_term: &str,
    ) -> async_graphql::Result<Option<Self>> {
        if search_term.len() < 2 {
            return Ok(None);
        }

        let starts_with_pattern = format!("{}%", search_term.to_lowercase());

        let result: Option<(Option<String>, Option<String>)> = sqlx::query_as(
            r"
            SELECT 
                decoded_address,
                name
            FROM 
                validators
            WHERE 
                name IS NOT NULL
                AND LOWER(TRIM(name)) LIKE $1  -- trim spaces and starts with only
            ORDER BY
                voting_power DESC
            LIMIT 1
            ",
        )
        .bind(&starts_with_pattern)
        .fetch_optional(pool)
        .await?;

        match result {
            Some((decoded_address, name)) => {
                if let Some(addr) = decoded_address {
                    Ok(Some(Self {
                        id: addr.clone(),
                        display_name: name.unwrap_or(addr),
                    }))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    /// Searches for all validators by name
    ///
    /// # Errors
    ///
    /// Returns an error if the database query fails
    pub async fn search_all_by_name(
        pool: &PgPool,
        search_term: &str,
    ) -> async_graphql::Result<Vec<Self>> {
        if search_term.len() < 2 {
            return Ok(Vec::new());
        }

        let starts_with_pattern = format!("{}%", search_term.to_lowercase());

        let results: Vec<(Option<String>, Option<String>)> = sqlx::query_as(
            r"
            SELECT 
                decoded_address,
                name
            FROM 
                validators
            WHERE 
                name IS NOT NULL
                AND LOWER(TRIM(name)) LIKE $1  -- trim spaces and starts with only
            ORDER BY
                voting_power DESC
            LIMIT 20
            ",
        )
        .bind(&starts_with_pattern)
        .fetch_all(pool)
        .await?;

        Ok(results
            .into_iter()
            .filter_map(|(decoded_address, name)| {
                decoded_address.map(|addr| Self {
                    id: addr.clone(),
                    display_name: name.unwrap_or(addr),
                })
            })
            .collect())
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
                v.decoded_address,
                v.name,
                v.website,
                v.description,
                v.state,
                v.bonding_state,
                vp.uptime_percentage::FLOAT8 as uptime_percentage,
                vp.total_tracked_blocks as uptime_block_window,
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
            ",
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
                validator_funding_streams vfs
            JOIN validators v ON v.identity_key = vfs.identity_key
            WHERE 
                v.decoded_address = $1
            ORDER BY
                stream_type, recipient_address
            ",
        )
        .bind(decoded_address)
        .fetch_all(pool)
        .await?;

        let current_height: i64 =
            sqlx::query_scalar("SELECT MAX(height) FROM explorer_block_details")
                .fetch_one(pool)
                .await?;

        let last_300_blocks: Vec<(i64, bool)> = sqlx::query_as(
            r"
            SELECT 
                vb.block_height,
                vb.signed
            FROM 
                validator_blocks vb
            JOIN validators v ON v.identity_key = vb.identity_key
            WHERE 
                v.decoded_address = $1
                AND vb.block_height > $2
            ORDER BY 
                vb.block_height DESC
            ",
        )
        .bind(decoded_address)
        .bind(current_height - 300)
        .fetch_all(pool)
        .await?;

        let last_300_blocks_array: Vec<BlockParticipation> = last_300_blocks
            .into_iter()
            .map(|(height, signed)| BlockParticipation { height, signed })
            .collect();

        Ok(Some(Self {
            id: info.decoded_address.clone().unwrap_or_default(),
            name: info.name,
            website: info.website,
            description: info.description,
            state: ValidatorState::from_db_string(&info.state),
            bonding_state: info
                .bonding_state
                .as_deref()
                .map_or(BondingState::Unspecified, BondingState::from_db_string),
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

#[derive(SimpleObject, Clone)]
#[graphql(rename_fields = "camelCase")]
#[allow(clippy::module_name_repetitions)]
pub struct ValidatorBlockUpdate {
    pub validator_id: String,
    pub block_height: i64,
    pub signed: bool,
}

#[derive(SimpleObject, Clone)]
#[graphql(rename_fields = "camelCase")]
pub struct ChainParametersUpdate {
    pub chain_id: String,
    pub current_block_height: i64,
    pub current_block_time: DateTime<Utc>,
    pub current_epoch: i64,
    pub epoch_duration: i64,
    pub next_epoch_in: i64,
    pub last_updated: DateTime<Utc>,
}
