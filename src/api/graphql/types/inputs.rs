use crate::api::graphql::types::RangeDirection;
use async_graphql::{Enum, InputObject};

#[derive(InputObject)]
pub struct BlockHeightRange {
    pub from: i32,
    pub to: i32,
}

#[derive(InputObject)]
pub struct LatestBlock {
    pub limit: i32,
}

#[derive(InputObject)]
pub struct BlocksSelector {
    pub latest: Option<LatestBlock>,
    pub range: Option<BlockHeightRange>,
}

#[derive(InputObject)]
pub struct LatestTransactions {
    pub limit: i32,
}

#[derive(InputObject)]
pub struct TransactionRange {
    pub from_tx_hash: String,
    pub direction: RangeDirection,
    pub limit: i32,
}

#[derive(InputObject)]
pub struct TransactionsSelector {
    pub latest: Option<LatestTransactions>,
    pub range: Option<TransactionRange>,
    pub client_id: Option<String>,
}

#[derive(InputObject)]
pub struct CollectionLimit {
    pub length: Option<i32>,
    pub offset: Option<i32>,
}

#[derive(InputObject)]
pub struct BlockFilter {
    pub height: Option<i32>,
}

#[derive(InputObject)]
pub struct TransactionFilter {
    pub hash: Option<String>,
    pub client_id: Option<String>,
    pub validator: Option<String>,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
pub enum TimePeriod {
    ALL,
    DAY,
    MONTH,
}

#[derive(InputObject)]
pub struct IbcStatsFilter {
    pub client_id: Option<String>,
    pub time_period: Option<TimePeriod>,
}

#[derive(InputObject, Default)]
pub struct IbcChannelFilter {
    pub client_id: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
pub enum ValidatorStateFilter {
    #[graphql(name = "ALL")]
    All,
    #[graphql(name = "ACTIVE")]
    Active,
    #[graphql(name = "INACTIVE")]
    Inactive,
}

#[derive(InputObject)]
pub struct ValidatorFilter {
    pub state: Option<ValidatorStateFilter>,
}
