use crate::api::graphql::scalars::DateTime;
use async_graphql::SimpleObject;

#[derive(SimpleObject, Clone)]
pub struct BlockUpdate {
    pub height: i64,
    pub created_at: DateTime,
    pub transactions_count: i32,
}

#[derive(SimpleObject)]
pub struct TransactionUpdate {
    pub id: i64,
    pub hash: String,
    pub raw: String,
}

#[derive(SimpleObject)]
pub struct TransactionCountUpdate {
    pub count: i64,
}

#[derive(SimpleObject)]
#[graphql(rename_fields = "camelCase")]
pub struct IbcTransactionUpdate {
    pub tx_hash: String,

    pub client_id: String,

    pub status: String,

    pub block_height: i64,

    pub timestamp: DateTime,

    pub is_status_update: bool,

    pub raw: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(rename_fields = "camelCase")]
pub struct TotalShieldedVolumeUpdate {
    pub value: String,
}
