mod block;
mod ibc;
mod search;
mod stats;
mod subscription;
mod transaction;

use async_graphql::Object;

pub use block::{get as resolve_block, resolve_blocks, resolve_blocks_collection};
pub use ibc::{
    resolve_ibc_channel_pairs, resolve_ibc_channel_pairs_by_client_id, resolve_ibc_stats,
    resolve_ibc_stats_by_client_id, resolve_total_shielded_volume,
}; 
pub use search::resolve_search;
pub use stats::resolve_stats;
pub use subscription::Root as SubscriptionRoot;
pub use transaction::{resolve_transaction, resolve_transactions, resolve_transactions_collection};


pub struct QueryRoot;

#[Object]
impl QueryRoot {
    
    async fn block(
        &self,
        ctx: &async_graphql::Context<'_>,
        height: i32,
    ) -> async_graphql::Result<Option<crate::api::graphql::types::Block>> {
        resolve_block(ctx, height).await
    }

    
    async fn blocks(
        &self,
        ctx: &async_graphql::Context<'_>,
        selector: crate::api::graphql::types::BlocksSelector,
    ) -> async_graphql::Result<Vec<crate::api::graphql::types::Block>> {
        resolve_blocks(ctx, selector).await
    }

    
    async fn blocks_collection(
        &self,
        ctx: &async_graphql::Context<'_>,
        limit: crate::api::graphql::types::CollectionLimit,
        filter: Option<crate::api::graphql::types::BlockFilter>,
    ) -> async_graphql::Result<crate::api::graphql::types::BlockCollection> {
        resolve_blocks_collection(ctx, limit, filter).await
    }

    
    async fn transaction(
        &self,
        ctx: &async_graphql::Context<'_>,
        hash: String,
    ) -> async_graphql::Result<Option<crate::api::graphql::types::Transaction>> {
        resolve_transaction(ctx, hash).await
    }

    
    async fn transactions(
        &self,
        ctx: &async_graphql::Context<'_>,
        selector: crate::api::graphql::types::TransactionsSelector,
    ) -> async_graphql::Result<Vec<crate::api::graphql::types::Transaction>> {
        resolve_transactions(ctx, selector).await
    }

    
    async fn transactions_collection(
        &self,
        ctx: &async_graphql::Context<'_>,
        limit: crate::api::graphql::types::CollectionLimit,
        filter: Option<crate::api::graphql::types::TransactionFilter>,
    ) -> async_graphql::Result<crate::api::graphql::types::TransactionCollection> {
        resolve_transactions_collection(ctx, limit, filter).await
    }

    
    async fn search(
        &self,
        ctx: &async_graphql::Context<'_>,
        slug: String,
    ) -> async_graphql::Result<Option<crate::api::graphql::types::SearchResult>> {
        resolve_search(ctx, slug).await
    }

    
    async fn stats(
        &self,
        ctx: &async_graphql::Context<'_>,
    ) -> async_graphql::Result<crate::api::graphql::types::Stats> {
        resolve_stats(ctx).await
    }

    
    
    async fn db_block(
        &self,
        ctx: &async_graphql::Context<'_>,
        height: i64,
    ) -> async_graphql::Result<Option<crate::api::graphql::types::DbBlock>> {
        crate::api::graphql::types::DbBlock::get_by_height(ctx, height).await
    }

    
    async fn db_blocks(
        &self,
        ctx: &async_graphql::Context<'_>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> async_graphql::Result<Vec<crate::api::graphql::types::DbBlock>> {
        crate::api::graphql::types::DbBlock::get_all(ctx, limit, offset).await
    }

    
    async fn db_latest_block(
        &self,
        ctx: &async_graphql::Context<'_>,
    ) -> async_graphql::Result<Option<crate::api::graphql::types::DbBlock>> {
        crate::api::graphql::types::DbBlock::get_latest(ctx).await
    }

    
    async fn db_raw_transaction(
        &self,
        ctx: &async_graphql::Context<'_>,
        tx_hash_hex: String,
    ) -> async_graphql::Result<Option<crate::api::graphql::types::DbRawTransaction>> {
        crate::api::graphql::types::DbRawTransaction::get_by_hash(ctx, tx_hash_hex).await
    }

    
    async fn db_raw_transactions(
        &self,
        ctx: &async_graphql::Context<'_>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> async_graphql::Result<Vec<crate::api::graphql::types::DbRawTransaction>> {
        crate::api::graphql::types::DbRawTransaction::get_all(ctx, limit, offset, None).await
    }

    
    async fn ibc_stats(
        &self,
        ctx: &async_graphql::Context<'_>,
        client_id: Option<String>,
        time_period: Option<String>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> async_graphql::Result<Vec<crate::api::graphql::types::IbcStats>> {
        resolve_ibc_stats(ctx, client_id, time_period, limit, offset).await
    }

    
    async fn ibc_stats_by_client_id(
        &self,
        ctx: &async_graphql::Context<'_>,
        client_id: String,
        time_period: Option<String>,
    ) -> async_graphql::Result<Option<crate::api::graphql::types::IbcStats>> {
        resolve_ibc_stats_by_client_id(ctx, client_id, time_period).await
    }

    
    async fn ibc_channel_pairs(
        &self,
        ctx: &async_graphql::Context<'_>,
        client_id: Option<String>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> async_graphql::Result<Vec<crate::api::graphql::types::ibc::ChannelPair>> {
        resolve_ibc_channel_pairs(ctx, client_id, limit, offset).await
    }

    
    async fn ibc_channel_pairs_by_client_id(
        &self,
        ctx: &async_graphql::Context<'_>,
        client_id: String,
    ) -> async_graphql::Result<Vec<crate::api::graphql::types::ibc::ChannelPair>> {
        resolve_ibc_channel_pairs_by_client_id(ctx, client_id).await
    }

    
    async fn ibc_total_shielded_volume(
        &self,
        ctx: &async_graphql::Context<'_>,
    ) -> async_graphql::Result<crate::api::graphql::types::ibc::TotalShieldedVolume> {
        resolve_total_shielded_volume(ctx).await
    }
}
