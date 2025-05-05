use crate::api::graphql::types::ibc::{ChannelPair, Stats, TotalShieldedVolume};
use async_graphql::{Context, Result};





pub async fn resolve_ibc_stats(
    ctx: &Context<'_>,
    client_id: Option<String>,
    time_period: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<Vec<Stats>> {
    Stats::get_all(ctx, client_id, time_period, limit, offset).await
}





pub async fn resolve_ibc_stats_by_client_id(
    ctx: &Context<'_>,
    client_id: String,
    time_period: Option<String>,
) -> Result<Option<Stats>> {
    Stats::get_by_client_id(ctx, client_id, time_period).await
}





pub async fn resolve_ibc_channel_pairs(
    ctx: &Context<'_>,
    client_id: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<Vec<ChannelPair>> {
    ChannelPair::get_all(ctx, client_id, limit, offset).await
}





pub async fn resolve_ibc_channel_pairs_by_client_id(
    ctx: &Context<'_>,
    client_id: String,
) -> Result<Vec<ChannelPair>> {
    ChannelPair::get_by_client_id(ctx, client_id).await
}





pub async fn resolve_total_shielded_volume(ctx: &Context<'_>) -> Result<TotalShieldedVolume> {
    TotalShieldedVolume::get(ctx).await
}
