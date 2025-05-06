use crate::api::graphql::types::ibc::{ChannelPair, Stats, TotalShieldedVolume};
use async_graphql::{Context, Result};

/// Resolves IBC stats with optional filtering
///
/// # Errors
/// Returns an error if the database query fails
pub async fn resolve_ibc_stats(
    ctx: &Context<'_>,
    client_id: Option<String>,
    time_period: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<Vec<Stats>> {
    Stats::get_all(ctx, client_id, time_period, limit, offset).await
}

/// Resolves an IBC stats entry by `client_id`
///
/// # Errors
/// Returns an error if the database query fails
pub async fn resolve_ibc_stats_by_client_id(
    ctx: &Context<'_>,
    client_id: String,
    time_period: Option<String>,
) -> Result<Option<Stats>> {
    Stats::get_by_client_id(ctx, client_id, time_period).await
}

/// Resolves IBC channel pairs with optional filtering
///
/// # Errors
/// Returns an error if the database query fails
pub async fn resolve_ibc_channel_pairs(
    ctx: &Context<'_>,
    client_id: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<Vec<ChannelPair>> {
    ChannelPair::get_all(ctx, client_id, limit, offset).await
}

/// Resolves IBC channel pairs for a specific client ID
///
/// # Errors
/// Returns an error if the database query fails
pub async fn resolve_ibc_channel_pairs_by_client_id(
    ctx: &Context<'_>,
    client_id: String,
) -> Result<Vec<ChannelPair>> {
    ChannelPair::get_by_client_id(ctx, client_id).await
}

/// Resolves total shielded volume across all IBC clients
///
/// # Errors
/// Returns an error if the database query fails
pub async fn resolve_total_shielded_volume(ctx: &Context<'_>) -> Result<TotalShieldedVolume> {
    TotalShieldedVolume::get(ctx).await
}
