use crate::api::graphql::types::ibc::{Stats, TotalShieldedVolume};
use crate::api::graphql::types::inputs::TimePeriod;
use async_graphql::{Context, Result};

/// Resolves IBC stats with optional filtering
///
/// # Errors
/// Returns an error if the database query fails
pub async fn resolve_ibc_stats(
    ctx: &Context<'_>,
    client_id: Option<String>,
    time_period: Option<TimePeriod>,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<Vec<Stats>> {
    let period_str = match time_period {
        Some(TimePeriod::DAY) => Some("24h".to_string()),
        Some(TimePeriod::MONTH) => Some("30d".to_string()),
        Some(TimePeriod::ALL) | None => None,
    };

    Stats::get_all(ctx, client_id, period_str, limit, offset).await
}

/// Resolves total shielded volume across all IBC clients
///
/// # Errors
/// Returns an error if the database query fails
pub async fn resolve_total_shielded_volume(ctx: &Context<'_>) -> Result<TotalShieldedVolume> {
    TotalShieldedVolume::get(ctx).await
}
