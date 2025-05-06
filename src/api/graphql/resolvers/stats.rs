use crate::api::graphql::{context::ApiContext, types::Stats};
use async_graphql::{Context, Result};

/// Resolves statistics information
///
/// # Errors
/// Returns an error if database queries fail
#[allow(clippy::module_name_repetitions)]
pub async fn resolve_stats(ctx: &Context<'_>) -> Result<Stats> {
    let db = &ctx.data_unchecked::<ApiContext>().db;

    let result = sqlx::query_as::<_, (i64,)>("SELECT COUNT(*) as count FROM explorer_transactions")
        .fetch_one(db)
        .await?;

    Ok(Stats {
        total_transactions_count: result.0,
    })
}
