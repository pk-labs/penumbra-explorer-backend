use crate::api::graphql::{
    resolvers::{block::get, transaction::resolve_transaction},
    types::{SearchResult, ValidatorSearchResult},
};
use async_graphql::{Context, Result};
use sqlx::PgPool;

/// Resolves a search request by slug
///
/// # Errors
/// Returns an error if database queries fail
#[allow(clippy::module_name_repetitions)]
pub async fn resolve_search(ctx: &Context<'_>, slug: String) -> Result<Option<SearchResult>> {
    let pool = ctx.data::<PgPool>()?;

    if let Ok(height) = slug.parse::<i32>() {
        if let Some(block) = get(ctx, height).await? {
            return Ok(Some(SearchResult::Block(block)));
        }
    }

    if let Some(tx) = resolve_transaction(ctx, slug.clone()).await? {
        return Ok(Some(SearchResult::Transaction(tx)));
    }

    if let Some(validator) = ValidatorSearchResult::search_by_address(pool, &slug).await? {
        return Ok(Some(SearchResult::Validator(validator)));
    }

    Ok(None)
}
