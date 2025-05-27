use crate::api::graphql::types::validator::{ValidatorDetails, ValidatorHomepageData};
use crate::api::graphql::types::ValidatorFilter;
use async_graphql::Context;
use sqlx::PgPool;

/// Resolves validators homepage query
///
/// # Errors
///
/// Returns an error if database queries fail
pub async fn resolve_validators_homepage(
    ctx: &Context<'_>,
    filter: Option<ValidatorFilter>,
) -> async_graphql::Result<ValidatorHomepageData> {
    ValidatorHomepageData::fetch_homepage_data(ctx, filter).await
}

/// Resolves validator details query
///
/// # Errors
///
/// Returns an error if database queries fail
pub async fn resolve_validator_details(
    ctx: &Context<'_>,
    decoded_address: String,
) -> async_graphql::Result<Option<ValidatorDetails>> {
    let pool = ctx.data::<PgPool>()?;
    ValidatorDetails::get_by_address(pool, &decoded_address).await
}
