use crate::api::graphql::types::validator::{ValidatorHomepageData, ValidatorDetails};
use crate::api::graphql::types::ValidatorFilter;
use async_graphql::Context;
use sqlx::PgPool;

pub async fn resolve_validators_homepage(
    ctx: &Context<'_>,
    filter: Option<ValidatorFilter>,
) -> async_graphql::Result<ValidatorHomepageData> {
    ValidatorHomepageData::fetch_homepage_data(ctx, filter).await
}

pub async fn resolve_validator_details(
    ctx: &Context<'_>,
    decoded_address: String,
) -> async_graphql::Result<Option<ValidatorDetails>> {
    let pool = ctx.data::<PgPool>()?;
    ValidatorDetails::get_by_address(pool, &decoded_address).await
}