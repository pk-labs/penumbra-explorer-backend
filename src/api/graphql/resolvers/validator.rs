use crate::api::graphql::types::validator::ValidatorHomepageData;
use crate::api::graphql::types::ValidatorFilter;
use async_graphql::Context;

pub async fn resolve_validators_homepage(
    ctx: &Context<'_>,
    filter: Option<ValidatorFilter>,
) -> async_graphql::Result<ValidatorHomepageData> {
    ValidatorHomepageData::fetch_homepage_data(ctx, filter).await
}