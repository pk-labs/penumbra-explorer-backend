mod bigdecimal;
mod datetime;

use crate::api::graphql::resolvers::{QueryRoot, SubscriptionRoot};
use async_graphql::{EmptyMutation, SchemaBuilder};

#[must_use]
#[allow(clippy::module_name_repetitions)]
pub fn register_scalars(
    builder: SchemaBuilder<QueryRoot, EmptyMutation, SubscriptionRoot>,
) -> SchemaBuilder<QueryRoot, EmptyMutation, SubscriptionRoot> {
    let builder = datetime::register(builder);
    bigdecimal::register(builder)
}

pub use bigdecimal::Decimal;
pub use datetime::DateTime;
