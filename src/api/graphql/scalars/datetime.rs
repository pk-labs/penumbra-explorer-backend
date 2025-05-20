use crate::api::graphql::resolvers::{QueryRoot, SubscriptionRoot};
use async_graphql::{
    EmptyMutation, InputValueError, InputValueResult, Scalar, ScalarType, SchemaBuilder, Value,
};
use sqlx::types::chrono::{DateTime as ChronoDateTime, Utc};

#[derive(Clone, Debug)]
pub struct DateTime(pub ChronoDateTime<Utc>);

#[Scalar]
impl ScalarType for DateTime {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(s) = value {
            match s.parse::<ChronoDateTime<Utc>>() {
                Ok(dt) => Ok(DateTime(dt)),
                Err(_) => Err(InputValueError::custom("Invalid DateTime format")),
            }
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_rfc3339())
    }
}

impl From<ChronoDateTime<Utc>> for DateTime {
    fn from(dt: ChronoDateTime<Utc>) -> Self {
        DateTime(dt)
    }
}

impl From<DateTime> for ChronoDateTime<Utc> {
    fn from(dt: DateTime) -> Self {
        dt.0
    }
}

pub fn register(
    builder: SchemaBuilder<QueryRoot, EmptyMutation, SubscriptionRoot>,
) -> SchemaBuilder<QueryRoot, EmptyMutation, SubscriptionRoot> {
    builder
}
