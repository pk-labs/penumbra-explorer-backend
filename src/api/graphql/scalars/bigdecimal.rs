use async_graphql::{InputValueError, InputValueResult, Scalar, ScalarType, Value};
use sqlx::types::BigDecimal;
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;

use crate::api::graphql::resolvers::{QueryRoot, SubscriptionRoot};
use async_graphql::{EmptyMutation, SchemaBuilder};


#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Decimal(pub BigDecimal);

impl Deref for Decimal {
    type Target = BigDecimal;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<BigDecimal> for Decimal {
    fn from(bd: BigDecimal) -> Self {
        Decimal(bd)
    }
}

impl From<Decimal> for BigDecimal {
    fn from(d: Decimal) -> Self {
        d.0
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[Scalar]
impl ScalarType for Decimal {
    fn parse(value: Value) -> InputValueResult<Self> {
        match value {
            Value::String(s) => BigDecimal::from_str(&s)
                .map(Decimal)
                .map_err(|e| InputValueError::custom(format!("Invalid decimal: {e}"))),
            Value::Number(ref n) => {
                if let Some(num) = n.as_i64() {
                    Ok(Decimal(BigDecimal::from(num)))
                } else if let Some(num) = n.as_f64() {
                    BigDecimal::from_str(&num.to_string())
                        .map(Decimal)
                        .map_err(|e| InputValueError::custom(format!("Invalid decimal: {e}")))
                } else {
                    Err(InputValueError::expected_type(value))
                }
            }
            _ => Err(InputValueError::expected_type(value)),
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}


pub fn register(
    builder: SchemaBuilder<QueryRoot, EmptyMutation, SubscriptionRoot>,
) -> SchemaBuilder<QueryRoot, EmptyMutation, SubscriptionRoot> {
    builder
}
