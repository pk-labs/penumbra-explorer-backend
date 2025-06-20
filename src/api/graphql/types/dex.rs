use crate::api::graphql::scalars::DateTime;
use async_graphql::{Object, SimpleObject};

#[derive(Debug, Clone)]
pub struct LiquidityPosition {
    pub position_id: String,
    pub trading_pair_asset1: String,
    pub trading_pair_asset2: String,
    pub reserves1_amount: String,
    pub reserves2_amount: String,
    pub state: String,
    pub fee_percentage: f64,
    pub updated_at: DateTime,
}

#[Object]
impl LiquidityPosition {
    async fn position_id(&self) -> &str {
        &self.position_id
    }

    async fn trading_pair_asset1(&self) -> &str {
        &self.trading_pair_asset1
    }

    async fn trading_pair_asset2(&self) -> &str {
        &self.trading_pair_asset2
    }

    async fn reserves1_amount(&self) -> &str {
        &self.reserves1_amount
    }

    async fn reserves2_amount(&self) -> &str {
        &self.reserves2_amount
    }

    async fn state(&self) -> &str {
        &self.state
    }

    async fn fee_percentage(&self) -> f64 {
        self.fee_percentage
    }

    async fn updated_at(&self) -> &DateTime {
        &self.updated_at
    }
}

#[derive(SimpleObject)]
pub struct LiquidityPositionCollection {
    pub items: Vec<LiquidityPosition>,
    pub total: i32,
}