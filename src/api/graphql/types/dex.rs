use crate::api::graphql::scalars::DateTime;
use async_graphql::{Enum, Object, SimpleObject};

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum LiquidityPositionState {
    #[graphql(name = "OPEN")]
    Open,
    #[graphql(name = "EXECUTING")]
    Executing,
    #[graphql(name = "CLOSED")]
    Closed,
    #[graphql(name = "WITHDRAWN")]
    Withdrawn,
}

impl LiquidityPositionState {
    pub fn from_string(state: &str) -> Self {
        match state {
            "Open" => Self::Open,
            "Executing" => Self::Executing,
            "Closed" => Self::Closed,
            "Withdrawn" => Self::Withdrawn,
            _ => Self::Open, // Default fallback
        }
    }
}

#[derive(Debug, Clone)]
pub struct LiquidityPosition {
    pub position_id: String,
    pub trading_pair_asset1: String,
    pub trading_pair_asset2: String,
    pub reserves1_amount: String,
    pub reserves2_amount: String,
    pub state: LiquidityPositionState,
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

    async fn state(&self) -> LiquidityPositionState {
        self.state
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

#[derive(Debug, Clone)]
pub struct RouteStep {
    pub route_step: i32,
    pub asset_id: String,
    pub amount: String,
}

#[Object]
impl RouteStep {
    async fn route_step(&self) -> i32 {
        self.route_step
    }

    async fn asset_id(&self) -> &str {
        &self.asset_id
    }

    async fn amount(&self) -> &str {
        &self.amount
    }
}

#[derive(Debug, Clone)]
pub struct IndividualSwap {
    pub swap_index: i32,
    pub input_amount: String,
    pub input_asset_id: String,
    pub output_amount: String,
    pub output_asset_id: String,
    pub route_steps: Vec<RouteStep>,
}

#[Object]
impl IndividualSwap {
    async fn swap_index(&self) -> i32 {
        self.swap_index
    }

    async fn input_amount(&self) -> &str {
        &self.input_amount
    }

    async fn input_asset_id(&self) -> &str {
        &self.input_asset_id
    }

    async fn output_amount(&self) -> &str {
        &self.output_amount
    }

    async fn output_asset_id(&self) -> &str {
        &self.output_asset_id
    }

    async fn route_steps(&self) -> &Vec<RouteStep> {
        &self.route_steps
    }
}

#[derive(Debug, Clone)]
pub struct BatchSwap {
    pub id: i32,
    pub execution_type: String,
    pub total_input_amount: String,
    pub total_input_asset_id: String,
    pub total_output_amount: String,
    pub total_output_asset_id: String,
    pub individual_swaps_count: i32,
    pub individual_swaps: Vec<IndividualSwap>,
}

#[Object]
impl BatchSwap {
    async fn id(&self) -> i32 {
        self.id
    }

    async fn execution_type(&self) -> &str {
        &self.execution_type
    }

    async fn total_input_amount(&self) -> &str {
        &self.total_input_amount
    }

    async fn total_input_asset_id(&self) -> &str {
        &self.total_input_asset_id
    }

    async fn total_output_amount(&self) -> &str {
        &self.total_output_amount
    }

    async fn total_output_asset_id(&self) -> &str {
        &self.total_output_asset_id
    }

    async fn individual_swaps_count(&self) -> i32 {
        self.individual_swaps_count
    }

    async fn individual_swaps(&self) -> &Vec<IndividualSwap> {
        &self.individual_swaps
    }
}

#[derive(Debug, Clone)]
pub struct SwapExecution {
    pub block_height: i64,
    pub timestamp: DateTime,
    pub batch_swaps: Vec<BatchSwap>,
}

#[Object]
impl SwapExecution {
    async fn block_height(&self) -> i64 {
        self.block_height
    }

    async fn timestamp(&self) -> &DateTime {
        &self.timestamp
    }

    async fn batch_swaps(&self) -> &Vec<BatchSwap> {
        &self.batch_swaps
    }
}
