use crate::parsing::{asset_id_to_denom, position_id_to_bech32};
use anyhow::Result;
use cometindex::ContextualizedEvent;
use serde_json::Value;
use sqlx::types::chrono::{DateTime, Utc};
use sqlx::types::BigDecimal;
use sqlx::PgTransaction;
use std::collections::HashMap;
use std::str::FromStr;
use tracing::{debug, error};

/// Asset management for DEX operations
pub struct AssetManager;

impl AssetManager {
    /// Ensure an asset exists in the `explorer_assets` table
    /// If the asset doesn't exist, insert it with the given height and timestamp
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn ensure_asset_exists(
        asset_id: &str,
        decoded_passet: &str,
        height: u64,
        timestamp: DateTime<Utc>,
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<()> {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM explorer_assets WHERE asset_id = $1)")
                .bind(asset_id)
                .fetch_one(dbtx.as_mut())
                .await?;

        if !exists {
            debug!("Inserting new asset: {} at height {}", asset_id, height);
            sqlx::query(
                r"
                INSERT INTO explorer_assets (asset_id, decoded_passet, first_seen_height, first_seen_time)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (asset_id) DO NOTHING
                "
            )
            .bind(asset_id)
            .bind(decoded_passet)
            .bind(i64::try_from(height).unwrap_or(i64::MAX))
            .bind(timestamp)
            .execute(dbtx.as_mut())
            .await?;
        }

        Ok(())
    }
}

/// Liquidity position data structure
#[derive(Debug, Clone)]
pub struct LiquidityPosition {
    pub position_id: String,
    pub decoded_position_id: String,
    pub trading_pair_asset1: String,
    pub trading_pair_asset2: String,
    pub reserves1_amount: BigDecimal,
    pub reserves2_amount: BigDecimal,
    pub state: String,
    pub fee_percentage: f64,
    pub created_height: i64,
    pub created_at: DateTime<Utc>,
    pub updated_height: i64,
    pub updated_at: DateTime<Utc>,
}

impl LiquidityPosition {
    /// Extract trading pair assets from a trading pair JSON
    fn extract_trading_pair_assets(trading_pair_json: &str) -> Result<(String, String)> {
        let trading_pair: Value = serde_json::from_str(trading_pair_json)?;

        let asset1 = trading_pair["asset1"]["inner"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing asset1.inner in trading pair"))?;

        let asset2 = trading_pair["asset2"]["inner"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing asset2.inner in trading pair"))?;

        Ok((asset1.to_string(), asset2.to_string()))
    }

    /// Extract position ID from position ID JSON
    fn extract_position_id(position_id_json: &str) -> Result<String> {
        let position_data: Value = serde_json::from_str(position_id_json)?;

        let position_id = position_data["inner"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing inner field in positionId"))?;

        Ok(position_id.to_string())
    }

    /// Extract reserves amount from reserves JSON, return `BigDecimal::from(0)` if empty
    fn extract_reserves_amount(reserves_json: &str) -> BigDecimal {
        let reserves: Value = match serde_json::from_str(reserves_json) {
            Ok(val) => val,
            Err(_) => return BigDecimal::from(0),
        };

        if reserves.as_object().map_or(true, serde_json::Map::is_empty) {
            BigDecimal::from(0)
        } else {
            let amount_str = match &reserves["lo"] {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                _ => "0".to_string(),
            };

            BigDecimal::from_str(&amount_str).unwrap_or_else(|_| BigDecimal::from(0))
        }
    }

    /// Extract trading fee from the event, convert from basis points to percentage, default to 0.00 if not present
    fn extract_trading_fee(event: &ContextualizedEvent) -> f64 {
        let fee_bps: i32 = Self::find_attribute_value(event, "tradingFee")
            .and_then(|fee_str| fee_str.parse().ok())
            .unwrap_or(0);

        let percentage = f64::from(fee_bps) / 100.0;
        (percentage * 100.0).round() / 100.0
    }

    /// Find attribute value by key in the event
    fn find_attribute_value(event: &ContextualizedEvent, key: &str) -> Option<String> {
        for attr in &event.event.attributes {
            if let Ok(attr_key) = attr.key_str() {
                if attr_key == key {
                    if let Ok(attr_value) = attr.value_str() {
                        return Some(attr_value.to_string());
                    }
                }
            }
        }
        None
    }

    /// Create a liquidity position from `EventPositionOpen`
    ///
    /// # Errors
    ///
    /// Returns an error if required attributes are missing or invalid.
    pub fn from_position_open_event(
        event: &ContextualizedEvent,
        height: u64,
        timestamp: DateTime<Utc>,
    ) -> Result<Self> {
        let position_id_json = Self::find_attribute_value(event, "positionId")
            .ok_or_else(|| anyhow::anyhow!("Missing positionId in EventPositionOpen"))?;

        let position_id = Self::extract_position_id(&position_id_json)?;

        let trading_pair_json = Self::find_attribute_value(event, "tradingPair")
            .ok_or_else(|| anyhow::anyhow!("Missing tradingPair in EventPositionOpen"))?;

        let (trading_pair_asset1, trading_pair_asset2) =
            Self::extract_trading_pair_assets(&trading_pair_json)?;

        let reserves1_json =
            Self::find_attribute_value(event, "reserves1").unwrap_or_else(|| "{}".to_string());
        let reserves2_json =
            Self::find_attribute_value(event, "reserves2").unwrap_or_else(|| "{}".to_string());

        let reserves1_amount = Self::extract_reserves_amount(&reserves1_json);
        let reserves2_amount = Self::extract_reserves_amount(&reserves2_json);

        let fee_percentage = Self::extract_trading_fee(event);

        // Decode the position ID to bech32 format for storage
        let decoded_position_id = position_id_to_bech32(&position_id)
            .unwrap_or_else(|_| position_id.clone());

        Ok(Self {
            position_id,
            decoded_position_id,
            trading_pair_asset1,
            trading_pair_asset2,
            reserves1_amount,
            reserves2_amount,
            state: "Open".to_string(),
            fee_percentage,
            created_height: i64::try_from(height).unwrap_or(i64::MAX),
            created_at: timestamp,
            updated_height: i64::try_from(height).unwrap_or(i64::MAX),
            updated_at: timestamp,
        })
    }

    /// Update reserves from `EventPositionExecution`
    ///
    /// # Errors
    ///
    /// Returns an error if required attributes are missing or invalid.
    pub fn update_from_execution_event(
        &mut self,
        event: &ContextualizedEvent,
        height: u64,
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        let reserves1_json =
            Self::find_attribute_value(event, "reserves1").unwrap_or_else(|| "{}".to_string());
        let reserves2_json =
            Self::find_attribute_value(event, "reserves2").unwrap_or_else(|| "{}".to_string());

        self.reserves1_amount = Self::extract_reserves_amount(&reserves1_json);
        self.reserves2_amount = Self::extract_reserves_amount(&reserves2_json);
        self.state = "Executing".to_string();
        self.updated_height = i64::try_from(height).unwrap_or(i64::MAX);
        self.updated_at = timestamp;

        Ok(())
    }

    /// Update state from `EventPositionClose`
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub fn update_from_close_event(
        &mut self,
        _event: &ContextualizedEvent,
        height: u64,
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        self.state = "Closed".to_string();
        self.updated_height = i64::try_from(height).unwrap_or(i64::MAX);
        self.updated_at = timestamp;

        Ok(())
    }

    /// Update state from `EventPositionWithdraw`
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub fn update_from_withdraw_event(
        &mut self,
        event: &ContextualizedEvent,
        height: u64,
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        if let Some(reserves1_json) = Self::find_attribute_value(event, "reserves1") {
            self.reserves1_amount = Self::extract_reserves_amount(&reserves1_json);
        }
        if let Some(reserves2_json) = Self::find_attribute_value(event, "reserves2") {
            self.reserves2_amount = Self::extract_reserves_amount(&reserves2_json);
        }

        self.state = "Withdrawn".to_string();
        self.updated_height = i64::try_from(height).unwrap_or(i64::MAX);
        self.updated_at = timestamp;

        Ok(())
    }

    /// Insert a new liquidity position into the database
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn insert(&self, dbtx: &mut PgTransaction<'_>) -> Result<()> {
        sqlx::query(
            r"
            INSERT INTO dex_liquidity_positions (
                position_id,
                decoded_position_id,
                trading_pair_asset1,
                trading_pair_asset2,
                reserves1_amount,
                reserves2_amount,
                state,
                fee_percentage,
                created_height,
                created_at,
                updated_height,
                updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ",
        )
        .bind(&self.position_id)
        .bind(&self.decoded_position_id)
        .bind(&self.trading_pair_asset1)
        .bind(&self.trading_pair_asset2)
        .bind(&self.reserves1_amount)
        .bind(&self.reserves2_amount)
        .bind(&self.state)
        .bind(
            BigDecimal::from_str(&format!("{:.2}", self.fee_percentage))
                .unwrap_or_else(|_| BigDecimal::from(0)),
        )
        .bind(self.created_height)
        .bind(self.created_at)
        .bind(self.updated_height)
        .bind(self.updated_at)
        .execute(dbtx.as_mut())
        .await?;

        Ok(())
    }

    /// Update an existing liquidity position in the database
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn update(&self, dbtx: &mut PgTransaction<'_>) -> Result<()> {
        sqlx::query(
            r"
            UPDATE dex_liquidity_positions
            SET 
                reserves1_amount = $2,
                reserves2_amount = $3,
                state = $4,
                updated_height = $5,
                updated_at = $6
            WHERE position_id = $1
            ",
        )
        .bind(&self.position_id)
        .bind(&self.reserves1_amount)
        .bind(&self.reserves2_amount)
        .bind(&self.state)
        .bind(self.updated_height)
        .bind(self.updated_at)
        .execute(dbtx.as_mut())
        .await?;

        Ok(())
    }

    /// Load an existing position from the database
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn load_from_db(
        position_id: &str,
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<Option<Self>> {
        let row = sqlx::query_as::<
            _,
            (
                String,
                Option<String>,
                String,
                String,
                BigDecimal,
                BigDecimal,
                String,
                BigDecimal,
                i64,
                DateTime<Utc>,
                i64,
                DateTime<Utc>,
            ),
        >(
            r"
            SELECT 
                position_id,
                decoded_position_id,
                trading_pair_asset1,
                trading_pair_asset2,
                reserves1_amount,
                reserves2_amount,
                state,
                fee_percentage,
                created_height,
                created_at,
                updated_height,
                updated_at
            FROM dex_liquidity_positions
            WHERE position_id = $1
            ",
        )
        .bind(position_id)
        .fetch_optional(dbtx.as_mut())
        .await?;

        if let Some((
            position_id,
            decoded_position_id,
            trading_pair_asset1,
            trading_pair_asset2,
            reserves1_amount,
            reserves2_amount,
            state,
            fee_percentage,
            created_height,
            created_at,
            updated_height,
            updated_at,
        )) = row
        {
            // If decoded_position_id is None, generate it from the base64 position_id
            let decoded_position_id = decoded_position_id
                .or_else(|| position_id_to_bech32(&position_id).ok())
                .unwrap_or_else(|| position_id.clone());

            Ok(Some(Self {
                position_id,
                decoded_position_id,
                trading_pair_asset1,
                trading_pair_asset2,
                reserves1_amount,
                reserves2_amount,
                state,
                fee_percentage: fee_percentage.to_string().parse().unwrap_or(0.0),
                created_height,
                created_at,
                updated_height,
                updated_at,
            }))
        } else {
            Ok(None)
        }
    }
}

/// Batch swap data structure (represents the entire batch)
#[derive(Debug, Clone)]
pub struct BatchSwap {
    pub block_height: i64,
    pub block_timestamp: DateTime<Utc>,
    pub execution_type: String,
    pub total_input_amount: BigDecimal,
    pub total_input_asset_id: String,
    pub total_output_amount: BigDecimal,
    pub total_output_asset_id: String,
    pub individual_swaps_count: i32,
    pub individual_swaps: Vec<IndividualSwap>,
    pub raw_execution_data: Value,
}

/// Individual swap within a batch
#[derive(Debug, Clone)]
pub struct IndividualSwap {
    pub swap_index: i32,
    pub input_amount: BigDecimal,
    pub input_asset_id: String,
    pub output_amount: BigDecimal,
    pub output_asset_id: String,
    pub route_steps_count: i32,
    pub route_steps: Vec<RouteStep>,
}

/// Route step within an individual swap
#[derive(Debug, Clone)]
pub struct RouteStep {
    pub route_step: i32,
    pub amount: BigDecimal,
    pub asset_id: String,
}

impl BatchSwap {
    /// Create `BatchSwap` from `EventBatchSwap`
    ///
    /// # Errors
    ///
    /// Returns an error if the event data is missing or invalid.
    pub fn from_batch_swap_event(
        event: &ContextualizedEvent,
        height: u64,
        timestamp: DateTime<Utc>,
    ) -> Result<Self> {
        let swap_execution_json = Self::find_batch_swap_execution(event)?;

        Self::parse_batch_swap(&swap_execution_json, height, timestamp, "Swap")
    }

    /// Create `BatchSwap` from `EventArbExecution`
    ///
    /// # Errors
    ///
    /// Returns an error if the event data is missing or invalid.
    pub fn from_arb_execution_event(
        event: &ContextualizedEvent,
        height: u64,
        timestamp: DateTime<Utc>,
    ) -> Result<Self> {
        let swap_execution_json =
            LiquidityPosition::find_attribute_value(event, "swapExecution")
                .ok_or_else(|| anyhow::anyhow!("Missing swapExecution in EventArbExecution"))?;

        Self::parse_batch_swap(&swap_execution_json, height, timestamp, "Arb")
    }

    /// Find all swap executions in `EventBatchSwap` (both directions if present)
    fn find_all_batch_swap_executions(event: &ContextualizedEvent) -> Result<Vec<String>> {
        let mut executions = Vec::new();

        if let Some(execution) =
            LiquidityPosition::find_attribute_value(event, "swapExecution1For2")
        {
            executions.push(execution);
        }
        if let Some(execution) =
            LiquidityPosition::find_attribute_value(event, "swapExecution2For1")
        {
            executions.push(execution);
        }

        if executions.is_empty() {
            Err(anyhow::anyhow!(
                "EventBatchSwap has no actual swap execution - only batchSwapOutputData"
            ))
        } else {
            Ok(executions)
        }
    }

    /// Find swapExecution1For2 or swapExecution2For1 in `EventBatchSwap` (legacy method)
    fn find_batch_swap_execution(event: &ContextualizedEvent) -> Result<String> {
        if let Some(execution) =
            LiquidityPosition::find_attribute_value(event, "swapExecution1For2")
        {
            return Ok(execution);
        }
        if let Some(execution) =
            LiquidityPosition::find_attribute_value(event, "swapExecution2For1")
        {
            return Ok(execution);
        }
        Err(anyhow::anyhow!(
            "EventBatchSwap has no actual swap execution - only batchSwapOutputData"
        ))
    }

    /// Parse batch swap JSON into `BatchSwap` struct
    fn parse_batch_swap(
        swap_execution_json: &str,
        height: u64,
        timestamp: DateTime<Utc>,
        execution_type: &str,
    ) -> Result<Self> {
        let execution_data: Value = serde_json::from_str(swap_execution_json)?;

        let total_input_amount_str = if execution_data["input"]["amount"].is_object()
            && execution_data["input"]["amount"]
                .as_object()
                .map_or(false, serde_json::Map::is_empty)
        {
            "0"
        } else {
            execution_data["input"]["amount"]["lo"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Missing input.amount.lo"))?
        };

        let total_input_asset_id = execution_data["input"]["assetId"]["inner"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing input.assetId.inner"))?;

        let total_output_amount_str = if execution_data["output"]["amount"].is_object()
            && execution_data["output"]["amount"]
                .as_object()
                .map_or(false, serde_json::Map::is_empty)
        {
            "0"
        } else {
            execution_data["output"]["amount"]["lo"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Missing output.amount.lo"))?
        };

        let total_output_asset_id = execution_data["output"]["assetId"]["inner"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing output.assetId.inner"))?;

        let traces_array = execution_data["traces"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("Missing or invalid traces array"))?;

        let individual_swaps_count = i32::try_from(traces_array.len())?;
        let individual_swaps = Self::parse_individual_swaps(traces_array)?;

        Ok(Self {
            block_height: i64::try_from(height).unwrap_or(i64::MAX),
            block_timestamp: timestamp,
            execution_type: execution_type.to_string(),
            total_input_amount: BigDecimal::from_str(total_input_amount_str)?,
            total_input_asset_id: total_input_asset_id.to_string(),
            total_output_amount: BigDecimal::from_str(total_output_amount_str)?,
            total_output_asset_id: total_output_asset_id.to_string(),
            individual_swaps_count,
            individual_swaps,
            raw_execution_data: execution_data,
        })
    }

    /// Parse traces array into individual swaps
    fn parse_individual_swaps(traces_array: &[Value]) -> Result<Vec<IndividualSwap>> {
        let mut individual_swaps = Vec::new();

        for (swap_index, trace) in traces_array.iter().enumerate() {
            let value_array = trace["value"].as_array().ok_or_else(|| {
                anyhow::anyhow!("Missing or invalid value array in trace {}", swap_index)
            })?;

            if value_array.len() < 2 {
                return Err(anyhow::anyhow!(
                    "Expected at least 2 elements in trace {}, found {}",
                    swap_index,
                    value_array.len()
                ));
            }

            let mut route_steps = Vec::new();
            for (step_index, step_value) in value_array.iter().enumerate() {
                let amount_str = if step_value["amount"].is_object()
                    && step_value["amount"]
                        .as_object()
                        .map_or(false, serde_json::Map::is_empty)
                {
                    "0"
                } else {
                    step_value["amount"]["lo"].as_str().ok_or_else(|| {
                        anyhow::anyhow!(
                            "Missing amount.lo in swap {} step {}",
                            swap_index,
                            step_index
                        )
                    })?
                };

                let asset_id = step_value["assetId"]["inner"].as_str().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Missing assetId.inner in swap {} step {}",
                        swap_index,
                        step_index
                    )
                })?;

                route_steps.push(RouteStep {
                    route_step: i32::try_from(step_index)?,
                    amount: BigDecimal::from_str(amount_str)?,
                    asset_id: asset_id.to_string(),
                });
            }

            let input_step = &route_steps[0];
            let output_step = &route_steps[route_steps.len() - 1];

            individual_swaps.push(IndividualSwap {
                swap_index: i32::try_from(swap_index)?,
                input_amount: input_step.amount.clone(),
                input_asset_id: input_step.asset_id.clone(),
                output_amount: output_step.amount.clone(),
                output_asset_id: output_step.asset_id.clone(),
                route_steps_count: i32::try_from(route_steps.len())?,
                route_steps,
            });
        }

        Ok(individual_swaps)
    }

    /// Insert batch swap into database and return the ID
    /// Insert batch swap into database and return the ID
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn insert(&self, dbtx: &mut PgTransaction<'_>) -> Result<i32> {
        let batch_id: i32 = sqlx::query_scalar(
            r"
            INSERT INTO dex_batch_swaps (
                block_height,
                block_timestamp,
                execution_type,
                total_input_amount,
                total_input_asset_id,
                total_output_amount,
                total_output_asset_id,
                individual_swaps_count,
                raw_execution_data
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id
            ",
        )
        .bind(self.block_height)
        .bind(self.block_timestamp)
        .bind(&self.execution_type)
        .bind(&self.total_input_amount)
        .bind(&self.total_input_asset_id)
        .bind(&self.total_output_amount)
        .bind(&self.total_output_asset_id)
        .bind(self.individual_swaps_count)
        .bind(&self.raw_execution_data)
        .fetch_one(dbtx.as_mut())
        .await?;

        Ok(batch_id)
    }

    /// Insert all individual swaps for this batch
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub async fn insert_individual_swaps(
        &self,
        batch_id: i32,
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<()> {
        for swap in &self.individual_swaps {
            // Insert individual swap and get the ID
            let individual_swap_id: i32 = sqlx::query_scalar(
                r"
                INSERT INTO dex_individual_swaps (
                    batch_swap_id,
                    swap_index,
                    input_amount,
                    input_asset_id,
                    output_amount,
                    output_asset_id,
                    route_steps_count
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING id
                ",
            )
            .bind(batch_id)
            .bind(swap.swap_index)
            .bind(&swap.input_amount)
            .bind(&swap.input_asset_id)
            .bind(&swap.output_amount)
            .bind(&swap.output_asset_id)
            .bind(swap.route_steps_count)
            .fetch_one(dbtx.as_mut())
            .await?;

            for route_step in &swap.route_steps {
                sqlx::query(
                    r"
                    INSERT INTO dex_individual_swap_routes (
                        individual_swap_id,
                        route_step,
                        amount,
                        asset_id
                    ) VALUES ($1, $2, $3, $4)
                    ",
                )
                .bind(individual_swap_id)
                .bind(route_step.route_step)
                .bind(&route_step.amount)
                .bind(&route_step.asset_id)
                .execute(dbtx.as_mut())
                .await?;
            }
        }

        Ok(())
    }
}

/// Event processor for DEX operations
pub struct Processor;

impl Processor {
    /// Process all DEX-related events for a block
    ///
    /// # Errors
    ///
    /// Returns an error if any event processing fails.
    pub async fn process_events(
        dbtx: &mut PgTransaction<'_>,
        events: &[ContextualizedEvent<'_>],
        height: u64,
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        debug!(
            "Processing DEX events for block {} with timestamp {}",
            height, timestamp
        );

        let mut positions_cache: HashMap<String, LiquidityPosition> = HashMap::new();

        for event in events {
            match event.event.kind.as_str() {
                "penumbra.core.component.dex.v1.EventPositionOpen" => {
                    debug!("Processing EventPositionOpen");
                    if let Err(e) = Self::process_position_open_event(
                        event,
                        height,
                        timestamp,
                        &mut positions_cache,
                        dbtx,
                    )
                    .await
                    {
                        error!("Error processing EventPositionOpen: {}", e);
                    }
                }
                "penumbra.core.component.dex.v1.EventPositionExecution" => {
                    debug!("Processing EventPositionExecution");
                    if let Err(e) = Self::process_position_execution_event(
                        event,
                        height,
                        timestamp,
                        &mut positions_cache,
                        dbtx,
                    )
                    .await
                    {
                        error!("Error processing EventPositionExecution: {}", e);
                    }
                }
                "penumbra.core.component.dex.v1.EventPositionClose" => {
                    debug!("Processing EventPositionClose");
                    if let Err(e) = Self::process_position_close_event(
                        event,
                        height,
                        timestamp,
                        &mut positions_cache,
                        dbtx,
                    )
                    .await
                    {
                        error!("Error processing EventPositionClose: {}", e);
                    }
                }
                "penumbra.core.component.dex.v1.EventPositionWithdraw" => {
                    debug!("Processing EventPositionWithdraw");
                    if let Err(e) = Self::process_position_withdraw_event(
                        event,
                        height,
                        timestamp,
                        &mut positions_cache,
                        dbtx,
                    )
                    .await
                    {
                        error!("Error processing EventPositionWithdraw: {}", e);
                    }
                }
                "penumbra.core.component.dex.v1.EventBatchSwap" => {
                    debug!("Processing EventBatchSwap");
                    if let Err(e) =
                        Self::process_batch_swap_event(event, height, timestamp, dbtx).await
                    {
                        error!("Error processing EventBatchSwap: {}", e);
                    }
                }
                "penumbra.core.component.dex.v1.EventArbExecution" => {
                    debug!("Processing EventArbExecution");
                    if let Err(e) =
                        Self::process_arb_execution_event(event, height, timestamp, dbtx).await
                    {
                        error!("Error processing EventArbExecution: {}", e);
                    }
                }
                _ => {}
            }
        }

        for position in positions_cache.values() {
            if position.created_height < i64::try_from(height).unwrap_or(i64::MAX) {
                if let Err(e) = position.update(dbtx).await {
                    error!("Error updating position {}: {}", position.position_id, e);
                }
            }
        }

        Ok(())
    }

    /// Process `EventPositionOpen` event
    async fn process_position_open_event(
        event: &ContextualizedEvent<'_>,
        height: u64,
        timestamp: DateTime<Utc>,
        positions_cache: &mut HashMap<String, LiquidityPosition>,
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<()> {
        let position = LiquidityPosition::from_position_open_event(event, height, timestamp)?;

        let decoded_asset1 = asset_id_to_denom(&position.trading_pair_asset1)
            .unwrap_or_else(|_| position.trading_pair_asset1.clone());
        AssetManager::ensure_asset_exists(
            &position.trading_pair_asset1,
            &decoded_asset1,
            height,
            timestamp,
            dbtx,
        )
        .await?;

        let decoded_asset2 = asset_id_to_denom(&position.trading_pair_asset2)
            .unwrap_or_else(|_| position.trading_pair_asset2.clone());
        AssetManager::ensure_asset_exists(
            &position.trading_pair_asset2,
            &decoded_asset2,
            height,
            timestamp,
            dbtx,
        )
        .await?;

        position.insert(dbtx).await?;

        positions_cache.insert(position.position_id.clone(), position);

        Ok(())
    }

    /// Process `EventPositionExecution` event
    async fn process_position_execution_event(
        event: &ContextualizedEvent<'_>,
        height: u64,
        timestamp: DateTime<Utc>,
        positions_cache: &mut HashMap<String, LiquidityPosition>,
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<()> {
        let position_id_json = LiquidityPosition::find_attribute_value(event, "positionId")
            .ok_or_else(|| anyhow::anyhow!("Missing positionId in EventPositionExecution"))?;

        let position_id = LiquidityPosition::extract_position_id(&position_id_json)?;

        let mut position = if let Some(cached_position) = positions_cache.get(&position_id).cloned()
        {
            cached_position
        } else {
            LiquidityPosition::load_from_db(&position_id, dbtx)
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!("Position {} not found for execution", position_id)
                })?
        };

        position.update_from_execution_event(event, height, timestamp)?;
        positions_cache.insert(position_id, position);

        Ok(())
    }

    /// Process `EventPositionClose` event
    async fn process_position_close_event(
        event: &ContextualizedEvent<'_>,
        height: u64,
        timestamp: DateTime<Utc>,
        positions_cache: &mut HashMap<String, LiquidityPosition>,
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<()> {
        let position_id_json = LiquidityPosition::find_attribute_value(event, "positionId")
            .ok_or_else(|| anyhow::anyhow!("Missing positionId in EventPositionClose"))?;

        let position_id = LiquidityPosition::extract_position_id(&position_id_json)?;

        let mut position = if let Some(cached_position) = positions_cache.get(&position_id).cloned()
        {
            cached_position
        } else {
            LiquidityPosition::load_from_db(&position_id, dbtx)
                .await?
                .ok_or_else(|| anyhow::anyhow!("Position {} not found for close", position_id))?
        };

        position.update_from_close_event(event, height, timestamp)?;
        positions_cache.insert(position_id, position);

        Ok(())
    }

    /// Process `EventPositionWithdraw` event
    async fn process_position_withdraw_event(
        event: &ContextualizedEvent<'_>,
        height: u64,
        timestamp: DateTime<Utc>,
        positions_cache: &mut HashMap<String, LiquidityPosition>,
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<()> {
        let position_id_json = LiquidityPosition::find_attribute_value(event, "positionId")
            .ok_or_else(|| anyhow::anyhow!("Missing positionId in EventPositionWithdraw"))?;

        let position_id = LiquidityPosition::extract_position_id(&position_id_json)?;

        let mut position = if let Some(cached_position) = positions_cache.get(&position_id).cloned()
        {
            cached_position
        } else {
            LiquidityPosition::load_from_db(&position_id, dbtx)
                .await?
                .ok_or_else(|| anyhow::anyhow!("Position {} not found for withdraw", position_id))?
        };

        position.update_from_withdraw_event(event, height, timestamp)?;
        positions_cache.insert(position_id, position);

        Ok(())
    }

    /// Process `EventBatchSwap` event (handles multiple execution directions)
    async fn process_batch_swap_event(
        event: &ContextualizedEvent<'_>,
        height: u64,
        timestamp: DateTime<Utc>,
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<()> {
        let Ok(swap_executions) = BatchSwap::find_all_batch_swap_executions(event) else {
            debug!(
                "Skipping EventBatchSwap at height {} - no actual swap execution",
                height
            );
            return Ok(());
        };

        let mut processed_count = 0;

        for swap_execution_json in swap_executions {
            let batch_swap = match BatchSwap::parse_batch_swap(
                &swap_execution_json,
                height,
                timestamp,
                "Swap",
            ) {
                Ok(batch) => batch,
                Err(e) => {
                    error!(
                        "Failed to parse batch swap execution at height {}: {}",
                        height, e
                    );
                    continue;
                }
            };

            if let Err(e) =
                Processor::ensure_batch_swap_assets(&batch_swap, height, timestamp, dbtx).await
            {
                error!(
                    "Failed to ensure assets for batch swap at height {}: {}",
                    height, e
                );
                continue;
            }

            let batch_id = match batch_swap.insert(dbtx).await {
                Ok(id) => id,
                Err(e) => {
                    error!("Failed to insert batch swap at height {}: {}", height, e);
                    continue;
                }
            };

            if let Err(e) = batch_swap.insert_individual_swaps(batch_id, dbtx).await {
                error!(
                    "Failed to insert individual swaps for batch {} at height {}: {}",
                    batch_id, height, e
                );
                continue;
            }

            debug!(
                "Processed EventBatchSwap: batch_id={}, individual_swaps_count={}",
                batch_id, batch_swap.individual_swaps_count
            );
            processed_count += 1;
        }

        if processed_count == 0 {
            error!("Failed to process any swap executions at height {}", height);
        } else {
            debug!(
                "Successfully processed {} swap executions at height {}",
                processed_count, height
            );
        }

        Ok(())
    }

    /// Process `EventArbExecution` event
    async fn process_arb_execution_event(
        event: &ContextualizedEvent<'_>,
        height: u64,
        timestamp: DateTime<Utc>,
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<()> {
        let batch_swap = BatchSwap::from_arb_execution_event(event, height, timestamp)?;

        Self::ensure_batch_swap_assets(&batch_swap, height, timestamp, dbtx).await?;

        let batch_id = batch_swap.insert(dbtx).await?;

        batch_swap.insert_individual_swaps(batch_id, dbtx).await?;

        debug!(
            "Processed EventArbExecution: batch_id={}, individual_swaps_count={}",
            batch_id, batch_swap.individual_swaps_count
        );

        Ok(())
    }

    /// Ensure all assets from a batch swap exist in `explorer_assets` table
    async fn ensure_batch_swap_assets(
        batch_swap: &BatchSwap,
        height: u64,
        timestamp: DateTime<Utc>,
        dbtx: &mut PgTransaction<'_>,
    ) -> Result<()> {
        let decoded_input_asset = asset_id_to_denom(&batch_swap.total_input_asset_id)
            .unwrap_or_else(|_| batch_swap.total_input_asset_id.clone());
        AssetManager::ensure_asset_exists(
            &batch_swap.total_input_asset_id,
            &decoded_input_asset,
            height,
            timestamp,
            dbtx,
        )
        .await?;

        let decoded_output_asset = asset_id_to_denom(&batch_swap.total_output_asset_id)
            .unwrap_or_else(|_| batch_swap.total_output_asset_id.clone());
        AssetManager::ensure_asset_exists(
            &batch_swap.total_output_asset_id,
            &decoded_output_asset,
            height,
            timestamp,
            dbtx,
        )
        .await?;

        for swap in &batch_swap.individual_swaps {
            let decoded_input = asset_id_to_denom(&swap.input_asset_id)
                .unwrap_or_else(|_| swap.input_asset_id.clone());
            AssetManager::ensure_asset_exists(
                &swap.input_asset_id,
                &decoded_input,
                height,
                timestamp,
                dbtx,
            )
            .await?;

            let decoded_output = asset_id_to_denom(&swap.output_asset_id)
                .unwrap_or_else(|_| swap.output_asset_id.clone());
            AssetManager::ensure_asset_exists(
                &swap.output_asset_id,
                &decoded_output,
                height,
                timestamp,
                dbtx,
            )
            .await?;

            for route_step in &swap.route_steps {
                let decoded_route_asset = asset_id_to_denom(&route_step.asset_id)
                    .unwrap_or_else(|_| route_step.asset_id.clone());
                AssetManager::ensure_asset_exists(
                    &route_step.asset_id,
                    &decoded_route_asset,
                    height,
                    timestamp,
                    dbtx,
                )
                .await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::types::chrono::Utc;

    #[test]
    fn test_empty_amount_parsing() {
        let swap_with_empty_output = r#"{"input":{"amount":{"lo":"1000000"},"assetId":{"inner":"drPksQaBNYwSOzgfkGOEdrd4kEDkeALeh58Ps+7cjQs="}},"output":{"amount":{},"assetId":{"inner":"KeqcLzNx9qSH5+lcJHBB9KNW+YPrBk5dKzvPMiypahA="}},"traces":[{"value":[{"amount":{"lo":"1000000"},"assetId":{"inner":"drPksQaBNYwSOzgfkGOEdrd4kEDkeALeh58Ps+7cjQs="}},{"amount":{},"assetId":{"inner":"KeqcLzNx9qSH5+lcJHBB9KNW+YPrBk5dKzvPMiypahA="}}]}]}"#;

        let result =
            BatchSwap::parse_batch_swap(swap_with_empty_output, 412_203, Utc::now(), "Swap");

        assert!(
            result.is_ok(),
            "Should successfully parse swap with empty amounts"
        );

        let batch_swap = result.unwrap();
        assert_eq!(batch_swap.total_output_amount, BigDecimal::from(0));
        assert_eq!(
            batch_swap.individual_swaps[0].output_amount,
            BigDecimal::from(0)
        );
    }

    #[test]
    fn test_fee_percentage_calculation() {
        fn convert_bps_to_percentage(fee_bps: i32) -> f64 {
            let percentage = f64::from(fee_bps) / 100.0;
            (percentage * 100.0).round() / 100.0
        }

        assert!((convert_bps_to_percentage(100) - 1.00).abs() < f64::EPSILON);
        assert!((convert_bps_to_percentage(10) - 0.10).abs() < f64::EPSILON);
        assert!((convert_bps_to_percentage(0) - 0.00).abs() < f64::EPSILON);
        assert!((convert_bps_to_percentage(50) - 0.50).abs() < f64::EPSILON);
        assert!((convert_bps_to_percentage(1) - 0.01).abs() < f64::EPSILON);
        assert!((convert_bps_to_percentage(250) - 2.50).abs() < f64::EPSILON);
    }
}
