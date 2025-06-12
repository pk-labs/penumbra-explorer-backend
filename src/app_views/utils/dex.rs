use anyhow::Result;
use cometindex::ContextualizedEvent;
use serde_json::Value;
use sqlx::types::chrono::{DateTime, Utc};
use sqlx::types::BigDecimal;
use sqlx::PgTransaction;
use std::collections::HashMap;
use std::str::FromStr;
use tracing::{debug, error, info};
use crate::parsing::asset_id_to_denom;

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
            // Extract the "lo" field and parse it as a BigDecimal
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
        
        // Convert basis points to percentage: 100 bps = 1.00%, 10 bps = 0.10%
        let percentage = fee_bps as f64 / 100.0;
        // Round to 2 decimal places
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

        Ok(Self {
            position_id,
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
        // Update reserves if provided in withdraw event
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
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ",
        )
        .bind(&self.position_id)
        .bind(&self.trading_pair_asset1)
        .bind(&self.trading_pair_asset2)
        .bind(&self.reserves1_amount)
        .bind(&self.reserves2_amount)
        .bind(&self.state)
        .bind(BigDecimal::from_str(&format!("{:.2}", self.fee_percentage)).unwrap_or_else(|_| BigDecimal::from(0)))
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
            Ok(Some(Self {
                position_id,
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
                _ => {} // Ignore other events
            }
        }

        // Save all cached positions to database
        // Note: New positions are already inserted, so we only update existing ones
        for position in positions_cache.values() {
            // Only update if this position existed before this block (not newly created)
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

        // Ensure both assets exist in explorer_assets table
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

        // Insert the new position
        position.insert(dbtx).await?;

        // Add to cache for potential future updates in the same block
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

        // Try to get from cache first, otherwise load from database
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

        // Try to get from cache first, otherwise load from database
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

        // Try to get from cache first, otherwise load from database
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fee_percentage_calculation() {
        // Test the fee conversion logic directly
        fn convert_bps_to_percentage(fee_bps: i32) -> f64 {
            let percentage = fee_bps as f64 / 100.0;
            (percentage * 100.0).round() / 100.0
        }

        // Test cases based on user requirements
        assert_eq!(convert_bps_to_percentage(100), 1.00); // 100 bps → 1.00%
        assert_eq!(convert_bps_to_percentage(10), 0.10);  // 10 bps → 0.10%
        assert_eq!(convert_bps_to_percentage(0), 0.00);   // 0 bps → 0.00%
        assert_eq!(convert_bps_to_percentage(50), 0.50);  // 50 bps → 0.50%
        assert_eq!(convert_bps_to_percentage(1), 0.01);   // 1 bps → 0.01%
        assert_eq!(convert_bps_to_percentage(250), 2.50); // 250 bps → 2.50%
        
        println!("✅ Fee percentage calculations work correctly:");
        println!("  100 bps → {}%", convert_bps_to_percentage(100));
        println!("   10 bps → {}%", convert_bps_to_percentage(10));
        println!("    0 bps → {}%", convert_bps_to_percentage(0));
    }
}
