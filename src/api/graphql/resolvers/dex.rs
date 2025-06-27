use crate::api::graphql::{
    context::ApiContext,
    scalars::DateTime,
    types::{
        BatchSwap, CollectionLimit, DexStats, IndividualSwap, LiquidityPosition,
        LiquidityPositionCollection, LiquidityPositionFilter, LiquidityPositionState,
        LiquidityPositionStateFilter, RouteStep, SwapExecution, SwapExecutionFilter,
    },
};
use async_graphql::Result;
use sqlx::Row;

/// Resolves liquidity positions with pagination
///
/// # Errors
/// Returns an error if the database query fails
pub async fn resolve_liquidity_positions(
    ctx: &async_graphql::Context<'_>,
    limit: CollectionLimit,
    filter: Option<LiquidityPositionFilter>,
) -> Result<LiquidityPositionCollection> {
    let db = &ctx.data_unchecked::<ApiContext>().db;

    let where_clause = match filter.as_ref().and_then(|f| f.state) {
        Some(LiquidityPositionStateFilter::Open) => "WHERE state IN ('Open', 'Executing')",
        Some(LiquidityPositionStateFilter::Closed) => "WHERE state IN ('Closed', 'Withdrawn')",
        None => "",
    };

    let count_query = format!("SELECT COUNT(*) FROM dex_liquidity_positions {where_clause}");
    let total_count: i64 = sqlx::query_scalar(&count_query).fetch_one(db).await?;

    let length = limit.length.unwrap_or(10);
    let offset = limit.offset.unwrap_or(0);

    let query = format!(
        r"
        SELECT 
            position_id,
            decoded_position_id,
            trading_pair_asset1,
            trading_pair_asset2,
            reserves1_amount::TEXT as reserves1_amount,
            reserves2_amount::TEXT as reserves2_amount,
            state,
            fee_percentage,
            updated_at
        FROM 
            dex_liquidity_positions
        {where_clause}
        ORDER BY 
            updated_at DESC
        LIMIT $1 OFFSET $2
        "
    );

    let rows = sqlx::query(&query)
        .bind(i64::from(length))
        .bind(i64::from(offset))
        .fetch_all(db)
        .await?;

    let positions = rows
        .into_iter()
        .map(|row| {
            let fee_percentage_decimal: sqlx::types::BigDecimal = row.get("fee_percentage");
            let fee_percentage = fee_percentage_decimal
                .to_string()
                .parse::<f64>()
                .unwrap_or(0.0);

            let position_id: String = row.get("decoded_position_id");

            LiquidityPosition {
                position_id,
                trading_pair_asset1: row.get("trading_pair_asset1"),
                trading_pair_asset2: row.get("trading_pair_asset2"),
                reserves1_amount: row.get("reserves1_amount"),
                reserves2_amount: row.get("reserves2_amount"),
                state: LiquidityPositionState::from_string(&row.get::<String, _>("state")),
                fee_percentage,
                updated_at: DateTime(row.get("updated_at")),
            }
        })
        .collect();

    Ok(LiquidityPositionCollection {
        items: positions,
        total: i32::try_from(total_count).unwrap_or(0),
    })
}

/// Resolves latest swap executions grouped by block height
///
/// # Errors
/// Returns an error if the database query fails
pub async fn resolve_latest_executions(
    ctx: &async_graphql::Context<'_>,
    filter: Option<SwapExecutionFilter>,
) -> Result<Vec<SwapExecution>> {
    let db = &ctx.data_unchecked::<ApiContext>().db;
    let block_heights = fetch_block_heights(db, filter).await?;

    let mut swap_executions = Vec::new();
    for block_height in block_heights {
        if let Some(execution) = fetch_swap_execution(db, block_height).await? {
            swap_executions.push(execution);
        }
    }

    Ok(swap_executions)
}

async fn fetch_block_heights(
    db: &sqlx::PgPool,
    filter: Option<SwapExecutionFilter>,
) -> Result<Vec<i64>> {
    if let Some(filter) = filter {
        if let Some(height) = filter.height {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM dex_batch_swaps WHERE block_height = $1)",
            )
            .bind(height)
            .fetch_one(db)
            .await?;

            return Ok(if exists { vec![height] } else { vec![] });
        }
    }

    sqlx::query_scalar::<_, i64>(
        r"
        SELECT DISTINCT block_height 
        FROM dex_batch_swaps 
        ORDER BY block_height DESC 
        LIMIT 10
        ",
    )
    .fetch_all(db)
    .await
    .map_err(Into::into)
}

async fn fetch_swap_execution(
    db: &sqlx::PgPool,
    block_height: i64,
) -> Result<Option<SwapExecution>> {
    let batch_swap_rows = sqlx::query(
        r"
        SELECT 
            bs.id,
            bs.block_timestamp,
            bs.execution_type,
            bs.total_input_amount::TEXT as total_input_amount,
            bs.total_input_asset_id,
            bs.total_output_amount::TEXT as total_output_amount,
            bs.total_output_asset_id,
            bs.individual_swaps_count
        FROM 
            dex_batch_swaps bs
        WHERE 
            bs.block_height = $1
        ORDER BY 
            bs.id ASC
        ",
    )
    .bind(block_height)
    .fetch_all(db)
    .await?;

    if batch_swap_rows.is_empty() {
        return Ok(None);
    }

    let timestamp = batch_swap_rows[0].get::<chrono::DateTime<chrono::Utc>, _>("block_timestamp");
    let mut batch_swaps = Vec::new();

    for batch_row in batch_swap_rows {
        let batch_swap = fetch_batch_swap(db, &batch_row).await?;
        batch_swaps.push(batch_swap);
    }

    Ok(Some(SwapExecution {
        block_height,
        timestamp: DateTime(timestamp),
        batch_swaps,
    }))
}

async fn fetch_batch_swap(
    db: &sqlx::PgPool,
    batch_row: &sqlx::postgres::PgRow,
) -> Result<BatchSwap> {
    let batch_swap_id: i32 = batch_row.get("id");
    let individual_swaps = fetch_individual_swaps(db, batch_swap_id).await?;

    Ok(BatchSwap {
        id: batch_swap_id,
        execution_type: batch_row.get("execution_type"),
        total_input_amount: batch_row.get("total_input_amount"),
        total_input_asset_id: batch_row.get("total_input_asset_id"),
        total_output_amount: batch_row.get("total_output_amount"),
        total_output_asset_id: batch_row.get("total_output_asset_id"),
        individual_swaps_count: batch_row.get("individual_swaps_count"),
        individual_swaps,
    })
}

async fn fetch_individual_swaps(
    db: &sqlx::PgPool,
    batch_swap_id: i32,
) -> Result<Vec<IndividualSwap>> {
    let individual_swap_rows = sqlx::query(
        r"
        SELECT 
            id,
            swap_index,
            input_amount::TEXT as input_amount,
            input_asset_id,
            output_amount::TEXT as output_amount,
            output_asset_id
        FROM 
            dex_individual_swaps
        WHERE 
            batch_swap_id = $1
        ORDER BY 
            swap_index ASC
        ",
    )
    .bind(batch_swap_id)
    .fetch_all(db)
    .await?;

    let mut individual_swaps = Vec::new();
    for swap_row in individual_swap_rows {
        let individual_swap_id: i32 = swap_row.get("id");
        let route_steps = fetch_route_steps(db, individual_swap_id).await?;

        individual_swaps.push(IndividualSwap {
            swap_index: swap_row.get("swap_index"),
            input_amount: swap_row.get("input_amount"),
            input_asset_id: swap_row.get("input_asset_id"),
            output_amount: swap_row.get("output_amount"),
            output_asset_id: swap_row.get("output_asset_id"),
            route_steps,
        });
    }

    Ok(individual_swaps)
}

async fn fetch_route_steps(db: &sqlx::PgPool, individual_swap_id: i32) -> Result<Vec<RouteStep>> {
    let route_step_rows = sqlx::query(
        r"
        SELECT 
            route_step,
            amount::TEXT as amount,
            asset_id
        FROM 
            dex_individual_swap_routes
        WHERE 
            individual_swap_id = $1
        ORDER BY 
            route_step ASC
        ",
    )
    .bind(individual_swap_id)
    .fetch_all(db)
    .await?;

    Ok(route_step_rows
        .into_iter()
        .map(|route_row| RouteStep {
            route_step: route_row.get("route_step"),
            asset_id: route_row.get("asset_id"),
            amount: route_row.get("amount"),
        })
        .collect())
}

/// Resolves DEX statistics
///
/// # Errors
/// Returns an error if the database query fails
pub async fn resolve_dex_stats(ctx: &async_graphql::Context<'_>) -> Result<DexStats> {
    let db = &ctx.data_unchecked::<ApiContext>().db;

    let total_executions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM dex_batch_swaps")
        .fetch_one(db)
        .await?;

    let open_positions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM dex_liquidity_positions WHERE state IN ('Open', 'Executing')")
            .fetch_one(db)
            .await?;

    Ok(DexStats {
        total_executions,
        open_positions,
    })
}
