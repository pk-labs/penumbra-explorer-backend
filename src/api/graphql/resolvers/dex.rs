use crate::api::graphql::{
    context::ApiContext,
    scalars::DateTime,
    types::{CollectionLimit, LiquidityPosition, LiquidityPositionCollection},
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
) -> Result<LiquidityPositionCollection> {
    let db = &ctx.data_unchecked::<ApiContext>().db;

    let total_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM dex_liquidity_positions"
    )
    .fetch_one(db)
    .await?;

    let length = limit.length.unwrap_or(10);
    let offset = limit.offset.unwrap_or(0);

    let rows = sqlx::query(
        r"
        SELECT 
            position_id,
            trading_pair_asset1,
            trading_pair_asset2,
            reserves1_amount::TEXT as reserves1_amount,
            reserves2_amount::TEXT as reserves2_amount,
            state,
            fee_percentage,
            updated_at
        FROM 
            dex_liquidity_positions
        ORDER BY 
            updated_at DESC
        LIMIT $1 OFFSET $2
        "
    )
    .bind(i64::from(length))
    .bind(i64::from(offset))
    .fetch_all(db)
    .await?;

    let positions = rows
        .into_iter()
        .map(|row| {
            let fee_percentage_decimal: sqlx::types::BigDecimal = row.get("fee_percentage");
            let fee_percentage = fee_percentage_decimal.to_string().parse::<f64>().unwrap_or(0.0);

            LiquidityPosition {
                position_id: row.get("position_id"),
                trading_pair_asset1: row.get("trading_pair_asset1"),
                trading_pair_asset2: row.get("trading_pair_asset2"),
                reserves1_amount: row.get("reserves1_amount"),
                reserves2_amount: row.get("reserves2_amount"),
                state: row.get("state"),
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