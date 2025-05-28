use crate::api::graphql::{
    context::ApiContext,
    types::{Block, BlockCollection, BlockFilter, CollectionLimit},
};
use async_graphql::Result;
use sqlx::Row;

/// Resolves a block by its height
///
/// # Errors
/// Returns an error if the database query fails
pub async fn get(ctx: &async_graphql::Context<'_>, height: i32) -> Result<Option<Block>> {
    let db = &ctx.data_unchecked::<ApiContext>().db;
    let row = sqlx::query(
        r"
        SELECT
            height,
            timestamp,
            raw_json
        FROM
            explorer_block_details
        WHERE
            height = $1
        ",
    )
    .bind(i64::from(height))
    .fetch_optional(db)
    .await?;

    Ok(row.map(|r| {
        let raw_json: Option<serde_json::Value> = r.get::<Option<serde_json::Value>, _>("raw_json");

        Block::new(
            i32::try_from(r.get::<i64, _>("height")).unwrap_or_default(),
            r.get("timestamp"),
            raw_json,
        )
    }))
}

/// Resolves blocks with pagination and optional filtering
///
/// # Errors
/// Returns an error if the database query fails
pub async fn resolve_blocks_collection(
    ctx: &async_graphql::Context<'_>,
    limit: CollectionLimit,
    filter: Option<BlockFilter>,
) -> Result<BlockCollection> {
    let db = &ctx.data_unchecked::<ApiContext>().db;

    let mut count_query = String::from("SELECT COUNT(*) FROM explorer_block_details");

    if let Some(filter) = &filter {
        if let Some(_height) = filter.height {
            count_query.push_str(" WHERE height = $1");
        }
    }

    let total_count: i64 = if let Some(filter) = &filter {
        if let Some(height) = filter.height {
            sqlx::query_scalar(&count_query)
                .bind(i64::from(height))
                .fetch_one(db)
                .await?
        } else {
            sqlx::query_scalar(&count_query).fetch_one(db).await?
        }
    } else {
        sqlx::query_scalar(&count_query).fetch_one(db).await?
    };

    let mut query = String::from("SELECT height, timestamp, raw_json FROM explorer_block_details");
    let mut params = Vec::new();

    if let Some(filter) = &filter {
        if let Some(height) = filter.height {
            query.push_str(" WHERE height = $1");
            params.push(i64::from(height));
        }
    }

    query.push_str(" ORDER BY height DESC");

    let length = limit.length.unwrap_or(10);
    let offset = limit.offset.unwrap_or(0);

    query.push_str(&format!(" LIMIT {length} OFFSET {offset}"));

    let mut query_builder = sqlx::query(&query);

    for param in params {
        query_builder = query_builder.bind(param);
    }

    let rows = query_builder.fetch_all(db).await?;

    let blocks = rows
        .into_iter()
        .map(|row| {
            let raw_json: Option<serde_json::Value> =
                row.get::<Option<serde_json::Value>, _>("raw_json");

            Block::new(
                i32::try_from(row.get::<i64, _>("height")).unwrap_or_default(),
                row.get("timestamp"),
                raw_json,
            )
        })
        .collect();

    Ok(BlockCollection {
        items: blocks,
        total: i32::try_from(total_count).unwrap_or(0),
    })
}
