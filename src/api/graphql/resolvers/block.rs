use crate::api::graphql::{
    context::ApiContext,
    types::{Block, BlockCollection, BlockFilter, BlocksSelector, CollectionLimit},
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
        let raw_json_str: String = r.get("raw_json");
        // Pass the raw JSON string directly without parsing
        let raw_json = if raw_json_str.is_empty() {
            None
        } else {
            // Store the raw JSON string in a serde_json::Value
            Some(serde_json::Value::String(raw_json_str))
        };

        Block::new(
            i32::try_from(r.get::<i64, _>("height")).unwrap_or_default(),
            r.get("timestamp"),
            raw_json,
        )
    }))
}

/// Resolves multiple blocks based on the provided selector
///
/// # Errors
/// Returns an error if the database query fails
pub async fn resolve_blocks(
    ctx: &async_graphql::Context<'_>,
    selector: BlocksSelector,
) -> Result<Vec<Block>> {
    let db = &ctx.data_unchecked::<ApiContext>().db;
    let (query, _params) = build_blocks_query(&selector);
    let mut query_builder = sqlx::query(&query);
    if let Some(range) = &selector.range {
        query_builder = query_builder
            .bind(i64::from(range.from))
            .bind(i64::from(range.to));
    } else if let Some(latest) = &selector.latest {
        query_builder = query_builder.bind(i64::from(latest.limit));
    }
    let rows = query_builder.fetch_all(db).await?;
    let blocks = rows
        .into_iter()
        .map(|row| {
            let raw_json_str: String = row.get("raw_json");
            let raw_json = if raw_json_str.is_empty() {
                None
            } else {
                Some(serde_json::Value::String(raw_json_str))
            };

            Block::new(
                i32::try_from(row.get::<i64, _>("height")).unwrap_or_default(),
                row.get("timestamp"),
                raw_json,
            )
        })
        .collect();
    Ok(blocks)
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
            let raw_json_str: String = row.get("raw_json");
            let raw_json = if raw_json_str.is_empty() {
                None
            } else {
                Some(serde_json::Value::String(raw_json_str))
            };

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

fn build_blocks_query(selector: &BlocksSelector) -> (String, usize) {
    let mut query = String::from("SELECT height, timestamp, raw_json FROM explorer_block_details");
    let param_count;
    if let Some(_range) = &selector.range {
        query.push_str(" WHERE height BETWEEN $1 AND $2 ORDER BY height DESC");
        param_count = 2;
    } else if let Some(_latest) = &selector.latest {
        query.push_str(" ORDER BY height DESC LIMIT $1");
        param_count = 1;
    } else {
        query.push_str(" ORDER BY height DESC LIMIT 10");
        param_count = 0;
    }
    (query, param_count)
}
