use crate::api::graphql::{
    pubsub::ibc,
    pubsub::PubSub,
    scalars::DateTime,
    types::ibc::TotalShieldedVolume,
    types::subscription::{
        BlockUpdate, IbcTransactionUpdate, TotalShieldedVolumeUpdate, TransactionCountUpdate,
        TransactionUpdate,
    },
    types::validator::ValidatorBlockUpdate,
};
use async_graphql::{Context, Result, Subscription};
use futures_util::stream::{Stream, StreamExt};
use sqlx::PgPool;
use std::sync::Arc;
use tracing::error;

pub struct Root;

#[Subscription]
impl Root {
    #[allow(clippy::unused_async)]
    async fn blocks(
        &self,
        ctx: &async_graphql::Context<'_>,
    ) -> async_graphql::Result<impl Stream<Item = BlockUpdate>> {
        let pubsub = ctx.data::<PubSub>()?.clone();
        let pool = Arc::new(ctx.data::<sqlx::PgPool>()?.clone());

        let receiver = pubsub.blocks_subscribe();
        let stream = tokio_stream::wrappers::BroadcastStream::new(receiver);

        Ok(StreamExt::filter_map(stream, move |result| {
            let pool_clone = Arc::clone(&pool);
            async move {
                match result {
                    Ok(height) => match get_block_data(pool_clone, height).await {
                        Ok((created_at, transactions_count)) => Some(BlockUpdate {
                            height,
                            created_at: DateTime(created_at),
                            transactions_count,
                        }),
                        Err(e) => {
                            tracing::error!("Failed to get block data: {}", e);
                            None
                        }
                    },
                    Err(_) => None,
                }
            }
        }))
    }

    #[allow(clippy::unused_async)]
    async fn transactions(
        &self,
        ctx: &async_graphql::Context<'_>,
    ) -> async_graphql::Result<impl Stream<Item = TransactionUpdate>> {
        let pubsub = ctx.data::<PubSub>()?.clone();
        let pool = Arc::new(ctx.data::<sqlx::PgPool>()?.clone());

        let receiver = pubsub.transactions_subscribe();
        let stream = tokio_stream::wrappers::BroadcastStream::new(receiver);

        Ok(StreamExt::filter_map(stream, move |result| {
            let pool_clone = Arc::clone(&pool);
            async move {
                match result {
                    Ok(block_height) => {
                        match get_transaction_data(pool_clone, block_height).await {
                            Ok((hash, raw_data)) => Some(TransactionUpdate {
                                id: block_height,
                                hash,
                                raw: raw_data,
                            }),
                            Err(e) => {
                                tracing::error!("Failed to get transaction data: {}", e);
                                None
                            }
                        }
                    }
                    Err(_) => None,
                }
            }
        }))
    }

    #[allow(clippy::unused_async)]
    async fn transaction_count(
        &self,
        ctx: &async_graphql::Context<'_>,
    ) -> async_graphql::Result<impl Stream<Item = TransactionCountUpdate>> {
        let pubsub = ctx.data::<PubSub>()?.clone();
        let pool = Arc::new(ctx.data::<PgPool>()?.clone());

        let receiver = pubsub.transaction_count_subscribe();
        let stream = tokio_stream::wrappers::BroadcastStream::new(receiver);

        let initial_count =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM explorer_transactions")
                .fetch_one(&*pool)
                .await
                .unwrap_or(0);

        let initial = futures_util::stream::once(async move {
            Some(TransactionCountUpdate {
                count: initial_count,
            })
        });

        let real_time = stream.map(|result| {
            if let Ok(count) = result {
                Some(TransactionCountUpdate { count })
            } else {
                tracing::error!("Failed to receive transaction count from broadcast channel");
                None
            }
        });

        Ok(futures_util::stream::select(initial, real_time).filter_map(|x| async move { x }))
    }

    #[allow(clippy::unused_async)]
    async fn ibc_transactions(
        &self,
        ctx: &Context<'_>,
        client_id: Option<String>,
        limit: Option<i32>,
        offset: Option<i32>,
    ) -> Result<impl Stream<Item = IbcTransactionUpdate>> {
        let pubsub = ctx.data::<PubSub>()?.clone();
        let pool = Arc::new(ctx.data::<PgPool>()?.clone());

        let limit = limit.unwrap_or(10);
        let offset = offset.unwrap_or(0);

        let initial_txs = match &client_id {
            Some(id) => {
                match ibc::get_ibc_transactions_by_client(pool.as_ref(), id, limit, offset).await {
                    Ok(txs) => txs
                        .into_iter()
                        .map(
                            |(tx_hash, client_id, status, block_height, timestamp, raw_data)| {
                                IbcTransactionUpdate {
                                    tx_hash: hex::encode_upper(&tx_hash),
                                    client_id,
                                    status,
                                    block_height,
                                    timestamp: DateTime(timestamp),
                                    is_status_update: false,
                                    raw: raw_data,
                                }
                            },
                        )
                        .collect::<Vec<_>>(),
                    Err(e) => {
                        error!("Failed to get initial IBC transactions by client: {}", e);
                        Vec::new()
                    }
                }
            }
            None => match ibc::get_all_ibc_transactions(pool.as_ref(), limit, offset).await {
                Ok(txs) => txs
                    .into_iter()
                    .map(
                        |(tx_hash, client_id, status, block_height, timestamp, raw_data)| {
                            IbcTransactionUpdate {
                                tx_hash: hex::encode_upper(&tx_hash),
                                client_id,
                                status,
                                block_height,
                                timestamp: DateTime(timestamp),
                                is_status_update: false,
                                raw: raw_data,
                            }
                        },
                    )
                    .collect::<Vec<_>>(),
                Err(e) => {
                    error!("Failed to get all initial IBC transactions: {}", e);
                    Vec::new()
                }
            },
        };

        let initial_stream = futures_util::stream::iter(initial_txs);

        let receiver = pubsub.ibc_transactions_subscribe();
        let stream = tokio_stream::wrappers::BroadcastStream::new(receiver);

        let real_time_stream = stream.filter_map(move |result| {
            let pool_clone = Arc::clone(&pool);
            let client_filter = client_id.clone();

            async move {
                match result {
                    Ok(event) => {
                        if let Some(filter) = &client_filter {
                            if event.client_id != *filter {
                                return None;
                            }
                        }

                        match get_ibc_tx_details(&pool_clone, &event.tx_hash).await {
                            Ok((block_height, timestamp, status, raw_data)) => {
                                Some(IbcTransactionUpdate {
                                    tx_hash: hex::encode_upper(&event.tx_hash),
                                    client_id: event.client_id,
                                    status,
                                    block_height,
                                    timestamp: DateTime(timestamp),
                                    is_status_update: event.is_status_update,
                                    raw: raw_data,
                                })
                            }
                            Err(e) => {
                                error!("Failed to get transaction details: {}", e);
                                None
                            }
                        }
                    }
                    Err(e) => {
                        error!("Error receiving IBC transaction update: {}", e);
                        None
                    }
                }
            }
        });

        let combined_stream = initial_stream.chain(real_time_stream);

        Ok(combined_stream)
    }

    #[allow(clippy::unused_async)]
    async fn total_shielded_volume(
        &self,
        ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = TotalShieldedVolumeUpdate>> {
        let pubsub = ctx.data::<PubSub>()?.clone();

        let _pool = Arc::new(ctx.data::<PgPool>()?.clone());

        let initial_value = match TotalShieldedVolume::get(ctx).await {
            Ok(value) => value.value,
            Err(e) => {
                error!("Failed to get initial total shielded volume: {:?}", e);
                "0".to_string()
            }
        };

        let initial_stream = futures_util::stream::once(async move {
            TotalShieldedVolumeUpdate {
                value: initial_value,
            }
        });

        let receiver = pubsub.total_shielded_volume_subscribe();
        let stream = tokio_stream::wrappers::BroadcastStream::new(receiver);

        let real_time_stream = stream.filter_map(move |result| async move {
            match result {
                Ok(value) => Some(TotalShieldedVolumeUpdate { value }),
                Err(e) => {
                    error!("Error receiving total shielded volume update: {}", e);
                    None
                }
            }
        });

        Ok(initial_stream.chain(real_time_stream))
    }

    async fn latest_blocks(
        &self,
        ctx: &Context<'_>,
        limit: Option<i32>,
    ) -> Result<impl Stream<Item = BlockUpdate> + '_> {
        let pubsub = ctx.data::<PubSub>()?.clone();
        let pool = Arc::new(ctx.data::<sqlx::PgPool>()?.clone());

        let limit = limit.unwrap_or(10);

        let initial_blocks = get_latest_blocks(Arc::clone(&pool), limit).await?;

        let initial_stream = futures_util::stream::iter(initial_blocks);

        let receiver = pubsub.blocks_subscribe();
        let stream = tokio_stream::wrappers::BroadcastStream::new(receiver);
        let real_time_stream = StreamExt::filter_map(stream, move |result| {
            let pool_clone = Arc::clone(&pool);
            async move {
                match result {
                    Ok(height) => match get_block_data(pool_clone, height).await {
                        Ok((created_at, transactions_count)) => Some(BlockUpdate {
                            height,
                            created_at: DateTime(created_at),
                            transactions_count,
                        }),
                        Err(e) => {
                            tracing::error!("Failed to get block data: {}", e);
                            None
                        }
                    },
                    Err(_) => None,
                }
            }
        });

        let combined_stream = StreamExt::chain(initial_stream, real_time_stream);

        Ok(combined_stream)
    }

    async fn latest_transactions(
        &self,
        ctx: &Context<'_>,
        limit: Option<i32>,
    ) -> Result<impl Stream<Item = TransactionUpdate> + '_> {
        let pubsub = ctx.data::<PubSub>()?.clone();
        let pool = Arc::new(ctx.data::<sqlx::PgPool>()?.clone());

        let limit = limit.unwrap_or(10);

        let initial_transactions = get_latest_transactions(Arc::clone(&pool), limit).await?;

        let initial_stream = futures_util::stream::iter(initial_transactions);

        let receiver = pubsub.transactions_subscribe();
        let stream = tokio_stream::wrappers::BroadcastStream::new(receiver);
        let real_time_stream = StreamExt::filter_map(stream, move |result| {
            let pool_clone = Arc::clone(&pool);
            async move {
                match result {
                    Ok(block_height) => {
                        match get_transaction_data(pool_clone, block_height).await {
                            Ok((hash, raw_data)) => Some(TransactionUpdate {
                                id: block_height,
                                hash,
                                raw: raw_data,
                            }),
                            Err(e) => {
                                tracing::error!("Failed to get transaction data: {}", e);
                                None
                            }
                        }
                    }
                    Err(_) => None,
                }
            }
        });

        let combined_stream = StreamExt::chain(initial_stream, real_time_stream);

        Ok(combined_stream)
    }

    async fn latest_ibc_transactions(
        &self,
        ctx: &Context<'_>,
        limit: Option<i32>,
        client_id: Option<String>,
    ) -> Result<impl Stream<Item = IbcTransactionUpdate> + '_> {
        let pubsub = ctx.data::<PubSub>()?.clone();
        let pool = Arc::new(ctx.data::<sqlx::PgPool>()?.clone());

        let limit = limit.unwrap_or(10);

        let initial_transactions = match &client_id {
            Some(id) => get_latest_ibc_transactions_by_client(Arc::clone(&pool), id, limit).await?,
            None => get_latest_ibc_transactions(Arc::clone(&pool), limit).await?,
        };

        let initial_stream = futures_util::stream::iter(initial_transactions);

        let receiver = pubsub.ibc_transactions_subscribe();
        let stream = tokio_stream::wrappers::BroadcastStream::new(receiver);

        let real_time_stream = stream.filter_map(move |result| {
            let pool_clone = Arc::clone(&pool);
            let client_filter = client_id.clone();

            async move {
                match result {
                    Ok(event) => {
                        if let Some(filter) = &client_filter {
                            if event.client_id != *filter {
                                return None;
                            }
                        }

                        match get_ibc_tx_details(&pool_clone, &event.tx_hash).await {
                            Ok((block_height, timestamp, status, raw_data)) => {
                                Some(IbcTransactionUpdate {
                                    tx_hash: hex::encode_upper(&event.tx_hash),
                                    client_id: event.client_id,
                                    status,
                                    block_height,
                                    timestamp: DateTime(timestamp),
                                    is_status_update: event.is_status_update,
                                    raw: raw_data,
                                })
                            }
                            Err(e) => {
                                error!("Failed to get transaction details: {}", e);
                                None
                            }
                        }
                    }
                    Err(_) => None,
                }
            }
        });

        let combined_stream = StreamExt::chain(initial_stream, real_time_stream);

        Ok(combined_stream)
    }

    async fn validator_blocks(
        &self,
        ctx: &Context<'_>,
        validator_id: String,
    ) -> Result<impl Stream<Item = ValidatorBlockUpdate> + '_> {
        let pool = ctx.data::<PgPool>()?;
        let pubsub = ctx.data::<PubSub>()?;
        
        let receiver = pubsub.validator_blocks_subscribe(validator_id, pool.clone()).await;
        let stream = tokio_stream::wrappers::BroadcastStream::new(receiver);
        
        Ok(stream.filter_map(|result| async move {
            match result {
                Ok(event) => Some(ValidatorBlockUpdate {
                    validator_id: event.validator_id,
                    block_height: event.block_height,
                    signed: event.signed,
                }),
                Err(e) => {
                    error!("Error receiving validator block update: {}", e);
                    None
                }
            }
        }))
    }
}

async fn get_block_data(
    pool: Arc<PgPool>,
    height: i64,
) -> Result<(chrono::DateTime<chrono::Utc>, i32), sqlx::Error> {
    let row = sqlx::query_as::<_, (chrono::DateTime<chrono::Utc>, i32)>(
        "SELECT timestamp, num_transactions FROM explorer_block_details WHERE height = $1",
    )
    .bind(height)
    .fetch_one(pool.as_ref())
    .await?;

    Ok(row)
}

async fn get_transaction_data(
    pool: Arc<PgPool>,
    block_height: i64,
) -> Result<(String, String), sqlx::Error> {
    let row = sqlx::query_as::<_, (Vec<u8>, String)>(
        "SELECT tx_hash, raw_data FROM explorer_transactions WHERE block_height = $1 LIMIT 1",
    )
    .bind(block_height)
    .fetch_one(pool.as_ref())
    .await?;

    let hash_hex = hex::encode_upper(&row.0);

    Ok((hash_hex, row.1))
}

async fn get_ibc_tx_details(
    pool: &Arc<PgPool>,
    tx_hash: &[u8],
) -> Result<(i64, chrono::DateTime<chrono::Utc>, String, String), sqlx::Error> {
    sqlx::query_as::<_, (i64, chrono::DateTime<chrono::Utc>, String, String)>(
        "SELECT block_height, timestamp, ibc_status, raw_data FROM explorer_transactions WHERE tx_hash = $1"
    )
        .bind(tx_hash)
        .fetch_one(pool.as_ref())
        .await
}

async fn get_latest_blocks(pool: Arc<PgPool>, limit: i32) -> Result<Vec<BlockUpdate>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (i64, chrono::DateTime<chrono::Utc>, i32)>(
        "SELECT height, timestamp, num_transactions FROM explorer_block_details
         ORDER BY height DESC LIMIT $1",
    )
    .bind(i64::from(limit))
    .fetch_all(pool.as_ref())
    .await?;

    let blocks = rows
        .into_iter()
        .rev()
        .map(|(height, timestamp, num_transactions)| BlockUpdate {
            height,
            created_at: DateTime(timestamp),
            transactions_count: num_transactions,
        })
        .collect();

    Ok(blocks)
}

async fn get_latest_transactions(
    pool: Arc<PgPool>,
    limit: i32,
) -> Result<Vec<TransactionUpdate>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (i64, Vec<u8>, String)>(
        "SELECT block_height, tx_hash, raw_data FROM explorer_transactions
         ORDER BY block_height DESC LIMIT $1",
    )
    .bind(i64::from(limit))
    .fetch_all(pool.as_ref())
    .await?;

    let transactions = rows
        .into_iter()
        .rev()
        .map(|(block_height, tx_hash, raw_data)| {
            let hash = hex::encode_upper(&tx_hash);

            TransactionUpdate {
                id: block_height,
                hash,
                raw: raw_data,
            }
        })
        .collect();

    Ok(transactions)
}

async fn get_latest_ibc_transactions(
    pool: Arc<PgPool>,
    limit: i32,
) -> Result<Vec<IbcTransactionUpdate>, sqlx::Error> {
    let rows = sqlx::query_as::<
        _,
        (
            Vec<u8>,
            String,
            String,
            i64,
            chrono::DateTime<chrono::Utc>,
            String,
        ),
    >(
        "SELECT tx_hash, ibc_client_id, ibc_status, block_height, timestamp, raw_data
         FROM explorer_transactions
         WHERE ibc_client_id IS NOT NULL
         ORDER BY timestamp DESC LIMIT $1",
    )
    .bind(i64::from(limit))
    .fetch_all(pool.as_ref())
    .await?;

    let transactions = rows
        .into_iter()
        .map(
            |(tx_hash, client_id, status, block_height, timestamp, raw_data)| {
                let hash = hex::encode_upper(&tx_hash);

                IbcTransactionUpdate {
                    tx_hash: hash,
                    client_id,
                    status,
                    block_height,
                    timestamp: DateTime(timestamp),
                    is_status_update: false,
                    raw: raw_data,
                }
            },
        )
        .collect();

    Ok(transactions)
}

async fn get_latest_ibc_transactions_by_client(
    pool: Arc<PgPool>,
    client_id: &str,
    limit: i32,
) -> Result<Vec<IbcTransactionUpdate>, sqlx::Error> {
    let rows = sqlx::query_as::<
        _,
        (
            Vec<u8>,
            String,
            String,
            i64,
            chrono::DateTime<chrono::Utc>,
            String,
        ),
    >(
        "SELECT tx_hash, ibc_client_id, ibc_status, block_height, timestamp, raw_data
         FROM explorer_transactions
         WHERE ibc_client_id = $1
         ORDER BY timestamp DESC LIMIT $2",
    )
    .bind(client_id)
    .bind(i64::from(limit))
    .fetch_all(pool.as_ref())
    .await?;

    let transactions = rows
        .into_iter()
        .map(
            |(tx_hash, client_id, status, block_height, timestamp, raw_data)| {
                let hash = hex::encode_upper(&tx_hash);

                IbcTransactionUpdate {
                    tx_hash: hash,
                    client_id,
                    status,
                    block_height,
                    timestamp: DateTime(timestamp),
                    is_status_update: false,
                    raw: raw_data,
                }
            },
        )
        .collect();

    Ok(transactions)
}
