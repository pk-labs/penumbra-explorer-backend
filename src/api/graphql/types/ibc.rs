use crate::api::graphql::scalars::DateTime;
use async_graphql::{Context, Result, SimpleObject};
use sqlx::Row;

#[derive(SimpleObject)]
#[graphql(rename_fields = "camelCase")]
pub struct ChannelPair {
    // Our local channel
    pub channel_id: String,
    // The channel ID on the counterparty chain
    pub counterparty_channel_id: Option<String>,
    // The client ID associated with this channel
    pub client_id: String,
    // The connection ID if available
    pub connection_id: Option<String>,
    // Count of pending transactions over this channel
    pub pending_tx_count: i64,
    // Count of completed transactions over this channel
    pub completed_tx_count: i64,
}

#[derive(SimpleObject)]
#[graphql(name = "IbcStats")]
pub struct Stats {
    pub client_id: String,
    pub shielded_volume: String,
    pub shielded_tx_count: i64,
    pub unshielded_volume: String,
    pub unshielded_tx_count: i64,
    pub pending_tx_count: i64,
    pub expired_tx_count: i64,
    #[graphql(name = "lastUpdated")]
    pub last_updated: Option<DateTime>,
}

#[derive(SimpleObject)]
#[graphql(rename_fields = "camelCase")]
pub struct TotalShieldedVolume {
    /// Total shielded volume across all IBC clients
    pub value: String,
}

impl ChannelPair {
    /// Gets channel pairs for a specific client ID
    ///
    /// # Errors
    /// Returns an error if the database query fails
    pub async fn get_by_client_id(ctx: &Context<'_>, client_id: String) -> Result<Vec<Self>> {
        let db = &ctx
            .data_unchecked::<crate::api::graphql::context::ApiContext>()
            .db;

        let rows = sqlx::query(
            r"
            SELECT
                c.channel_id,
                c.client_id,
                c.connection_id,
                c.counterparty_channel_id,
                COUNT(CASE WHEN t.ibc_status = 'pending' THEN 1 ELSE NULL END) AS pending_tx_count,
                COUNT(CASE WHEN t.ibc_status = 'completed' THEN 1 ELSE NULL END) AS completed_tx_count
            FROM
                ibc_channels c
            LEFT JOIN
                explorer_transactions t ON c.channel_id = t.ibc_channel_id
            WHERE
                c.client_id = $1
            GROUP BY
                c.channel_id, c.client_id, c.connection_id, c.counterparty_channel_id
            ORDER BY
                c.channel_id
            ",
        )
            .bind(client_id)
            .fetch_all(db)
            .await?;

        Ok(rows
            .into_iter()
            .map(|row| ChannelPair {
                channel_id: row.get("channel_id"),
                client_id: row.get("client_id"),
                connection_id: row.get("connection_id"),
                counterparty_channel_id: row.get("counterparty_channel_id"),
                pending_tx_count: row.get("pending_tx_count"),
                completed_tx_count: row.get("completed_tx_count"),
            })
            .collect())
    }

    /// Gets all channel pairs with optional filtering
    ///
    /// # Errors
    /// Returns an error if the database query fails
    pub async fn get_all(
        ctx: &Context<'_>,
        client_id: Option<String>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<Self>> {
        let db = &ctx
            .data_unchecked::<crate::api::graphql::context::ApiContext>()
            .db;

        let limit = limit.unwrap_or(100);
        let offset = offset.unwrap_or(0);

        let mut query =
            r"
            SELECT
                c.channel_id,
                c.client_id,
                c.connection_id,
                c.counterparty_channel_id,
                COUNT(CASE WHEN t.ibc_status = 'pending' THEN 1 ELSE NULL END) AS pending_tx_count,
                COUNT(CASE WHEN t.ibc_status = 'completed' THEN 1 ELSE NULL END) AS completed_tx_count
            FROM
                ibc_channels c
            LEFT JOIN
                explorer_transactions t ON c.channel_id = t.ibc_channel_id
            ".to_string();

        if client_id.is_some() {
            query.push_str(" WHERE c.client_id = $1");
        }

        query.push_str(
            " GROUP BY c.channel_id, c.client_id, c.connection_id, c.counterparty_channel_id",
        );
        query.push_str(" ORDER BY c.channel_id");
        query.push_str(&format!(" LIMIT {limit} OFFSET {offset}"));

        let rows = if let Some(client_id_val) = client_id {
            sqlx::query(&query)
                .bind(client_id_val)
                .fetch_all(db)
                .await?
        } else {
            sqlx::query(&query).fetch_all(db).await?
        };

        Ok(rows
            .into_iter()
            .map(|row| ChannelPair {
                channel_id: row.get("channel_id"),
                client_id: row.get("client_id"),
                connection_id: row.get("connection_id"),
                counterparty_channel_id: row.get("counterparty_channel_id"),
                pending_tx_count: row.get("pending_tx_count"),
                completed_tx_count: row.get("completed_tx_count"),
            })
            .collect())
    }
}

impl Stats {
    /// Gets the appropriate view name based on the time period
    fn get_view_name(time_period: Option<&str>) -> &'static str {
        match time_period {
            Some("24h") => "ibc_client_summary_24h",
            Some("30d") => "ibc_client_summary_30d",
            _ => "ibc_client_summary",
        }
    }

    /// Gets IBC stats with optional filtering
    ///
    /// # Errors
    /// Returns an error if the database query fails
    pub async fn get_all(
        ctx: &Context<'_>,
        client_id: Option<String>,
        time_period: Option<String>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<Self>> {
        let db = &ctx
            .data_unchecked::<crate::api::graphql::context::ApiContext>()
            .db;

        let limit = limit.unwrap_or(100);
        let offset = offset.unwrap_or(0);
        let view_name = Self::get_view_name(time_period.as_deref());

        let mut query = format!(
            "SELECT
                client_id,
                shielded_volume::TEXT as shielded_volume,
                shielded_tx_count,
                unshielded_volume::TEXT as unshielded_volume,
                unshielded_tx_count,
                pending_tx_count,
                expired_tx_count,
                last_updated
            FROM {view_name}"
        );

        if client_id.is_some() {
            query.push_str(" WHERE client_id = $1");
        }

        query.push_str(" ORDER BY client_id");
        query.push_str(&format!(" LIMIT {limit} OFFSET {offset}"));

        let rows = if let Some(client_id_val) = client_id {
            sqlx::query(&query)
                .bind(client_id_val)
                .fetch_all(db)
                .await?
        } else {
            sqlx::query(&query).fetch_all(db).await?
        };

        Ok(rows
            .into_iter()
            .map(|row| Stats {
                client_id: row.get("client_id"),
                shielded_volume: row.get("shielded_volume"),
                shielded_tx_count: row.get("shielded_tx_count"),
                unshielded_volume: row.get("unshielded_volume"),
                unshielded_tx_count: row.get("unshielded_tx_count"),
                pending_tx_count: row.get("pending_tx_count"),
                expired_tx_count: row.get("expired_tx_count"),
                last_updated: row
                    .get::<Option<chrono::DateTime<chrono::Utc>>, _>("last_updated")
                    .map(DateTime),
            })
            .collect())
    }

    /// Gets a specific IBC stats entry by `client_id`
    ///
    /// # Errors
    /// Returns an error if the database query fails
    pub async fn get_by_client_id(
        ctx: &Context<'_>,
        client_id: String,
        time_period: Option<String>,
    ) -> Result<Option<Self>> {
        let db = &ctx
            .data_unchecked::<crate::api::graphql::context::ApiContext>()
            .db;

        let view_name = Self::get_view_name(time_period.as_deref());

        let row = sqlx::query(&format!(
            "SELECT
                client_id,
                shielded_volume::TEXT as shielded_volume,
                shielded_tx_count,
                unshielded_volume::TEXT as unshielded_volume,
                unshielded_tx_count,
                pending_tx_count,
                expired_tx_count,
                last_updated
            FROM {view_name}
            WHERE client_id = $1"
        ))
        .bind(client_id)
        .fetch_optional(db)
        .await?;

        Ok(row.map(|row| Stats {
            client_id: row.get("client_id"),
            shielded_volume: row.get("shielded_volume"),
            shielded_tx_count: row.get("shielded_tx_count"),
            unshielded_volume: row.get("unshielded_volume"),
            unshielded_tx_count: row.get("unshielded_tx_count"),
            pending_tx_count: row.get("pending_tx_count"),
            expired_tx_count: row.get("expired_tx_count"),
            last_updated: row
                .get::<Option<chrono::DateTime<chrono::Utc>>, _>("last_updated")
                .map(DateTime),
        }))
    }
}

impl TotalShieldedVolume {
    /// Gets total shielded volume across all IBC clients
    ///
    /// # Errors
    /// Returns an error if the database query fails
    pub async fn get(ctx: &Context<'_>) -> Result<Self> {
        let db = &ctx
            .data_unchecked::<crate::api::graphql::context::ApiContext>()
            .db;

        let total = sqlx::query_scalar::<_, String>(
            r"
            SELECT
                COALESCE(SUM(shielded_volume), '0')::TEXT
            FROM
                ibc_client_summary
            ",
        )
        .fetch_one(db)
        .await?;

        Ok(Self { value: total })
    }
}
