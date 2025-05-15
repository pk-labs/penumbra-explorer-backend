use anyhow::Result;
use bytes::{BufMut, BytesMut};
use hyper::client::HttpConnector;
use hyper::{Body, Client, Request, StatusCode};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use prost::Message;
use std::convert::TryFrom;
use tracing::{error, info};

#[derive(Clone, PartialEq, Eq, Message)]
pub struct QueryClientStatesRequest {}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct QueryClientStatesResponse {
    #[prost(message, repeated, tag = "1")]
    pub client_states: Vec<IdentifiedClientState>,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct IdentifiedClientState {
    #[prost(string, tag = "1")]
    pub client_id: String,
    #[prost(message, optional, tag = "2")]
    pub client_state: Option<Any>,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct QueryClientStatusRequest {
    #[prost(string, tag = "1")]
    pub client_id: String,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct QueryClientStatusResponse {
    #[prost(string, tag = "1")]
    pub status: String,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct Any {
    #[prost(string, tag = "1")]
    pub type_url: String,
    #[prost(bytes, tag = "2")]
    pub value: Vec<u8>,
}

// For querying client connections
#[derive(Clone, PartialEq, Eq, Message)]
pub struct QueryClientConnectionsRequest {
    #[prost(string, tag = "1")]
    pub client_id: String,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct QueryClientConnectionsResponse {
    #[prost(string, repeated, tag = "1")]
    pub connection_paths: Vec<String>,
    #[prost(bytes, tag = "2")]
    pub proof: Vec<u8>,
    #[prost(message, optional, tag = "3")]
    pub proof_height: Option<Height>,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct QueryConnectionChannelsRequest {
    #[prost(string, tag = "1")]
    pub connection: String,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct QueryConnectionChannelsResponse {
    #[prost(message, repeated, tag = "1")]
    pub channels: Vec<IdentifiedChannel>,
    #[prost(message, optional, tag = "2")]
    pub height: Option<Height>,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct IdentifiedChannel {
    #[prost(int32, tag = "1")]
    pub state: i32,
    #[prost(int32, tag = "2")]
    pub ordering: i32,
    #[prost(message, optional, tag = "3")]
    pub counterparty: Option<Counterparty>,
    #[prost(string, repeated, tag = "4")]
    pub connection_hops: Vec<String>,
    #[prost(string, tag = "5")]
    pub version: String,
    #[prost(string, tag = "6")]
    pub port_id: String,
    #[prost(string, tag = "7")]
    pub channel_id: String,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct Counterparty {
    #[prost(string, tag = "1")]
    pub port_id: String,
    #[prost(string, tag = "2")]
    pub channel_id: String,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct Height {
    #[prost(uint64, tag = "1")]
    pub revision_number: u64,
    #[prost(uint64, tag = "2")]
    pub revision_height: u64,
}

// Enum definitions for channel state and ordering
// These are only used in the code, not directly in protocol buffer messages
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
#[repr(i32)]
pub enum State {
    #[default]
    Uninitialized = 0,
    Init = 1,
    TryOpen = 2,
    Open = 3,
    Closed = 4,
}

impl TryFrom<i32> for State {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Uninitialized),
            1 => Ok(Self::Init),
            2 => Ok(Self::TryOpen),
            3 => Ok(Self::Open),
            4 => Ok(Self::Closed),
            _ => Err(anyhow::anyhow!("Invalid State value: {}", value)),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
#[repr(i32)]
pub enum Order {
    #[default]
    None = 0,
    Unordered = 1,
    Ordered = 2,
}

impl TryFrom<i32> for Order {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Unordered),
            2 => Ok(Self::Ordered),
            _ => Err(anyhow::anyhow!("Invalid Order value: {}", value)),
        }
    }
}

pub const CLIENT_STATUS_ACTIVE: &str = "Active";
pub const CLIENT_STATUS_FROZEN: &str = "Frozen";
pub const CLIENT_STATUS_EXPIRED: &str = "Expired";
pub const CLIENT_STATUS_UNKNOWN: &str = "Unknown";

#[allow(clippy::module_name_repetitions)]
pub struct GrpcClient {
    client: Client<HttpsConnector<HttpConnector>>,
    base_url: String,
}

impl GrpcClient {
    #[must_use]
    pub fn new(host: &str, port: u16) -> Self {
        let https = HttpsConnectorBuilder::new()
            .with_native_roots()
            .https_only()
            .enable_http1()
            .enable_http2()
            .build();

        let client = Client::builder().build(https);
        let base_url = format!("https://{host}:{port}");

        Self { client, base_url }
    }

    /// # Errors
    /// Returns an error if the request fails
    pub async fn unary<Req, Resp>(&self, path: &str, request: Req) -> Result<Resp>
    where
        Req: Message,
        Resp: Message + Default,
    {
        let req_bytes = request.encode_to_vec();

        let mut frame = BytesMut::with_capacity(req_bytes.len() + 5);
        frame.put_u8(0);
        #[allow(clippy::cast_possible_truncation)]
        frame.put_u32(req_bytes.len() as u32);
        frame.put_slice(&req_bytes);

        let uri = format!("{}{}", self.base_url, path);
        let req = Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .body(Body::from(frame.freeze()))?;

        let resp = self.client.request(req).await?;

        if resp.status() != StatusCode::OK {
            return Err(anyhow::anyhow!("HTTP error: {}", resp.status()));
        }

        let body = hyper::body::to_bytes(resp.into_body()).await?;

        if body.len() < 5 {
            return Err(anyhow::anyhow!("Response too short"));
        }

        let compression = body[0];
        if compression != 0 {
            return Err(anyhow::anyhow!("Compressed responses not supported"));
        }

        let length = ((body[1] as usize) << 24)
            | ((body[2] as usize) << 16)
            | ((body[3] as usize) << 8)
            | (body[4] as usize);

        if body.len() < 5 + length {
            return Err(anyhow::anyhow!("Response truncated"));
        }

        let mut resp = Resp::default();
        resp.merge(&body[5..5 + length])?;

        Ok(resp)
    }
}

/// # Errors
/// Returns an error if the gRPC call fails
pub async fn query_all_client_ids(client: &GrpcClient) -> Result<Vec<String>> {
    let request = QueryClientStatesRequest {};

    let response: QueryClientStatesResponse = client
        .unary("/ibc.core.client.v1.Query/ClientStates", request)
        .await?;

    let client_ids = response
        .client_states
        .into_iter()
        .map(|state| state.client_id)
        .collect();

    Ok(client_ids)
}

/// # Errors
/// Returns an error if the gRPC call fails
pub async fn query_client_status(client: &GrpcClient, client_id: &str) -> Result<String> {
    let request = QueryClientStatusRequest {
        client_id: client_id.to_string(),
    };

    let response: QueryClientStatusResponse = client
        .unary("/ibc.core.client.v1.Query/ClientStatus", request)
        .await?;

    Ok(response.status)
}

/// Query all clients and their statuses
///
/// # Errors
/// Returns an error if the gRPC calls fail
pub async fn query_all_clients(client: &GrpcClient) -> Result<Vec<(String, String)>> {
    let client_ids = query_all_client_ids(client).await?;

    if client_ids.is_empty() {
        info!("No IBC clients found");
        return Ok(Vec::new());
    }

    let mut results = Vec::with_capacity(client_ids.len());

    for client_id in &client_ids {
        match query_client_status(client, client_id).await {
            Ok(status) => {
                results.push((client_id.clone(), status));
            }
            Err(e) => {
                error!("Error querying status for client {}: {}", client_id, e);
                results.push((client_id.clone(), format!("Error: {e}")));
            }
        }
    }

    Ok(results)
}

/// # Errors
/// Returns an error if the gRPC call fails
pub async fn query_client_connections(client: &GrpcClient, client_id: &str) -> Result<Vec<String>> {
    let request = QueryClientConnectionsRequest {
        client_id: client_id.to_string(),
    };

    let response: QueryClientConnectionsResponse = client
        .unary("/ibc.core.connection.v1.Query/ClientConnections", request)
        .await?;

    Ok(response.connection_paths)
}

/// # Errors
/// Returns an error if the gRPC call fails
/// Returns a vector of tuples containing (`port_id`, `channel_id`, `counterparty_channel_id`)
/// Only returns channels with `STATE_OPEN` status
pub async fn query_connection_channels(
    client: &GrpcClient,
    connection: &str,
) -> Result<Vec<(String, String, String)>> {
    let request = QueryConnectionChannelsRequest {
        connection: connection.to_string(),
    };

    let response: QueryConnectionChannelsResponse = client
        .unary("/ibc.core.channel.v1.Query/ConnectionChannels", request)
        .await?;

    let channels = response
        .channels
        .into_iter()
        .filter(|channel| {
            if let Ok(State::Open) = State::try_from(channel.state) {
                true
            } else {
                info!(
                    "Skipping channel {} with non-open state {}",
                    channel.channel_id, channel.state
                );
                false
            }
        })
        .map(|channel| {
            let counterparty_channel_id = channel
                .counterparty
                .as_ref()
                .map_or_else(|| "unknown".to_string(), |cp| cp.channel_id.clone());

            (channel.port_id, channel.channel_id, counterparty_channel_id)
        })
        .collect();

    Ok(channels)
}

/// # Errors
/// Returns an error if the gRPC calls fail
pub async fn query_all_client_channels(
    client: &GrpcClient,
) -> Result<Vec<(String, Vec<(String, String, String)>)>> {
    let client_ids = query_all_client_ids(client).await?;

    if client_ids.is_empty() {
        info!("No IBC clients found");
        return Ok(Vec::new());
    }

    let mut results = Vec::with_capacity(client_ids.len());

    for client_id in &client_ids {
        let mut client_channels = Vec::new();

        match query_client_connections(client, client_id).await {
            Ok(connections) => {
                for connection in connections {
                    match query_connection_channels(client, &connection).await {
                        Ok(channels) => {
                            for channel_info in channels {
                                client_channels.push(channel_info);
                            }
                        }
                        Err(e) => {
                            error!(
                                "Error querying channels for connection {}: {}",
                                connection, e
                            );
                        }
                    }
                }
            }
            Err(e) => {
                error!("Error querying connections for client {}: {}", client_id, e);
            }
        }

        results.push((client_id.clone(), client_channels));
    }

    Ok(results)
}
