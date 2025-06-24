use crate::api::graphql::schema::PenumbraSchema;
use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use axum::{
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse},
};

pub async fn graphql_handler(
    State(schema): State<PenumbraSchema>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    schema.execute(req.0).await.into()
}

pub async fn graphiql() -> impl IntoResponse {
    Html(playground_source(GraphQLPlaygroundConfig::new("/graphql").subscription_endpoint("/graphql/ws")))
}

pub async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

#[must_use]
pub fn create_subscription_service(schema: PenumbraSchema) -> GraphQLSubscription<PenumbraSchema> {
    GraphQLSubscription::new(schema)
}
