use crate::api::graphql::schema::PenumbraSchema;
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
    Html(
        r#"
<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8">
    <title>GraphiQL</title>
    <style>
      body { height: 100%; margin: 0; width: 100%; overflow: hidden; }
      #graphiql { height: 100vh; }
    </style>
    <script
      crossorigin
      src="https://unpkg.com/react@18/umd/react.production.min.js"
    ></script>
    <script
      crossorigin
      src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"
    ></script>
    <link rel="stylesheet" href="https://unpkg.com/graphiql/graphiql.min.css" />
  </head>
  <body>
    <div id="graphiql">Loading...</div>
    <script
      src="https://unpkg.com/graphiql/graphiql.min.js"
      type="application/javascript"
    ></script>
    <script>
      ReactDOM.render(
        React.createElement(GraphiQL, {
          fetcher: GraphiQL.createFetcher({
            url: '/graphql',
            subscriptionUrl: 'ws://' + window.location.host + '/graphql/ws',
          }),
        }),
        document.getElementById('graphiql'),
      );
    </script>
  </body>
</html>
    "#,
    )
}

pub async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

#[must_use]
pub fn create_subscription_service(schema: PenumbraSchema) -> GraphQLSubscription<PenumbraSchema> {
    GraphQLSubscription::new(schema)
}
