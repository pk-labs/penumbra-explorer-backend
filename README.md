<p align="center">
  <a href="https://github.com/tokio-rs/tokio">
    <img src="https://img.shields.io/badge/powered%20by-tokio-blue?style=flat&logo=rust" alt="Powered by Tokio" />
  </a>
  <a href="https://github.com/penumbra-zone/penumbra/tree/main/crates/bin/pindexer">
    <img src="https://img.shields.io/badge/built%20with-pindexer-blueviolet?style=flat" alt="Built with Pindexer" />
  </a>
  <a href="https://github.com/penumbra-zone/penumbra/tree/main/crates/util/cometindex">
    <img src="https://img.shields.io/badge/index%20by-cometindex-6A0DAD?style=flat" alt="Index by CometIndex" />
  </a>
  <a href="https://www.postgresql.org/">
    <img src="https://img.shields.io/badge/database-postgresql-blue?style=flat&logo=postgresql" alt="Database PostgreSQL" />
  </a>
  <a href="https://www.docker.com/">
    <img src="https://img.shields.io/badge/containerized-docker-2496ED?style=flat&logo=docker" alt="Containerized with Docker" />
  </a>
  <br />
  <a href="https://github.com/rust-lang/rustfmt">
    <img src="https://img.shields.io/badge/code--style-rustfmt-fc8d62?style=flat" alt="Code Style: rustfmt" />
  </a>
  <a href="https://github.com/rust-lang/rust-clippy">
    <img src="https://img.shields.io/badge/linted%20with-clippy-ffc832?style=flat" alt="Linted with Clippy" />
  </a>
  <br />
  <br />
</p>

# Penumbra Explorer Backend

Backend indexer for exploring the Penumbra blockchain built with Rust

## Getting Started

1. Set up Rust (1.83.0 or later)
2. Install dependencies with `cargo build`
3. Run the application with:
   ```sh
   cargo run -- \
     -s "postgresql://user:password@source-host:5432/source-db?sslmode=require" \
     -d "postgresql://user:password@dest-host:5432/dest-db" \
     --genesis-json genesis.json
   ```

### Cargo Scripts

| Script                                                                  | Description                                     |
|-------------------------------------------------------------------------|-------------------------------------------------|
| `cargo build --release`                                                 | Build app in release mode                       |
| `cargo run -- -s "SOURCE_DB" -d "DEST_DB" --genesis-json genesis.json`  | Run application with required parameters        |
| `cargo test`                                                            | Run tests                                       |
| `cargo test <test_name>`                                                | Run a specific test                             |
| `cargo clippy --all-targets --all-features --workspace -- -W clippy::pedantic -D warnings` | Lint using strictest clippy rules |
| `cargo fmt`                                                             | Format code using rustfmt                       |

### Project Structure

| Directory         | Description                                                   |
|-------------------|---------------------------------------------------------------|
| `migrations/`     | Database migration scripts                                    |
| `src/api/`        | API implementation (GraphQL resolvers, handlers)              |
| `src/api/graphql/`| GraphQL schema, resolvers, and types                          |
| `src/app_views/`  | The heart of the blockchain indexing process                  |
| `src/`            | Core application code                                         |

## Docker

You can also run the application using Docker:

```sh
docker-compose up -d
```

View logs:
```sh
docker-compose logs -f app
```

## API

The GraphQL API is accessible at:
- GraphQL API: `/graphql`
- GraphQL Playground: `/graphql/playground`
- Public playground for testing: [https://api.explorer.penumbra.zone/](https://api.explorer.penumbra.zone/)

### WebSocket Support

The Penumbra Explorer Backend supports real-time updates via WebSocket GraphQL subscriptions. This enables live streaming of new blocks, transactions, and transaction count changes directly to subscribed clients.

Example subscriptions:
```graphql
subscription {
  latestBlocks(limit: 10) {
    height
    createdAt
    transactionsCount
  }
}

subscription {
  latestTransactions(limit: 10) {
    id
    hash
    raw
  }
}

subscription {
  transactionCount {
    count
  }
}
```

### Example Queries

You can try the following sample GraphQL queries in the public playground:

#### Full Transaction Query
```graphql
query {
  transaction(hash: "84A8AD3E364E1F98A7C73A5D411737F634A7802265C71A528EA9720BD2ED8095") {
    hash
    block {
      height
      createdAt
    }
    body {
      memo
      parameters {
        chainId
        fee {
          amount
        }
      }
    }
    raw
    rawJson
  }
}
```

#### Full Block Query (replace with actual query if available)
```graphql
query {
  block(height: 123456) {
    height
    createdAt
    transactions {
      hash
      body {
        memo
      }
    }
  }
}
```

## Configuration

### Command Line Arguments

- `-s, --source-db-url`: Source database URL (required)
- `-d, --dest-db-url`: Destination database URL (required)
- `--genesis-json`: Path to Genesis JSON file (required)
- `--from-height`: Starting block height (optional)
- `--to-height`: Ending block height (optional)
- `--batch-size`: Batch size for processing blocks (default: 100)
- `--polling-interval-ms`: Polling interval in milliseconds (default: 1000)

## Testing

Tests are organized by module and can be run using `cargo test`. Critical components have unit tests covering core functionality.

### Test Coverage

Key components with test coverage:
- Transaction queue management
- Parsing utilities
- Block and transaction processing

## Linting and Code Style

The project uses:
- rustfmt for formatting: `cargo fmt`
- Clippy for linting with strict rules: `cargo clippy --all-targets --all-features --workspace -- -W clippy::pedantic -D warnings`

The codebase follows Rust's naming conventions:
- snake_case for variables/functions
- CamelCase for types/traits
