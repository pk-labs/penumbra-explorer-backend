use anyhow::Result;
use clap::Parser;
use dotenv::dotenv;
use penumbra_explorer::{Explorer, ExplorerOptions};
use std::env;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let mut opts = ExplorerOptions::parse();

    if opts.source_db_url.is_empty() {
        opts.source_db_url = env::var("SOURCE_DB_URL").unwrap_or_else(|_| {
            eprintln!("ERROR: SOURCE_DB_URL not provided via argument or environment variable");
            std::process::exit(1);
        });
    }

    if opts.dest_db_url.is_empty() {
        opts.dest_db_url = env::var("DEST_DB_URL").unwrap_or_else(|_| {
            eprintln!("ERROR: DEST_DB_URL not provided via argument or environment variable");
            std::process::exit(1);
        });
    }

    if let Ok(genesis_path) = env::var("GENESIS_JSON") {
        opts.genesis_json = genesis_path;
    }

    if let Ok(from_height) = env::var("FROM_HEIGHT") {
        if let Ok(height) = from_height.parse::<u64>() {
            opts.from_height = Some(height);
        }
    }

    if let Ok(to_height) = env::var("TO_HEIGHT") {
        if let Ok(height) = to_height.parse::<u64>() {
            opts.to_height = Some(height);
        }
    }

    if let Ok(batch_size) = env::var("BATCH_SIZE") {
        if let Ok(size) = batch_size.parse::<u64>() {
            opts.batch_size = size;
        }
    }

    if let Ok(polling_interval) = env::var("POLLING_INTERVAL_MS") {
        if let Ok(interval) = polling_interval.parse::<u64>() {
            opts.polling_interval_ms = interval;
        }
    }

    tracing::info!("Configuration:");
    tracing::info!("  Source DB URL: {}", sensitive_url(&opts.source_db_url));
    tracing::info!("  Destination DB URL: {}", sensitive_url(&opts.dest_db_url));
    tracing::info!("  Genesis JSON: {}", opts.genesis_json);
    tracing::info!("  From Height: {:?}", opts.from_height);
    tracing::info!("  To Height: {:?}", opts.to_height);
    tracing::info!("  Batch Size: {}", opts.batch_size);
    tracing::info!("  Polling Interval (ms): {}", opts.polling_interval_ms);

    penumbra_explorer::db_migrations::run_migrations(&opts.dest_db_url)?;
    penumbra_explorer::grpc::start_ibc_status_scheduler();

    let explorer = Explorer::new(opts);
    explorer.run().await?;

    Ok(())
}

fn sensitive_url(url: &str) -> String {
    if let Some(auth_start) = url.find("://") {
        if let Some(auth_end) = url[auth_start + 3..].find('@') {
            let prefix = &url[0..auth_start + 3];
            let suffix = &url[auth_start + 3 + auth_end..];
            return format!("{prefix}***REDACTED***{suffix}");
        }
    }
    url.to_string()
}
