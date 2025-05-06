use clap::Parser;

#[derive(Parser, Clone, Debug)]
#[command(version, about = "Penumbra Explorer Backend")]
#[allow(clippy::module_name_repetitions)]
pub struct ExplorerOptions {
    /// The database URL for the source raw events
    #[arg(short = 's', long, default_value = "")]
    pub source_db_url: String,

    /// The database URL for the destination compiled events
    #[arg(short = 'd', long, default_value = "")]
    pub dest_db_url: String,

    /// The genesis JSON file path
    #[arg(long, default_value = "genesis.json")]
    pub genesis_json: String,

    /// The height to start processing from (inclusive)
    #[arg(long)]
    pub from_height: Option<u64>,

    /// The height to process until (inclusive)
    #[arg(long)]
    pub to_height: Option<u64>,

    /// The number of blocks to process in a batch
    #[arg(long, default_value = "100")]
    pub batch_size: u64,

    /// The interval in milliseconds to poll for new blocks
    #[arg(long, default_value = "1000")]
    pub polling_interval_ms: u64,
}
