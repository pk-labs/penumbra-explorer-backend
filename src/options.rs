use clap::Parser;

#[derive(Parser, Clone, Debug)]
#[command(version, about = "Penumbra Explorer Backend")]
#[allow(clippy::module_name_repetitions)]
pub struct ExplorerOptions {
    
    #[arg(short = 's', long, default_value = "")]
    pub source_db_url: String,

    
    #[arg(short = 'd', long, default_value = "")]
    pub dest_db_url: String,

    
    #[arg(long, default_value = "genesis.json")]
    pub genesis_json: String,

    
    #[arg(long)]
    pub from_height: Option<u64>,

    
    #[arg(long)]
    pub to_height: Option<u64>,

    
    #[arg(long, default_value = "100")]
    pub batch_size: u64,

    
    #[arg(long, default_value = "1000")]
    pub polling_interval_ms: u64,
}
