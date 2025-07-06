mod arbitrum;

use std::sync::Arc;
use crate::arbitrum::arbitrum::{start};
use alloy::providers::{ProviderBuilder, WsConnect};
use clap::{Parser, Subcommand};
use tracing::info;
use tracing_subscriber::EnvFilter;

/// Liquidation bot command interface
#[derive(Debug, Parser)]
#[command(name="bot", about="Liquidation Bot", long_about = None)]
struct Cli {
    #[command(subcommand)]
    commands: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Start liquidation bot
    #[command()]
    Start,

    /// Stop liquidation bot
    #[command()]
    Stop,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    info!("App is starting ...");

    let args = Cli::parse();
    match args.commands {
        Commands::Start => {
            info!("Starting Liquidation bot");

            let provider = ProviderBuilder::new()
                .connect_ws(WsConnect::new(arbitrum::arbitrum::WS_URL))
                .await?;
            // let block_number = get_block_number(&provider).await?;
            // get_block(&provider, block_number).await?;
            // get_logs(&provider).await?;
            // get_headers(&provider).await?;
            // get_borrows(Box::new(provider)).await?;
            start(Arc::new(provider)).await?;
        }
        Commands::Stop => {
            info!("Stopping Liquidation bot");
        }
    }

    Ok(())
}
