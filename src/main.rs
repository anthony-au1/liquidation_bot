use liquidation_bot;

use crate::liquidation_bot::arbitrum::arbitrum::AaveDataProvider;
use alloy::providers::{ProviderBuilder, WsConnect};
use clap::{Parser, Subcommand};
use liquidation_bot::arbitrum::arbitrum::{start, Cache, WS_URL};
use std::sync::Arc;
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

    info!("app is starting ...");

    let args = Cli::parse();
    match args.commands {
        Commands::Start => {
            info!("starting liquidation bot");

            let provider = ProviderBuilder::new()
                .connect_ws(WsConnect::new(WS_URL))
                .await?;
            // let block_number = get_block_number(&provider).await?;
            // get_block(&provider, block_number).await?;
            // get_logs(&provider).await?;
            // get_headers(&provider).await?;
            // get_borrows(Box::new(provider)).await?;

            let cache = Cache::default();
            let data_provider = AaveDataProvider::new(&provider)?;
            start(Arc::new(cache), Arc::new(data_provider)).await?;
        }
        Commands::Stop => {
            info!("stopping liquidation bot");
        }
    }

    Ok(())
}
