extern crate core;

use liquidation_bot;

use alloy::providers::{ProviderBuilder, WsConnect};
use alloy_primitives::Address;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use bitvec::order::Lsb0;
use bitvec::prelude::BitVec;
use clap::{Parser, Subcommand};
use liquidation_bot::arbitrum::arbitrum::AaveDataProvider;
use liquidation_bot::arbitrum::arbitrum::{Cache, WS_URL, start};
use serde::Serialize;
use std::sync::Arc;
use tokio::net::TcpListener;
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
            let cache = Arc::new(Cache::default());

            let app = Router::new()
                .route("/state", get(get_full_state))
                .with_state(cache.clone());
            let listener = TcpListener::bind("0.0.0.0:3000").await?;
            let data_provider = Arc::new(AaveDataProvider::new(&provider)?);

            tokio::select! {
                res = axum::serve(listener, app) => {
                    res?;
                }
                res = start(cache, data_provider) => {
                    res?;
                }
            }
        }
        Commands::Stop => {
            info!("stopping liquidation bot");
        }
    }

    Ok(())
}

#[derive(Serialize)]
struct FullState {
    users: Vec<User>,
    users_num: usize,
    decimals: Vec<f64>,
}

#[derive(Serialize)]
struct User {
    name: Address,
    row: usize,
    use_as_collateral: Vec<bool>,
}

impl User {
    fn new(name: Address, row: usize, use_as_collateral: BitVec<usize, Lsb0>) -> Self {
        let use_as_collateral = use_as_collateral.iter().by_vals().collect();

        Self {
            name,
            row,
            use_as_collateral,
        }
    }
}

async fn get_full_state(State(cache): State<Arc<Cache>>) -> (StatusCode, Json<FullState>) {
    let users = cache
        .users
        .iter()
        .map(|entry| {
            User::new(
                entry.key().clone(),
                entry.value().row_num,
                entry.value().use_as_collateral.clone(),
            )
        })
        .collect::<Vec<_>>();

    let users_num = *cache.users_num.read().await;
    let decimals = {
        let (decimals, _) = &*cache.decimals.read().await;
        decimals.to_vec()
    };


    let full_state = FullState { users, users_num, decimals };

    (StatusCode::CREATED, Json(full_state))
}
