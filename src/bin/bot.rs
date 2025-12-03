extern crate core;

use alloy::providers::{ProviderBuilder, WsConnect};
use alloy::transports::http::reqwest::Url;
use axum::Router;
use axum::routing::get;
use clap::{Parser, Subcommand};
use liquidation_bot;
use liquidation_bot::arbitrum::arbitrum::{AAVE_ORACLE_ADDRESS, AAVE_PROTOCOL_DATA_PROVIDER_ADDRESS, ANKR_URL, Cache, D_RPC_URL, GROVE_URL, IAaveOracle, IAaveProtocolDataProvider, IL2Pool, L2_POOL_ADDRESS, POKT_URL, WS_URL, build_breaker, start, WS_SECOND_URL};
use liquidation_bot::arbitrum::arbitrum::{AaveProvider, RPC_URL};
use liquidation_bot::arbitrum::stats::{
    AppState, get_borrowed_all_state, get_borrowed_matrix_row_state, get_borrowed_matrix_state,
    get_borrowed_state, get_collateral_all_state, get_collateral_matrix_row_state,
    get_collateral_matrix_state, get_collateral_state, get_decimals_state, get_full_state,
    get_health_factor_state, get_health_factors_state, get_liquidation_threshold_state,
    get_liquidity_index_state, get_liquidity_state, get_price_decimals_state, get_prices_state,
    get_reserve_all_state, get_reserve_state, get_test_probe_state, get_tokens_state,
    get_user_account_data_state, get_user_state, get_users_state, get_variable_borrow_index_state,
    get_variable_borrow_state,
};
use std::panic;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

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
    setup_panic_hook();
    // init();

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,liquidation_bot=debug"));

    let stdout_layer = fmt::layer()
        .with_target(false)
        .with_thread_ids(true)
        .with_level(true)
        .pretty()
        .with_ansi(false);

    let file_appender = RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_prefix("lb")
        .filename_suffix("json")
        .build("logs")
        .expect("log directory create failed");

    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    let json_layer = fmt::layer().json().with_writer(non_blocking).with_filter(
        tracing_subscriber::filter::filter_fn(|meta| {
            !meta
                .target()
                .starts_with("liquidation_bot::arbitrum::stats")
        }),
    );

    let stats_file_appender = RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_prefix("stats")
        .filename_suffix("json")
        .build("logs/stats")
        .expect("log/stats directory create failed");

    let (stats_non_blocking, _stats_guard) = tracing_appender::non_blocking(stats_file_appender);

    let stats_json_layer = fmt::layer()
        .json()
        .with_writer(stats_non_blocking)
        .with_filter(tracing_subscriber::filter::filter_fn(|meta| {
            meta.target()
                .starts_with("liquidation_bot::arbitrum::stats")
        }));

    tracing_subscriber::registry()
        .with(filter)
        .with(stdout_layer)
        .with(json_layer)
        .with(stats_json_layer)
        .init();

    info!("app is starting ...");

    let args = Cli::parse();
    match args.commands {
        Commands::Start => {
            info!("starting liquidation bot");

            let provider = ProviderBuilder::new()
                .connect_ws(WsConnect::new(WS_URL))
                .await?;
            let provider2 = ProviderBuilder::new()
                .connect_ws(WsConnect::new(WS_SECOND_URL))
                .await?;
            let rpc_provider = ProviderBuilder::new().connect_http(Url::parse(RPC_URL)?);
            let pokt_provider = ProviderBuilder::new().connect_http(Url::parse(POKT_URL)?);
            let grove_provider = ProviderBuilder::new().connect_http(Url::parse(GROVE_URL)?);
            let drpc_provider = ProviderBuilder::new().connect_http(Url::parse(D_RPC_URL)?);
            let ankr_provider = ProviderBuilder::new().connect_http(Url::parse(ANKR_URL)?);

            let cache = Arc::new(Cache::default());

            let state = AppState {
                cache: cache.clone(),
                provider: provider.clone(),
                provider2: provider2.clone(),
                rpc_provider: rpc_provider.clone(),
                pokt_provider: pokt_provider.clone(),
                grove_provider: grove_provider.clone(),
                drpc_provider: drpc_provider.clone(),
                ankr_provider: ankr_provider.clone(),
            };

            let app = Router::new()
                .route("/full_state", get(get_full_state))
                .route("/users", get(get_users_state))
                .route("/user/{row_num}", get(get_user_state))
                .route("/decimals", get(get_decimals_state))
                .route("/tokens", get(get_tokens_state))
                .route("/price_decimals", get(get_price_decimals_state))
                .route("/reserve", get(get_reserve_all_state))
                .route("/reserve/{row_num}", get(get_reserve_state))
                .route("/collateral", get(get_collateral_all_state))
                .route("/collateral/{row_num}", get(get_collateral_state))
                .route("/collateral_matrix", get(get_collateral_matrix_state))
                .route(
                    "/collateral_matrix/{row_num}",
                    get(get_collateral_matrix_row_state),
                )
                .route("/borrowed", get(get_borrowed_all_state))
                .route("/borrowed/{row_num}", get(get_borrowed_state))
                .route("/borrowed_matrix", get(get_borrowed_matrix_state))
                .route(
                    "/borrowed_matrix/{row_num}",
                    get(get_borrowed_matrix_row_state),
                )
                .route("/liquidity", get(get_liquidity_state))
                .route("/liquidity_index", get(get_liquidity_index_state))
                .route("/variable_borrow", get(get_variable_borrow_state))
                .route(
                    "/variable_borrow_index",
                    get(get_variable_borrow_index_state),
                )
                .route(
                    "/liquidation_threshold",
                    get(get_liquidation_threshold_state),
                )
                .route("/prices", get(get_prices_state))
                .route("/health_factors/{row_num}", get(get_health_factor_state))
                .route("/health_factors", get(get_health_factors_state))
                .route("/health_factor/{user}", get(get_user_account_data_state))
                .route("/test_probe/{window_size}", get(get_test_probe_state))
                .with_state(state);
            let listener = TcpListener::bind("0.0.0.0:3000").await?;

            let data_provider = Arc::new(AaveProvider {
                aave_protocol_data_provider: IAaveProtocolDataProvider::new(
                    AAVE_PROTOCOL_DATA_PROVIDER_ADDRESS.parse()?,
                    rpc_provider.clone(),
                ),
                aave_oracle: IAaveOracle::new(AAVE_ORACLE_ADDRESS.parse()?, provider2.clone()),
                aave_l2_pool: IL2Pool::new(L2_POOL_ADDRESS.parse()?, rpc_provider.clone()),
                provider: provider.clone(),
                aave_protocol_data_provider_fallback: vec![
                    (
                        build_breaker(),
                        IAaveProtocolDataProvider::new(
                            AAVE_PROTOCOL_DATA_PROVIDER_ADDRESS.parse()?,
                            rpc_provider.clone(),
                        ),
                    ),
                    (
                        build_breaker(),
                        IAaveProtocolDataProvider::new(
                            AAVE_PROTOCOL_DATA_PROVIDER_ADDRESS.parse()?,
                            provider.clone(),
                        ),
                    ),
                    (
                        build_breaker(),
                        IAaveProtocolDataProvider::new(
                            AAVE_PROTOCOL_DATA_PROVIDER_ADDRESS.parse()?,
                            rpc_provider.clone(),
                        ),
                    ),
                    (
                        build_breaker(),
                        IAaveProtocolDataProvider::new(
                            AAVE_PROTOCOL_DATA_PROVIDER_ADDRESS.parse()?,
                            pokt_provider.clone(),
                        ),
                    ),
                    (
                        build_breaker(),
                        IAaveProtocolDataProvider::new(
                            AAVE_PROTOCOL_DATA_PROVIDER_ADDRESS.parse()?,
                            grove_provider.clone(),
                        ),
                    ),
                    (
                        build_breaker(),
                        IAaveProtocolDataProvider::new(
                            AAVE_PROTOCOL_DATA_PROVIDER_ADDRESS.parse()?,
                            drpc_provider.clone(),
                        ),
                    ),
                    (
                        build_breaker(),
                        IAaveProtocolDataProvider::new(
                            AAVE_PROTOCOL_DATA_PROVIDER_ADDRESS.parse()?,
                            ankr_provider.clone(),
                        ),
                    ),
                ],
                aave_oracle_fallback: vec![
                    (
                        build_breaker(),
                        IAaveOracle::new(AAVE_ORACLE_ADDRESS.parse()?, provider.clone()),
                    ),
                    (
                        build_breaker(),
                        IAaveOracle::new(AAVE_ORACLE_ADDRESS.parse()?, rpc_provider.clone()),
                    ),
                    (
                        build_breaker(),
                        IAaveOracle::new(AAVE_ORACLE_ADDRESS.parse()?, pokt_provider.clone()),
                    ),
                    (
                        build_breaker(),
                        IAaveOracle::new(AAVE_ORACLE_ADDRESS.parse()?, grove_provider.clone()),
                    ),
                    (
                        build_breaker(),
                        IAaveOracle::new(AAVE_ORACLE_ADDRESS.parse()?, drpc_provider.clone()),
                    ),
                    (
                        build_breaker(),
                        IAaveOracle::new(AAVE_ORACLE_ADDRESS.parse()?, ankr_provider.clone()),
                    ),
                ],
                aave_l2_pool_fallback: vec![
                    (
                        build_breaker(),
                        IL2Pool::new(L2_POOL_ADDRESS.parse()?, rpc_provider.clone()),
                    ),
                    (
                        build_breaker(),
                        IL2Pool::new(L2_POOL_ADDRESS.parse()?, provider.clone()),
                    ),
                    (
                        build_breaker(),
                        IL2Pool::new(L2_POOL_ADDRESS.parse()?, pokt_provider.clone()),
                    ),
                    (
                        build_breaker(),
                        IL2Pool::new(L2_POOL_ADDRESS.parse()?, grove_provider.clone()),
                    ),
                    (
                        build_breaker(),
                        IL2Pool::new(L2_POOL_ADDRESS.parse()?, drpc_provider.clone()),
                    ),
                    (
                        build_breaker(),
                        IL2Pool::new(L2_POOL_ADDRESS.parse()?, ankr_provider.clone()),
                    ),
                ],
                provider_fallback: vec![],
            });

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

fn setup_panic_hook() {
    let default_hook = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        let msg = if let Some(s) = info.payload().downcast_ref::<&str>() {
            *s
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.as_str()
        } else {
            "unknown panic"
        };

        let location = if let Some(loc) = info.location() {
            format!("{}:{}", loc.file(), loc.line())
        } else {
            "unknown location".into()
        };

        let bt = std::backtrace::Backtrace::force_capture();

        error!("PANIC: {}\nAt: {}\nBacktrace:\n{:?}", msg, location, bt);

        default_hook(info);
    }));
}
