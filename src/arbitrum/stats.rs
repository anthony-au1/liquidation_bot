use crate::arbitrum::arbitrum::{
    build_breaker, AaveDataProvider, Cache, DataProvider,
    F64Converter, IAaveOracle, IAaveProtocolDataProvider, IL2Pool, Index, RayOperations,
    ReserveData, Scaler, UserReserveData, AAVE_ORACLE_ADDRESS, AAVE_PROTOCOL_DATA_PROVIDER_ADDRESS, L2_POOL_ADDRESS,
};
use alloy::providers::Provider;
use alloy::transports::http::reqwest::StatusCode;
use alloy_primitives::{Address, U256};
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::Json;
use bitvec::order::Lsb0;
use bitvec::prelude::BitVec;
use futures::future::try_join_all;
use itertools::Itertools;
use ndarray::Axis;
use rand::Rng;
use serde::ser::SerializeSeq;
use serde::{Serialize, Serializer};
use std::sync::Arc;
use tokio::try_join;
use tracing::info;

#[derive(Clone)]
pub struct AppState<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    pub cache: Arc<Cache>,
    pub provider: P,
    pub provider2: P,
    pub rpc_provider: P,
    pub pokt_provider: P,
    pub grove_provider: P,
    pub drpc_provider: P,
    pub ankr_provider: P,
}

#[derive(Serialize)]
pub struct UserState {
    pub users: Vec<User>,
    pub users_num: usize,
}

#[derive(Serialize, Debug)]
pub struct FullState {
    pub users: Vec<User>,
    pub users_num: usize,

    pub decimals: Vec<f64>,
    pub tokens: Vec<Address>,
    pub price_decimals: Vec<f64>,

    pub reserve: Vec<Vec<String>>,
    pub collateral: Vec<Vec<String>>,
    pub collateral_matrix: Vec<Vec<f64>>,

    pub borrowed: Vec<Vec<String>>,
    pub borrowed_matrix: Vec<Vec<f64>>,

    pub liquidity: Vec<IndexRate>,
    pub liquidity_index: Vec<f64>,

    pub variable_borrow: Vec<IndexRate>,
    pub variable_borrow_index: Vec<f64>,

    pub liquidation_threshold: Vec<f64>,

    pub prices: Vec<f64>,

    pub health_factors: Vec<f64>,
}

#[derive(Serialize, Debug)]
pub struct User {
    pub name: Address,
    pub row: usize,
    pub use_as_collateral: Vec<bool>,
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

#[derive(Serialize, Debug)]
pub struct IndexRate {
    pub index: String,
    pub rate: String,
}

impl IndexRate {
    fn new(index: String, rate: String) -> Self {
        Self { index, rate }
    }
}

async fn build_data_provider<P>(
    provider: &P,
    provider2: &P,
    rpc_provider: &P,
    pokt_provider: &P,
    grove_provider: &P,
    drpc_provider: &P,
    ankr_provider: &P,
) -> eyre::Result<AaveDataProvider<P>>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    Ok(AaveDataProvider {
        aave_protocol_data_provider: IAaveProtocolDataProvider::new(
            AAVE_PROTOCOL_DATA_PROVIDER_ADDRESS.parse()?,
            provider2.clone(),
        ),
        aave_oracle: IAaveOracle::new(AAVE_ORACLE_ADDRESS.parse()?, provider2.clone()),
        aave_l2_pool: IL2Pool::new(L2_POOL_ADDRESS.parse()?, provider2.clone()),
        provider: provider2.clone(),
        aave_protocol_data_provider_fallback: vec![
            (
                build_breaker(),
                IAaveProtocolDataProvider::new(
                    AAVE_PROTOCOL_DATA_PROVIDER_ADDRESS.parse()?,
                    provider2.clone(),
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
                IAaveOracle::new(AAVE_ORACLE_ADDRESS.parse()?, provider2.clone()),
            ),
            (
                build_breaker(),
                IAaveOracle::new(AAVE_ORACLE_ADDRESS.parse()?, provider.clone()),
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
                IL2Pool::new(L2_POOL_ADDRESS.parse()?, provider2.clone()),
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
    })
}

async fn get_user(row_num: usize, cache: Arc<Cache>) -> Option<User> {
    cache.users.iter().find_map(|entry| {
        if entry.row_num == row_num {
            Some(User::new(
                entry.key().clone(),
                entry.value().row_num,
                entry.value().use_as_collateral.clone(),
            ))
        } else {
            None
        }
    })
}

pub async fn get_user_state<P>(
    Path(row_num): Path<usize>,
    State(state): State<AppState<P>>,
) -> impl IntoResponse
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let user = get_user(row_num, state.cache).await;

    match user {
        Some(u) => {
            info!("get_user_state: row_num = {} and user = {:?}", row_num, u);
            (StatusCode::OK, Json(u)).into_response()
        }
        None => {
            info!("get_user_state: user for row_num = {} not found", row_num);
            (
                StatusCode::NOT_FOUND,
                format!("User for row {} not found", row_num),
            )
                .into_response()
        }
    }
}

async fn get_users(cache: Arc<Cache>) -> (Vec<User>, usize) {
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
    (users, users_num)
}

pub async fn get_users_state<P>(State(state): State<AppState<P>>) -> (StatusCode, Json<UserState>)
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let (users, users_num) = get_users(state.cache).await;

    info!(
        "get_users_state: users = {:?} and users_num = {}",
        users, users_num
    );

    let user_state = UserState { users, users_num };
    (StatusCode::OK, Json(user_state))
}

async fn get_decimals(cache: Arc<Cache>) -> Vec<f64> {
    let (decimals, _) = &*cache.decimals.read().await;
    decimals.to_vec()
}

pub async fn get_decimals_state<P>(State(state): State<AppState<P>>) -> (StatusCode, Json<Vec<f64>>)
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let decimals = get_decimals(state.cache).await;

    info!("get_decimals_state: decimals = {:?}", decimals);

    (StatusCode::OK, Json(decimals))
}

async fn get_tokens(cache: Arc<Cache>) -> Vec<Address> {
    let (tokens, _) = &*cache.tokens.read().await;
    tokens.to_vec()
}

pub async fn get_tokens_state<P>(
    State(state): State<AppState<P>>,
) -> (StatusCode, Json<Vec<Address>>)
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let tokens = get_tokens(state.cache).await;

    info!("get_tokens_state: tokens = {:?}", tokens);

    (StatusCode::OK, Json(tokens))
}

async fn get_price_decimals(cache: Arc<Cache>) -> Vec<f64> {
    let (price_decimals, _) = &*cache.price_decimals.read().await;
    price_decimals.to_vec()
}

pub async fn get_price_decimals_state<P>(
    State(state): State<AppState<P>>,
) -> (StatusCode, Json<Vec<f64>>)
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let price_decimals = get_price_decimals(state.cache).await;

    info!(
        "get_price_decimals_state: price_decimals = {:?}",
        price_decimals
    );

    (StatusCode::OK, Json(price_decimals))
}

async fn get_reserve(row_num: usize, cache: Arc<Cache>) -> Option<Vec<String>> {
    let reserve = cache.reserve.read().await.to_vec();
    let row_lock = reserve.get(row_num)?;
    let (row, _, _) = &*row_lock.read().await;
    Some(row.iter().map(U256::to_string).collect())
}

pub async fn get_reserve_state<P>(
    Path(row_num): Path<usize>,
    State(state): State<AppState<P>>,
) -> impl IntoResponse
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let reserve = get_reserve(row_num, state.cache).await;

    match reserve {
        Some(r) => {
            info!(
                "get_reserve_state: row_num = {} and reserve = {:?}",
                row_num, r
            );
            (StatusCode::OK, Json(r)).into_response()
        }
        None => {
            info!(
                "get_reserve_state: reserve for row_num = {} not found",
                row_num
            );
            (
                StatusCode::NOT_FOUND,
                format!("reserve for row {} not found", row_num),
            )
                .into_response()
        }
    }
}

async fn get_reserve_all(cache: Arc<Cache>) -> Vec<Vec<String>> {
    let mut reserve_vec = vec![];
    let reserves = cache.reserve.read().await.to_vec();
    for res_lock in reserves {
        let (res, _, _) = &*res_lock.read().await;
        let values = res.iter().map(U256::to_string).collect();
        reserve_vec.push(values);
    }
    reserve_vec
}

pub async fn get_reserve_all_state<P>(
    State(state): State<AppState<P>>,
) -> (StatusCode, Json<Vec<Vec<String>>>)
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let reserve = get_reserve_all(state.cache).await;

    info!("get_reserve_all_state: reserve = {:?}", reserve);

    (StatusCode::OK, Json(reserve))
}

async fn get_collateral(row_num: usize, cache: Arc<Cache>) -> Option<Vec<String>> {
    let collateral = cache.collateral.read().await.to_vec();
    let col_lock = collateral.get(row_num)?;
    let (row, _, _) = &*col_lock.read().await;
    Some(row.iter().map(U256::to_string).collect())
}

pub async fn get_collateral_state<P>(
    Path(row_num): Path<usize>,
    State(state): State<AppState<P>>,
) -> impl IntoResponse
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let collateral = get_collateral(row_num, state.cache).await;

    match collateral {
        Some(c) => {
            info!(
                "get_collateral_state: row_num = {} and collateral = {:?}",
                row_num, c
            );
            (StatusCode::OK, Json(c)).into_response()
        }
        None => {
            info!(
                "get_collateral_state: collateral for row_num = {} not found",
                row_num
            );
            (
                StatusCode::NOT_FOUND,
                format!("collateral for row {} not found", row_num),
            )
                .into_response()
        }
    }
}

async fn get_collateral_all(cache: Arc<Cache>) -> Vec<Vec<String>> {
    let mut collateral_vec = vec![];
    let collateral = cache.collateral.read().await.to_vec();
    for col_lock in collateral {
        let (col, _, _) = &*col_lock.read().await;
        let values = col.iter().map(U256::to_string).collect();
        collateral_vec.push(values);
    }
    collateral_vec
}

pub async fn get_collateral_all_state<P>(
    State(state): State<AppState<P>>,
) -> (StatusCode, Json<Vec<Vec<String>>>)
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let collateral = get_collateral_all(state.cache).await;

    info!("get_collateral_all_state: collateral = {:?}", collateral);

    (StatusCode::OK, Json(collateral))
}

async fn get_collateral_matrix_row(row_num: usize, cache: Arc<Cache>) -> Option<Vec<f64>> {
    let collateral = &*cache.collateral_matrix.read().await;
    collateral
        .rows()
        .into_iter()
        .nth(row_num)
        .map(|row| row.to_vec())
}

pub async fn get_collateral_matrix_row_state<P>(
    Path(row_num): Path<usize>,
    State(state): State<AppState<P>>,
) -> impl IntoResponse
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let collateral_matrix_row = get_collateral_matrix_row(row_num, state.cache).await;

    match collateral_matrix_row {
        Some(c) => {
            info!(
                "get_collateral_matrix_row_state: row_num = {} and collateral_matrix_row = {:?}",
                row_num, c
            );

            (StatusCode::OK, Json(c)).into_response()
        }
        None => {
            info!(
                "get_collateral_matrix_row_state: collateral_matrix_row for row_num = {} not found",
                row_num
            );

            (
                StatusCode::NOT_FOUND,
                format!("collateral for row {} not found", row_num),
            )
                .into_response()
        }
    }
}

async fn get_collateral_matrix(cache: Arc<Cache>) -> Vec<Vec<f64>> {
    let collaterals = &*cache.collateral_matrix.read().await;
    collaterals
        .axis_iter(Axis(0))
        .map(|row| row.to_vec())
        .collect::<Vec<_>>()
}

pub async fn get_collateral_matrix_state<P>(
    State(state): State<AppState<P>>,
) -> (StatusCode, Json<Vec<Vec<f64>>>)
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let collateral_matrix = get_collateral_matrix(state.cache).await;

    info!(
        "get_collateral_matrix_state: collateral_matrix = {:?}",
        collateral_matrix
    );

    (StatusCode::OK, Json(collateral_matrix))
}

async fn get_borrowed(row_num: usize, cache: Arc<Cache>) -> Option<Vec<String>> {
    let borrowed = cache.borrowed.read().await.to_vec();
    let bor_lock = borrowed.get(row_num)?;
    let (row, _, _) = &*bor_lock.read().await;
    Some(row.iter().map(U256::to_string).collect())
}

pub async fn get_borrowed_state<P>(
    Path(row_num): Path<usize>,
    State(state): State<AppState<P>>,
) -> impl IntoResponse
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let borrowed = get_borrowed(row_num, state.cache).await;

    match borrowed {
        Some(b) => {
            info!(
                "get_borrowed_state: row_num = {} and borrowed = {:?}",
                row_num, b
            );

            (StatusCode::OK, Json(b)).into_response()
        }
        None => {
            info!(
                "get_borrowed_state: borrowed for row_num = {} not found",
                row_num
            );

            (
                StatusCode::NOT_FOUND,
                format!("borrowed for row {} not found", row_num),
            )
                .into_response()
        }
    }
}

async fn get_borrowed_all(cache: Arc<Cache>) -> Vec<Vec<String>> {
    let mut borrowed_vec = vec![];
    let borrowed = cache.borrowed.read().await.to_vec();
    for bor_lock in borrowed {
        let (bor, _, _) = &*bor_lock.read().await;
        let values = bor.iter().map(U256::to_string).collect();
        borrowed_vec.push(values);
    }
    borrowed_vec
}

pub async fn get_borrowed_all_state<P>(
    State(state): State<AppState<P>>,
) -> (StatusCode, Json<Vec<Vec<String>>>)
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let borrowed = get_borrowed_all(state.cache).await;

    info!("get_borrowed_all_state: borrowed = {:?}", borrowed);

    (StatusCode::OK, Json(borrowed))
}

async fn get_borrowed_matrix_row(row_num: usize, cache: Arc<Cache>) -> Option<Vec<f64>> {
    let borrowed = &*cache.borrowed_matrix.read().await;

    borrowed
        .rows()
        .into_iter()
        .nth(row_num)
        .map(|row| row.to_vec())
}

pub async fn get_borrowed_matrix_row_state<P>(
    Path(row_num): Path<usize>,
    State(state): State<AppState<P>>,
) -> impl IntoResponse
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let borrowed_matrix_row = get_borrowed_matrix_row(row_num, state.cache).await;

    match borrowed_matrix_row {
        Some(b) => {
            info!(
                "get_borrowed_matrix_row_state: row_num = {} and borrowed_matrix_row = {:?}",
                row_num, b
            );

            (StatusCode::OK, Json(b)).into_response()
        }
        None => {
            info!(
                "get_borrowed_matrix_row_state: borrowed_matrix_row for row_num = {} and not found",
                row_num
            );
            (
                StatusCode::NOT_FOUND,
                format!("borrowed for row {} not found", row_num),
            )
                .into_response()
        }
    }
}

async fn get_borrowed_matrix(cache: Arc<Cache>) -> Vec<Vec<f64>> {
    let borrowed = &*cache.borrowed_matrix.read().await;
    borrowed
        .axis_iter(Axis(0))
        .map(|row| row.to_vec())
        .collect::<Vec<_>>()
}

pub async fn get_borrowed_matrix_state<P>(
    State(state): State<AppState<P>>,
) -> (StatusCode, Json<Vec<Vec<f64>>>)
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let borrowed_matrix = get_borrowed_matrix(state.cache).await;

    info!(
        "get_borrowed_matrix_state: borrowed_matrix = {:?}",
        borrowed_matrix
    );

    (StatusCode::OK, Json(borrowed_matrix))
}

async fn get_liquidity(cache: Arc<Cache>) -> Vec<IndexRate> {
    let (indexes, _) = &*cache.liquidity.read().await;
    indexes
        .iter()
        .map(|Index { index, rate, .. }| IndexRate::new(index.to_string(), rate.to_string()))
        .collect::<Vec<_>>()
}

pub async fn get_liquidity_state<P>(
    State(state): State<AppState<P>>,
) -> (StatusCode, Json<Vec<IndexRate>>)
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let liquidity = get_liquidity(state.cache).await;

    info!("get_liquidity_state: liquidity = {:?}", liquidity);

    (StatusCode::OK, Json(liquidity))
}

async fn get_liquidity_index(cache: Arc<Cache>) -> Vec<f64> {
    let (indexes, _) = &*cache.liquidity_index.read().await;
    indexes.to_vec()
}

pub async fn get_liquidity_index_state<P>(
    State(state): State<AppState<P>>,
) -> (StatusCode, Json<Vec<f64>>)
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let liquidity_index = get_liquidity_index(state.cache).await;

    info!(
        "get_liquidity_index_state: liquidity_index = {:?}",
        liquidity_index
    );

    (StatusCode::OK, Json(liquidity_index))
}

async fn get_variable_borrow(cache: Arc<Cache>) -> Vec<IndexRate> {
    let (indexes, _) = &*cache.variable_borrow.read().await;
    indexes
        .iter()
        .map(|Index { index, rate, .. }| IndexRate::new(index.to_string(), rate.to_string()))
        .collect::<Vec<_>>()
}

pub async fn get_variable_borrow_state<P>(
    State(state): State<AppState<P>>,
) -> (StatusCode, Json<Vec<IndexRate>>)
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let variable_borrow = get_variable_borrow(state.cache).await;

    info!(
        "get_variable_borrow_state: variable_borrow = {:?}",
        variable_borrow
    );

    (StatusCode::OK, Json(variable_borrow))
}

async fn get_variable_borrow_index(cache: Arc<Cache>) -> Vec<f64> {
    let (indexes, _) = &*cache.variable_borrow_index.read().await;
    indexes.to_vec()
}

pub async fn get_variable_borrow_index_state<P>(
    State(state): State<AppState<P>>,
) -> (StatusCode, Json<Vec<f64>>)
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let variable_borrow_index = get_variable_borrow_index(state.cache).await;

    info!(
        "get_variable_borrow_index_state: variable_borrow_index = {:?}",
        variable_borrow_index
    );

    (StatusCode::OK, Json(variable_borrow_index))
}

async fn get_liquidation_threshold(cache: Arc<Cache>) -> Vec<f64> {
    let (lt, _) = &*cache.liquidation_threshold.read().await;
    lt.to_vec()
}

pub async fn get_liquidation_threshold_state<P>(
    State(state): State<AppState<P>>,
) -> (StatusCode, Json<Vec<f64>>)
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let liquidation_threshold = get_liquidation_threshold(state.cache).await;

    info!(
        "get_liquidation_threshold_state: liquidation_threshold = {:?}",
        liquidation_threshold
    );

    (StatusCode::OK, Json(liquidation_threshold))
}

async fn get_prices(cache: Arc<Cache>) -> Vec<f64> {
    let (prices, _) = &*cache.prices.read().await;
    prices.to_vec()
}

pub async fn get_prices_state<P>(State(state): State<AppState<P>>) -> (StatusCode, Json<Vec<f64>>)
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let prices = get_prices(state.cache).await;

    info!("get_prices_state: prices = {:?}", prices);

    (StatusCode::OK, Json(prices))
}

async fn get_health_factor(row_num: usize, cache: Arc<Cache>) -> f64 {
    let (hf, _) = &*cache.health_factors.read().await;
    hf[row_num]
}

pub async fn get_health_factor_state<P>(
    Path(row_num): Path<usize>,
    State(state): State<AppState<P>>,
) -> (StatusCode, Json<f64>)
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let health_factors = get_health_factor(row_num, state.cache).await;

    info!(
        "get_health_factor_state: row_num = {}, health_factor = {:?}",
        row_num, health_factors
    );

    (StatusCode::OK, Json(health_factors))
}

async fn get_health_factors(cache: Arc<Cache>) -> Vec<f64> {
    let (hf, _) = &*cache.health_factors.read().await;
    hf.to_vec()
}

pub async fn get_health_factors_state<P>(
    State(state): State<AppState<P>>,
) -> (StatusCode, Json<Vec<f64>>)
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let health_factors = get_health_factors(state.cache).await;

    info!(
        "get_health_factors_state: health_factors = {:?}",
        health_factors
    );

    (StatusCode::OK, Json(health_factors))
}

pub async fn get_full_state<P>(State(state): State<AppState<P>>) -> (StatusCode, Json<FullState>)
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let AppState { cache, .. } = state;

    let (users, users_num) = get_users(cache.clone()).await;
    let decimals = get_decimals(cache.clone()).await;
    let tokens = get_tokens(cache.clone()).await;
    let price_decimals = get_price_decimals(cache.clone()).await;
    let reserve = get_reserve_all(cache.clone()).await;
    let collateral = get_collateral_all(cache.clone()).await;
    let collateral_matrix = get_collateral_matrix(cache.clone()).await;
    let borrowed = get_borrowed_all(cache.clone()).await;
    let borrowed_matrix = get_borrowed_matrix(cache.clone()).await;
    let liquidity = get_liquidity(cache.clone()).await;
    let liquidity_index = get_liquidity_index(cache.clone()).await;
    let variable_borrow = get_variable_borrow(cache.clone()).await;
    let variable_borrow_index = get_variable_borrow_index(cache.clone()).await;
    let liquidation_threshold = get_liquidation_threshold(cache.clone()).await;
    let prices = get_prices(cache.clone()).await;
    let health_factors = get_health_factors(cache).await;

    let full_state = FullState {
        users,
        users_num,
        decimals,
        tokens,
        price_decimals,
        reserve,
        collateral,
        collateral_matrix,
        borrowed,
        borrowed_matrix,
        liquidity,
        liquidity_index,
        variable_borrow,
        variable_borrow_index,
        liquidation_threshold,
        prices,
        health_factors,
    };

    info!("get_full_state: full_state = {:?}", full_state);

    (StatusCode::OK, Json(full_state))
}

pub async fn get_user_account_data_state<P>(
    Path(user): Path<Address>,
    State(state): State<AppState<P>>,
) -> Result<(StatusCode, Json<(String, String)>), (StatusCode, String)>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let data_provider = build_data_provider(
        &state.provider,
        &state.provider2,
        &state.rpc_provider,
        &state.pokt_provider,
        &state.grove_provider,
        &state.drpc_provider,
        &state.ankr_provider,
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:?}")))?;

    let uad = data_provider
        .get_user_account_data(&user)
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("{e:?}")))?;

    let hf = uad.health_factor;

    info!(
        "get_user_account_data_state: user = {}, hf = {:?}",
        user, hf
    );

    Ok((
        StatusCode::OK,
        Json((hf.to_string(), hf.as_f64_wad().to_string())),
    ))
}

#[derive(Serialize, Debug)]
pub struct TestProbe {
    pub hf: Vec<f64>,
    pub users: Vec<Address>,
    pub tokens: Vec<Address>,
    pub decimals: Vec<f64>,
    pub price_decimals: Vec<f64>,
    pub liquidation_threshold: Vec<f64>,

    #[serde(serialize_with = "u256_vec_to_string")]
    pub liquidity: Vec<U256>,
    pub liquidity_index: Vec<f64>,

    #[serde(serialize_with = "u256_vec_to_string")]
    pub variable_borrow: Vec<U256>,
    pub variable_borrow_index: Vec<f64>,
    pub prices: Vec<f64>,

    #[serde(serialize_with = "u256_2d_to_string")]
    pub reserve_scaled: Vec<Vec<U256>>,

    #[serde(serialize_with = "u256_2d_to_string")]
    pub collateral_scaled: Vec<Vec<U256>>,
    pub collateral: Vec<Vec<f64>>,

    #[serde(serialize_with = "u256_2d_to_string")]
    pub borrowed_scaled: Vec<Vec<U256>>,
    pub borrowed: Vec<Vec<f64>>,

    pub hf_aave: Vec<f64>,

    #[serde(serialize_with = "u256_vec_to_string")]
    pub liquidity_aave: Vec<U256>,
    pub liquidity_index_aave: Vec<f64>,

    #[serde(serialize_with = "u256_vec_to_string")]
    pub variable_borrow_aave: Vec<U256>,
    pub variable_borrow_index_aave: Vec<f64>,

    #[serde(serialize_with = "u256_2d_to_string")]
    pub reserve_scaled_aave: Vec<Vec<U256>>,

    #[serde(serialize_with = "u256_2d_to_string")]
    pub collateral_scaled_aave: Vec<Vec<U256>>,
    pub collateral_aave: Vec<Vec<f64>>,

    #[serde(serialize_with = "u256_2d_to_string")]
    pub borrowed_scaled_aave: Vec<Vec<U256>>,
    pub borrowed_aave: Vec<Vec<f64>>,

    pub hf_diff: Vec<f64>,

    #[serde(serialize_with = "u256_vec_to_string")]
    pub liquidity_diff: Vec<U256>,
    pub liquidity_index_diff: Vec<f64>,

    #[serde(serialize_with = "u256_vec_to_string")]
    pub variable_borrow_diff: Vec<U256>,
    pub variable_borrow_index_diff: Vec<f64>,

    #[serde(serialize_with = "u256_2d_to_string")]
    pub reserve_scaled_diff: Vec<Vec<U256>>,

    #[serde(serialize_with = "u256_2d_to_string")]
    pub collateral_scaled_diff: Vec<Vec<U256>>,
    pub collateral_diff: Vec<Vec<f64>>,

    #[serde(serialize_with = "u256_2d_to_string")]
    pub borrowed_scaled_diff: Vec<Vec<U256>>,
    pub borrowed_diff: Vec<Vec<f64>>,
}

pub fn u256_vec_to_string<S>(vals: &Vec<U256>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let strs = vals.iter().map(|v| v.to_string()).collect::<Vec<_>>();
    strs.serialize(serializer)
}

fn u256_2d_to_string<S>(matrix: &Vec<Vec<U256>>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut outer_seq = serializer.serialize_seq(Some(matrix.len()))?;
    for row in matrix {
        let row_as_strings = row.iter().map(|v| v.to_string()).collect::<Vec<_>>();
        outer_seq.serialize_element(&row_as_strings)?;
    }
    outer_seq.end()
}

pub async fn get_test_probe_state<P>(
    Path(mut window_size): Path<usize>,
    State(state): State<AppState<P>>,
) -> Result<(StatusCode, Json<TestProbe>), (StatusCode, String)>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let data_provider = build_data_provider(
        &state.provider,
        &state.provider2,
        &state.rpc_provider,
        &state.pokt_provider,
        &state.grove_provider,
        &state.drpc_provider,
        &state.ankr_provider,
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:?}")))?;

    let data_provider = Arc::new(data_provider);

    let hf = {
        let (hf, _) = &*state.cache.health_factors.read().await;
        hf.to_vec()
    };

    let mut start = 0;
    if hf.len() > window_size {
        let range = 0..hf.len() - window_size;
        let mut rng = rand::rng();
        start = rng.random_range(range);
    } else {
        window_size = hf.len();
    }

    let users = state
        .cache
        .users
        .iter()
        .filter(|user| user.row_num >= start && user.row_num < start + window_size)
        .sorted_by_key(|user| user.row_num)
        .map(|user| user.key().clone())
        .collect::<Vec<_>>();

    let tokens = {
        let (tokens, _) = &*state.cache.tokens.read().await;
        tokens.to_vec()
    };

    let decimals = {
        let (decimals, _) = &*state.cache.decimals.read().await;
        decimals.to_vec()
    };

    let price_decimals = {
        let (price_decimals, _) = &*state.cache.price_decimals.read().await;
        price_decimals.to_vec()
    };

    let liquidation_threshold = {
        let (lt, _) = &*state.cache.liquidation_threshold.read().await;
        lt.to_vec()
    };

    let liquidity = {
        let (li, _) = &*state.cache.liquidity.read().await;
        li.iter().map(|li| li.index).collect::<Vec<_>>()
    };

    let liquidity_index = {
        let (li, _) = &*state.cache.liquidity_index.read().await;
        li.to_vec()
    };

    let variable_borrow = {
        let (vb, _) = &*state.cache.variable_borrow.read().await;
        vb.iter().map(|vb| vb.index).collect::<Vec<_>>()
    };

    let variable_borrow_index = {
        let (vb, _) = &*state.cache.variable_borrow_index.read().await;
        vb.to_vec()
    };

    let prices = {
        let (prices, _) = &*state.cache.prices.read().await;
        prices.to_vec()
    };

    let reserve_scaled = {
        let mut reserve_scaled = vec![];
        let reserve = state.cache.reserve.read().await[start..start + window_size].to_vec();

        for res_lock in reserve.iter() {
            let (res, _, _) = &*res_lock.read().await;
            reserve_scaled.push(res.to_vec());
        }
        reserve_scaled
    };

    let collateral_scaled = {
        let mut collateral_scaled = vec![];
        let collateral = state.cache.collateral.read().await.to_vec();
        for col_lock in collateral[start..start + window_size].iter() {
            let (col, _, _) = &*col_lock.read().await;
            collateral_scaled.push(col.to_vec());
        }
        collateral_scaled
    };

    let collateral = {
        let mut collateral = vec![];
        let col = &*state.cache.collateral_matrix.read().await;
        for row in start..start + window_size {
            let row = col.row(row);
            collateral.push(
                row.iter()
                    .enumerate()
                    .map(|(col, x)| x * liquidity_index[col])
                    .collect::<Vec<_>>(),
            );
        }
        collateral
    };

    let borrowed_scaled = {
        let mut borrowed_scaled = vec![];
        let borrowed = state.cache.borrowed.read().await.to_vec();
        for bor_lock in borrowed[start..start + window_size].iter() {
            let (bor, _, _) = &*bor_lock.read().await;
            borrowed_scaled.push(bor.to_vec());
        }
        borrowed_scaled
    };

    let mut borrowed = {
        let mut borrowed = vec![];
        let bor = &*state.cache.borrowed_matrix.read().await;
        for row in start..start + window_size {
            let row = bor.row(row);
            borrowed.push(
                row.iter()
                    .enumerate()
                    .map(|(col, x)| x * variable_borrow_index[col])
                    .collect::<Vec<_>>(),
            );
        }
        borrowed
    };

    let uad_tasks = users.iter().enumerate().map(|(idx, user)| {
        let provider = data_provider.clone();
        let user = user.clone();
        async move { Ok::<_, eyre::Error>((idx, provider.get_user_account_data(&user).await?)) }
    });

    let rd_tasks = tokens.iter().enumerate().map(|(col, token)| {
        let provider = data_provider.clone();
        async move { Ok::<_, eyre::Error>((col, provider.get_reserve_data(&token).await?)) }
    });

    let urd_tasks = users.iter().enumerate().flat_map(|(row, user)| {
        let user = user.clone();
        let provider = data_provider.clone();

        tokens.iter().enumerate().map(move |(col, token)| {
            let provider = provider.clone();
            let token = token.clone();
            let user = user.clone();

            async move {
                Ok::<_, eyre::Error>((
                    row,
                    col,
                    provider.get_user_reserve_data(&token, &user).await?,
                ))
            }
        })
    });

    let (uad_results, rd_results, urd_results) = try_join!(
        try_join_all(uad_tasks),
        try_join_all(rd_tasks),
        try_join_all(urd_tasks),
    )
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:?}")))?;

    let mut hf_aave = vec![0.0; uad_results.len()];
    for (idx, uad) in uad_results {
        hf_aave[idx] = uad.health_factor.as_f64_wad();
    }

    let mut liquidity_aave = vec![U256::default(); tokens.len()];
    let mut liquidity_index_aave = vec![0.0; tokens.len()];
    let mut variable_borrow_aave = vec![U256::default(); tokens.len()];
    let mut variable_borrow_index_aave = vec![0.0; tokens.len()];

    for (
        col,
        ReserveData {
            liquidity_index,
            variable_borrow_index,
            ..
        },
    ) in rd_results
    {
        liquidity_aave[col] = liquidity_index;
        liquidity_index_aave[col] = liquidity_index.as_f64_ray();

        variable_borrow_aave[col] = variable_borrow_index;
        variable_borrow_index_aave[col] = variable_borrow_index.as_f64_ray();
    }

    let mut reserve_scaled_aave = vec![vec![U256::default(); tokens.len()]; window_size];
    let mut reserve_aave = vec![vec![0.0; tokens.len()]; window_size];

    let mut collateral_scaled_aave = vec![vec![U256::default(); tokens.len()]; window_size];
    let mut collateral_aave = vec![vec![0.0; tokens.len()]; window_size];

    let mut borrowed_scaled_aave = vec![vec![U256::default(); tokens.len()]; window_size];
    let mut borrowed_aave = vec![vec![0.0; tokens.len()]; window_size];

    for (
        row,
        col,
        UserReserveData {
            current_atoken_balance,
            current_variable_debt,
            usage_as_collateral_enabled,
        },
    ) in urd_results
    {
        if usage_as_collateral_enabled {
            collateral_scaled_aave[row][col] = current_atoken_balance
                .to_ray(decimals[col])
                .to_scaled(liquidity_aave[col]);
            collateral_aave[row][col] = current_atoken_balance.as_f64(decimals[col]);
        } else {
            reserve_scaled_aave[row][col] = current_atoken_balance
                .to_ray(decimals[col])
                .to_scaled(liquidity_aave[col]);
            reserve_aave[row][col] = current_atoken_balance.as_f64(decimals[col]);
        }
        borrowed_scaled_aave[row][col] = current_variable_debt
            .to_ray(decimals[col])
            .to_scaled(variable_borrow_aave[col]);
        borrowed_aave[row][col] = current_variable_debt.as_f64(decimals[col]);
    }

    let hf_diff = {
        hf_aave
            .iter()
            .zip(hf[start..start + window_size].iter())
            .map(|(hf_aave, hf)| (hf_aave - hf).abs())
            .collect::<Vec<_>>()
    };

    let liquidity_diff = {
        liquidity_aave
            .iter()
            .zip(liquidity.iter())
            .map(|(l_aave, l)| l_aave.abs_diff(l.clone()))
            .collect::<Vec<_>>()
    };

    let liquidity_index_diff = {
        liquidity_index_aave
            .iter()
            .zip(liquidity_index.iter())
            .map(|(l_aave, l)| (l_aave - l).abs())
            .collect::<Vec<_>>()
    };

    let variable_borrow_diff = {
        variable_borrow_aave
            .iter()
            .zip(variable_borrow.iter())
            .map(|(vb_aave, vb)| vb_aave.abs_diff(vb.clone()))
            .collect::<Vec<_>>()
    };

    let variable_borrow_index_diff = {
        variable_borrow_index_aave
            .iter()
            .zip(variable_borrow_index.iter())
            .map(|(vb_aave, vb)| (vb_aave - vb).abs())
            .collect::<Vec<_>>()
    };

    let reserve_scaled_diff = {
        reserve_scaled_aave
            .iter()
            .zip(reserve_scaled.iter())
            .map(|(r, r2)| {
                r.iter()
                    .zip(r2.iter())
                    .map(|(c, c2)| c.abs_diff(c2.clone()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    };

    let collateral_scaled_diff = {
        collateral_scaled_aave
            .iter()
            .zip(collateral_scaled.iter())
            .map(|(r, r2)| {
                r.iter()
                    .zip(r2.iter())
                    .map(|(c, c2)| c.abs_diff(c2.clone()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    };

    let collateral_diff = {
        collateral_aave
            .iter()
            .zip(collateral.iter())
            .map(|(r, r2)| {
                r.iter()
                    .zip(r2.iter())
                    .map(|(c, c2)| (c - c2).abs())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    };

    let borrowed_scaled_diff = {
        borrowed_scaled_aave
            .iter()
            .zip(borrowed_scaled.iter())
            .map(|(r, r2)| {
                r.iter()
                    .zip(r2.iter())
                    .map(|(c, c2)| c.abs_diff(c2.clone()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    };

    let borrowed_diff = {
        borrowed_aave
            .iter()
            .zip(borrowed.iter())
            .map(|(r, r2)| {
                r.iter()
                    .zip(r2.iter())
                    .map(|(c, c2)| (c - c2).abs())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    };

    let test_probe = TestProbe {
        hf: hf[start..start + window_size].to_vec(),
        users,
        tokens,
        decimals,
        price_decimals,
        liquidation_threshold,
        liquidity,
        liquidity_index,
        variable_borrow,
        variable_borrow_index,
        prices,
        reserve_scaled,
        collateral_scaled,
        collateral,
        borrowed_scaled,
        borrowed,

        hf_aave,
        liquidity_aave,
        liquidity_index_aave,
        variable_borrow_aave,
        variable_borrow_index_aave,
        reserve_scaled_aave,
        collateral_scaled_aave,
        collateral_aave,
        borrowed_scaled_aave,
        borrowed_aave,

        hf_diff,
        liquidity_diff,
        liquidity_index_diff,
        variable_borrow_diff,
        variable_borrow_index_diff,
        reserve_scaled_diff,
        collateral_scaled_diff,
        collateral_diff,
        borrowed_scaled_diff,
        borrowed_diff,
    };

    info!("get_test_probe_state: test_probe = {:?}", test_probe);

    Ok((StatusCode::OK, Json(test_probe)))
}
