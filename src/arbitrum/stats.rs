use crate::arbitrum::arbitrum::{Cache, F64Converter, IL2Pool, Index};
use alloy::providers::Provider;
use alloy::transports::http::reqwest::StatusCode;
use alloy_primitives::{Address, U256};
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::Json;
use bitvec::order::Lsb0;
use bitvec::prelude::BitVec;
use ndarray::Axis;
use serde::Serialize;
use std::sync::Arc;
use tracing::info;

#[derive(Clone)]
pub struct AppState<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    pub cache: Arc<Cache>,
    pub provider: Arc<P>,
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
    let reserve = &*cache.reserve.read().await;
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
    let reserves = &*cache.reserve.read().await;
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
    let collateral = &*cache.collateral.read().await;
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
    let collaterals = &*cache.collateral.read().await;
    for col_lock in collaterals {
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
    let borrowed = &*cache.borrowed.read().await;
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
    let borrowed = &*cache.borrowed.read().await;
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

const L2_POOL_ADDRESS: &str = "0x794a61358D6845594F94dc1DB02A252b5b4814aD";

pub async fn get_user_account_data_state<P>(
    Path(user): Path<Address>,
    State(state): State<AppState<P>>,
) -> (StatusCode, Json<(String, String)>)
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let aave_l2_pool = IL2Pool::new(L2_POOL_ADDRESS.parse().unwrap(), state.provider.clone());
    let hf = aave_l2_pool
        .getUserAccountData(user.clone())
        .call()
        .await
        .unwrap()
        .healthFactor;

    (
        StatusCode::OK,
        Json((hf.to_string(), hf.as_f64_wad().to_string())),
    )
}
