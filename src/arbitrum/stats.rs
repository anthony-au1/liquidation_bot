use crate::arbitrum::arbitrum::{Cache, Index};
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

#[derive(Serialize)]
pub struct UserState {
    pub users: Vec<User>,
    pub users_num: usize,
}

#[derive(Serialize)]
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

#[derive(Serialize)]
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

#[derive(Serialize)]
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

pub async fn get_user_state(
    Path(row_num): Path<usize>,
    State(cache): State<Arc<Cache>>,
) -> impl IntoResponse {
    let user = get_user(row_num, cache).await;

    match user {
        Some(u) => (StatusCode::OK, Json(u)).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            format!("User for row {} not found", row_num),
        )
            .into_response(),
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

pub async fn get_users_state(State(cache): State<Arc<Cache>>) -> (StatusCode, Json<UserState>) {
    let (users, users_num) = get_users(cache).await;
    let user_state = UserState { users, users_num };

    (StatusCode::OK, Json(user_state))
}

async fn get_decimals(cache: Arc<Cache>) -> Vec<f64> {
    let (decimals, _) = &*cache.decimals.read().await;
    decimals.to_vec()
}

pub async fn get_decimals_state(State(cache): State<Arc<Cache>>) -> (StatusCode, Json<Vec<f64>>) {
    let decimals = get_decimals(cache).await;

    (StatusCode::OK, Json(decimals))
}

async fn get_tokens(cache: Arc<Cache>) -> Vec<Address> {
    let (tokens, _) = &*cache.tokens.read().await;
    tokens.to_vec()
}

pub async fn get_tokens_state(State(cache): State<Arc<Cache>>) -> (StatusCode, Json<Vec<Address>>) {
    let tokens = get_tokens(cache).await;

    (StatusCode::OK, Json(tokens))
}

async fn get_price_decimals(cache: Arc<Cache>) -> Vec<f64> {
    let (price_decimals, _) = &*cache.price_decimals.read().await;
    price_decimals.to_vec()
}

pub async fn get_price_decimals_state(
    State(cache): State<Arc<Cache>>,
) -> (StatusCode, Json<Vec<f64>>) {
    let price_decimals = get_price_decimals(cache).await;

    (StatusCode::OK, Json(price_decimals))
}

async fn get_reserve(row_num: usize, cache: Arc<Cache>) -> Option<Vec<String>> {
    let reserve = &*cache.reserve.read().await;
    let row_lock = reserve.get(row_num)?;
    let (row, _, _) = &*row_lock.read().await;
    Some(row.iter().map(U256::to_string).collect())
}

pub async fn get_reserve_state(
    Path(row_num): Path<usize>,
    State(cache): State<Arc<Cache>>,
) -> impl IntoResponse {
    let reserve = get_reserve(row_num, cache).await;

    match reserve {
        Some(r) => (StatusCode::OK, Json(r)).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            format!("reserve for row {} not found", row_num),
        )
            .into_response(),
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

pub async fn get_reserve_all_state(
    State(cache): State<Arc<Cache>>,
) -> (StatusCode, Json<Vec<Vec<String>>>) {
    let reserve = get_reserve_all(cache).await;

    (StatusCode::OK, Json(reserve))
}

async fn get_collateral(row_num: usize, cache: Arc<Cache>) -> Option<Vec<String>> {
    let collateral = &*cache.collateral.read().await;
    let col_lock = collateral.get(row_num)?;
    let (row, _, _) = &*col_lock.read().await;
    Some(row.iter().map(U256::to_string).collect())
}

pub async fn get_collateral_state(
    Path(row_num): Path<usize>,
    State(cache): State<Arc<Cache>>,
) -> impl IntoResponse {
    let collateral = get_collateral(row_num, cache).await;

    match collateral {
        Some(c) => (StatusCode::OK, Json(c)).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            format!("collateral for row {} not found", row_num),
        )
            .into_response(),
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

pub async fn get_collateral_all_state(
    State(cache): State<Arc<Cache>>,
) -> (StatusCode, Json<Vec<Vec<String>>>) {
    let collateral = get_collateral_all(cache).await;

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

pub async fn get_collateral_matrix_row_state(
    Path(row_num): Path<usize>,
    State(cache): State<Arc<Cache>>,
) -> impl IntoResponse {
    let collateral_matrix_row = get_collateral_matrix_row(row_num, cache).await;

    match collateral_matrix_row {
        Some(c) => (StatusCode::OK, Json(c)).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            format!("collateral for row {} not found", row_num),
        )
            .into_response(),
    }
}

async fn get_collateral_matrix(cache: Arc<Cache>) -> Vec<Vec<f64>> {
    let collaterals = &*cache.collateral_matrix.read().await;
    collaterals
        .axis_iter(Axis(0))
        .map(|row| row.to_vec())
        .collect::<Vec<_>>()
}

pub async fn get_collateral_matrix_state(
    State(cache): State<Arc<Cache>>,
) -> (StatusCode, Json<Vec<Vec<f64>>>) {
    let collateral_matrix = get_collateral_matrix(cache).await;

    (StatusCode::OK, Json(collateral_matrix))
}

async fn get_borrowed(row_num: usize, cache: Arc<Cache>) -> Option<Vec<String>> {
    let borrowed = &*cache.borrowed.read().await;
    let bor_lock = borrowed.get(row_num)?;
    let (row, _, _) = &*bor_lock.read().await;
    Some(row.iter().map(U256::to_string).collect())
}

pub async fn get_borrowed_state(
    Path(row_num): Path<usize>,
    State(cache): State<Arc<Cache>>,
) -> impl IntoResponse {
    let borrowed = get_borrowed(row_num, cache).await;

    match borrowed {
        Some(b) => (StatusCode::OK, Json(b)).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            format!("borrowed for row {} not found", row_num),
        )
            .into_response(),
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

pub async fn get_borrowed_all_state(
    State(cache): State<Arc<Cache>>,
) -> (StatusCode, Json<Vec<Vec<String>>>) {
    let borrowed = get_borrowed_all(cache).await;

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

pub async fn get_borrowed_matrix_row_state(
    Path(row_num): Path<usize>,
    State(cache): State<Arc<Cache>>,
) -> impl IntoResponse {
    let borrowed_matrix_row = get_borrowed_matrix_row(row_num, cache).await;

    match borrowed_matrix_row {
        Some(c) => (StatusCode::OK, Json(c)).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            format!("borrowed for row {} not found", row_num),
        )
            .into_response(),
    }
}

async fn get_borrowed_matrix(cache: Arc<Cache>) -> Vec<Vec<f64>> {
    let borrowed = &*cache.borrowed_matrix.read().await;
    borrowed
        .axis_iter(Axis(0))
        .map(|row| row.to_vec())
        .collect::<Vec<_>>()
}

pub async fn get_borrowed_matrix_state(
    State(cache): State<Arc<Cache>>,
) -> (StatusCode, Json<Vec<Vec<f64>>>) {
    let borrowed_matrix = get_borrowed_matrix(cache).await;

    (StatusCode::OK, Json(borrowed_matrix))
}

async fn get_liquidity(cache: Arc<Cache>) -> Vec<IndexRate> {
    let (indexes, _) = &*cache.liquidity.read().await;
    indexes
        .iter()
        .map(|Index { index, rate, .. }| IndexRate::new(index.to_string(), rate.to_string()))
        .collect::<Vec<_>>()
}

pub async fn get_liquidity_state(
    State(cache): State<Arc<Cache>>,
) -> (StatusCode, Json<Vec<IndexRate>>) {
    let liquidity = get_liquidity(cache).await;

    (StatusCode::OK, Json(liquidity))
}

async fn get_liquidity_index(cache: Arc<Cache>) -> Vec<f64> {
    let (indexes, _) = &*cache.liquidity_index.read().await;
    indexes.to_vec()
}

pub async fn get_liquidity_index_state(
    State(cache): State<Arc<Cache>>,
) -> (StatusCode, Json<Vec<f64>>) {
    let liquidity_index = get_liquidity_index(cache).await;

    (StatusCode::OK, Json(liquidity_index))
}

async fn get_variable_borrow(cache: Arc<Cache>) -> Vec<IndexRate> {
    let (indexes, _) = &*cache.variable_borrow.read().await;
    indexes
        .iter()
        .map(|Index { index, rate, .. }| IndexRate::new(index.to_string(), rate.to_string()))
        .collect::<Vec<_>>()
}

pub async fn get_variable_borrow_state(
    State(cache): State<Arc<Cache>>,
) -> (StatusCode, Json<Vec<IndexRate>>) {
    let variable_borrow = get_variable_borrow(cache).await;

    (StatusCode::OK, Json(variable_borrow))
}

async fn get_variable_borrow_index(cache: Arc<Cache>) -> Vec<f64> {
    let (indexes, _) = &*cache.variable_borrow_index.read().await;
    indexes.to_vec()
}

pub async fn get_variable_borrow_index_state(
    State(cache): State<Arc<Cache>>,
) -> (StatusCode, Json<Vec<f64>>) {
    let variable_borrow_index = get_variable_borrow_index(cache).await;

    (StatusCode::OK, Json(variable_borrow_index))
}

async fn get_liquidation_threshold(cache: Arc<Cache>) -> Vec<f64> {
    let (lt, _) = &*cache.liquidation_threshold.read().await;
    lt.to_vec()
}

pub async fn get_liquidation_threshold_state(
    State(cache): State<Arc<Cache>>,
) -> (StatusCode, Json<Vec<f64>>) {
    let liquidation_threshold = get_liquidation_threshold(cache).await;

    (StatusCode::OK, Json(liquidation_threshold))
}

async fn get_prices(cache: Arc<Cache>) -> Vec<f64> {
    let (prices, _) = &*cache.prices.read().await;
    prices.to_vec()
}

pub async fn get_prices_state(State(cache): State<Arc<Cache>>) -> (StatusCode, Json<Vec<f64>>) {
    let prices = get_prices(cache).await;

    (StatusCode::OK, Json(prices))
}

async fn get_health_factors(cache: Arc<Cache>) -> Vec<f64> {
    let (hf, _) = &*cache.health_factors.read().await;
    hf.to_vec()
}

pub async fn get_health_factors_state(
    State(cache): State<Arc<Cache>>,
) -> (StatusCode, Json<Vec<f64>>) {
    let health_factors = get_health_factors(cache).await;

    (StatusCode::OK, Json(health_factors))
}

pub async fn get_full_state(State(cache): State<Arc<Cache>>) -> (StatusCode, Json<FullState>) {
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

    (StatusCode::OK, Json(full_state))
}
