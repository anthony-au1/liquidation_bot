use crate::arbitrum::arbitrum::IAaveProtocolDataProvider::TokenData;
use crate::arbitrum::arbitrum::IChainlinkAggregator::{AnswerUpdated, IChainlinkAggregatorEvents};
use crate::arbitrum::arbitrum::IL2Pool::{
    Borrow, IL2PoolEvents, LiquidationCall, Repay, ReserveDataUpdated,
    ReserveUsedAsCollateralDisabled, ReserveUsedAsCollateralEnabled, Supply, Withdraw,
};
use crate::arbitrum::arbitrum::{
    AaveEvents, Cache, DataProvider, F64Converter, HFRequest, Index, RayOperations, ReserveData,
    RqDate, Scaler, SyncRequest, SyncTarget, Token, TokenDetails, UserData, UserReserveData,
    UserSettings, liquidation_threshold_update, listen_events, listen_hf_calc, listen_price_update,
    listen_sync, setup,
};
use crate::arbitrum::events::{
    answer_updated, borrow, create_user, liquidation_call, repay, reserve_data_updated,
    reserve_used_as_collateral_disabled, reserve_used_as_collateral_enabled, supply, withdraw,
};
use alloy_primitives::aliases::U40;
use alloy_primitives::{Address, I256, U256};
use async_trait::async_trait;
use bitvec::bitvec;
use bitvec::order::Lsb0;
use bitvec::prelude::BitVec;
use chrono::Utc;
use eyre::eyre;
use ndarray::{Array1, Array2};
use std::collections::HashMap;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::sync::mpsc::channel;
use tokio::task;
use tokio::time::sleep;

trait F64Helper: F64Converter {
    fn as_f64_decimal_18(&self) -> f64;
    fn as_f64_decimal_6(&self) -> f64;
    fn as_f64_decimal_12(&self) -> f64;
    fn as_f64_decimal_27(&self) -> f64;
}

trait U256Helper {
    fn as_u256(&self, decimals: usize) -> U256;
    fn as_u256_decimal_18(&self) -> U256;
    fn as_u256_decimal_6(&self) -> U256;
    fn as_u256_decimal_12(&self) -> U256;
    fn as_u256_decimal_27(&self) -> U256;
}

impl<T> U256Helper for T
where
    T: Copy + TryInto<u128>,
    <T as TryInto<u128>>::Error: Debug,
{
    fn as_u256(&self, decimals: usize) -> U256 {
        U256::from((*self).try_into().unwrap()) * U256::from(10).pow(U256::from(decimals))
    }

    fn as_u256_decimal_18(&self) -> U256 {
        self.as_u256(18)
    }

    fn as_u256_decimal_6(&self) -> U256 {
        self.as_u256(6)
    }

    fn as_u256_decimal_12(&self) -> U256 {
        self.as_u256(12)
    }

    fn as_u256_decimal_27(&self) -> U256 {
        self.as_u256(27)
    }
}

impl F64Helper for U256 {
    fn as_f64_decimal_18(&self) -> f64 {
        self.as_f64(10_f64.powf(18_f64))
    }

    fn as_f64_decimal_6(&self) -> f64 {
        self.as_f64(10_f64.powf(6_f64))
    }

    fn as_f64_decimal_12(&self) -> f64 {
        self.as_f64(10_f64.powf(12_f64))
    }

    fn as_f64_decimal_27(&self) -> f64 {
        self.as_f64(10_f64.powf(27_f64))
    }
}

struct DummyDataProvider;

impl DummyDataProvider {
    fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl DataProvider for DummyDataProvider {
    async fn get_all_reserves_tokens(&self) -> eyre::Result<Vec<TokenData>> {
        let mut token_data = vec![];
        let (_, tokens) = generate_cache_and_tokens(0).await?;
        for (token_address, TokenDetails { name, .. }) in tokens {
            token_data.push(TokenData {
                symbol: name,
                tokenAddress: token_address,
            })
        }

        Ok(token_data)
    }

    async fn get_source_of_asset(&self, token_address: &Address) -> eyre::Result<Address> {
        let (_, tokens) = generate_cache_and_tokens(0).await?;
        let price_source = tokens
            .get(token_address)
            .map(|details| details.price_source.clone())
            .ok_or_else(|| eyre!("Source not found for asset {:?}", token_address))?;

        Ok(price_source)
    }

    async fn listen_events<F, Fut>(&self, callback: F) -> eyre::Result<()>
    where
        F: Fn(IL2PoolEvents) -> Fut + Send + 'static,
        Fut: Future<Output = eyre::Result<()>> + Send,
    {
        let user = Address::from_str("0x1Af54C553cefD1792CbFcF41B711834d657ea61D")?;
        let event = Supply {
            reserve: Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?,
            user: user.clone(),
            onBehalfOf: user,
            amount: 6.as_u256_decimal_18(),
            referralCode: 0,
        };
        callback(IL2PoolEvents::Supply(event)).await
    }

    async fn listen_price_update<F, Fut>(
        &self,
        price_source: &Address,
        callback: F,
    ) -> eyre::Result<()>
    where
        F: Fn(IChainlinkAggregatorEvents) -> Fut + Send + 'static,
        Fut: Future<Output = eyre::Result<()>> + Send,
    {
        let (_, tokens) = generate_cache_and_tokens(0).await?;

        let name = tokens
            .iter()
            .find(
                |(
                    _,
                    TokenDetails {
                        price_source: ps, ..
                    },
                )| ps == price_source,
            )
            .map(|(_, TokenDetails { name, .. })| name.clone())
            .ok_or_else(|| eyre!("Price update not found for asset {:?}", price_source))?;

        let current = match &name[..] {
            "AAVE" => 161_230_000_000_i128,
            "USDC" => 261_230_000_000_i128,
            "DAI" => 361_230_000_000_i128,
            _ => 561_230_000_000_i128,
        };

        let event = AnswerUpdated {
            // 161230000000 / 10^8 = 1612.30 USD
            current: I256::try_from(current)?,
            roundId: U256::from(0),
            timestamp: U256::from(Utc::now().timestamp()),
        };

        callback(IChainlinkAggregatorEvents::AnswerUpdated(event)).await
    }

    async fn get_reserve_configuration_data(&self, token: &Address) -> eyre::Result<f64> {
        let lt = match token.clone() {
            addr if addr == Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")? => {
                0.78
            }
            addr if addr == Address::from_str("0x1Af54C113cefD1792CbFcF41B711834d657ea61D")? => 0.8,
            addr if addr == Address::from_str("0x1Af54C113cefD1792CbFcF41B711824d657eb61D")? => {
                0.75
            }
            _ => return Err(eyre!("Invalid token address")),
        };

        Ok(lt)
    }

    async fn get_user_reserve_data(
        &self,
        token: &Address,
        _: &Address,
    ) -> eyre::Result<UserReserveData> {
        let urd = match token {
            t if *t == Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")? => {
                UserReserveData::new(10.as_u256_decimal_18(), 10.as_u256_decimal_18(), false)
            }
            t if *t == Address::from_str("0x1Af54C113cefD1792CbFcF41B711834d657ea61D")? => {
                UserReserveData::new(20.as_u256_decimal_6(), 20.as_u256_decimal_6(), true)
            }
            t if *t == Address::from_str("0x1Af54C113cefD1792CbFcF41B711824d657eb61D")? => {
                UserReserveData::new(30.as_u256_decimal_12(), 30.as_u256_decimal_12(), false)
            }
            _ => UserReserveData::new(40.as_u256_decimal_18(), 40.as_u256_decimal_18(), true),
        };

        Ok(urd)
    }

    async fn get_reserve_data(&self, token: &Address) -> eyre::Result<ReserveData> {
        let now = Utc::now().timestamp();
        let rd = match token {
            t if *t == Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")? => {
                ReserveData::new(
                    45.as_u256(24),
                    5.as_u256(25),
                    1045.as_u256(24),
                    105.as_u256(25),
                    U40::from(now),
                )
            }
            t if *t == Address::from_str("0x1Af54C113cefD1792CbFcF41B711834d657ea61D")? => {
                ReserveData::new(
                    35.as_u256(24),
                    4.as_u256(25),
                    1035.as_u256(24),
                    104.as_u256(25),
                    U40::from(now),
                )
            }
            t if *t == Address::from_str("0x1Af54C113cefD1792CbFcF41B711824d657eb61D")? => {
                ReserveData::new(
                    25.as_u256(24),
                    3.as_u256(25),
                    1025.as_u256(24),
                    103.as_u256(25),
                    U40::from(now),
                )
            }
            _ => ReserveData::new(
                55.as_u256(24),
                6.as_u256(25),
                1055.as_u256(24),
                106.as_u256(25),
                U40::from(now),
            ),
        };

        Ok(rd)
    }

    async fn get_decimals(&self, token: &Address) -> eyre::Result<f64> {
        let decimals = match token {
            t if *t == Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")? => {
                10_f64.powf(18_f64)
            }
            t if *t == Address::from_str("0x1Af54C113cefD1792CbFcF41B711834d657ea61D")? => {
                10_f64.powf(6_f64)
            }
            t if *t == Address::from_str("0x1Af54C113cefD1792CbFcF41B711824d657eb61D")? => {
                10_f64.powf(12_f64)
            }
            _ => 10_f64.powf(18_f64),
        };

        Ok(decimals)
    }

    async fn get_price_decimals(&self, price_source: &Address) -> eyre::Result<f64> {
        let decimals = match price_source {
            t if *t == Address::from_str("0xba5DdD1f9d7F570dc94a51479a000E3BCE967196")? => {
                10_f64.powf(18_f64)
            }
            t if *t == Address::from_str("0xaf88d065e77c8cC2239327C5EDb3A432268e5831")? => {
                10_f64.powf(6_f64)
            }
            t if *t == Address::from_str("0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1")? => {
                10_f64.powf(12_f64)
            }
            _ => 10_f64.powf(18_f64),
        };

        Ok(decimals)
    }
}

async fn generate_cache_and_tokens(
    user_num: usize,
) -> eyre::Result<(Cache, HashMap<Address, TokenDetails>)> {
    let cache = Cache::default();

    let mut tokens = HashMap::new();
    tokens.insert(
        Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?,
        TokenDetails::new(
            String::from("AAVE"),
            Address::from_str("0xba5DdD1f9d7F570dc94a51479a000E3BCE967196")?,
            0,
            10_f64.powf(18_f64),
            10_f64.powf(18_f64),
        ),
    );
    tokens.insert(
        Address::from_str("0x1Af54C113cefD1792CbFcF41B711834d657ea61D")?,
        TokenDetails::new(
            String::from("USDC"),
            Address::from_str("0xaf88d065e77c8cC2239327C5EDb3A432268e5831")?,
            1,
            10_f64.powf(6_f64),
            10_f64.powf(6_f64),
        ),
    );
    tokens.insert(
        Address::from_str("0x1Af54C113cefD1792CbFcF41B711824d657eb61D")?,
        TokenDetails::new(
            String::from("DAI"),
            Address::from_str("0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1")?,
            2,
            10_f64.powf(12_f64),
            10_f64.powf(12_f64),
        ),
    );

    let now = Utc::now().timestamp_micros();
    *cache.health_factors.write().await = (Array1::from_elem(0, 0.0), now);
    if user_num > 0 {
        let mut user_addr = Address::from_str("0x1Af54C553cefD1792CbFcF41B711834d657ea61D")?;

        for i in 0..user_num {
            cache.users.insert(
                user_addr,
                UserSettings::new(i, BitVec::<usize, Lsb0>::from_iter([true, false, false])),
            );
            user_addr = user_addr.create((i + 1) as u64);

            let collaterals = &mut *cache.collateral.write().await;
            collaterals.push(RwLock::new((
                Array1::from_vec(vec![U256::default(); 3]),
                now,
                now,
            )));

            let reserves = &mut *cache.reserve.write().await;
            reserves.push(RwLock::new((
                Array1::from_vec(vec![U256::default(); 3]),
                now,
                now,
            )));

            let borrowed = &mut *cache.borrowed.write().await;
            borrowed.push(RwLock::new((
                Array1::from_vec(vec![U256::default(); 3]),
                now,
                now,
            )));

            let (hf, _) = &mut *cache.health_factors.write().await;
            let mut hf_vec = hf.to_vec();
            hf_vec.push(0.0);
            *hf = Array1::from_vec(hf_vec);
        }

        *cache.collateral_matrix.write().await = Array2::from_elem((0, tokens.len()), 0.0);
        *cache.borrowed_matrix.write().await = Array2::from_elem((0, tokens.len()), 0.0);
    }

    *cache.prices.write().await = (Array1::from_elem(tokens.len(), 0.0), now);
    *cache.liquidation_threshold.write().await = (Array1::from_elem(tokens.len(), 0.0), now);
    *cache.decimals.write().await = (
        Array1::from_vec(vec![
            10_f64.powf(18_f64),
            10_f64.powf(6_f64),
            10_f64.powf(12_f64),
        ]),
        now,
    );
    *cache.liquidity.write().await = (
        Array1::from_vec(vec![
            Index::new(1045.as_u256(24), 45.as_u256(25), now),
            Index::new(1035.as_u256(24), 35.as_u256(25), now),
            Index::new(1025.as_u256(24), 25.as_u256(25), now),
        ]),
        now,
    );
    *cache.liquidity_index.write().await = (
        Array1::from_vec(vec![
            1045.as_u256(24).as_f64_ray(),
            1035.as_u256(24).as_f64_ray(),
            1025.as_u256(24).as_f64_ray(),
        ]),
        now,
    );
    *cache.variable_borrow.write().await = (
        Array1::from_vec(vec![
            Index::new(105.as_u256(25), 5.as_u256(26), now),
            Index::new(104.as_u256(25), 4.as_u256(26), now),
            Index::new(103.as_u256(25), 3.as_u256(26), now),
        ]),
        now,
    );
    *cache.variable_borrow_index.write().await = (
        Array1::from_vec(vec![
            105.as_u256(25).as_f64_ray(),
            104.as_u256(25).as_f64_ray(),
            103.as_u256(25).as_f64_ray(),
        ]),
        now,
    );

    Ok((cache, tokens))
}

async fn get_all_user_data(
    cache: &Cache,
    user_row_num: usize,
) -> eyre::Result<(Vec<U256>, Vec<U256>, Vec<U256>)> {
    let (collateral, reserve, borrowed) = {
        let collaterals = &*cache.collateral.read().await;
        let (collateral, _, _) = &*collaterals
            .get(user_row_num)
            .ok_or_else(|| eyre!("row = {} not found in collateral", user_row_num))?
            .read()
            .await;

        let reserves = &*cache.reserve.read().await;
        let (reserve, _, _) = &*reserves
            .get(user_row_num)
            .ok_or_else(|| eyre!("row = {} not found in reserve", user_row_num))?
            .read()
            .await;

        let borrowed = &*cache.borrowed.read().await;
        let (borrowed, _, _) = &*borrowed
            .get(user_row_num)
            .ok_or_else(|| eyre!("row = {} not found in borrowed", user_row_num))?
            .read()
            .await;

        (
            Array1::to_vec(collateral),
            Array1::to_vec(reserve),
            Array1::to_vec(borrowed),
        )
    };

    Ok((collateral, reserve, borrowed))
}

#[tokio::test]
async fn test_sync_collateral() -> eyre::Result<()> {
    let rq_date = Utc::now().timestamp_micros();
    let cache = Cache::default();

    let now = Utc::now().timestamp_micros();
    {
        *cache.decimals.write().await = (
            Array1::from_vec(vec![
                10_f64.powf(18_f64),
                10_f64.powf(6_f64),
                10_f64.powf(12_f64),
            ]),
            now,
        );

        *cache.liquidity.write().await = (
            Array1::from_vec(vec![
                Index::new(1045.as_u256(24), 45.as_u256(24), now),
                Index::new(1035.as_u256(24), 35.as_u256(24), now),
                Index::new(1025.as_u256(24), 25.as_u256(24), now),
            ]),
            now,
        );
    }

    let (decimals, _) = &*cache.decimals.read().await;
    let col1 = 10
        .as_u256_decimal_18()
        .to_ray(decimals[0])
        .to_scaled(cache.liquidity.read().await.0[0].index);
    let col2 = 20
        .as_u256_decimal_6()
        .to_ray(decimals[1])
        .to_scaled(cache.liquidity.read().await.0[1].index);
    let col3 = 30
        .as_u256_decimal_12()
        .to_ray(decimals[2])
        .to_scaled(cache.liquidity.read().await.0[2].index);

    let row = vec![col1, col2, col3];
    let row_len = row.len();
    {
        *cache.collateral.write().await =
            vec![RwLock::new((Array1::from_vec(row.clone()), now, now))];
        *cache.collateral_matrix.write().await = Array2::from_elem((1, row_len), 0.0);
    }

    cache.sync_collateral(&SyncTarget::Row(0), rq_date).await?;

    let mut expected = Array2::from_shape_vec(
        (1, row_len),
        vec![col1.as_f64_ray(), col2.as_f64_ray(), col3.as_f64_ray()],
    )?;
    {
        let collateral_matrix = &*cache.collateral_matrix.read().await;
        assert_eq!(collateral_matrix, expected);
    }

    let col4 = 40
        .as_u256_decimal_18()
        .to_ray(decimals[0])
        .to_scaled(cache.liquidity.read().await.0[0].index);
    let col5 = 50
        .as_u256_decimal_6()
        .to_ray(decimals[1])
        .to_scaled(cache.liquidity.read().await.0[1].index);
    let col6 = 60
        .as_u256_decimal_12()
        .to_ray(decimals[2])
        .to_scaled(cache.liquidity.read().await.0[2].index);

    let row = vec![col4, col5, col6];
    {
        let collateral = &mut *cache.collateral.write().await;
        collateral.push(RwLock::new((Array1::from_vec(row.clone()), 0, 0)));
    }

    cache.sync_collateral(&SyncTarget::Row(1), rq_date).await?;

    expected.push_row(
        Array1::from_vec(vec![
            col4.as_f64_ray(),
            col5.as_f64_ray(),
            col6.as_f64_ray(),
        ])
        .view(),
    )?;
    {
        let collateral_matrix = &*cache.collateral_matrix.read().await;
        assert_eq!(collateral_matrix, expected);
    }

    let col7 = 70
        .as_u256_decimal_18()
        .to_ray(decimals[0])
        .to_scaled(cache.liquidity.read().await.0[0].index);
    let col8 = 80
        .as_u256_decimal_6()
        .to_ray(decimals[1])
        .to_scaled(cache.liquidity.read().await.0[1].index);
    let col9 = 90
        .as_u256_decimal_12()
        .to_ray(decimals[2])
        .to_scaled(cache.liquidity.read().await.0[2].index);

    let row = vec![col7, col8, col9];
    {
        let collateral = &mut *cache.collateral.write().await;
        collateral.push(RwLock::new((Array1::from_vec(row.clone()), 0, 0)));
    }

    cache.sync_collateral(&SyncTarget::Row(2), rq_date).await?;

    expected.push_row(
        Array1::from_vec(vec![
            col7.as_f64_ray(),
            col8.as_f64_ray(),
            col9.as_f64_ray(),
        ])
        .view(),
    )?;
    {
        let collateral_matrix = &*cache.collateral_matrix.read().await;
        assert_eq!(collateral_matrix, expected);
    }

    let col_updated1 = 400
        .as_u256_decimal_18()
        .to_ray(decimals[0])
        .to_scaled(cache.liquidity.read().await.0[0].index);
    let col_updated2 = 500
        .as_u256_decimal_6()
        .to_ray(decimals[1])
        .to_scaled(cache.liquidity.read().await.0[1].index);
    let col_updated3 = 600
        .as_u256_decimal_12()
        .to_ray(decimals[2])
        .to_scaled(cache.liquidity.read().await.0[2].index);

    {
        let collateral = &mut *cache.collateral.write().await;
        let (col_row, _, _) = &mut *collateral
            .get(1)
            .ok_or_else(|| eyre!("row = 1 not found in collateral"))?
            .write()
            .await;
        col_row[0] = col_updated1;
        col_row[1] = col_updated2;
        col_row[2] = col_updated3;
    }

    cache.sync_collateral(&SyncTarget::Row(1), rq_date).await?;

    expected[(1, 0)] = col_updated1.as_f64_ray();
    expected[(1, 1)] = col_updated2.as_f64_ray();
    expected[(1, 2)] = col_updated3.as_f64_ray();

    {
        let collateral_matrix = &*cache.collateral_matrix.read().await;
        assert_eq!(collateral_matrix, expected);
    }

    Ok(())
}

#[tokio::test]
async fn test_sync_borrowed() -> eyre::Result<()> {
    let rq_date = Utc::now().timestamp_micros();
    let cache = Cache::default();

    let now = Utc::now().timestamp_micros();
    {
        *cache.decimals.write().await = (
            Array1::from_vec(vec![
                10_f64.powf(18_f64),
                10_f64.powf(6_f64),
                10_f64.powf(12_f64),
            ]),
            now,
        );

        *cache.variable_borrow.write().await = (
            Array1::from_vec(vec![
                Index::new(105.as_u256(25), 5.as_u256(25), now),
                Index::new(104.as_u256(25), 4.as_u256(25), now),
                Index::new(103.as_u256(25), 3.as_u256(25), now),
            ]),
            now,
        );
    }

    let (decimals, _) = &*cache.decimals.read().await;
    let bor1 = 10
        .as_u256_decimal_18()
        .to_ray(decimals[0])
        .to_scaled(cache.variable_borrow.read().await.0[0].index);
    let bor2 = 20
        .as_u256_decimal_6()
        .to_ray(decimals[1])
        .to_scaled(cache.variable_borrow.read().await.0[1].index);
    let bor3 = 30
        .as_u256_decimal_12()
        .to_ray(decimals[2])
        .to_scaled(cache.variable_borrow.read().await.0[2].index);

    let row = vec![bor1, bor2, bor3];
    let row_len = row.len();
    {
        *cache.borrowed.write().await =
            vec![RwLock::new((Array1::from_vec(row.clone()), now, now))];
        *cache.borrowed_matrix.write().await = Array2::from_elem((1, row_len), 0.0);
    }

    cache.sync_borrowed(&SyncTarget::Row(0), rq_date).await?;

    let mut expected = Array2::from_shape_vec(
        (1, row_len),
        vec![bor1.as_f64_ray(), bor2.as_f64_ray(), bor3.as_f64_ray()],
    )?;
    {
        let borrowed_matrix = &*cache.borrowed_matrix.read().await;
        assert_eq!(borrowed_matrix, expected);
    }

    let bor4 = 40
        .as_u256_decimal_18()
        .to_ray(decimals[0])
        .to_scaled(cache.variable_borrow.read().await.0[0].index);
    let bor5 = 50
        .as_u256_decimal_6()
        .to_ray(decimals[1])
        .to_scaled(cache.variable_borrow.read().await.0[1].index);
    let bor6 = 60
        .as_u256_decimal_12()
        .to_ray(decimals[2])
        .to_scaled(cache.variable_borrow.read().await.0[2].index);

    let row = vec![bor4, bor5, bor6];
    {
        let borrowed = &mut *cache.borrowed.write().await;
        borrowed.push(RwLock::new((Array1::from_vec(row.clone()), 0, 0)));
    }

    cache.sync_borrowed(&SyncTarget::Row(1), rq_date).await?;

    expected.push_row(
        Array1::from_vec(vec![
            bor4.as_f64_ray(),
            bor5.as_f64_ray(),
            bor6.as_f64_ray(),
        ])
        .view(),
    )?;
    {
        let borrowed_matrix = &*cache.borrowed_matrix.read().await;
        assert_eq!(borrowed_matrix, expected);
    }

    let bor7 = 70
        .as_u256_decimal_18()
        .to_ray(decimals[0])
        .to_scaled(cache.variable_borrow.read().await.0[0].index);
    let bor8 = 80
        .as_u256_decimal_6()
        .to_ray(decimals[1])
        .to_scaled(cache.variable_borrow.read().await.0[1].index);
    let bor9 = 90
        .as_u256_decimal_12()
        .to_ray(decimals[2])
        .to_scaled(cache.variable_borrow.read().await.0[2].index);

    let row = vec![bor7, bor8, bor9];
    {
        let borrowed = &mut *cache.borrowed.write().await;
        borrowed.push(RwLock::new((Array1::from_vec(row.clone()), 0, 0)));
    }

    cache.sync_borrowed(&SyncTarget::Row(2), rq_date).await?;

    expected.push_row(
        Array1::from_vec(vec![
            bor7.as_f64_ray(),
            bor8.as_f64_ray(),
            bor9.as_f64_ray(),
        ])
        .view(),
    )?;
    {
        let borrowed_matrix = &*cache.borrowed_matrix.read().await;
        assert_eq!(borrowed_matrix, expected);
    }

    let bor_updated1 = 400
        .as_u256_decimal_18()
        .to_ray(decimals[0])
        .to_scaled(cache.variable_borrow.read().await.0[0].index);
    let bor_updated2 = 500
        .as_u256_decimal_6()
        .to_ray(decimals[1])
        .to_scaled(cache.variable_borrow.read().await.0[1].index);
    let bor_updated3 = 600
        .as_u256_decimal_12()
        .to_ray(decimals[2])
        .to_scaled(cache.variable_borrow.read().await.0[2].index);

    {
        let borrowed = &mut *cache.borrowed.write().await;
        let (bor_row, _, _) = &mut *borrowed
            .get(1)
            .ok_or_else(|| eyre!("row = 1 not found in borrowed"))?
            .write()
            .await;
        bor_row[0] = bor_updated1;
        bor_row[1] = bor_updated2;
        bor_row[2] = bor_updated3;
    }

    cache.sync_borrowed(&SyncTarget::Row(1), rq_date).await?;

    expected[(1, 0)] = bor_updated1.as_f64_ray();
    expected[(1, 1)] = bor_updated2.as_f64_ray();
    expected[(1, 2)] = bor_updated3.as_f64_ray();

    {
        let borrowed_matrix = &*cache.borrowed_matrix.read().await;
        assert_eq!(borrowed_matrix, expected);
    }

    Ok(())
}

#[tokio::test]
async fn test_sync_user() -> eyre::Result<()> {
    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(1).await?;
    let user = cache
        .users
        .iter()
        .next()
        .ok_or_else(|| eyre!("no users found"))?
        .key()
        .clone();

    cache
        .sync_user(&user, &tokens, dummy_data_provider.clone())
        .await?;

    assert_eq!(cache.contains(&user), true);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;
    let (decimal, _) = &*cache.decimals.read().await;

    assert_eq!(
        collateral,
        vec![
            U256::default(),
            20.as_u256_decimal_6()
                .to_ray(decimal[1])
                .to_scaled(cache.liquidity.read().await.0[1].index),
            U256::default()
        ]
    );
    assert_eq!(
        reserve,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimal[0])
                .to_scaled(cache.liquidity.read().await.0[0].index),
            U256::default(),
            30.as_u256_decimal_12()
                .to_ray(decimal[2])
                .to_scaled(cache.liquidity.read().await.0[2].index),
        ]
    );
    assert_eq!(
        borrowed,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimal[0])
                .to_scaled(cache.variable_borrow.read().await.0[0].index),
            20.as_u256_decimal_6()
                .to_ray(decimal[1])
                .to_scaled(cache.variable_borrow.read().await.0[1].index),
            30.as_u256_decimal_12()
                .to_ray(decimal[2])
                .to_scaled(cache.variable_borrow.read().await.0[2].index),
        ]
    );

    Ok(())
}

#[tokio::test]
async fn test_init_user() -> eyre::Result<()> {
    // 1 case - cache has this user

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(1).await?;
    let user = cache
        .users
        .iter()
        .next()
        .ok_or_else(|| eyre!("no users found"))?
        .key()
        .clone();

    cache.init_user(&user, &tokens, dummy_data_provider).await?;

    assert_eq!(cache.contains(&user), true);
    assert_eq!(cache.users.len(), 1);

    // 2 case - new user

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(0).await?;
    let user = Address::from_str("0x1Af54C553cefD1792CbFcF41B711834d657ea61D")?;

    cache.init_user(&user, &tokens, dummy_data_provider).await?;

    assert_eq!(cache.contains(&user), true);
    assert_eq!(cache.users.len(), 1);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;
    let (decimals, _) = &*cache.decimals.read().await;

    assert_eq!(
        collateral,
        vec![
            U256::default(),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.liquidity.read().await.0[1].index),
            U256::default()
        ]
    );
    assert_eq!(
        reserve,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.liquidity.read().await.0[0].index),
            U256::default(),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.liquidity.read().await.0[2].index),
        ]
    );
    assert_eq!(
        borrowed,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.variable_borrow.read().await.0[0].index),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.variable_borrow.read().await.0[1].index),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.variable_borrow.read().await.0[2].index),
        ]
    );

    Ok(())
}

#[tokio::test]
async fn test_get_user_data() -> eyre::Result<()> {
    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(1).await?;
    let user = cache
        .users
        .iter()
        .next()
        .ok_or_else(|| eyre!("no users found"))?
        .key()
        .clone();

    let UserData {
        reserve_scaled,
        collateral_scaled,
        borrowed_scaled,
        liquidity_indexes,
        liquidity_rates,
        variable_borrow_indexes,
        variable_borrow_rates,
        last_update_timestamps,
        user_settings,
    } = cache
        .get_user_data(dummy_data_provider, &tokens, &user)
        .await?;

    assert_eq!(cache.contains(&user), true);
    assert_eq!(cache.users.len(), 1);

    let (decimals, _) = &*cache.decimals.read().await;

    assert_eq!(
        collateral_scaled,
        vec![
            U256::default(),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.liquidity.read().await.0[1].index),
            U256::default()
        ]
    );
    assert_eq!(
        reserve_scaled,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.liquidity.read().await.0[0].index),
            U256::default(),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.liquidity.read().await.0[2].index),
        ]
    );
    assert_eq!(
        borrowed_scaled,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.variable_borrow.read().await.0[0].index),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.variable_borrow.read().await.0[1].index),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.variable_borrow.read().await.0[2].index),
        ]
    );

    assert_eq!(
        liquidity_indexes,
        vec![1045.as_u256(24), 1035.as_u256(24), 1025.as_u256(24)]
    );
    assert_eq!(
        liquidity_rates,
        vec![45.as_u256(24), 35.as_u256(24), 25.as_u256(24)]
    );
    assert_eq!(
        variable_borrow_indexes,
        vec![105.as_u256(25), 104.as_u256(25), 103.as_u256(25)]
    );
    assert_eq!(
        variable_borrow_rates,
        vec![5.as_u256(25), 4.as_u256(25), 3.as_u256(25)]
    );
    assert_eq!(
        last_update_timestamps,
        vec![U40::from(Utc::now().timestamp()); 3]
    );
    assert_eq!(user_settings.row_num, 0);
    assert_eq!(user_settings.use_as_collateral, bitvec![0, 1, 0]);

    Ok(())
}

#[tokio::test]
async fn test_contains() -> eyre::Result<()> {
    let (cache, _) = generate_cache_and_tokens(1).await?;
    let user = cache
        .users
        .iter()
        .next()
        .ok_or_else(|| eyre!("no users found"))?
        .key()
        .clone();

    assert_eq!(cache.contains(&user), true);

    Ok(())
}

#[tokio::test]
async fn test_remove_user() -> eyre::Result<()> {
    let (cache, _) = generate_cache_and_tokens(1).await?;
    let user = cache
        .users
        .iter()
        .next()
        .ok_or_else(|| eyre!("no users found"))?
        .key()
        .clone();
    cache.remove_user(&user);

    assert_eq!(cache.contains(&user), false);

    Ok(())
}

#[tokio::test]
async fn test_subscribe() -> eyre::Result<()> {
    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(1).await?;
    let test_message = String::from("test_message");
    struct Message(String);

    let msg = test_message.clone();
    let cb = move |c: Arc<Cache>, _, _, Message(text)| {
        let test_message = test_message.clone();
        async move {
            assert_eq!(text, test_message);

            let collaterals = &*c.collateral.read().await;
            let (col, _, _) = &mut *collaterals
                .get(0)
                .ok_or_else(|| eyre!("row = 0 not found in collateral"))?
                .write()
                .await;
            *col = Array1::from(vec![
                70.as_u256_decimal_18(),
                70.as_u256_decimal_6(),
                70.as_u256_decimal_12(),
            ]);

            Ok(())
        }
    };

    let cache = Arc::new(cache);
    let senders = Cache::subscribe(
        cache.clone(),
        1,
        1,
        dummy_data_provider,
        Arc::new(tokens),
        cb,
    )
    .await?;

    senders
        .get(0)
        .ok_or_else(|| eyre!("no senders found"))?
        .send(Message(msg))
        .await?;

    tokio::time::sleep(Duration::from_secs(1)).await;

    let collaterals = &*cache.collateral.read().await;
    let (col, _, _) = &mut *collaterals
        .get(0)
        .ok_or_else(|| eyre!("row = 0 not found in collateral"))?
        .write()
        .await;
    assert_eq!(
        *col,
        Array1::from(vec![
            70.as_u256_decimal_18(),
            70.as_u256_decimal_6(),
            70.as_u256_decimal_12(),
        ])
    );

    Ok(())
}

#[tokio::test]
async fn test_calc_hf() -> eyre::Result<()> {
    // 1 case - hf calculation for 1 user

    let (cache, _) = generate_cache_and_tokens(1).await?;
    let user = cache
        .users
        .iter()
        .next()
        .ok_or_else(|| eyre!("no users found"))?
        .key()
        .clone();

    {
        let (decimals, _) = &*cache.decimals.read().await;

        let (lt, _) = &mut *cache.liquidation_threshold.write().await;
        *lt = Array1::from_vec(vec![0.0, 0.75, 0.8]);

        let (price, _) = &mut *cache.prices.write().await;
        *price = Array1::from_vec(vec![120_000.0, 4000.0, 200.0]);

        let (hf, _) = &mut *cache.health_factors.write().await;
        *hf = Array1::from_vec(vec![0.0]);

        let collaterals = &*cache.collateral.read().await;
        let (col_row, _, _) = &mut *collaterals
            .get(0)
            .ok_or_else(|| eyre!("row = 0 not found in collateral"))?
            .write()
            .await;
        *col_row = Array1::from_vec(vec![
            2.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.liquidity.read().await.0[0].index),
            10.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.liquidity.read().await.0[1].index),
            200.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.liquidity.read().await.0[2].index),
        ]);

        let borrowed = &*cache.borrowed.read().await;
        let (bor_row, _, _) = &mut *borrowed
            .get(0)
            .ok_or_else(|| eyre!("row = 0 not found in borrowed"))?
            .write()
            .await;
        *bor_row = Array1::from_vec(vec![
            1.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.variable_borrow.read().await.0[0].index),
            15.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.variable_borrow.read().await.0[1].index),
            U256::default(),
        ]);
    }

    cache
        .calc_hf(Some(&user), Utc::now().timestamp_millis())
        .await?;

    let hf = {
        let (hf, _) = &*cache.health_factors.read().await;
        hf.to_vec()
    };

    assert_eq!(hf, vec![0.34444444444444444]);

    // 2 case - hf calculation for all users

    let (cache, _) = generate_cache_and_tokens(3).await?;

    {
        let (decimals, _) = &*cache.decimals.read().await;

        let (lt, _) = &mut *cache.liquidation_threshold.write().await;
        *lt = Array1::from_vec(vec![0.0, 0.75, 0.8]);

        let (price, _) = &mut *cache.prices.write().await;
        *price = Array1::from_vec(vec![120_000.0, 4000.0, 200.0]);

        let (hf, _) = &mut *cache.health_factors.write().await;
        *hf = Array1::from_vec(vec![0.0; 3]);

        let col = 2
            .as_u256_decimal_18()
            .to_ray(decimals[0])
            .to_scaled(cache.liquidity.read().await.0[0].index);
        let col2 = 10
            .as_u256_decimal_6()
            .to_ray(decimals[1])
            .to_scaled(cache.liquidity.read().await.0[1].index);
        let col3 = 200
            .as_u256_decimal_12()
            .to_ray(decimals[2])
            .to_scaled(cache.liquidity.read().await.0[2].index);

        let collaterals = &mut *cache.collateral.write().await;
        collaterals.push(RwLock::new((Array1::from_vec(vec![col, col2, col3]), 0, 0)));
        collaterals.push(RwLock::new((Array1::from_vec(vec![col, col2, col3]), 0, 0)));
        let (col_row, _, _) = &mut *collaterals
            .get(0)
            .ok_or_else(|| eyre!("row = 0 not found in collateral"))?
            .write()
            .await;
        *col_row = Array1::from_vec(vec![col, col2, col3]);

        let bor = 1
            .as_u256_decimal_18()
            .to_ray(decimals[0])
            .to_scaled(cache.variable_borrow.read().await.0[0].index);
        let bor2 = 15
            .as_u256_decimal_6()
            .to_ray(decimals[1])
            .to_scaled(cache.variable_borrow.read().await.0[1].index);

        let borrowed = &mut *cache.borrowed.write().await;
        borrowed.push(RwLock::new((
            Array1::from_vec(vec![bor, bor2, U256::default()]),
            0,
            0,
        )));
        borrowed.push(RwLock::new((
            Array1::from_vec(vec![bor, bor2, U256::default()]),
            0,
            0,
        )));
        let (bor_row, _, _) = &mut *borrowed
            .get(0)
            .ok_or_else(|| eyre!("row = 0 not found in borrowed"))?
            .write()
            .await;
        *bor_row = Array1::from_vec(vec![bor, bor2, U256::default()]);

        *cache.collateral_matrix.write().await = Array2::from_elem((0, 3), 0.);
        let col_matrix = &mut *cache.collateral_matrix.write().await;
        col_matrix.push_row(
            Array1::from_vec(vec![col.as_f64_ray(), col2.as_f64_ray(), col3.as_f64_ray()]).view(),
        )?;
        col_matrix.push_row(
            Array1::from_vec(vec![col.as_f64_ray(), col2.as_f64_ray(), col3.as_f64_ray()]).view(),
        )?;
        col_matrix.push_row(
            Array1::from_vec(vec![col.as_f64_ray(), col2.as_f64_ray(), col3.as_f64_ray()]).view(),
        )?;

        *cache.borrowed_matrix.write().await = Array2::from_elem((0, 3), 0.);
        let bor_matrix = &mut *cache.borrowed_matrix.write().await;
        bor_matrix
            .push_row(Array1::from_vec(vec![bor.as_f64_ray(), bor2.as_f64_ray(), 0.0]).view())?;
        bor_matrix
            .push_row(Array1::from_vec(vec![bor.as_f64_ray(), bor2.as_f64_ray(), 0.0]).view())?;
        bor_matrix
            .push_row(Array1::from_vec(vec![bor.as_f64_ray(), bor2.as_f64_ray(), 0.0]).view())?;
    }

    cache.calc_hf(None, Utc::now().timestamp_millis()).await?;

    let hf = {
        let (hf, _) = &*cache.health_factors.read().await;
        hf.to_vec()
    };

    assert_eq!(
        hf,
        vec![
            0.34444444444444444,
            0.34444444444444444,
            0.34444444444444444
        ]
    );

    Ok(())
}

struct CreateUserDataProvider;

impl CreateUserDataProvider {
    fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl DataProvider for CreateUserDataProvider {
    async fn get_all_reserves_tokens(&self) -> eyre::Result<Vec<TokenData>> {
        todo!()
    }

    async fn get_source_of_asset(&self, _: &Address) -> eyre::Result<Address> {
        todo!()
    }

    async fn listen_events<F, Fut>(&self, _: F) -> eyre::Result<()>
    where
        F: Fn(IL2PoolEvents) -> Fut + Send + 'static,
        Fut: Future<Output = eyre::Result<()>> + Send,
    {
        todo!()
    }

    async fn listen_price_update<F, Fut>(&self, _: &Address, _: F) -> eyre::Result<()>
    where
        F: Fn(IChainlinkAggregatorEvents) -> Fut + Send + 'static,
        Fut: Future<Output = eyre::Result<()>> + Send,
    {
        todo!()
    }

    async fn get_reserve_configuration_data(&self, _: &Address) -> eyre::Result<f64> {
        todo!()
    }

    async fn get_user_reserve_data(
        &self,
        _: &Address,
        _: &Address,
    ) -> eyre::Result<UserReserveData> {
        Err(eyre::eyre!("mock error"))
    }

    async fn get_reserve_data(&self, _: &Address) -> eyre::Result<ReserveData> {
        Err(eyre::eyre!("mock error"))
    }

    async fn get_decimals(&self, _: &Address) -> eyre::Result<f64> {
        todo!()
    }

    async fn get_price_decimals(&self, _: &Address) -> eyre::Result<f64> {
        todo!()
    }
}

#[tokio::test]
async fn test_create_user() -> eyre::Result<()> {
    // 1 case - user exists

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(1).await?;
    let user = cache
        .users
        .iter()
        .next()
        .ok_or_else(|| eyre!("no users found"))?
        .key()
        .clone();
    let rq_date = Utc::now().timestamp_micros();

    let (sync_tx, _) = channel::<SyncRequest>(1);

    assert_eq!(cache.contains(&user), true);

    let created = create_user(
        rq_date,
        &cache,
        dummy_data_provider,
        &tokens,
        &user,
        &sync_tx,
    )
    .await?;

    assert_eq!(cache.contains(&user), true);
    assert_eq!(created, false);

    // 2 case - create new user

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(0).await?;
    let rq_date = Utc::now().timestamp_micros();
    let user = Address::from_str("0x1Af54C553cefD1792CbFcF41B711834d657ea61D")?;

    let (sync_tx, mut sync_rc) = channel::<SyncRequest>(1);

    let sync_handler = task::spawn(async move {
        let msg = sync_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("sync channel closed"))?;
        assert_eq!(SyncRequest::Both(SyncTarget::Row(0), rq_date), msg);

        Ok::<_, eyre::Error>(())
    });

    let created = create_user(
        rq_date,
        &cache,
        dummy_data_provider.clone(),
        &tokens,
        &user,
        &sync_tx,
    )
    .await?;
    let _ = sync_handler.await?;

    assert_eq!(created, true);

    let UserData {
        reserve_scaled,
        collateral_scaled,
        borrowed_scaled,
        liquidity_indexes,
        liquidity_rates,
        variable_borrow_indexes,
        variable_borrow_rates,
        last_update_timestamps,
        user_settings,
    } = cache
        .get_user_data(dummy_data_provider, &tokens, &user)
        .await?;

    assert_eq!(cache.contains(&user), true);
    assert_eq!(cache.users.len(), 1);

    let (decimals, _) = &*cache.decimals.read().await;

    assert_eq!(
        collateral_scaled,
        vec![
            U256::default(),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.liquidity.read().await.0[1].index),
            U256::default()
        ]
    );
    assert_eq!(
        reserve_scaled,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.liquidity.read().await.0[0].index),
            U256::default(),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.liquidity.read().await.0[2].index),
        ]
    );
    assert_eq!(
        borrowed_scaled,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.variable_borrow.read().await.0[0].index),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.variable_borrow.read().await.0[1].index),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.variable_borrow.read().await.0[2].index),
        ]
    );
    assert_eq!(
        liquidity_indexes,
        vec![1045.as_u256(24), 1035.as_u256(24), 1025.as_u256(24)]
    );
    assert_eq!(
        liquidity_rates,
        vec![45.as_u256(24), 35.as_u256(24), 25.as_u256(24)]
    );
    assert_eq!(
        variable_borrow_indexes,
        vec![105.as_u256(25), 104.as_u256(25), 103.as_u256(25)]
    );
    assert_eq!(
        variable_borrow_rates,
        vec![5.as_u256(25), 4.as_u256(25), 3.as_u256(25)]
    );
    assert_eq!(
        last_update_timestamps,
        vec![U40::from(Utc::now().timestamp()); 3]
    );
    assert_eq!(user_settings.row_num, 0);
    assert_eq!(user_settings.use_as_collateral, bitvec![0, 1, 0]);

    // 3 case - error

    let create_user_data_provider = Arc::new(CreateUserDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(0).await?;

    let err = create_user(
        rq_date,
        &cache,
        create_user_data_provider,
        &tokens,
        &user,
        &sync_tx,
    )
    .await;

    assert_eq!(err.is_err(), true);
    let e = err.unwrap_err();

    assert_eq!(e.to_string(), "mock error");

    Ok(())
}

#[tokio::test]
async fn test_supply() -> eyre::Result<()> {
    // 1 case - create new user

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(0).await?;

    let user = Address::from_str("0x1Af54C553cefD1792CbFcF41B711834d657ea61D")?;
    let rq_date = Utc::now().timestamp_micros();

    let event = Supply {
        reserve: tokens
            .keys()
            .next()
            .ok_or_else(|| eyre!("no keys"))?
            .clone(),
        user: user.clone(),
        onBehalfOf: user.clone(),
        amount: 10.as_u256_decimal_18(),
        referralCode: 0,
    };

    let (sync_tx, mut sync_rc) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    let sync_handler = task::spawn(async move {
        let msg = sync_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("sync channel closed"))?;
        assert_eq!(SyncRequest::Both(SyncTarget::Row(0), rq_date), msg);

        Ok::<_, eyre::Error>(())
    });

    let cache = Arc::new(cache);
    assert_eq!(cache.users.len(), 0);

    supply(
        cache.clone(),
        dummy_data_provider,
        Arc::new(tokens),
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;
    let _ = sync_handler.await?;

    assert_eq!(cache.users.len(), 1);
    assert_eq!(cache.users.contains_key(&user), true);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;
    let (decimals, _) = &*cache.decimals.read().await;

    assert_eq!(
        collateral,
        vec![
            U256::default(),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.liquidity.read().await.0[1].index),
            U256::default()
        ]
    );
    assert_eq!(
        reserve,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.liquidity.read().await.0[0].index),
            U256::default(),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.liquidity.read().await.0[2].index),
        ]
    );
    assert_eq!(
        borrowed,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.variable_borrow.read().await.0[0].index),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.variable_borrow.read().await.0[1].index),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.variable_borrow.read().await.0[2].index),
        ]
    );

    // 2 case - collateral new event

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(1).await?;
    let cache = Arc::new(cache);
    let user = cache
        .users
        .iter()
        .next()
        .ok_or_else(|| eyre::eyre!("no users"))?
        .key()
        .clone();
    let token = Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?;

    let rq_date = Utc::now().timestamp_micros();

    let event = Supply {
        reserve: token.clone(),
        user: user.clone(),
        onBehalfOf: user.clone(),
        amount: 20.as_u256_decimal_18(),
        referralCode: 0,
    };

    let (sync_tx, mut sync_rc) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    let sync_handler = task::spawn(async move {
        let msg = sync_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("sync channel closed"))?;
        assert_eq!(
            SyncRequest::Collateral(SyncTarget::Cell(0, 0), rq_date),
            msg
        );

        Ok::<_, eyre::Error>(())
    });

    assert_eq!(cache.users.len(), 1);

    supply(
        cache.clone(),
        dummy_data_provider,
        Arc::new(tokens),
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;
    let _ = sync_handler.await?;

    assert_eq!(cache.users.len(), 1);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

    assert_eq!(
        collateral,
        vec![
            20.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.liquidity.read().await.0[0].index),
            U256::default(),
            U256::default()
        ]
    );
    assert_eq!(reserve, vec![U256::default(); 3]);
    assert_eq!(borrowed, vec![U256::default(); 3]);

    // 3 case - reserve new event

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(1).await?;
    let cache = Arc::new(cache);
    let user = cache
        .users
        .iter()
        .next()
        .ok_or_else(|| eyre::eyre!("no users"))?
        .key()
        .clone();
    let token = Address::from_str("0x1Af54C113cefD1792CbFcF41B711834d657ea61D")?;

    let rq_date = Utc::now().timestamp_micros();

    let event = Supply {
        reserve: token.clone(),
        user: user.clone(),
        onBehalfOf: user.clone(),
        amount: 30.as_u256_decimal_6(),
        referralCode: 0,
    };

    let (sync_tx, _) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    assert_eq!(cache.users.len(), 1);

    supply(
        cache.clone(),
        dummy_data_provider,
        Arc::new(tokens),
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;

    assert_eq!(cache.users.len(), 1);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

    assert_eq!(collateral, vec![U256::default(); 3]);
    assert_eq!(
        reserve,
        vec![
            U256::default(),
            30.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.liquidity.read().await.0[1].index),
            U256::default()
        ]
    );
    assert_eq!(borrowed, vec![U256::default(); 3]);

    // 4 case - collateral skip event

    let rq_date = Utc::now().timestamp_micros();
    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(1).await?;
    let cache = Arc::new(cache);
    let user = cache
        .users
        .iter()
        .next()
        .ok_or_else(|| eyre::eyre!("no users"))?
        .key()
        .clone();
    let token = Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?;

    let event = Supply {
        reserve: token.clone(),
        user: user.clone(),
        onBehalfOf: user.clone(),
        amount: 40.as_u256_decimal_18(),
        referralCode: 0,
    };

    let (sync_tx, _) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    assert_eq!(cache.users.len(), 1);

    supply(
        cache.clone(),
        dummy_data_provider,
        Arc::new(tokens),
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;

    assert_eq!(cache.users.len(), 1);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

    assert_eq!(collateral, vec![U256::default(); 3]);
    assert_eq!(reserve, vec![U256::default(); 3]);
    assert_eq!(borrowed, vec![U256::default(); 3]);

    // 5 case - reserve skip event

    let rq_date = Utc::now().timestamp_micros();
    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(1).await?;
    let cache = Arc::new(cache);
    let user = cache
        .users
        .iter()
        .next()
        .ok_or_else(|| eyre::eyre!("no users"))?
        .key()
        .clone();
    let token = Address::from_str("0x1Af54C113cefD1792CbFcF41B711834d657ea61D")?;

    let event = Supply {
        reserve: token.clone(),
        user: user.clone(),
        onBehalfOf: user.clone(),
        amount: 50.as_u256_decimal_18(),
        referralCode: 0,
    };

    let (sync_tx, _) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    assert_eq!(cache.users.len(), 1);

    supply(
        cache.clone(),
        dummy_data_provider,
        Arc::new(tokens),
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;

    assert_eq!(cache.users.len(), 1);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

    assert_eq!(collateral, vec![U256::default(); 3]);
    assert_eq!(reserve, vec![U256::default(); 3]);
    assert_eq!(borrowed, vec![U256::default(); 3]);

    // 6 case - sync event

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(10).await?;
    let cache = Arc::new(cache);
    let rq_date = Utc::now().timestamp_micros();
    let user = Address::from_str("0x1Af54C553cefD1792CbFcF41B711834d657ea61D")?;
    let token = Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?;

    let event = Supply {
        reserve: token.clone(),
        user: user.clone(),
        onBehalfOf: user.clone(),
        amount: 40.as_u256_decimal_18(),
        referralCode: 0,
    };

    {
        let collateral = &*cache.collateral.read().await;
        let (_, last_sync, last_modified) = &mut *collateral
            .get(0)
            .ok_or_else(|| eyre!("can't get row = 0 from collateral"))?
            .write()
            .await;
        *last_modified = Utc::now().timestamp_micros();
        *last_sync = Utc::now().timestamp_micros() - 1 - 24 * 60 * 60 * 1000_000;
    }

    let (sync_tx, mut sync_rc) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    let sync_handler = task::spawn(async move {
        let msg = sync_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("sync channel closed"))?;
        assert_eq!(SyncRequest::Both(SyncTarget::Row(0), rq_date), msg);

        Ok::<_, eyre::Error>(())
    });

    assert_eq!(cache.users.len(), 10);

    supply(
        cache.clone(),
        dummy_data_provider,
        Arc::new(tokens),
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;
    let _ = sync_handler.await?;

    assert_eq!(cache.users.len(), 10);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

    assert_eq!(
        collateral,
        vec![
            U256::default(),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.liquidity.read().await.0[1].index),
            U256::default()
        ]
    );
    assert_eq!(
        reserve,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.liquidity.read().await.0[0].index),
            U256::default(),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.liquidity.read().await.0[2].index),
        ]
    );
    assert_eq!(
        borrowed,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.variable_borrow.read().await.0[0].index),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.variable_borrow.read().await.0[1].index),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.variable_borrow.read().await.0[2].index),
        ]
    );

    Ok(())
}

#[tokio::test]
async fn test_setup() -> eyre::Result<()> {
    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (_, t) = generate_cache_and_tokens(0).await?;

    let tokens = setup(dummy_data_provider).await?;

    assert_eq!(tokens.len(), 3);

    for (
        token_address,
        TokenDetails {
            name, price_source, ..
        },
    ) in tokens
    {
        assert_eq!(t.contains_key(&token_address), true);

        let TokenDetails {
            name: n,
            price_source: p_s,
            ..
        } = t
            .get(&token_address)
            .ok_or_else(|| eyre::eyre!("token address not found"))?;

        assert_eq!(name, *n);
        assert_eq!(price_source, *p_s);
    }

    Ok(())
}

#[tokio::test]
async fn test_listen_events() -> eyre::Result<()> {
    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (event_tx, mut event_rc) = channel::<AaveEvents>(1);

    let user = Address::from_str("0x1Af54C553cefD1792CbFcF41B711834d657ea61D")?;
    let supply_event = Supply {
        reserve: Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?,
        user: user.clone(),
        onBehalfOf: user,
        amount: 6.as_u256_decimal_18(),
        referralCode: 0,
    };

    let event_handler = task::spawn(async move {
        let event = event_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("event channel closed"))?;

        let pool_events = {
            if let AaveEvents::IL2PoolEvents(_, _) = event {
                true
            } else {
                false
            }
        };

        assert_eq!(pool_events, true);

        if let AaveEvents::IL2PoolEvents(event, _) = event {
            let supply = {
                if let IL2PoolEvents::Supply(s) = event {
                    assert_eq!(s.reserve, supply_event.reserve);
                    assert_eq!(s.user, supply_event.user);
                    assert_eq!(s.onBehalfOf, supply_event.onBehalfOf);
                    assert_eq!(s.amount, supply_event.amount);

                    true
                } else {
                    false
                }
            };

            assert_eq!(supply, true);
        }

        Ok::<_, eyre::Error>(())
    });

    listen_events(dummy_data_provider, event_tx).await?;
    let _ = event_handler.await?;

    Ok(())
}

#[tokio::test]
async fn test_listen_price_update() -> eyre::Result<()> {
    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (_, tokens) = generate_cache_and_tokens(0).await?;
    let (event_tx, mut event_rc) = channel::<AaveEvents>(1);

    let tokens = Arc::new(tokens);
    let t = tokens.clone();
    let event_handler = task::spawn(async move {
        for _ in 0..3 {
            let event = event_rc
                .recv()
                .await
                .ok_or_else(|| eyre::eyre!("event channel closed"))?;

            let agg_events = {
                if let AaveEvents::IChainlinkAggregatorEvents(_, _, _) = event {
                    true
                } else {
                    false
                }
            };

            assert_eq!(agg_events, true);

            if let AaveEvents::IChainlinkAggregatorEvents(
                IChainlinkAggregatorEvents::AnswerUpdated(au),
                token,
                _,
            ) = event
            {
                let name = t
                    .get(&token)
                    .ok_or_else(|| eyre::eyre!("token not found"))?
                    .name
                    .clone();

                let new_price = match &name[..] {
                    "AAVE" => 161_230_000_000_i128,
                    "USDC" => 261_230_000_000_i128,
                    "DAI" => 361_230_000_000_i128,
                    _ => 561_230_000_000_i128,
                };

                assert_eq!(au.current, alloy_primitives::I256::try_from(new_price)?);
            }
        }

        Ok::<_, eyre::Error>(())
    });

    listen_price_update(dummy_data_provider, &tokens, event_tx).await?;
    let _ = event_handler.await?;

    Ok(())
}

#[tokio::test]
async fn test_liquidation_threshold_update() -> eyre::Result<()> {
    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(1).await?;
    let (hf_tx, mut hf_rc) = channel::<HFRequest>(1);
    let cache = Arc::new(cache);

    {
        let (lt, _) = &mut *cache.liquidation_threshold.write().await;
        *lt = Array1::from_vec(vec![0.9, 0.9, 0.9]);
    }
    let now = Utc::now().timestamp_micros();
    let new_update = now;
    let hf_handler = task::spawn(async move {
        let msg = hf_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("hf channel closed"))?;

        let rq_date = match msg {
            HFRequest::User(_, last_modified) => last_modified,
            HFRequest::Full(last_modified) => last_modified,
        };

        assert!(now < rq_date);
        assert_eq!(HFRequest::Full(rq_date), msg);

        Ok::<_, eyre::Error>(())
    });

    liquidation_threshold_update(
        cache.clone(),
        Arc::new(tokens),
        dummy_data_provider.clone(),
        hf_tx,
    )
    .await?;
    let _ = hf_handler.await?;

    let (lt, last_modified) = &*cache.liquidation_threshold.read().await;
    assert_eq!(*lt, Array1::from_vec(vec![0.78, 0.8, 0.75]));
    assert!(*last_modified > new_update);

    Ok(())
}

#[tokio::test]
async fn test_listen_sync() -> eyre::Result<()> {
    let cache = generate_cache_and_tokens(1)
        .await
        .map(|(cache, _)| Arc::new(cache))?;

    let (decimals, _) = &*cache.decimals.read().await;
    let col1 = 1
        .as_u256_decimal_18()
        .to_ray(decimals[0])
        .to_scaled(cache.liquidity.read().await.0[0].index);
    let col2 = 2
        .as_u256_decimal_6()
        .to_ray(decimals[1])
        .to_scaled(cache.liquidity.read().await.0[1].index);
    let col3 = 3
        .as_u256_decimal_12()
        .to_ray(decimals[2])
        .to_scaled(cache.liquidity.read().await.0[2].index);

    let bor4 = 4
        .as_u256_decimal_18()
        .to_ray(decimals[0])
        .to_scaled(cache.variable_borrow.read().await.0[0].index);
    let bor5 = 5
        .as_u256_decimal_6()
        .to_ray(decimals[1])
        .to_scaled(cache.variable_borrow.read().await.0[1].index);
    let bor6 = 6
        .as_u256_decimal_12()
        .to_ray(decimals[2])
        .to_scaled(cache.variable_borrow.read().await.0[2].index);

    {
        let collaterals = &*cache.collateral.read().await;
        let (col, _, _) = &mut *collaterals
            .get(0)
            .ok_or_else(|| eyre::eyre!("no collaterals"))?
            .write()
            .await;
        *col = Array1::from_vec(vec![col1, col2, col3]);

        let borrowed = &*cache.borrowed.read().await;
        let (bor, _, _) = &mut *borrowed
            .get(0)
            .ok_or_else(|| eyre::eyre!("no borrowed"))?
            .write()
            .await;
        *bor = Array1::from_vec(vec![bor4, bor5, bor6]);
    }

    let senders = listen_sync(cache.clone(), 1, 1).await?;
    let sender = senders.get(0).ok_or_else(|| eyre::eyre!("senders empty"))?;
    let rq_date = Utc::now().timestamp_micros();
    sender
        .send(SyncRequest::Both(SyncTarget::Row(0), rq_date))
        .await?;

    sleep(Duration::from_secs(1)).await;

    let col_matrix = &*cache.collateral_matrix.read().await;
    assert_eq!(
        col_matrix,
        Array2::from_shape_vec(
            (1, 3),
            vec![col1.as_f64_ray(), col2.as_f64_ray(), col3.as_f64_ray()]
        )?
    );
    assert_eq!(col_matrix.nrows(), 1);

    let bor_matrix = &*cache.borrowed_matrix.read().await;
    assert_eq!(
        bor_matrix,
        Array2::from_shape_vec(
            (1, 3),
            vec![bor4.as_f64_ray(), bor5.as_f64_ray(), bor6.as_f64_ray()]
        )?
    );
    assert_eq!(bor_matrix.nrows(), 1);

    Ok(())
}

#[tokio::test]
async fn test_hf_calc() -> eyre::Result<()> {
    let cache = generate_cache_and_tokens(2)
        .await
        .map(|(cache, _)| Arc::new(cache))?;
    let user = Address::from_str("0x1Af54C553cefD1792CbFcF41B711834d657ea61D")?;

    let (decimals, _) = &*cache.decimals.read().await;
    let col1 = 1
        .as_u256_decimal_18()
        .to_ray(decimals[0])
        .to_scaled(cache.liquidity.read().await.0[0].index);
    let col2 = 2
        .as_u256_decimal_6()
        .to_ray(decimals[1])
        .to_scaled(cache.liquidity.read().await.0[1].index);
    let col3 = 3
        .as_u256_decimal_12()
        .to_ray(decimals[2])
        .to_scaled(cache.liquidity.read().await.0[2].index);
    let col4 = 4
        .as_u256_decimal_18()
        .to_ray(decimals[0])
        .to_scaled(cache.liquidity.read().await.0[0].index);
    let col5 = 5
        .as_u256_decimal_6()
        .to_ray(decimals[1])
        .to_scaled(cache.liquidity.read().await.0[1].index);
    let col6 = 6
        .as_u256_decimal_12()
        .to_ray(decimals[2])
        .to_scaled(cache.liquidity.read().await.0[2].index);

    let bor1 = 1
        .as_u256_decimal_18()
        .to_ray(decimals[0])
        .to_scaled(cache.variable_borrow.read().await.0[0].index);
    let bor2 = 2
        .as_u256_decimal_6()
        .to_ray(decimals[1])
        .to_scaled(cache.variable_borrow.read().await.0[1].index);
    let bor3 = 3
        .as_u256_decimal_12()
        .to_ray(decimals[2])
        .to_scaled(cache.variable_borrow.read().await.0[2].index);
    let bor4 = 4
        .as_u256_decimal_18()
        .to_ray(decimals[0])
        .to_scaled(cache.variable_borrow.read().await.0[0].index);
    let bor5 = 5
        .as_u256_decimal_6()
        .to_ray(decimals[1])
        .to_scaled(cache.variable_borrow.read().await.0[1].index);
    let bor6 = 6
        .as_u256_decimal_12()
        .to_ray(decimals[2])
        .to_scaled(cache.variable_borrow.read().await.0[2].index);

    {
        let (prices, _) = &mut *cache.prices.write().await;
        *prices = Array1::from_vec(vec![120_000.0, 4000.0, 200.0]);

        let (lt, _) = &mut *cache.liquidation_threshold.write().await;
        *lt = Array1::from_vec(vec![0.78, 0.8, 0.75]);

        let collaterals = &*cache.collateral.read().await;
        let (col, _, _) = &mut *collaterals
            .get(0)
            .ok_or_else(|| eyre::eyre!("no collaterals"))?
            .write()
            .await;
        *col = Array1::from_vec(vec![col1, col2, col3]);

        let (col, _, _) = &mut *collaterals
            .get(1)
            .ok_or_else(|| eyre::eyre!("no collaterals"))?
            .write()
            .await;
        *col = Array1::from_vec(vec![col4, col5, col6]);

        let borrowed = &*cache.borrowed.read().await;
        let (bor, _, _) = &mut *borrowed
            .get(0)
            .ok_or_else(|| eyre::eyre!("no borrowed"))?
            .write()
            .await;
        *bor = Array1::from_vec(vec![bor4, bor5, bor6]);

        let (bor, _, _) = &mut *borrowed
            .get(1)
            .ok_or_else(|| eyre::eyre!("no borrowed"))?
            .write()
            .await;
        *bor = Array1::from_vec(vec![bor1, bor2, bor3]);
    }

    let senders = listen_hf_calc(cache.clone(), 1, 1).await?;
    let sender = senders.get(0).ok_or_else(|| eyre::eyre!("senders empty"))?;
    let rq_date = Utc::now().timestamp_micros();
    sender.send(HFRequest::User(user.clone(), rq_date)).await?;

    sleep(Duration::from_secs(1)).await;

    {
        let (hf, _) = &*cache.health_factors.read().await;
        assert_eq!(hf, Array1::from_vec(vec![0.20041899441340782, 0.0]));
    }

    {
        let col_matrix = &mut *cache.collateral_matrix.write().await;
        col_matrix.push_row(
            Array1::from_vec(vec![
                col1.as_f64_ray(),
                col2.as_f64_ray(),
                col3.as_f64_ray(),
            ])
            .view(),
        )?;
        col_matrix.push_row(
            Array1::from_vec(vec![
                col4.as_f64_ray(),
                col5.as_f64_ray(),
                col6.as_f64_ray(),
            ])
            .view(),
        )?;

        let bor_matrix = &mut *cache.borrowed_matrix.write().await;
        bor_matrix.push_row(
            Array1::from_vec(vec![
                bor4.as_f64_ray(),
                bor5.as_f64_ray(),
                bor6.as_f64_ray(),
            ])
            .view(),
        )?;
        bor_matrix.push_row(
            Array1::from_vec(vec![
                bor1.as_f64_ray(),
                bor2.as_f64_ray(),
                bor3.as_f64_ray(),
            ])
            .view(),
        )?;
    }

    sender.send(HFRequest::Full(rq_date)).await?;

    sleep(Duration::from_secs(1)).await;

    let (hf, _) = &*cache.health_factors.read().await;
    assert_eq!(
        hf,
        Array1::from_vec(vec![0.20041899441340782, 3.042768273716952])
    );

    Ok(())
}

#[tokio::test]
async fn test_answer_updated() -> eyre::Result<()> {
    let (cache, tokens) = generate_cache_and_tokens(1)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;
    let dummy_data_provider = Arc::new(DummyDataProvider::new());

    let token = Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?;

    let event = AnswerUpdated {
        current: alloy_primitives::I256::try_from(999_001_612_300_000_000_000_000_000_i128)?,
        roundId: U256::from(0),
        timestamp: U256::from(Utc::now().timestamp()),
    };
    let rq_date = Utc::now().timestamp_micros();

    let (hf_tx, mut hf_rc) = channel::<HFRequest>(1);

    let hf_handler = task::spawn(async move {
        let msg = hf_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("hf channel closed"))?;
        assert_eq!(HFRequest::Full(rq_date), msg);

        Ok::<_, eyre::Error>(())
    });

    answer_updated(
        cache.clone(),
        dummy_data_provider,
        tokens.clone(),
        (event, Token(token.clone()), hf_tx, RqDate(rq_date)),
    )
    .await?;
    let _ = hf_handler.await?;

    let idx = tokens
        .get(&token)
        .ok_or_else(|| eyre::eyre!("no token = {:?}", token))?
        .order;

    let (prices, last_modified) = &*cache.prices.read().await;

    assert_eq!(prices[idx] as f32, 999_001_612.30);
    assert_eq!(*last_modified, rq_date);

    Ok(())
}

#[tokio::test]
async fn test_withdraw() -> eyre::Result<()> {
    // 1 case - create new user

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(0)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;

    let user = Address::from_str("0x1Af54C553cefD1792CbFcF41B711834d657ea61D")?;
    let rq_date = Utc::now().timestamp_micros();

    let event = Withdraw {
        reserve: tokens
            .keys()
            .next()
            .ok_or_else(|| eyre!("no keys"))?
            .clone(),
        user: user.clone(),
        to: user.clone(),
        amount: 10.as_u256_decimal_18(),
    };

    let (sync_tx, mut sync_rc) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    let sync_handler = task::spawn(async move {
        let msg = sync_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("sync channel closed"))?;
        assert_eq!(SyncRequest::Both(SyncTarget::Row(0), rq_date), msg);

        Ok::<_, eyre::Error>(())
    });

    assert_eq!(cache.users.len(), 0);

    withdraw(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;
    let _ = sync_handler.await?;

    assert_eq!(cache.users.len(), 1);
    assert_eq!(cache.users.contains_key(&user), true);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;
    let (decimals, _) = &*cache.decimals.read().await;

    assert_eq!(
        collateral,
        vec![
            U256::default(),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.liquidity.read().await.0[1].index),
            U256::default()
        ]
    );
    assert_eq!(
        reserve,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.liquidity.read().await.0[0].index),
            U256::default(),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.liquidity.read().await.0[2].index),
        ]
    );
    assert_eq!(
        borrowed,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.variable_borrow.read().await.0[0].index),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.variable_borrow.read().await.0[1].index),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.variable_borrow.read().await.0[2].index),
        ]
    );

    // 2 case - collateral new event

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(1)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;

    {
        let collateral = &*cache.collateral.read().await;
        let (row, _, _) = &mut *collateral
            .get(0)
            .ok_or_else(|| eyre!("can't get row = 0 from collateral"))?
            .write()
            .await;
        *row = Array1::from_vec(vec![
            100.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.liquidity.read().await.0[0].index),
            U256::default(),
            U256::default(),
        ]);
    }

    let user = cache
        .users
        .iter()
        .next()
        .ok_or_else(|| eyre::eyre!("no users"))?
        .key()
        .clone();
    let token = Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?;

    let rq_date = Utc::now().timestamp_micros();

    let event = Withdraw {
        reserve: token.clone(),
        user: user.clone(),
        to: user.clone(),
        amount: 20.as_u256_decimal_18(),
    };

    let (sync_tx, mut sync_rc) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    let sync_handler = task::spawn(async move {
        let msg = sync_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("sync channel closed"))?;
        assert_eq!(
            SyncRequest::Collateral(SyncTarget::Cell(0, 0), rq_date),
            msg
        );

        Ok::<_, eyre::Error>(())
    });

    assert_eq!(cache.users.len(), 1);

    withdraw(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;
    let _ = sync_handler.await?;

    assert_eq!(cache.users.len(), 1);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

    assert_eq!(
        collateral,
        vec![
            80.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.liquidity.read().await.0[0].index),
            U256::default(),
            U256::default()
        ]
    );
    assert_eq!(reserve, vec![U256::default(); 3]);
    assert_eq!(borrowed, vec![U256::default(); 3]);

    // 3 case - reserve new event

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(1)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;

    {
        let reserve = &*cache.reserve.read().await;
        let (res, _, _) = &mut *reserve
            .get(0)
            .ok_or_else(|| eyre!("can't get row = 0 from collateral"))?
            .write()
            .await;
        *res = Array1::from_vec(vec![
            U256::default(),
            100.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.liquidity.read().await.0[1].index),
            U256::default(),
        ]);
    }

    let user = cache
        .users
        .iter()
        .next()
        .ok_or_else(|| eyre::eyre!("no users"))?
        .key()
        .clone();
    let token = Address::from_str("0x1Af54C113cefD1792CbFcF41B711834d657ea61D")?;

    let rq_date = Utc::now().timestamp_micros();

    let event = Withdraw {
        reserve: token.clone(),
        user: user.clone(),
        to: user.clone(),
        amount: 30.as_u256_decimal_6(),
    };

    let (sync_tx, _) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    assert_eq!(cache.users.len(), 1);

    withdraw(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;

    assert_eq!(cache.users.len(), 1);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

    assert_eq!(collateral, vec![U256::default(); 3]);
    assert_eq!(
        reserve,
        vec![
            U256::default(),
            70.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.liquidity.read().await.0[1].index),
            U256::default()
        ]
    );
    assert_eq!(borrowed, vec![U256::default(); 3]);

    // 4 case - collateral skip event

    let rq_date = Utc::now().timestamp_micros();
    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(1)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;
    let user = cache
        .users
        .iter()
        .next()
        .ok_or_else(|| eyre::eyre!("no users"))?
        .key()
        .clone();
    let token = Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?;

    let event = Withdraw {
        reserve: token.clone(),
        user: user.clone(),
        to: user.clone(),
        amount: 40.as_u256_decimal_18(),
    };

    let (sync_tx, _) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    assert_eq!(cache.users.len(), 1);

    withdraw(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;

    assert_eq!(cache.users.len(), 1);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

    assert_eq!(collateral, vec![U256::default(); 3]);
    assert_eq!(reserve, vec![U256::default(); 3]);
    assert_eq!(borrowed, vec![U256::default(); 3]);

    // 5 case - reserve skip event

    let rq_date = Utc::now().timestamp_micros();
    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(1)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;
    let user = cache
        .users
        .iter()
        .next()
        .ok_or_else(|| eyre::eyre!("no users"))?
        .key()
        .clone();
    let token = Address::from_str("0x1Af54C113cefD1792CbFcF41B711834d657ea61D")?;

    let event = Withdraw {
        reserve: token.clone(),
        user: user.clone(),
        to: user.clone(),
        amount: 50.as_u256_decimal_18(),
    };

    let (sync_tx, _) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    assert_eq!(cache.users.len(), 1);

    withdraw(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;

    assert_eq!(cache.users.len(), 1);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

    assert_eq!(collateral, vec![U256::default(); 3]);
    assert_eq!(reserve, vec![U256::default(); 3]);
    assert_eq!(borrowed, vec![U256::default(); 3]);

    // 6 case - sync event

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(10)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;
    let rq_date = Utc::now().timestamp_micros();
    let user = Address::from_str("0x1Af54C553cefD1792CbFcF41B711834d657ea61D")?;
    let token = Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?;

    let event = Withdraw {
        reserve: token.clone(),
        user: user.clone(),
        to: user.clone(),
        amount: 40.as_u256_decimal_18(),
    };

    {
        let collateral = &*cache.collateral.read().await;
        let (_, _, last_modified) = &mut *collateral
            .get(0)
            .ok_or_else(|| eyre!("can't get row = 0 from collateral"))?
            .write()
            .await;

        *last_modified = Utc::now().timestamp_micros();
    }

    let (sync_tx, mut sync_rc) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    let sync_handler = task::spawn(async move {
        let msg = sync_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("sync channel closed"))?;
        assert_eq!(SyncRequest::Both(SyncTarget::Row(0), rq_date), msg);

        Ok::<_, eyre::Error>(())
    });

    assert_eq!(cache.users.len(), 10);

    withdraw(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;
    let _ = sync_handler.await?;

    assert_eq!(cache.users.len(), 10);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

    assert_eq!(
        collateral,
        vec![
            U256::default(),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.liquidity.read().await.0[1].index),
            U256::default()
        ]
    );
    assert_eq!(
        reserve,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.liquidity.read().await.0[0].index),
            U256::default(),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.liquidity.read().await.0[2].index),
        ]
    );
    assert_eq!(
        borrowed,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.variable_borrow.read().await.0[0].index),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.variable_borrow.read().await.0[1].index),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.variable_borrow.read().await.0[2].index),
        ]
    );

    Ok(())
}

#[tokio::test]
async fn test_borrow() -> eyre::Result<()> {
    // 1 case - create new user

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(0)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;

    let user = Address::from_str("0x1Af54C553cefD1792CbFcF41B711834d657ea61D")?;
    let rq_date = Utc::now().timestamp_micros();

    let event = Borrow {
        reserve: tokens
            .keys()
            .next()
            .ok_or_else(|| eyre!("no keys"))?
            .clone(),
        user: user.clone(),
        onBehalfOf: user,
        amount: 10.as_u256_decimal_18(),
        interestRateMode: 2,
        borrowRate: U256::from(5_000_000_000_000_000_000_000_0000u128),
        referralCode: 0,
    };

    let (sync_tx, mut sync_rc) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    let sync_handler = task::spawn(async move {
        let msg = sync_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("sync channel closed"))?;
        assert_eq!(SyncRequest::Both(SyncTarget::Row(0), rq_date), msg);

        Ok::<_, eyre::Error>(())
    });

    assert_eq!(cache.users.len(), 0);

    borrow(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;
    let _ = sync_handler.await?;

    assert_eq!(cache.users.len(), 1);
    assert_eq!(cache.users.contains_key(&user), true);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;
    let (decimals, _) = &*cache.decimals.read().await;

    assert_eq!(
        collateral,
        vec![
            U256::default(),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.liquidity.read().await.0[1].index),
            U256::default()
        ]
    );
    assert_eq!(
        reserve,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.liquidity.read().await.0[0].index),
            U256::default(),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.liquidity.read().await.0[2].index),
        ]
    );
    assert_eq!(
        borrowed,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.variable_borrow.read().await.0[0].index),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.variable_borrow.read().await.0[1].index),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.variable_borrow.read().await.0[2].index),
        ]
    );

    // 2 case - borrowed new event

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(1)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;
    let (decimals, _) = &*cache.decimals.read().await;

    {
        let borrowed = &*cache.borrowed.read().await;
        let (bor, _, _) = &mut *borrowed
            .get(0)
            .ok_or_else(|| eyre!("can't get row = 0 from borrowed"))?
            .write()
            .await;
        *bor = Array1::from_vec(vec![
            100.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.variable_borrow.read().await.0[0].index),
            U256::default(),
            U256::default(),
        ]);
    }

    let user = cache
        .users
        .iter()
        .next()
        .ok_or_else(|| eyre::eyre!("no users"))?
        .key()
        .clone();
    let token = Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?;

    let rq_date = Utc::now().timestamp_micros();

    let event = Borrow {
        reserve: token.clone(),
        user: user.clone(),
        onBehalfOf: user,
        amount: 20.as_u256_decimal_18(),
        interestRateMode: 2,
        borrowRate: U256::from(5_000_000_000_000_000_000_000_0000u128),
        referralCode: 0,
    };

    let (sync_tx, mut sync_rc) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    let sync_handler = task::spawn(async move {
        let msg = sync_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("sync channel closed"))?;
        assert_eq!(SyncRequest::Borrowed(SyncTarget::Cell(0, 0), rq_date), msg);

        Ok::<_, eyre::Error>(())
    });

    assert_eq!(cache.users.len(), 1);

    borrow(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;
    let _ = sync_handler.await?;

    assert_eq!(cache.users.len(), 1);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

    assert_eq!(collateral, vec![U256::default(); 3]);
    assert_eq!(reserve, vec![U256::default(); 3]);
    assert_eq!(
        borrowed,
        vec![
            120.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.variable_borrow.read().await.0[0].index),
            U256::default(),
            U256::default()
        ]
    );

    // 3 case - borrowed skip event

    let rq_date = Utc::now().timestamp_micros();
    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(1)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;
    let user = cache
        .users
        .iter()
        .next()
        .ok_or_else(|| eyre::eyre!("no users"))?
        .key()
        .clone();
    let token = Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?;

    let event = Borrow {
        reserve: token.clone(),
        user: user.clone(),
        onBehalfOf: user,
        amount: 30
            .as_u256_decimal_18()
            .to_ray(decimals[0])
            .to_scaled(cache.variable_borrow.read().await.0[0].index),
        interestRateMode: 2,
        borrowRate: U256::from(5_000_000_000_000_000_000_000_0000u128),
        referralCode: 0,
    };

    let (sync_tx, _) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    assert_eq!(cache.users.len(), 1);

    borrow(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;

    assert_eq!(cache.users.len(), 1);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

    assert_eq!(collateral, vec![U256::default(); 3]);
    assert_eq!(reserve, vec![U256::default(); 3]);
    assert_eq!(borrowed, vec![U256::default(); 3]);

    // 4 case - sync event

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(10)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;
    let rq_date = Utc::now().timestamp_micros();
    let user = Address::from_str("0x1Af54C553cefD1792CbFcF41B711834d657ea61D")?;
    let token = Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?;

    let event = Borrow {
        reserve: token.clone(),
        user: user.clone(),
        onBehalfOf: user,
        amount: 40
            .as_u256_decimal_18()
            .to_ray(decimals[0])
            .to_scaled(cache.variable_borrow.read().await.0[0].index),
        interestRateMode: 2,
        borrowRate: U256::from(5_000_000_000_000_000_000_000_0000u128),
        referralCode: 0,
    };

    {
        let borrowed = &*cache.borrowed.read().await;
        let (_, _, last_modified) = &mut *borrowed
            .get(0)
            .ok_or_else(|| eyre!("can't get row = 0 from borrowed"))?
            .write()
            .await;
        *last_modified = Utc::now().timestamp_micros();
    }

    let (sync_tx, mut sync_rc) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    let sync_handler = task::spawn(async move {
        let msg = sync_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("sync channel closed"))?;
        assert_eq!(SyncRequest::Both(SyncTarget::Row(0), rq_date), msg);

        Ok::<_, eyre::Error>(())
    });

    assert_eq!(cache.users.len(), 10);

    borrow(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;
    let _ = sync_handler.await?;

    assert_eq!(cache.users.len(), 10);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

    assert_eq!(
        collateral,
        vec![
            U256::default(),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.liquidity.read().await.0[1].index),
            U256::default()
        ]
    );
    assert_eq!(
        reserve,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.liquidity.read().await.0[0].index),
            U256::default(),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.liquidity.read().await.0[2].index)
        ]
    );
    assert_eq!(
        borrowed,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.variable_borrow.read().await.0[0].index),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.variable_borrow.read().await.0[1].index),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.variable_borrow.read().await.0[2].index)
        ]
    );

    Ok(())
}

#[tokio::test]
async fn test_repay() -> eyre::Result<()> {
    // 1 case - create new user

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(0)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;

    let user = Address::from_str("0x1Af54C553cefD1792CbFcF41B711834d657ea61D")?;
    let rq_date = Utc::now().timestamp_micros();

    let event = Repay {
        reserve: tokens
            .keys()
            .next()
            .ok_or_else(|| eyre!("no keys"))?
            .clone(),
        user: user.clone(),
        repayer: user,
        amount: 10.as_u256_decimal_18(),
        useATokens: false,
    };

    let (sync_tx, mut sync_rc) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    let sync_handler = task::spawn(async move {
        let msg = sync_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("sync channel closed"))?;
        assert_eq!(SyncRequest::Both(SyncTarget::Row(0), rq_date), msg);

        Ok::<_, eyre::Error>(())
    });

    assert_eq!(cache.users.len(), 0);

    repay(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;
    let _ = sync_handler.await?;

    assert_eq!(cache.users.len(), 1);
    assert_eq!(cache.users.contains_key(&user), true);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;
    let (decimals, _) = &*cache.decimals.read().await;

    assert_eq!(
        collateral,
        vec![
            U256::default(),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.liquidity.read().await.0[1].index),
            U256::default()
        ]
    );
    assert_eq!(
        reserve,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.liquidity.read().await.0[0].index),
            U256::default(),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.liquidity.read().await.0[2].index),
        ]
    );
    assert_eq!(
        borrowed,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.variable_borrow.read().await.0[0].index),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.variable_borrow.read().await.0[1].index),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.variable_borrow.read().await.0[2].index),
        ]
    );

    // 2 case - repay new event

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(1)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;

    {
        let borrowed = &*cache.borrowed.read().await;
        let (bor, _, _) = &mut *borrowed
            .get(0)
            .ok_or_else(|| eyre!("can't get row = 0 from borrowed"))?
            .write()
            .await;
        *bor = Array1::from_vec(vec![
            100.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.variable_borrow.read().await.0[0].index),
            U256::default(),
            U256::default(),
        ]);
    }

    let user = cache
        .users
        .iter()
        .next()
        .ok_or_else(|| eyre::eyre!("no users"))?
        .key()
        .clone();
    let token = Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?;

    let rq_date = Utc::now().timestamp_micros();

    let event = Repay {
        reserve: token,
        user: user.clone(),
        repayer: user,
        amount: 10.as_u256_decimal_18(),
        useATokens: false,
    };

    let (sync_tx, mut sync_rc) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    let sync_handler = task::spawn(async move {
        let msg = sync_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("sync channel closed"))?;
        assert_eq!(SyncRequest::Borrowed(SyncTarget::Cell(0, 0), rq_date), msg);

        Ok::<_, eyre::Error>(())
    });

    assert_eq!(cache.users.len(), 1);

    repay(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;
    let _ = sync_handler.await?;

    assert_eq!(cache.users.len(), 1);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

    assert_eq!(collateral, vec![U256::default(); 3]);
    assert_eq!(reserve, vec![U256::default(); 3]);
    assert_eq!(
        borrowed,
        vec![
            90.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.variable_borrow.read().await.0[0].index),
            U256::default(),
            U256::default()
        ]
    );

    // 3 case - borrowed skip event

    let rq_date = Utc::now().timestamp_micros();
    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(1)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;
    let user = cache
        .users
        .iter()
        .next()
        .ok_or_else(|| eyre::eyre!("no users"))?
        .key()
        .clone();
    let token = Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?;

    let event = Repay {
        reserve: token,
        user: user.clone(),
        repayer: user,
        amount: 30.as_u256_decimal_18(),
        useATokens: false,
    };

    let (sync_tx, _) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    assert_eq!(cache.users.len(), 1);

    repay(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;

    assert_eq!(cache.users.len(), 1);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

    assert_eq!(collateral, vec![U256::default(); 3]);
    assert_eq!(reserve, vec![U256::default(); 3]);
    assert_eq!(borrowed, vec![U256::default(); 3]);

    // 4 case - sync event

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(10)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;
    let rq_date = Utc::now().timestamp_micros();
    let user = Address::from_str("0x1Af54C553cefD1792CbFcF41B711834d657ea61D")?;
    let token = Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?;

    let event = Repay {
        reserve: token,
        user: user.clone(),
        repayer: user,
        amount: 40.as_u256_decimal_18(),
        useATokens: false,
    };

    {
        let borrowed = &*cache.borrowed.read().await;
        let (_, _, last_modified) = &mut *borrowed
            .get(0)
            .ok_or_else(|| eyre!("can't get row = 0 from borrowed"))?
            .write()
            .await;
        *last_modified = Utc::now().timestamp_micros();
    }

    let (sync_tx, mut sync_rc) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    let sync_handler = task::spawn(async move {
        let msg = sync_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("sync channel closed"))?;
        assert_eq!(SyncRequest::Both(SyncTarget::Row(0), rq_date), msg);

        Ok::<_, eyre::Error>(())
    });

    assert_eq!(cache.users.len(), 10);

    repay(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;
    let _ = sync_handler.await?;

    assert_eq!(cache.users.len(), 10);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

    assert_eq!(
        collateral,
        vec![
            U256::default(),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.liquidity.read().await.0[1].index),
            U256::default()
        ]
    );
    assert_eq!(
        reserve,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.liquidity.read().await.0[0].index),
            U256::default(),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.liquidity.read().await.0[2].index),
        ]
    );
    assert_eq!(
        borrowed,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.variable_borrow.read().await.0[0].index),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.variable_borrow.read().await.0[1].index),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.variable_borrow.read().await.0[2].index),
        ]
    );

    Ok(())
}

#[tokio::test]
async fn test_reserve_used_as_collateral_enabled() -> eyre::Result<()> {
    // 1 case - create new user

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(0)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;

    let user = Address::from_str("0x1Af54C553cefD1792CbFcF41B711834d657ea61D")?;
    let rq_date = Utc::now().timestamp_micros();

    let event = ReserveUsedAsCollateralEnabled {
        reserve: tokens
            .keys()
            .next()
            .ok_or_else(|| eyre!("no keys"))?
            .clone(),
        user,
    };

    let (sync_tx, mut sync_rc) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    let sync_handler = task::spawn(async move {
        let msg = sync_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("sync channel closed"))?;
        assert_eq!(SyncRequest::Both(SyncTarget::Row(0), rq_date), msg);

        Ok::<_, eyre::Error>(())
    });

    assert_eq!(cache.users.len(), 0);

    reserve_used_as_collateral_enabled(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;
    let _ = sync_handler.await?;

    assert_eq!(cache.users.len(), 1);
    assert_eq!(cache.users.contains_key(&user), true);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;
    let (decimals, _) = &*cache.decimals.read().await;

    assert_eq!(
        collateral,
        vec![
            U256::default(),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.liquidity.read().await.0[1].index),
            U256::default()
        ]
    );
    assert_eq!(
        reserve,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.liquidity.read().await.0[0].index),
            U256::default(),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.liquidity.read().await.0[2].index),
        ]
    );
    assert_eq!(
        borrowed,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.variable_borrow.read().await.0[0].index),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.variable_borrow.read().await.0[1].index),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.variable_borrow.read().await.0[2].index),
        ]
    );

    // 2 case - reserve_used_as_collateral_enabled new event

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(1)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;

    {
        let reserve = &*cache.reserve.read().await;
        let (res, _, _) = &mut *reserve
            .get(0)
            .ok_or_else(|| eyre!("can't get row = 0 from reserve"))?
            .write()
            .await;
        *res = Array1::from_vec(vec![
            U256::default(),
            U256::default(),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.liquidity.read().await.0[2].index),
        ]);
    }

    let user = cache
        .users
        .iter()
        .next()
        .ok_or_else(|| eyre::eyre!("no users"))?
        .key()
        .clone();
    let token = Address::from_str("0x1Af54C113cefD1792CbFcF41B711824d657eb61D")?;

    let rq_date = Utc::now().timestamp_micros();

    let event = ReserveUsedAsCollateralEnabled {
        reserve: token,
        user,
    };

    let (sync_tx, mut sync_rc) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    let sync_handler = task::spawn(async move {
        let msg = sync_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("sync channel closed"))?;
        assert_eq!(
            SyncRequest::Collateral(SyncTarget::Cell(0, 2), rq_date),
            msg
        );

        Ok::<_, eyre::Error>(())
    });

    assert_eq!(cache.users.len(), 1);

    reserve_used_as_collateral_enabled(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;
    let _ = sync_handler.await?;

    assert_eq!(cache.users.len(), 1);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

    assert_eq!(
        collateral,
        vec![
            U256::default(),
            U256::default(),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.liquidity.read().await.0[2].index)
        ]
    );
    assert_eq!(reserve, vec![U256::default(); 3]);
    assert_eq!(borrowed, vec![U256::default(); 3]);

    // 3 case - reserve_used_as_collateral_enabled skip event

    let rq_date = Utc::now().timestamp_micros();
    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(1)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;
    let user = cache
        .users
        .iter()
        .next()
        .ok_or_else(|| eyre::eyre!("no users"))?
        .key()
        .clone();
    let token = Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?;

    let event = ReserveUsedAsCollateralEnabled {
        reserve: token,
        user,
    };

    let (sync_tx, _) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    assert_eq!(cache.users.len(), 1);

    reserve_used_as_collateral_enabled(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;

    assert_eq!(cache.users.len(), 1);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

    assert_eq!(collateral, vec![U256::default(); 3]);
    assert_eq!(reserve, vec![U256::default(); 3]);
    assert_eq!(borrowed, vec![U256::default(); 3]);

    // 4 case - reserve_used_as_collateral_enabled sync event

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(10)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;
    let rq_date = Utc::now().timestamp_micros();
    let user = Address::from_str("0x1Af54C553cefD1792CbFcF41B711834d657ea61D")?;
    let token = Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?;

    let event = ReserveUsedAsCollateralEnabled {
        reserve: token,
        user,
    };

    {
        let reserve = &*cache.reserve.read().await;
        let (_, _, last_modified) = &mut *reserve
            .get(0)
            .ok_or_else(|| eyre!("can't get row = 0 from reserve"))?
            .write()
            .await;
        *last_modified = Utc::now().timestamp_micros();
    }

    let (sync_tx, mut sync_rc) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    let sync_handler = task::spawn(async move {
        let msg = sync_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("sync channel closed"))?;
        assert_eq!(SyncRequest::Both(SyncTarget::Row(0), rq_date), msg);

        Ok::<_, eyre::Error>(())
    });

    assert_eq!(cache.users.len(), 10);

    reserve_used_as_collateral_enabled(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;
    let _ = sync_handler.await?;

    assert_eq!(cache.users.len(), 10);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

    assert_eq!(
        collateral,
        vec![
            U256::default(),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.liquidity.read().await.0[1].index),
            U256::default()
        ]
    );
    assert_eq!(
        reserve,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.liquidity.read().await.0[0].index),
            U256::default(),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.liquidity.read().await.0[2].index),
        ]
    );
    assert_eq!(
        borrowed,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.variable_borrow.read().await.0[0].index),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.variable_borrow.read().await.0[1].index),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.variable_borrow.read().await.0[2].index),
        ]
    );

    Ok(())
}

#[tokio::test]
async fn test_reserve_used_as_collateral_disabled() -> eyre::Result<()> {
    // 1 case - create new user

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(0)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;

    let user = Address::from_str("0x1Af54C553cefD1792CbFcF41B711834d657ea61D")?;
    let rq_date = Utc::now().timestamp_micros();

    let event = ReserveUsedAsCollateralDisabled {
        reserve: tokens
            .keys()
            .next()
            .ok_or_else(|| eyre!("no keys"))?
            .clone(),
        user,
    };

    let (sync_tx, mut sync_rc) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    let sync_handler = task::spawn(async move {
        let msg = sync_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("sync channel closed"))?;
        assert_eq!(SyncRequest::Both(SyncTarget::Row(0), rq_date), msg);

        Ok::<_, eyre::Error>(())
    });

    assert_eq!(cache.users.len(), 0);

    reserve_used_as_collateral_disabled(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;
    let _ = sync_handler.await?;

    assert_eq!(cache.users.len(), 1);
    assert_eq!(cache.users.contains_key(&user), true);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;
    let (decimals, _) = &*cache.decimals.read().await;

    assert_eq!(
        collateral,
        vec![
            U256::default(),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.liquidity.read().await.0[1].index),
            U256::default()
        ]
    );
    assert_eq!(
        reserve,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.liquidity.read().await.0[0].index),
            U256::default(),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.liquidity.read().await.0[2].index),
        ]
    );
    assert_eq!(
        borrowed,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.variable_borrow.read().await.0[0].index),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.variable_borrow.read().await.0[1].index),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.variable_borrow.read().await.0[2].index),
        ]
    );

    // 2 case - reserve_used_as_collateral_disabled new event

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(1)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;

    {
        let collateral = &*cache.collateral.read().await;
        let (col, _, _) = &mut *collateral
            .get(0)
            .ok_or_else(|| eyre!("can't get row = 0 from collateral"))?
            .write()
            .await;
        *col = Array1::from_vec(vec![
            U256::default(),
            U256::default(),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.liquidity.read().await.0[2].index),
        ]);
    }

    let user = cache
        .users
        .iter()
        .next()
        .ok_or_else(|| eyre::eyre!("no users"))?
        .key()
        .clone();
    let token = Address::from_str("0x1Af54C113cefD1792CbFcF41B711824d657eb61D")?;

    let rq_date = Utc::now().timestamp_micros();

    let event = ReserveUsedAsCollateralDisabled {
        reserve: token,
        user,
    };

    let (sync_tx, mut sync_rc) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    let sync_handler = task::spawn(async move {
        let msg = sync_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("sync channel closed"))?;
        assert_eq!(
            SyncRequest::Collateral(SyncTarget::Cell(0, 2), rq_date),
            msg
        );

        Ok::<_, eyre::Error>(())
    });

    assert_eq!(cache.users.len(), 1);

    reserve_used_as_collateral_disabled(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;
    let _ = sync_handler.await?;

    assert_eq!(cache.users.len(), 1);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

    assert_eq!(
        reserve,
        vec![
            U256::default(),
            U256::default(),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.liquidity.read().await.0[2].index)
        ]
    );
    assert_eq!(collateral, vec![U256::default(); 3]);
    assert_eq!(borrowed, vec![U256::default(); 3]);

    // 3 case - reserve_used_as_collateral_disabled skip event

    let rq_date = Utc::now().timestamp_micros();
    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(1)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;
    let user = cache
        .users
        .iter()
        .next()
        .ok_or_else(|| eyre::eyre!("no users"))?
        .key()
        .clone();
    let token = Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?;

    let event = ReserveUsedAsCollateralDisabled {
        reserve: token,
        user,
    };

    let (sync_tx, _) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    assert_eq!(cache.users.len(), 1);

    reserve_used_as_collateral_disabled(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;

    assert_eq!(cache.users.len(), 1);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

    assert_eq!(collateral, vec![U256::default(); 3]);
    assert_eq!(reserve, vec![U256::default(); 3]);
    assert_eq!(borrowed, vec![U256::default(); 3]);

    // 4 case - reserve_used_as_collateral_disabled sync event

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(10)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;
    let rq_date = Utc::now().timestamp_micros();
    let user = Address::from_str("0x1Af54C553cefD1792CbFcF41B711834d657ea61D")?;
    let token = Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?;

    let event = ReserveUsedAsCollateralDisabled {
        reserve: token,
        user,
    };

    {
        let reserve = &*cache.reserve.read().await;
        let (_, _, last_modified) = &mut *reserve
            .get(0)
            .ok_or_else(|| eyre!("can't get row = 0 from reserve"))?
            .write()
            .await;
        *last_modified = Utc::now().timestamp_micros();
    }

    let (sync_tx, mut sync_rc) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    let sync_handler = task::spawn(async move {
        let msg = sync_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("sync channel closed"))?;
        assert_eq!(SyncRequest::Both(SyncTarget::Row(0), rq_date), msg);

        Ok::<_, eyre::Error>(())
    });

    assert_eq!(cache.users.len(), 10);

    reserve_used_as_collateral_disabled(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;
    let _ = sync_handler.await?;

    assert_eq!(cache.users.len(), 10);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

    assert_eq!(
        collateral,
        vec![
            U256::default(),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.liquidity.read().await.0[1].index),
            U256::default()
        ]
    );
    assert_eq!(
        reserve,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.liquidity.read().await.0[0].index),
            U256::default(),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.liquidity.read().await.0[2].index),
        ]
    );
    assert_eq!(
        borrowed,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.variable_borrow.read().await.0[0].index),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.variable_borrow.read().await.0[1].index),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.variable_borrow.read().await.0[2].index),
        ]
    );

    Ok(())
}

#[tokio::test]
async fn test_liquidation_call() -> eyre::Result<()> {
    // 1 case - create new user

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(0)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;

    let user = Address::from_str("0x1Af54C553cefD1792CbFcF41B711834d657ea61D")?;
    let rq_date = Utc::now().timestamp_micros();

    let col_token = Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?;
    let bor_token = Address::from_str("0x1Af54C113cefD1792CbFcF41B711834d657ea61D")?;

    let event = LiquidationCall {
        collateralAsset: col_token,
        debtAsset: bor_token,
        user: user.clone(),
        debtToCover: 10.as_u256_decimal_6(),
        liquidatedCollateralAmount: 10.as_u256_decimal_18(),
        liquidator: user,
        receiveAToken: false,
    };

    let (sync_tx, mut sync_rc) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    let sync_handler = task::spawn(async move {
        let msg = sync_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("sync channel closed"))?;
        assert_eq!(SyncRequest::Both(SyncTarget::Row(0), rq_date), msg);

        Ok::<_, eyre::Error>(())
    });

    assert_eq!(cache.users.len(), 0);

    liquidation_call(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;
    let _ = sync_handler.await?;

    assert_eq!(cache.users.len(), 1);
    assert_eq!(cache.users.contains_key(&user), true);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;
    let (decimals, _) = &*cache.decimals.read().await;

    assert_eq!(
        collateral,
        vec![
            U256::default(),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.liquidity.read().await.0[1].index),
            U256::default()
        ]
    );
    assert_eq!(
        reserve,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.liquidity.read().await.0[0].index),
            U256::default(),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.liquidity.read().await.0[2].index),
        ]
    );
    assert_eq!(
        borrowed,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimals[0])
                .to_scaled(cache.variable_borrow.read().await.0[0].index),
            20.as_u256_decimal_6()
                .to_ray(decimals[1])
                .to_scaled(cache.variable_borrow.read().await.0[1].index),
            30.as_u256_decimal_12()
                .to_ray(decimals[2])
                .to_scaled(cache.variable_borrow.read().await.0[2].index),
        ]
    );

    // 2 case - liquidation_call new event

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(1)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;
    let (decimal, _) = &*cache.decimals.read().await;

    {
        let collateral = &*cache.collateral.read().await;
        let (col, _, _) = &mut *collateral
            .get(0)
            .ok_or_else(|| eyre!("can't get row = 0 from collateral"))?
            .write()
            .await;
        *col = Array1::from_vec(vec![
            30.as_u256_decimal_18()
                .to_ray(decimal[0])
                .to_scaled(cache.liquidity.read().await.0[0].index),
            U256::default(),
            U256::default(),
        ]);

        let borrowed = &*cache.borrowed.read().await;
        let (bor, _, _) = &mut *borrowed
            .get(0)
            .ok_or_else(|| eyre!("can't get row = 0 from borrowed"))?
            .write()
            .await;
        *bor = Array1::from_vec(vec![
            U256::default(),
            100_000
                .as_u256_decimal_6()
                .to_ray(decimal[1])
                .to_scaled(cache.variable_borrow.read().await.0[1].index),
            U256::default(),
        ]);
    }

    let user = cache
        .users
        .iter()
        .next()
        .ok_or_else(|| eyre::eyre!("no users"))?
        .key()
        .clone();

    let col_token = Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?;
    let bor_token = Address::from_str("0x1Af54C113cefD1792CbFcF41B711834d657ea61D")?;

    let rq_date = Utc::now().timestamp_micros();

    let event = LiquidationCall {
        collateralAsset: col_token,
        debtAsset: bor_token,
        user: user.clone(),
        debtToCover: 50_000.as_u256_decimal_6(),
        liquidatedCollateralAmount: 10.as_u256_decimal_18(),
        liquidator: user,
        receiveAToken: false,
    };

    let (sync_tx, mut sync_rc) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    let sync_handler = task::spawn(async move {
        let msg = sync_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("sync channel closed"))?;
        assert_eq!(
            SyncRequest::Collateral(SyncTarget::Cell(0, 0), rq_date),
            msg
        );

        let msg = sync_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("sync channel closed"))?;
        assert_eq!(SyncRequest::Borrowed(SyncTarget::Cell(0, 1), rq_date), msg);

        Ok::<_, eyre::Error>(())
    });

    assert_eq!(cache.users.len(), 1);

    liquidation_call(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;
    let _ = sync_handler.await?;

    assert_eq!(cache.users.len(), 1);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

    assert_eq!(
        reserve,
        vec![U256::default(), U256::default(), U256::default()]
    );
    assert_eq!(
        collateral,
        vec![
            20.as_u256_decimal_18()
                .to_ray(decimal[0])
                .to_scaled(cache.liquidity.read().await.0[0].index),
            U256::default(),
            U256::default()
        ]
    );
    assert_eq!(
        borrowed,
        vec![
            U256::default(),
            50_000
                .as_u256_decimal_6()
                .to_ray(decimal[1])
                .to_scaled(cache.variable_borrow.read().await.0[1].index),
            U256::default()
        ]
    );

    // 3 case - liquidation_call skip event

    let rq_date = Utc::now().timestamp_micros();
    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(1)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;
    let user = cache
        .users
        .iter()
        .next()
        .ok_or_else(|| eyre::eyre!("no users"))?
        .key()
        .clone();

    let col_token = Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?;
    let bor_token = Address::from_str("0x1Af54C113cefD1792CbFcF41B711834d657ea61D")?;

    let event = LiquidationCall {
        collateralAsset: col_token,
        debtAsset: bor_token,
        user: user.clone(),
        debtToCover: 50_000.as_u256_decimal_6(),
        liquidatedCollateralAmount: 10.as_u256_decimal_18(),
        liquidator: user,
        receiveAToken: false,
    };

    let (sync_tx, _) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    assert_eq!(cache.users.len(), 1);

    liquidation_call(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;

    assert_eq!(cache.users.len(), 1);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

    assert_eq!(collateral, vec![U256::default(); 3]);
    assert_eq!(reserve, vec![U256::default(); 3]);
    assert_eq!(borrowed, vec![U256::default(); 3]);

    // 4 case - liquidation_call sync event

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(10)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;
    let rq_date = Utc::now().timestamp_micros();
    let user = Address::from_str("0x1Af54C553cefD1792CbFcF41B711834d657ea61D")?;

    let col_token = Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?;
    let bor_token = Address::from_str("0x1Af54C113cefD1792CbFcF41B711834d657ea61D")?;

    let event = LiquidationCall {
        collateralAsset: col_token,
        debtAsset: bor_token,
        user: user.clone(),
        debtToCover: 50_000.as_u256_decimal_6(),
        liquidatedCollateralAmount: 10.as_u256_decimal_18(),
        liquidator: user,
        receiveAToken: false,
    };

    {
        let borrowed = &*cache.borrowed.read().await;
        let (_, _, last_modified) = &mut *borrowed
            .get(0)
            .ok_or_else(|| eyre!("can't get row = 0 from borrowed"))?
            .write()
            .await;
        *last_modified = Utc::now().timestamp_micros();
    }

    let (sync_tx, mut sync_rc) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    let sync_handler = task::spawn(async move {
        let msg = sync_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("sync channel closed"))?;
        assert_eq!(SyncRequest::Both(SyncTarget::Row(0), rq_date), msg);

        Ok::<_, eyre::Error>(())
    });

    assert_eq!(cache.users.len(), 10);

    liquidation_call(
        cache.clone(),
        dummy_data_provider,
        tokens,
        (event, sync_tx, hf_tx, RqDate(rq_date)),
    )
    .await?;
    let _ = sync_handler.await?;

    assert_eq!(cache.users.len(), 10);

    let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

    assert_eq!(
        collateral,
        vec![
            U256::default(),
            20.as_u256_decimal_6()
                .to_ray(decimal[1])
                .to_scaled(cache.liquidity.read().await.0[1].index),
            U256::default()
        ]
    );
    assert_eq!(
        reserve,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimal[0])
                .to_scaled(cache.liquidity.read().await.0[0].index),
            U256::default(),
            30.as_u256_decimal_12()
                .to_ray(decimal[2])
                .to_scaled(cache.liquidity.read().await.0[2].index),
        ]
    );
    assert_eq!(
        borrowed,
        vec![
            10.as_u256_decimal_18()
                .to_ray(decimal[0])
                .to_scaled(cache.variable_borrow.read().await.0[0].index),
            20.as_u256_decimal_6()
                .to_ray(decimal[1])
                .to_scaled(cache.variable_borrow.read().await.0[1].index),
            30.as_u256_decimal_12()
                .to_ray(decimal[2])
                .to_scaled(cache.variable_borrow.read().await.0[2].index),
        ]
    );

    Ok(())
}

#[tokio::test]
async fn test_u256_to_f64() -> eyre::Result<()> {
    let divider_6 = 10_f64.powi(6);
    let divider_12 = 10_f64.powi(12);
    let divider_18 = 10_f64.powi(18);
    let divider_27 = 10_f64.powi(27);

    let a = U256::from(1000_000_000_000_123456_u128);
    let b = U256::from(1000_000_000_000_123456123456_u128);
    let c = U256::from(1000_000_000_000_123456123456123456_u128);
    let d = U256::from(100_000_000_000_123456123456123456123456123_u128);

    let a2 = a.as_f64(divider_6);
    let b2 = b.as_f64(divider_12);
    let c2 = c.as_f64(divider_18);
    let d2 = d.as_f64(divider_27);

    assert_eq!(a2, a2);
    assert_eq!(b2, b2);
    assert_eq!(c2, c2);
    assert_eq!(d2, d2);

    Ok(())
}

#[tokio::test]
async fn test_reserve_data_updated() -> eyre::Result<()> {
    let (cache, tokens) = generate_cache_and_tokens(1)
        .await
        .map(|(cache, tokens)| (Arc::new(cache), Arc::new(tokens)))?;
    let dummy_data_provider = Arc::new(DummyDataProvider::new());

    let token = Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?;

    let event = ReserveDataUpdated {
        reserve: token,
        liquidityRate: 1045.as_u256(24),
        stableBorrowRate: 105.as_u256(25),
        variableBorrowRate: 105.as_u256(25),
        liquidityIndex: 1045.as_u256(24),
        variableBorrowIndex: 105.as_u256(25),
    };

    let rq_date = Utc::now().timestamp_micros();

    let (hf_tx, mut hf_rc) = channel::<HFRequest>(1);

    let hf_handler = task::spawn(async move {
        let msg = hf_rc
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("hf channel closed"))?;
        assert_eq!(HFRequest::Full(rq_date), msg);

        Ok::<_, eyre::Error>(())
    });

    sleep(Duration::from_secs(5)).await;

    reserve_data_updated(
        cache.clone(),
        dummy_data_provider,
        tokens.clone(),
        (event, hf_tx, RqDate(rq_date)),
    )
    .await?;
    let _ = hf_handler.await?;

    let (liquidity, _) = &*cache.liquidity.read().await;
    let (li1, li2, li3) = (
        U256::from(1045.as_u256(24)),
        U256::from(1035000057434360730593607306_u128),
        U256::from(1025000040628170979198376459_u128),
    );

    assert_eq!(liquidity[0].index, li1);
    assert_eq!(liquidity[0].rate, 1045.as_u256(24));
    assert_ne!(liquidity[0].last_update, rq_date);

    assert_eq!(liquidity[1].index, li2);
    assert_eq!(liquidity[1].rate, 35.as_u256(25));
    assert_ne!(liquidity[1].last_update, rq_date);

    assert_eq!(liquidity[2].index, li3);
    assert_eq!(liquidity[2].rate, 25.as_u256(25));
    assert_ne!(liquidity[2].last_update, rq_date);

    let (liquidity_index, _) = &*cache.liquidity_index.read().await;
    assert_eq!(
        liquidity_index,
        Array1::from_vec(vec![li1.as_f64_ray(), li2.as_f64_ray(), li3.as_f64_ray()])
    );

    let (vb, _) = &*cache.variable_borrow.read().await;
    let (vbi1, vbi2, vbi3) = (
        U256::from(105.as_u256(25)),
        U256::from(1040000065956367326230339929_u128),
        U256::from(1030000048991628614916286150_u128),
    );

    assert_eq!(vb[0].index, vbi1);
    assert_eq!(vb[0].rate, 105.as_u256(25));
    assert_ne!(vb[0].last_update, rq_date);

    assert_eq!(vb[1].index, vbi2);
    assert_eq!(vb[1].rate, 4.as_u256(26));
    assert_ne!(vb[1].last_update, rq_date);

    assert_eq!(vb[2].index, vbi3);
    assert_eq!(vb[2].rate, 3.as_u256(26));
    assert_ne!(vb[2].last_update, rq_date);

    let (vbi, _) = &*cache.variable_borrow_index.read().await;
    assert_eq!(
        vbi,
        Array1::from_vec(vec![
            vbi1.as_f64_ray(),
            vbi2.as_f64_ray(),
            vbi3.as_f64_ray()
        ])
    );

    Ok(())
}

#[tokio::test]
async fn test_ray_ops() -> eyre::Result<()> {
    let decimals = 1_000_000.0;
    let value = U256::from(20) * U256::from(decimals);

    let liquidity_index = U256::from(104) * U256::from(10).pow(U256::from(25));
    let amount = value.to_ray(decimals);
    let amount_scaled = amount.to_scaled(liquidity_index);
    let amount_scaled_expected = U256::from(19_230_769_230_000_000_000_000_000_000_u128);

    let amount_scaled_as_f64 = amount_scaled.as_f64_ray();
    let amount_scaled_expected_as_f64 = amount_scaled_expected.as_f64_ray();

    assert_eq!(
        amount_scaled_as_f64 as f32,
        amount_scaled_expected_as_f64 as f32
    );

    Ok(())
}

#[tokio::test]
async fn test_some_numbers() -> eyre::Result<()> {
    const DECIMALS_18: f64 = 1e18;
    let mut total = 1
        .as_u256_decimal_18()
        .to_ray(DECIMALS_18)
        .to_scaled(1045.as_u256(24));

    total -= 1
        .as_u256_decimal_18()
        .to_ray(DECIMALS_18)
        .to_scaled(1051.as_u256(24));

    total += 10
        .as_u256_decimal_18()
        .to_ray(DECIMALS_18)
        .to_scaled(1054.as_u256(24));

    assert_eq!(total, U256::from(9493129047280486755506052494_u128));

    let mut total = 1
        .as_u256_decimal_18()
        .to_ray(DECIMALS_18)
        .to_scaled(105.as_u256(25));

    total += 10
        .as_u256_decimal_18()
        .to_ray(DECIMALS_18)
        .to_scaled(1057.as_u256(24));

    total -= 10
        .as_u256_decimal_18()
        .to_ray(DECIMALS_18)
        .to_scaled(1058.as_u256(24));

    let dt = U256::from(1);
    let dt_spy = dt.ray_div(U256::from(31_536_000));
    let vbi_new = 106.as_u256(25).ray_mul(
        U256::from(1_000_000_000_000_000_000_000_000_000_u128)
            + U256::from(6.as_u256(26)).ray_mul(dt_spy),
    );

    total -= 1
        .as_u256_decimal_18()
        .to_ray(DECIMALS_18)
        .to_scaled(vbi_new);

    assert_eq!(total, U256::from(17926840264096303005033358_u128));

    const DECIMALS_6: f64 = 1e6;
    let mut total = 2
        .as_u256_decimal_6()
        .to_ray(DECIMALS_6)
        .to_scaled(1035.as_u256(24));

    total += 2
        .as_u256_decimal_6()
        .to_ray(DECIMALS_6)
        .to_scaled(1035.as_u256(24));

    total -= 1
        .as_u256_decimal_6()
        .to_ray(DECIMALS_6)
        .to_scaled(1036.as_u256(24));

    assert_eq!(total, U256::from(2899483334265942961595135509_u128));

    let mut total = 1
        .as_u256_decimal_6()
        .to_ray(DECIMALS_6)
        .to_scaled(104.as_u256(25));

    total += 20
        .as_u256_decimal_6()
        .to_ray(DECIMALS_6)
        .to_scaled(1042.as_u256(24));

    total -= 20
        .as_u256_decimal_6()
        .to_ray(DECIMALS_6)
        .to_scaled(1043.as_u256(24));

    assert_eq!(total, U256::from(979941009923361879460760034_u128));

    const DECIMALS_12: f64 = 1e12;
    let mut total = 3
        .as_u256_decimal_12()
        .to_ray(DECIMALS_12)
        .to_scaled(1025.as_u256(24));

    total += 30
        .as_u256_decimal_12()
        .to_ray(DECIMALS_12)
        .to_scaled(1025.as_u256(24));

    total -= 13
        .as_u256_decimal_12()
        .to_ray(DECIMALS_12)
        .to_scaled(1026.as_u256(24));

    total -= 10
        .as_u256_decimal_12()
        .to_ray(DECIMALS_12)
        .to_scaled(1029.as_u256(24));

    assert_eq!(total, U256::from(9806383665596156754365865996_u128));

    let mut total = 1
        .as_u256_decimal_12()
        .to_ray(DECIMALS_12)
        .to_scaled(103.as_u256(25));

    total += 30
        .as_u256_decimal_12()
        .to_ray(DECIMALS_12)
        .to_scaled(1032.as_u256(24));

    total -= 30
        .as_u256_decimal_12()
        .to_ray(DECIMALS_12)
        .to_scaled(1033.as_u256(24));

    assert_eq!(total, U256::from(999014897193691932320573917_u128));

    Ok(())
}

#[tokio::test]
async fn test_hf_lookup() -> eyre::Result<()> {
    let hf = Array1::from_vec(vec![4.1, 0.9, 0.0, 2.1, 0.99, 1.1, 1.0]);
    let result = hf
        .iter()
        .enumerate()
        .filter_map(|(i, &v)| if v < 1.0 { Some(i) } else { None })
        .collect::<Vec<_>>();

    assert_eq!(result, vec![1, 2, 4]);

    Ok(())
}
