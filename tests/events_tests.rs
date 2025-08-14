use alloy_primitives::{Address, I256, U256};
use async_trait::async_trait;
use bitvec::bitvec;
use bitvec::prelude::Lsb0;
use chrono::Utc;
use eyre::eyre;
use liquidation_bot::arbitrum::arbitrum::IAaveProtocolDataProvider::TokenData;
use liquidation_bot::arbitrum::arbitrum::IChainlinkAggregator::{
    AnswerUpdated, IChainlinkAggregatorEvents,
};
use liquidation_bot::arbitrum::arbitrum::IL2Pool::{IL2PoolEvents, Supply, Withdraw};
use liquidation_bot::arbitrum::arbitrum::{
    Cache, DataProvider, UserReserveData, UserSettings, start,
};
use ndarray::{Array1, Array2};
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio::task;
use tokio::time::sleep;

const AAVE: &str = "0x1Ac54C113cefD1792CbFcF41B711824d657eb61D";
const USDC: &str = "0x1Af54C113cefD1792CbFcF41B711834d657ea61D";
const DAI: &str = "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1";

const AAVE_PRICE_SOURCE: &str = "0xba5DdD1f9d7F570dc94a51479a000E3BCE967196";
const USDC_PRICE_SOURCE: &str = "0x1Af54C113cefD1792CbFcA41B711834d657ea61D";
const DAI_PRICE_SOURCE: &str = "0x1Af54C113cefD1792CbFcF41B711824d657eb61D";

const USER1: &str = "0x1Af54C553cefD1792CbFcF41B711834d657ea61D";

trait F64Helper {
    fn as_f64_decimal_18(&self) -> f64;
    fn as_f64_decimal_6(&self) -> f64;
    fn as_f64_decimal_12(&self) -> f64;
}

trait U256Helper {
    fn as_u256_decimal_18(&self) -> U256;
    fn as_u256_decimal_6(&self) -> U256;
    fn as_u256_decimal_12(&self) -> U256;
}

impl<T> U256Helper for T
where
    T: Copy + TryInto<u128>,
    <T as TryInto<u128>>::Error: Debug,
{
    fn as_u256_decimal_18(&self) -> U256 {
        U256::from((*self).try_into().unwrap()) * U256::from(10).pow(U256::from(18))
    }

    fn as_u256_decimal_6(&self) -> U256 {
        U256::from((*self).try_into().unwrap()) * U256::from(10).pow(U256::from(6))
    }

    fn as_u256_decimal_12(&self) -> U256 {
        U256::from((*self).try_into().unwrap()) * U256::from(10).pow(U256::from(12))
    }
}

impl F64Helper for U256 {
    fn as_f64_decimal_18(&self) -> f64 {
        self.saturating_to::<u128>() as f64 / 10_f64.powf(18_f64)
    }

    fn as_f64_decimal_6(&self) -> f64 {
        self.saturating_to::<u128>() as f64 / 10_f64.powf(6_f64)
    }

    fn as_f64_decimal_12(&self) -> f64 {
        self.saturating_to::<u128>() as f64 / 10_f64.powf(12_f64)
    }
}

struct SharedDataProvider;

#[async_trait]
impl DataProvider for SharedDataProvider {
    async fn get_all_reserves_tokens(&self) -> eyre::Result<Vec<TokenData>> {
        let mut token_data = Vec::with_capacity(3);
        token_data.push(TokenData {
            symbol: String::from("AAVE"),
            tokenAddress: Address::from_str(AAVE)?,
        });
        token_data.push(TokenData {
            symbol: String::from("USDC"),
            tokenAddress: Address::from_str(USDC)?,
        });
        token_data.push(TokenData {
            symbol: String::from("DAI"),
            tokenAddress: Address::from_str(DAI)?,
        });

        Ok(token_data)
    }

    async fn get_source_of_asset(&self, token: &Address) -> eyre::Result<Address> {
        let price_souce = match token {
            t if *t == Address::from_str(AAVE)? => Address::from_str(AAVE_PRICE_SOURCE)?,
            t if *t == Address::from_str(USDC)? => Address::from_str(USDC_PRICE_SOURCE)?,
            t if *t == Address::from_str(DAI)? => Address::from_str(DAI_PRICE_SOURCE)?,
            _ => return Err(eyre!("price source for token = {:?} not found", token)),
        };

        Ok(price_souce)
    }

    async fn listen_events<F, Fut>(&self, _: F) -> eyre::Result<()>
    where
        F: Fn(IL2PoolEvents) -> Fut + Send + 'static,
        Fut: Future<Output = eyre::Result<()>> + Send,
    {
        unimplemented!("listen_events not implemented")
    }

    async fn listen_price_update<F, Fut>(&self, _: &Address, _: F) -> eyre::Result<()>
    where
        F: Fn(IChainlinkAggregatorEvents) -> Fut + Send + 'static,
        Fut: Future<Output = eyre::Result<()>> + Send,
    {
        unimplemented!("listen_price_update not implemented")
    }

    async fn get_reserve_configuration_data(&self, token: &Address) -> eyre::Result<f64> {
        let lt = match token {
            addr if *addr == Address::from_str(AAVE)? => 0.78,
            addr if *addr == Address::from_str(USDC)? => 0.8,
            addr if *addr == Address::from_str(DAI)? => 0.75,
            _ => return Err(eyre!("price source for token = {:?} not found", token)),
        };

        Ok(lt)
    }

    async fn get_user_reserve_data(
        &self,
        token: &Address,
        user: &Address,
    ) -> eyre::Result<UserReserveData> {
        match user {
            u if *u == Address::from_str(USER1)? => {
                let urd = match token {
                    t if *t == Address::from_str(AAVE)? => {
                        UserReserveData::new(1.as_u256_decimal_18(), 1.as_u256_decimal_18(), false)
                    }
                    t if *t == Address::from_str(USDC)? => {
                        UserReserveData::new(2.as_u256_decimal_6(), 1.as_u256_decimal_6(), true)
                    }
                    t if *t == Address::from_str(DAI)? => {
                        UserReserveData::new(3.as_u256_decimal_12(), 1.as_u256_decimal_12(), true)
                    }
                    _ => return Err(eyre!("token = {:?} not found", token)),
                };
                Ok(urd)
            }
            _ => {
                let urd = match token {
                    t if *t == Address::from_str(AAVE)? => {
                        UserReserveData::new(1.as_u256_decimal_18(), 1.as_u256_decimal_18(), false)
                    }
                    t if *t == Address::from_str(USDC)? => {
                        UserReserveData::new(2.as_u256_decimal_6(), 1.as_u256_decimal_6(), true)
                    }
                    t if *t == Address::from_str(DAI)? => {
                        UserReserveData::new(3.as_u256_decimal_12(), 1.as_u256_decimal_12(), true)
                    }
                    _ => return Err(eyre!("token = {:?} not found", token)),
                };
                Ok(urd)
            }
        }
    }

    async fn get_decimal(&self, token: &Address) -> eyre::Result<f64> {
        let decimal = match token {
            addr if *addr == Address::from_str(AAVE)? => 10_f64.powf(18_f64),
            addr if *addr == Address::from_str(USDC)? => 10_f64.powf(6_f64),
            addr if *addr == Address::from_str(DAI)? => 10_f64.powf(12_f64),
            _ => return Err(eyre!("decimal for token = {:?} not found", token)),
        };

        Ok(decimal)
    }
}

struct DummyDataProvider {
    shared_data_provider: SharedDataProvider,
    listen_events_call_counter: Mutex<usize>,
    listen_price_update_call_counter: Mutex<usize>,
}

impl DummyDataProvider {
    fn new() -> Self {
        Self {
            shared_data_provider: SharedDataProvider {},
            listen_events_call_counter: Mutex::new(0),
            listen_price_update_call_counter: Mutex::new(0),
        }
    }
}

#[async_trait]
impl DataProvider for DummyDataProvider {
    async fn get_all_reserves_tokens(&self) -> eyre::Result<Vec<TokenData>> {
        self.shared_data_provider.get_all_reserves_tokens().await
    }

    async fn get_source_of_asset(&self, token: &Address) -> eyre::Result<Address> {
        self.shared_data_provider.get_source_of_asset(token).await
    }

    async fn listen_events<F, Fut>(&self, callback: F) -> eyre::Result<()>
    where
        F: Fn(IL2PoolEvents) -> Fut + Send + 'static,
        Fut: Future<Output = eyre::Result<()>> + Send,
    {
        sleep(Duration::from_secs(1)).await;

        let user = Address::from_str(USER1)?;

        let count = {
            let mut count = self.listen_events_call_counter.lock().await;
            *count += 1;
            *count
        };

        match count {
            1 => {
                callback(IL2PoolEvents::Supply(Supply {
                    reserve: Address::from_str(AAVE)?,
                    user,
                    onBehalfOf: user,
                    amount: 1.as_u256_decimal_18(),
                    referralCode: 0,
                }))
                .await
            }
            2 => {
                callback(IL2PoolEvents::Supply(Supply {
                    reserve: Address::from_str(USDC)?,
                    user,
                    onBehalfOf: user,
                    amount: 2.as_u256_decimal_6(),
                    referralCode: 0,
                }))
                .await
            }
            3 => {
                callback(IL2PoolEvents::Supply(Supply {
                    reserve: Address::from_str(DAI)?,
                    user,
                    onBehalfOf: user,
                    amount: 30.as_u256_decimal_12(),
                    referralCode: 0,
                }))
                .await
            }
            4 => {
                callback(IL2PoolEvents::Withdraw(Withdraw {
                    reserve: Address::from_str(AAVE)?,
                    user,
                    to: user,
                    amount: 1.as_u256_decimal_18(),
                }))
                .await
            }
            5 => {
                callback(IL2PoolEvents::Withdraw(Withdraw {
                    reserve: Address::from_str(USDC)?,
                    user,
                    to: user,
                    amount: 1.as_u256_decimal_6(),
                }))
                .await
            }
            6 => {
                callback(IL2PoolEvents::Withdraw(Withdraw {
                    reserve: Address::from_str(DAI)?,
                    user,
                    to: user,
                    amount: 13.as_u256_decimal_12(),
                }))
                .await
            }
            _ => Err(eyre!("no listen_events events")),
        }
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
        sleep(Duration::from_secs(1)).await;

        let count = {
            let mut count = self.listen_price_update_call_counter.lock().await;
            *count += 1;
            *count
        };

        let current = match price_source {
            ps if *ps == Address::from_str(AAVE_PRICE_SOURCE)? => 161_230_000_000_i128,
            ps if *ps == Address::from_str(USDC_PRICE_SOURCE)? => 261_230_000_000_i128,
            ps if *ps == Address::from_str(DAI_PRICE_SOURCE)? => 361_230_000_000_i128,
            _ => return Err(eyre!("price_source = {:?} not found", price_source)),
        };

        let event = match count {
            _ => AnswerUpdated {
                // 161230000000 / 10^8 = 1612.30 USD
                current: I256::try_from(current)?,
                roundId: U256::default(),
                timestamp: U256::from(Utc::now().timestamp()),
            },
        };

        callback(IChainlinkAggregatorEvents::AnswerUpdated(event)).await
    }

    async fn get_reserve_configuration_data(&self, token: &Address) -> eyre::Result<f64> {
        self.shared_data_provider
            .get_reserve_configuration_data(token)
            .await
    }

    async fn get_user_reserve_data(
        &self,
        token: &Address,
        user: &Address,
    ) -> eyre::Result<UserReserveData> {
        self.shared_data_provider
            .get_user_reserve_data(token, user)
            .await
    }

    async fn get_decimal(&self, token: &Address) -> eyre::Result<f64> {
        self.shared_data_provider.get_decimal(token).await
    }
}

#[tokio::test]
async fn test_events() -> eyre::Result<()> {
    let cache = Arc::new(Cache::default());
    let provider = Arc::new(DummyDataProvider::new());

    let c = cache.clone();
    task::spawn(async move {
        let _ = start(c, provider).await;
    });

    sleep(Duration::from_secs(10)).await;

    let user = Address::from_str(USER1)?;

    let expected = Cache::default();
    {
        let user_num = &mut *expected.users_num.write().await;
        *user_num = 1;

        let mut use_as_collateral = bitvec![usize, Lsb0; 0; 3];
        use_as_collateral.set(0, false);
        use_as_collateral.set(1, true);
        use_as_collateral.set(2, true);
        expected
            .users
            .insert(user, UserSettings::new(0, use_as_collateral));

        let now = Utc::now().timestamp_micros();

        let (lt, last_modified) = &mut *expected.liquidation_threshold.write().await;
        *lt = Array1::from_vec(vec![0.78, 0.8, 0.75]);
        *last_modified = now;

        let (prices, last_modified) = &mut *expected.prices.write().await;
        *prices = Array1::from_vec(vec![1612.30, 2612.30, 3612.30]);
        *last_modified = now;

        let (reserves, last_sync, last_modified) = &mut *expected.reserve.write().await;
        reserves.push(RwLock::new(Array1::from_vec(vec![
            U256::default(),
            U256::default(),
            U256::default(),
        ])));
        (*last_sync, *last_modified) = (now, now);

        let (collaterals, last_sync, last_modified) = &mut *expected.collateral.write().await;
        collaterals.push(RwLock::new(Array1::from_vec(vec![
            U256::default(),
            3.as_u256_decimal_6(),
            20.as_u256_decimal_12(),
        ])));
        (*last_sync, *last_modified) = (now, now);

        let col_matrix = &mut *expected.collateral_matrix.write().await;
        *col_matrix = Array2::from_shape_vec((1, 3), vec![0.0, 3.0, 20.0])?;

        let (borroweds, last_sync, last_modified) = &mut *expected.borrowed.write().await;
        borroweds.push(RwLock::new(Array1::from_vec(vec![
            1.as_u256_decimal_18(),
            1.as_u256_decimal_6(),
            1.as_u256_decimal_12(),
        ])));
        (*last_sync, *last_modified) = (now, now);

        let bor_matrix = &mut *expected.borrowed_matrix.write().await;
        *bor_matrix = Array2::from_shape_vec((1, 3), vec![1.0, 1.0, 1.0])?;

        let (hf, last_modified) = &mut *expected.health_factors.write().await;
        *hf = Array1::from_vec(vec![7.714022126095777]);
        *last_modified = now;
    }

    {
        let user_num = &*cache.users_num.read().await;
        let user_num_expected = &*expected.users_num.read().await;

        assert_eq!(*user_num, *user_num_expected);
    }

    {
        let entry = cache
            .users
            .iter()
            .next()
            .ok_or_else(|| eyre!("no users found"))?;
        let (user, user_details) = (entry.key(), entry.value());

        let entry_expected = expected
            .users
            .iter()
            .next()
            .ok_or_else(|| eyre!("no users found in expected"))?;
        let (user_expected, user_details_expected) = (entry_expected.key(), entry_expected.value());

        assert_eq!(*user, *user_expected);
        assert_eq!(user_details.row_num, user_details_expected.row_num);
        assert_eq!(
            user_details.use_as_collateral,
            user_details_expected.use_as_collateral
        );
    }

    {
        let (lt, last_modified) = &*cache.liquidation_threshold.read().await;
        let (lt_expected, last_modified_expected) = &*expected.liquidation_threshold.read().await;

        assert!(last_modified < last_modified_expected);
        assert_eq!(lt, lt_expected);
    }

    {
        let (price, last_modified) = &*cache.prices.read().await;
        let (price_expected, last_modified_expected) = &*expected.prices.read().await;

        assert!(last_modified < last_modified_expected);
        assert_eq!(price, price_expected);
    }

    {
        let (reserves, last_sync, last_modified) = &*cache.reserve.read().await;
        let (reserves_expected, last_sync_expected, last_modified_expected) =
            &*expected.reserve.read().await;

        assert!(last_sync < last_sync_expected);
        assert!(last_modified < last_modified_expected);

        let reserve = &*reserves
            .get(0)
            .ok_or_else(|| eyre!("no reserves found"))?
            .read()
            .await;
        let reserve_expected = &*reserves_expected
            .get(0)
            .ok_or_else(|| eyre!("no reserves expected found"))?
            .read()
            .await;

        assert_eq!(reserve, reserve_expected);
    }

    {
        let (collaterals, last_sync, last_modified) = &*cache.collateral.read().await;
        let (collaterals_expected, last_sync_expected, last_modified_expected) =
            &*expected.collateral.read().await;

        assert!(last_sync < last_sync_expected);
        assert!(last_modified < last_modified_expected);

        let collateral = &*collaterals
            .get(0)
            .ok_or_else(|| eyre!("no collaterals found"))?
            .read()
            .await;
        let collateral_expected = &*collaterals_expected
            .get(0)
            .ok_or_else(|| eyre!("no collaterals expected found"))?
            .read()
            .await;

        assert_eq!(collateral, collateral_expected);
    }

    {
        let col_matrix = &*cache.collateral_matrix.read().await;
        let col_matrix_expected = &*expected.collateral_matrix.read().await;

        assert_eq!(col_matrix, col_matrix_expected);
    }

    {
        let (borroweds, last_sync, last_modified) = &*cache.borrowed.read().await;
        let (borroweds_expected, last_sync_expected, last_modified_expected) =
            &*expected.borrowed.read().await;

        assert!(last_sync < last_sync_expected);
        assert!(last_modified < last_modified_expected);

        let borrowed = &*borroweds
            .get(0)
            .ok_or_else(|| eyre!("no borroweds found"))?
            .read()
            .await;
        let borrowed_expected = &*borroweds_expected
            .get(0)
            .ok_or_else(|| eyre!("no borroweds expected found"))?
            .read()
            .await;

        assert_eq!(borrowed, borrowed_expected);
    }

    {
        let bor_matrix = &*cache.borrowed_matrix.read().await;
        let bor_matrix_expected = &*expected.borrowed_matrix.read().await;

        assert_eq!(bor_matrix, bor_matrix_expected);
    }

    {
        let (hf, last_modified) = &*cache.health_factors.read().await;
        let (hf_expected, last_modified_expected) = &*expected.health_factors.read().await;

        assert!(last_modified < last_modified_expected);
        assert_eq!(hf, hf_expected);
    }

    Ok(())
}
