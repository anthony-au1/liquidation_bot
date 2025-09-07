use alloy_primitives::aliases::U40;
use alloy_primitives::{Address, I256, U256, U512};
use async_trait::async_trait;
use bitvec::bitvec;
use bitvec::prelude::Lsb0;
use chrono::Utc;
use eyre::eyre;
use liquidation_bot::arbitrum::arbitrum::IAaveProtocolDataProvider::TokenData;
use liquidation_bot::arbitrum::arbitrum::IChainlinkAggregator::{
    AnswerUpdated, IChainlinkAggregatorEvents,
};
use liquidation_bot::arbitrum::arbitrum::IL2Pool::{
    Borrow, IL2PoolEvents, LiquidationCall, Repay, ReserveDataUpdated,
    ReserveUsedAsCollateralDisabled, ReserveUsedAsCollateralEnabled, Supply, Withdraw,
};
use liquidation_bot::arbitrum::arbitrum::{
    start, Cache, DataProvider, Index, ReserveData, UserReserveData, UserSettings,
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
    fn as_f64(&self, divisor: f64) -> f64;
    fn as_f64_ray(&self) -> f64;
}

trait U256Helper {
    fn as_u256(&self, decimals: usize) -> U256;
    fn as_u256_decimal_18(&self) -> U256;
    fn as_u256_decimal_6(&self) -> U256;
    fn as_u256_decimal_12(&self) -> U256;
    fn as_u256_decimal_27(&self) -> U256;
}

trait RayOperations {
    fn ray_mul(self, b: U256) -> U256;
    fn ray_div(self, b: U256) -> U256;
    fn to_ray(self, decimals: f64) -> U256;
}
trait Scaler {
    fn to_scaled(self, index: U256) -> U256;
    fn to_current(self, index: U256) -> U256;
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

const POW64: [f64; 4] = [
    1.0,                                                          // 2^0
    18446744073709551616.0,                                       // 2^64
    340282366920938463463374607431768211456.0,                    // 2^128
    6277101735386680763835789423207666416102355444464034512896.0, // 2^192
];

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

    #[inline(always)]
    fn as_f64(&self, divisor: f64) -> f64 {
        let limbs = self.into_limbs();

        let mut result = 0.0;
        for i in 0..4 {
            result += limbs[i] as f64 * POW64[i];
        }
        result / divisor
    }

    #[inline(always)]
    fn as_f64_ray(&self) -> f64 {
        self.as_f64(1e27)
    }
}

const RAY: u128 = 1_000_000_000_000_000_000_000_000_000; // 1e27

impl RayOperations for U256 {
    fn ray_mul(self, b: U256) -> U256 {
        // (a * b + RAY/2) / RAY
        let result = (U512::from(self) * U512::from(b) + U512::from(RAY / 2)) / U512::from(RAY);
        U256::from(result)
    }

    fn ray_div(self, b: U256) -> U256 {
        // (a * RAY + b/2) / b
        let b = U512::from(b);
        let half_b = b / U512::from(2u8);
        let result = (U512::from(self) * U512::from(RAY) + half_b) / b;
        U256::from(result)
    }

    fn to_ray(self, decimals: f64) -> U256 {
        self * U256::from(RAY / (decimals as u128))
    }
}

impl Scaler for U256 {
    fn to_scaled(self, index: U256) -> U256 {
        self.ray_div(index) // (self * RAY) / index
    }

    fn to_current(self, index: U256) -> U256 {
        self.ray_mul(index) // (self * index) / RAY
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

    async fn get_reserve_data(&self, token: &Address) -> eyre::Result<ReserveData> {
        let now = Utc::now().timestamp();
        let rd = match token {
            t if *t == Address::from_str(AAVE)? => ReserveData::new(
                45.as_u256(25),
                5.as_u256(26),
                1045.as_u256(24),
                105.as_u256(25),
                U40::from(now),
            ),
            t if *t == Address::from_str(USDC)? => ReserveData::new(
                35.as_u256(25),
                4.as_u256(26),
                1035.as_u256(24),
                104.as_u256(25),
                U40::from(now),
            ),
            t if *t == Address::from_str(DAI)? => ReserveData::new(
                25.as_u256(25),
                3.as_u256(26),
                1025.as_u256(24),
                103.as_u256(25),
                U40::from(now),
            ),
            _ => return Err(eyre!("token = {:?} not found", token)),
        };
        Ok(rd)
    }

    async fn get_decimals(&self, token: &Address) -> eyre::Result<f64> {
        let decimals = match token {
            addr if *addr == Address::from_str(AAVE)? => 10_f64.powf(18_f64),
            addr if *addr == Address::from_str(USDC)? => 10_f64.powf(6_f64),
            addr if *addr == Address::from_str(DAI)? => 10_f64.powf(12_f64),
            _ => return Err(eyre!("decimals for token = {:?} not found", token)),
        };

        Ok(decimals)
    }

    async fn get_price_decimals(&self, price_source: &Address) -> eyre::Result<f64> {
        let decimals = match price_source {
            addr if *addr == Address::from_str(AAVE_PRICE_SOURCE)? => 10_f64.powf(18_f64),
            addr if *addr == Address::from_str(USDC_PRICE_SOURCE)? => 10_f64.powf(6_f64),
            addr if *addr == Address::from_str(DAI_PRICE_SOURCE)? => 10_f64.powf(12_f64),
            _ => {
                return Err(eyre!(
                    "decimals for price source = {:?} not found",
                    price_source
                ));
            }
        };

        Ok(decimals)
    }
}

struct DummyDataProvider {
    shared_data_provider: SharedDataProvider,
    listen_events_call_counter: Mutex<usize>,
    listen_price_update_call_counter: Mutex<usize>,
    listen_price_update_call_counter2: Mutex<usize>,
    listen_price_update_call_counter3: Mutex<usize>,
}

impl DummyDataProvider {
    fn new() -> Self {
        Self {
            shared_data_provider: SharedDataProvider {},
            listen_events_call_counter: Mutex::new(0),
            listen_price_update_call_counter: Mutex::new(0),
            listen_price_update_call_counter2: Mutex::new(0),
            listen_price_update_call_counter3: Mutex::new(0),
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
                callback(IL2PoolEvents::ReserveDataUpdated(ReserveDataUpdated {
                    reserve: Address::from_str(AAVE)?,
                    liquidityRate: 55.as_u256(25),
                    stableBorrowRate: 6.as_u256(26),
                    variableBorrowRate: 6.as_u256(26),
                    liquidityIndex: 105.as_u256(25),
                    variableBorrowIndex: 1055.as_u256(24),
                }))
                .await
            }
            2 => {
                callback(IL2PoolEvents::Supply(Supply {
                    reserve: Address::from_str(AAVE)?,
                    user,
                    onBehalfOf: user,
                    amount: 1.as_u256_decimal_18(),
                    referralCode: 0,
                }))
                .await
            }
            3 => {
                callback(IL2PoolEvents::ReserveDataUpdated(ReserveDataUpdated {
                    reserve: Address::from_str(USDC)?,
                    liquidityRate: 35.as_u256(25),
                    stableBorrowRate: 4.as_u256(26),
                    variableBorrowRate: 4.as_u256(26),
                    liquidityIndex: 1035.as_u256(24),
                    variableBorrowIndex: 104.as_u256(25),
                }))
                .await
            }
            4 => {
                callback(IL2PoolEvents::Supply(Supply {
                    reserve: Address::from_str(USDC)?,
                    user,
                    onBehalfOf: user,
                    amount: 2.as_u256_decimal_6(),
                    referralCode: 0,
                }))
                .await
            }
            5 => {
                callback(IL2PoolEvents::ReserveDataUpdated(ReserveDataUpdated {
                    reserve: Address::from_str(DAI)?,
                    liquidityRate: 25.as_u256(25),
                    stableBorrowRate: 3.as_u256(26),
                    variableBorrowRate: 3.as_u256(26),
                    liquidityIndex: 1025.as_u256(24),
                    variableBorrowIndex: 103.as_u256(25),
                }))
                .await
            }
            6 => {
                callback(IL2PoolEvents::Supply(Supply {
                    reserve: Address::from_str(DAI)?,
                    user,
                    onBehalfOf: user,
                    amount: 30.as_u256_decimal_12(),
                    referralCode: 0,
                }))
                .await
            }
            7 => {
                callback(IL2PoolEvents::ReserveDataUpdated(ReserveDataUpdated {
                    reserve: Address::from_str(AAVE)?,
                    liquidityRate: 55.as_u256(25),
                    stableBorrowRate: 6.as_u256(26),
                    variableBorrowRate: 6.as_u256(26),
                    liquidityIndex: 1051.as_u256(24),
                    variableBorrowIndex: 1056.as_u256(24),
                }))
                .await
            }
            8 => {
                callback(IL2PoolEvents::Withdraw(Withdraw {
                    reserve: Address::from_str(AAVE)?,
                    user,
                    to: user,
                    amount: 1.as_u256_decimal_18(),
                }))
                .await
            }
            9 => {
                callback(IL2PoolEvents::ReserveDataUpdated(ReserveDataUpdated {
                    reserve: Address::from_str(USDC)?,
                    liquidityRate: 35.as_u256(25),
                    stableBorrowRate: 4.as_u256(26),
                    variableBorrowRate: 4.as_u256(26),
                    liquidityIndex: 1036.as_u256(24),
                    variableBorrowIndex: 1041.as_u256(24),
                }))
                .await
            }
            10 => {
                callback(IL2PoolEvents::Withdraw(Withdraw {
                    reserve: Address::from_str(USDC)?,
                    user,
                    to: user,
                    amount: 1.as_u256_decimal_6(),
                }))
                .await
            }
            11 => {
                callback(IL2PoolEvents::ReserveDataUpdated(ReserveDataUpdated {
                    reserve: Address::from_str(DAI)?,
                    liquidityRate: 25.as_u256(25),
                    stableBorrowRate: 3.as_u256(26),
                    variableBorrowRate: 3.as_u256(26),
                    liquidityIndex: 1026.as_u256(24),
                    variableBorrowIndex: 1031.as_u256(24),
                }))
                .await
            }
            12 => {
                callback(IL2PoolEvents::Withdraw(Withdraw {
                    reserve: Address::from_str(DAI)?,
                    user,
                    to: user,
                    amount: 13.as_u256_decimal_12(),
                }))
                .await
            }
            13 => {
                callback(IL2PoolEvents::ReserveDataUpdated(ReserveDataUpdated {
                    reserve: Address::from_str(AAVE)?,
                    liquidityRate: 55.as_u256(25),
                    stableBorrowRate: 6.as_u256(26),
                    variableBorrowRate: 6.as_u256(26),
                    liquidityIndex: 1052.as_u256(24),
                    variableBorrowIndex: 1057.as_u256(24),
                }))
                .await
            }
            14 => {
                callback(IL2PoolEvents::Borrow(Borrow {
                    reserve: Address::from_str(AAVE)?,
                    user: user.clone(),
                    onBehalfOf: user,
                    amount: 10.as_u256_decimal_18(),
                    interestRateMode: 2,
                    borrowRate: U256::from(5_000_000_000_000_000_000_000_0000u128),
                    referralCode: 0,
                }))
                .await
            }
            15 => {
                callback(IL2PoolEvents::ReserveDataUpdated(ReserveDataUpdated {
                    reserve: Address::from_str(USDC)?,
                    liquidityRate: 35.as_u256(25),
                    stableBorrowRate: 4.as_u256(26),
                    variableBorrowRate: 4.as_u256(26),
                    liquidityIndex: 1037.as_u256(24),
                    variableBorrowIndex: 1042.as_u256(24),
                }))
                .await
            }
            16 => {
                callback(IL2PoolEvents::Borrow(Borrow {
                    reserve: Address::from_str(USDC)?,
                    user: user.clone(),
                    onBehalfOf: user,
                    amount: 20.as_u256_decimal_6(),
                    interestRateMode: 2,
                    borrowRate: U256::from(5_000_000_000_000_000_000_000_0000u128),
                    referralCode: 0,
                }))
                .await
            }
            17 => {
                callback(IL2PoolEvents::ReserveDataUpdated(ReserveDataUpdated {
                    reserve: Address::from_str(DAI)?,
                    liquidityRate: 25.as_u256(25),
                    stableBorrowRate: 3.as_u256(26),
                    variableBorrowRate: 3.as_u256(26),
                    liquidityIndex: 1027.as_u256(24),
                    variableBorrowIndex: 1032.as_u256(24),
                }))
                .await
            }
            18 => {
                callback(IL2PoolEvents::Borrow(Borrow {
                    reserve: Address::from_str(DAI)?,
                    user: user.clone(),
                    onBehalfOf: user,
                    amount: 30.as_u256_decimal_12(),
                    interestRateMode: 2,
                    borrowRate: U256::from(5_000_000_000_000_000_000_000_0000u128),
                    referralCode: 0,
                }))
                .await
            }
            19 => {
                callback(IL2PoolEvents::ReserveDataUpdated(ReserveDataUpdated {
                    reserve: Address::from_str(AAVE)?,
                    liquidityRate: 55.as_u256(25),
                    stableBorrowRate: 6.as_u256(26),
                    variableBorrowRate: 6.as_u256(26),
                    liquidityIndex: 1053.as_u256(24),
                    variableBorrowIndex: 1058.as_u256(24),
                }))
                .await
            }
            20 => {
                callback(IL2PoolEvents::Repay(Repay {
                    reserve: Address::from_str(AAVE)?,
                    user: user.clone(),
                    repayer: user,
                    amount: 10.as_u256_decimal_18(),
                    useATokens: false,
                }))
                .await
            }
            21 => {
                callback(IL2PoolEvents::ReserveDataUpdated(ReserveDataUpdated {
                    reserve: Address::from_str(USDC)?,
                    liquidityRate: 35.as_u256(25),
                    stableBorrowRate: 4.as_u256(26),
                    variableBorrowRate: 4.as_u256(26),
                    liquidityIndex: 1038.as_u256(24),
                    variableBorrowIndex: 1043.as_u256(24),
                }))
                .await
            }
            22 => {
                callback(IL2PoolEvents::Repay(Repay {
                    reserve: Address::from_str(USDC)?,
                    user: user.clone(),
                    repayer: user,
                    amount: 20.as_u256_decimal_6(),
                    useATokens: false,
                }))
                .await
            }
            23 => {
                callback(IL2PoolEvents::ReserveDataUpdated(ReserveDataUpdated {
                    reserve: Address::from_str(DAI)?,
                    liquidityRate: 25.as_u256(25),
                    stableBorrowRate: 3.as_u256(26),
                    variableBorrowRate: 3.as_u256(26),
                    liquidityIndex: 1028.as_u256(24),
                    variableBorrowIndex: 1033.as_u256(24),
                }))
                .await
            }
            24 => {
                callback(IL2PoolEvents::Repay(Repay {
                    reserve: Address::from_str(DAI)?,
                    user: user.clone(),
                    repayer: user,
                    amount: 30.as_u256_decimal_12(),
                    useATokens: false,
                }))
                .await
            }
            25 => {
                callback(IL2PoolEvents::ReserveDataUpdated(ReserveDataUpdated {
                    reserve: Address::from_str(AAVE)?,
                    liquidityRate: 55.as_u256(25),
                    stableBorrowRate: 6.as_u256(26),
                    variableBorrowRate: 6.as_u256(26),
                    liquidityIndex: 1054.as_u256(24),
                    variableBorrowIndex: 1059.as_u256(24),
                }))
                .await
            }
            26 => {
                callback(IL2PoolEvents::Supply(Supply {
                    reserve: Address::from_str(AAVE)?,
                    user,
                    onBehalfOf: user,
                    amount: 10.as_u256_decimal_18(),
                    referralCode: 0,
                }))
                .await
            }
            27 => {
                callback(IL2PoolEvents::ReserveUsedAsCollateralEnabled(
                    ReserveUsedAsCollateralEnabled {
                        reserve: Address::from_str(AAVE)?,
                        user,
                    },
                ))
                .await
            }
            28 => {
                callback(IL2PoolEvents::ReserveUsedAsCollateralDisabled(
                    ReserveUsedAsCollateralDisabled {
                        reserve: Address::from_str(AAVE)?,
                        user,
                    },
                ))
                .await
            }
            29 => {
                callback(IL2PoolEvents::ReserveDataUpdated(ReserveDataUpdated {
                    reserve: Address::from_str(AAVE)?,
                    liquidityRate: 55.as_u256(25),
                    stableBorrowRate: 6.as_u256(26),
                    variableBorrowRate: 6.as_u256(26),
                    liquidityIndex: 1055.as_u256(24),
                    variableBorrowIndex: 106.as_u256(25),
                }))
                .await
            }
            30 => {
                callback(IL2PoolEvents::ReserveDataUpdated(ReserveDataUpdated {
                    reserve: Address::from_str(DAI)?,
                    liquidityRate: 25.as_u256(25),
                    stableBorrowRate: 3.as_u256(26),
                    variableBorrowRate: 3.as_u256(26),
                    liquidityIndex: 1029.as_u256(24),
                    variableBorrowIndex: 1034.as_u256(24),
                }))
                .await
            }
            31 => {
                callback(IL2PoolEvents::LiquidationCall(LiquidationCall {
                    collateralAsset: Address::from_str(DAI)?,
                    debtAsset: Address::from_str(AAVE)?,
                    user: user.clone(),
                    debtToCover: 1.as_u256_decimal_18(),
                    liquidatedCollateralAmount: 10.as_u256_decimal_12(),
                    liquidator: user,
                    receiveAToken: false,
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
        sleep(Duration::from_secs(3)).await;

        let current = match price_source {
            ps if *ps == Address::from_str(AAVE_PRICE_SOURCE)? => {
                let count = {
                    let mut count = self.listen_price_update_call_counter.lock().await;
                    *count += 1;
                    *count
                };

                match count {
                    1 => 161_230_000_000_000_000_0000_i128,
                    2 => 131_230_000_000_000_000_0000_i128,
                    _ => 171_230_000_000_000_000_0000_i128,
                }
            }
            ps if *ps == Address::from_str(USDC_PRICE_SOURCE)? => {
                let count = {
                    let mut count = self.listen_price_update_call_counter2.lock().await;
                    *count += 1;
                    *count
                };

                match count {
                    1 => 261_230_000_0_i128,
                    2 => 231_230_000_0_i128,
                    _ => 281_230_000_0_i128,
                }
            }
            ps if *ps == Address::from_str(DAI_PRICE_SOURCE)? => {
                let count = {
                    let mut count = self.listen_price_update_call_counter3.lock().await;
                    *count += 1;
                    *count
                };

                match count {
                    1 => 361_230_000_000_000_0_i128,
                    2 => 311_230_000_000_000_0_i128,
                    _ => 401_230_000_000_000_0_i128,
                }
            }
            _ => return Err(eyre!("price_source = {:?} not found", price_source)),
        };

        let event = AnswerUpdated {
            // 161230000000 / 10^8 = 1612.30 USD
            current: I256::try_from(current)?,
            roundId: U256::default(),
            timestamp: U256::from(Utc::now().timestamp()),
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

    async fn get_reserve_data(&self, token: &Address) -> eyre::Result<ReserveData> {
        self.shared_data_provider.get_reserve_data(token).await
    }

    async fn get_decimals(&self, token: &Address) -> eyre::Result<f64> {
        self.shared_data_provider.get_decimals(token).await
    }

    async fn get_price_decimals(&self, price_source: &Address) -> eyre::Result<f64> {
        self.shared_data_provider
            .get_price_decimals(price_source)
            .await
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

    sleep(Duration::from_secs(40)).await;

    let user = Address::from_str(USER1)?;

    let expected = Cache::default();
    {
        let now = Utc::now().timestamp_micros();

        *expected.decimals.write().await = (
            Array1::from_vec(vec![
                10_f64.powf(18_f64),
                10_f64.powf(6_f64),
                10_f64.powf(12_f64),
            ]),
            now,
        );

        let (decimals, _) = &*expected.decimals.read().await;

        let user_num = &mut *expected.users_num.write().await;
        *user_num = 1;

        let mut use_as_collateral = bitvec![usize, Lsb0; 0; 3];
        use_as_collateral.set(0, false);
        use_as_collateral.set(1, true);
        use_as_collateral.set(2, true);
        expected
            .users
            .insert(user, UserSettings::new(0, use_as_collateral));

        let (lt, last_modified) = &mut *expected.liquidation_threshold.write().await;
        *lt = Array1::from_vec(vec![0.78, 0.8, 0.75]);
        *last_modified = now;

        let (prices, last_modified) = &mut *expected.prices.write().await;
        *prices = Array1::from_vec(vec![1712.30, 2812.30, 4012.30]);
        *last_modified = now;

        *expected.liquidity.write().await = (
            Array1::from_vec(vec![
                Index::new(1045.as_u256(24), 45.as_u256(24), now),
                Index::new(1035.as_u256(24), 35.as_u256(24), now),
                Index::new(1025.as_u256(24), 25.as_u256(24), now),
            ]),
            now,
        );
        *expected.liquidity_index.write().await = (
            Array1::from_vec(vec![
                1045.as_u256(24).as_f64_ray(),
                1035.as_u256(24).as_f64_ray(),
                1025.as_u256(24).as_f64_ray(),
            ]),
            now,
        );
        *expected.variable_borrow.write().await = (
            Array1::from_vec(vec![
                Index::new(105.as_u256(25), 5.as_u256(25), now),
                Index::new(104.as_u256(25), 4.as_u256(25), now),
                Index::new(103.as_u256(25), 3.as_u256(25), now),
            ]),
            now,
        );
        *expected.variable_borrow_index.write().await = (
            Array1::from_vec(vec![
                105.as_u256(25).as_f64_ray(),
                104.as_u256(25).as_f64_ray(),
                103.as_u256(25).as_f64_ray(),
            ]),
            now,
        );

        let mut res1 = 1
            .as_u256_decimal_18()
            .to_ray(decimals[0])
            .to_scaled(1045.as_u256(24));

        res1 -= 1
            .as_u256_decimal_18()
            .to_ray(decimals[0])
            .to_scaled(1051.as_u256(24));

        res1 += 10
            .as_u256_decimal_18()
            .to_ray(decimals[0])
            .to_scaled(1054.as_u256(24));

        let reserves = &mut *expected.reserve.write().await;
        reserves.push(RwLock::new((
            Array1::from_vec(vec![res1, U256::default(), U256::default()]),
            now,
            now,
        )));

        let mut col2 = 2
            .as_u256_decimal_6()
            .to_ray(decimals[1])
            .to_scaled(1035.as_u256(24));

        col2 += 2
            .as_u256_decimal_6()
            .to_ray(decimals[1])
            .to_scaled(1035.as_u256(24));

        col2 -= 1
            .as_u256_decimal_6()
            .to_ray(decimals[1])
            .to_scaled(1036.as_u256(24));

        let mut col3 = 3
            .as_u256_decimal_12()
            .to_ray(decimals[2])
            .to_scaled(1025.as_u256(24));

        col3 += 30
            .as_u256_decimal_12()
            .to_ray(decimals[2])
            .to_scaled(1025.as_u256(24));

        col3 -= 13
            .as_u256_decimal_12()
            .to_ray(decimals[2])
            .to_scaled(1026.as_u256(24));

        col3 -= 10
            .as_u256_decimal_12()
            .to_ray(decimals[2])
            .to_scaled(1029.as_u256(24));

        let collaterals = &mut *expected.collateral.write().await;
        collaterals.push(RwLock::new((
            Array1::from_vec(vec![U256::default(), col2, col3]),
            now,
            now,
        )));

        let col_matrix = &mut *expected.collateral_matrix.write().await;
        *col_matrix =
            Array2::from_shape_vec((1, 3), vec![0.0, col2.as_f64_ray(), col3.as_f64_ray()])?;

        let mut bor1 = 1
            .as_u256_decimal_18()
            .to_ray(decimals[0])
            .to_scaled(105.as_u256(25));

        bor1 += 10
            .as_u256_decimal_18()
            .to_ray(decimals[0])
            .to_scaled(1057.as_u256(24));

        bor1 -= 10
            .as_u256_decimal_18()
            .to_ray(decimals[0])
            .to_scaled(1058.as_u256(24));

        let dt = U256::from(1);
        let dt_spy = dt.ray_div(U256::from(31_536_000));
        let vbi_new = 106.as_u256(25)
            .ray_mul(U256::from(RAY) + U256::from(6.as_u256(26))
                .ray_mul(dt_spy));

        bor1 -= 1
            .as_u256_decimal_18()
            .to_ray(decimals[0])
            .to_scaled(vbi_new);

        let bor2 = 1
            .as_u256_decimal_6()
            .to_ray(decimals[1])
            .to_scaled(expected.variable_borrow.read().await.0[1].index);
        let bor3 = 1
            .as_u256_decimal_12()
            .to_ray(decimals[2])
            .to_scaled(expected.variable_borrow.read().await.0[2].index);

        let borroweds = &mut *expected.borrowed.write().await;
        borroweds.push(RwLock::new((
            Array1::from_vec(vec![bor1, bor2, bor3]),
            now,
            now,
        )));

        let bor_matrix = &mut *expected.borrowed_matrix.write().await;
        *bor_matrix = Array2::from_shape_vec(
            (1, 3),
            vec![bor1.as_f64_ray(), bor2.as_f64_ray(), bor3.as_f64_ray()],
        )?;

        let (hf, last_modified) = &mut *expected.health_factors.write().await;
        *hf = Array1::from_vec(vec![5.3983777272783815]);
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

        let price = price.iter().map(|p| p.clone() as f32).collect::<Vec<f32>>();
        let price_expected = price_expected
            .iter()
            .map(|p| p.clone() as f32)
            .collect::<Vec<f32>>();

        assert_eq!(price, price_expected);
    }

    {
        let reserves = &*cache.reserve.read().await;
        let (res, last_sync, last_modified) = &*reserves
            .get(0)
            .ok_or_else(|| eyre!("can't get row = 0 from reserves"))?
            .write()
            .await;

        let reserves_expected = &*expected.reserve.read().await;
        let (res_expected, last_sync_expected, last_modified_expected) = &*reserves_expected
            .get(0)
            .ok_or_else(|| eyre!("can't get row = 0 from reserves expected"))?
            .write()
            .await;

        assert!(last_sync < last_sync_expected);
        assert!(last_modified < last_modified_expected);
        assert_eq!(res, res_expected);
    }

    {
        let collaterals = &*cache.collateral.read().await;
        let (col, last_sync, last_modified) = &*collaterals
            .get(0)
            .ok_or_else(|| eyre!("can't get row = 0 from collaterals"))?
            .write()
            .await;

        let collaterals_expected = &*expected.collateral.read().await;
        let (col_expected, last_sync_expected, last_modified_expected) = &*collaterals_expected
            .get(0)
            .ok_or_else(|| eyre!("can't get row = 0 from collateral expected"))?
            .write()
            .await;

        assert!(last_sync < last_sync_expected);
        assert!(last_modified < last_modified_expected);
        assert_eq!(col, col_expected);
    }

    {
        let col_matrix = &*cache.collateral_matrix.read().await;
        let col_matrix_expected = &*expected.collateral_matrix.read().await;

        assert_eq!(col_matrix, col_matrix_expected);
    }

    {
        let borrowed = &*cache.borrowed.read().await;
        let (bor, last_sync, last_modified) = &*borrowed
            .get(0)
            .ok_or_else(|| eyre!("can't get row = 0 from borrowed"))?
            .write()
            .await;

        let borrowed_expected = &*expected.borrowed.read().await;
        let (bor_expected, last_sync_expected, last_modified_expected) = &*borrowed_expected
            .get(0)
            .ok_or_else(|| eyre!("can't get row = 0 from borrowed expected"))?
            .write()
            .await;

        assert!(last_sync < last_sync_expected);
        assert!(last_modified < last_modified_expected);
        assert_eq!(bor, bor_expected);
        // assert!(compare_arrays(bor, bor_expected));
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

fn compare_arrays(a: &Array1<U256>, b: &Array1<U256>) -> bool {
    if a.len() != b.len() {
        return false;
    }

    a.iter()
        .zip(b.iter())
        .all(|(a, b)| a.abs_diff(*b) < U256::from(10))
}
