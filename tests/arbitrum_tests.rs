#[cfg(test)]
mod arbitrum_tests {
    use alloy::eips::eip7002::SYSTEM_ADDRESS;
    use alloy_primitives::Address;
    use async_trait::async_trait;
    use bitvec::bitvec;
    use bitvec::prelude::Lsb0;
    use chrono::Utc;
    use eyre::eyre;
    use liquidation_bot::arbitrum::arbitrum::IAaveProtocolDataProvider::TokenData;
    use liquidation_bot::arbitrum::arbitrum::IChainlinkAggregator::{
        AnswerUpdated, IChainlinkAggregatorEvents,
    };
    use liquidation_bot::arbitrum::arbitrum::IL2Pool::{IL2PoolEvents, Supply};
    use liquidation_bot::arbitrum::arbitrum::{
        Cache, DataProvider, UserDetails, UserReserveData, UserSettings, start,
    };
    use ndarray::Array1;
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
                addr if *addr == Address::from_str(AAVE)? => 7800.0,
                addr if *addr == Address::from_str(USDC)? => 8000.0,
                addr if *addr == Address::from_str(DAI)? => 7500.0,
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
                            UserReserveData::new(1.0, 0.5, false)
                        }
                        t if *t == Address::from_str(USDC)? => UserReserveData::new(2.0, 1.0, true),
                        t if *t == Address::from_str(DAI)? => UserReserveData::new(3.0, 1.0, true),
                        _ => return Err(eyre!("token = {:?} not found", token)),
                    };
                    Ok(urd)
                }
                _ => {
                    let urd = match token {
                        t if *t == Address::from_str(AAVE)? => {
                            UserReserveData::new(1.0, 0.5, false)
                        }
                        t if *t == Address::from_str(USDC)? => UserReserveData::new(2.0, 1.0, true),
                        t if *t == Address::from_str(DAI)? => UserReserveData::new(3.0, 1.0, true),
                        _ => return Err(eyre!("token = {:?} not found", token)),
                    };
                    Ok(urd)
                }
            }
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

            let event = match count {
                1 => Supply {
                    reserve: Address::from_str(AAVE)?,
                    user: user.clone(),
                    onBehalfOf: user,
                    amount: alloy_primitives::U256::from(0.1),
                    referralCode: 0,
                },
                2 => Supply {
                    reserve: Address::from_str(USDC)?,
                    user: user.clone(),
                    onBehalfOf: user,
                    amount: alloy_primitives::U256::from(2.0),
                    referralCode: 0,
                },
                3 => Supply {
                    reserve: Address::from_str(DAI)?,
                    user: user.clone(),
                    onBehalfOf: user,
                    amount: alloy_primitives::U256::from(30.0),
                    referralCode: 0,
                },
                _ => return Err(eyre!("no listen_events events")),
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
                    current: alloy_primitives::I256::try_from(current)?,
                    roundId: alloy_primitives::U256::from(0),
                    timestamp: alloy_primitives::U256::from(Utc::now().timestamp()),
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
    }

    #[tokio::test]
    async fn test_supply() -> eyre::Result<()> {
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
            cache
                .users
                .insert(user, UserSettings::new(0, use_as_collateral));

            let now = Utc::now().timestamp();

            let (lt, last_modified) = &mut *cache.liquidation_threshold.write().await;
            *lt = Array1::from_vec(vec![7800.0, 8000.0, 7500.0]);
            *last_modified = now;

            let (prices, last_modified) = &mut *cache.prices.write().await;
            *prices = Array1::from_vec(vec![1612.30, 2612.30, 3612.30]);
            *last_modified = now;

            let (reserves, last_sync, last_modified) = &mut *cache.reserve.write().await;
            reserves.push(RwLock::new(Array1::from_vec(vec![1.1, 0.0, 0.0])));
            (*last_sync, *last_modified) = (now, now);

            let (collaterals, last_sync, last_modified) = &mut *cache.collateral.write().await;
            collaterals.push(RwLock::new(Array1::from_vec(vec![0.0, 4.0, 33.0])));
            (*last_sync, *last_modified) = (now, now);

            let (borroweds, last_sync, last_modified) = &mut *cache.borrowed.write().await;
            borroweds.push(RwLock::new(Array1::from_vec(vec![0.5, 1.0, 1.0])));
            (*last_sync, *last_modified) = (now, now);
        }

        // assert_eq!(cache, expected);

        Ok(())
    }
}
