#[cfg(test)]
mod arbitrum_tests {
    use alloy_primitives::Address;
    use async_trait::async_trait;
    use eyre::eyre;
    use liquidation_bot::arbitrum::arbitrum::IAaveProtocolDataProvider::TokenData;
    use liquidation_bot::arbitrum::arbitrum::IChainlinkAggregator::IChainlinkAggregatorEvents;
    use liquidation_bot::arbitrum::arbitrum::IL2Pool::{IL2PoolEvents, Supply};
    use liquidation_bot::arbitrum::arbitrum::{start, AaveDataProvider, DataProvider, UserReserveData};
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;
    use tokio::time::sleep;

    struct SharedDataProvider;

    #[async_trait]
    impl DataProvider for SharedDataProvider {
        async fn get_all_reserves_tokens(&self) -> eyre::Result<Vec<TokenData>> {
            let mut token_data = Vec::with_capacity(3);
            token_data.push(TokenData {
                symbol: String::from("AAVE"),
                tokenAddress: Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?,
            });
            token_data.push(TokenData {
                symbol: String::from("USDC"),
                tokenAddress: Address::from_str("0x1Af54C113cefD1792CbFcF41B711834d657ea61D")?,
            });
            token_data.push(TokenData {
                symbol: String::from("DAI"),
                tokenAddress: Address::from_str("0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1")?,
            });

            Ok(token_data)
        }

        async fn get_source_of_asset(&self, token: &Address) -> eyre::Result<Address> {
            let price_souce = match token {
                t if *t == Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")? => {
                    Address::from_str("0xba5DdD1f9d7F570dc94a51479a000E3BCE967196")?
                }
                t if *t == Address::from_str("0x1Af54C113cefD1792CbFcF41B711834d657ea61D")? => {
                    Address::from_str("0x1Af54C113cefD1792CbFcF41B711834d657ea61D")?
                }
                t if *t == Address::from_str("0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1")? => {
                    Address::from_str("0x1Af54C113cefD1792CbFcF41B711824d657eb61D")?
                }
                _ => return Err(eyre!("price source for token = {:?} not found", token)),
            };

            Ok(price_souce)
        }

        async fn listen_events<F, Fut>(&self, callback: F) -> eyre::Result<()>
        where
            F: Fn(IL2PoolEvents) -> Fut + Send + 'static,
            Fut: Future<Output = eyre::Result<()>> + Send,
        {
            unimplemented!("listen_events not implemented")
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
            unimplemented!("listen_price_update not implemented")
        }

        async fn get_reserve_configuration_data(&self, token: &Address) -> eyre::Result<f64> {
            let price_souce = match token {
                addr if *addr
                    == Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")? =>
                {
                    7800.0
                }
                addr if *addr
                    == Address::from_str("0x1Af54C113cefD1792CbFcF41B711834d657ea61D")? =>
                {
                    8000.0
                }
                addr if *addr
                    == Address::from_str("0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1")? =>
                {
                    7500.0
                }
                _ => return Err(eyre!("price source for token = {:?} not found", token)),
            };

            Ok(price_souce)
        }

        async fn get_user_reserve_data(
            &self,
            token: &Address,
            _: &Address,
        ) -> eyre::Result<UserReserveData> {
            let urd = match token {
                t if *t == Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")? => {
                    UserReserveData::new(1.0, 0.5, false)
                }
                t if *t == Address::from_str("0x1Af54C113cefD1792CbFcF41B711834d657ea61D")? => {
                    UserReserveData::new(2.0, 1.0, true)
                }
                t if *t == Address::from_str("0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1")? => {
                    UserReserveData::new(3.0, 1.0, true)
                }
                _ => return Err(eyre!("token = {:?} not found", token)),
            };

            Ok(urd)
        }
    }

    struct DummyDataProvider {
        shared_data_provider: SharedDataProvider,
        listen_events_call_counter: Mutex<usize>,
    }

    impl DummyDataProvider {
        fn new() -> Self {
            Self {
                shared_data_provider: SharedDataProvider {},
                listen_events_call_counter: Mutex::new(0),
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

            let user = Address::from_str("0x1Af54C553cefD1792CbFcF41B711834d657ea61D")?;

            let count = {
                let mut count = self.listen_events_call_counter.lock().await;
                *count += 1;
                *count
            };
            match count {
                1 => {
                    let event = Supply {
                        reserve: Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")?,
                        user: user.clone(),
                        onBehalfOf: user,
                        amount: alloy_primitives::U256::from(0.1),
                        referralCode: 0,
                    };
                    callback(IL2PoolEvents::Supply(event)).await
                },
                2 => {
                    let event = Supply {
                        reserve: Address::from_str("0x1Af54C113cefD1792CbFcF41B711834d657ea61D")?,
                        user: user.clone(),
                        onBehalfOf: user,
                        amount: alloy_primitives::U256::from(2.0),
                        referralCode: 0,
                    };
                    callback(IL2PoolEvents::Supply(event)).await
                },
                3 => {
                    let event = Supply {
                        reserve: Address::from_str("0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1")?,
                        user: user.clone(),
                        onBehalfOf: user,
                        amount: alloy_primitives::U256::from(30.0),
                        referralCode: 0,
                    };
                    callback(IL2PoolEvents::Supply(event)).await
                },
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
            Err(eyre!("no listen_price_update events"))
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
        let provider = Arc::new(DummyDataProvider::new());
        start(provider).await?;
        
        sleep(Duration::from_secs(5)).await;
        
         
        
        Ok(())
    }
}
