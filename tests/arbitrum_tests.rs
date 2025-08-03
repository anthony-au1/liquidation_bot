#[cfg(test)]
mod arbitrum_tests {
    use alloy_primitives::Address;
    use async_trait::async_trait;
    use futures::future::Shared;
    use liquidation_bot::arbitrum::arbitrum::{DataProvider, UserReserveData};
    use liquidation_bot::arbitrum::arbitrum::IAaveProtocolDataProvider::TokenData;
    use liquidation_bot::arbitrum::arbitrum::IChainlinkAggregator::IChainlinkAggregatorEvents;
    use liquidation_bot::arbitrum::arbitrum::IL2Pool::IL2PoolEvents;
    use super::*;

    struct SharedDataProvider;

    #[async_trait]
    impl DataProvider for SharedDataProvider {
        async fn get_all_reserves_tokens(&self) -> eyre::Result<Vec<TokenData>> {
            todo!()
        }

        async fn get_source_of_asset(&self, token: &Address) -> eyre::Result<Address> {
            todo!()
        }

        async fn listen_events<F, Fut>(&self, callback: F) -> eyre::Result<()>
        where
            F: Fn(IL2PoolEvents) -> Fut + Send + 'static,
            Fut: Future<Output=eyre::Result<()>> + Send
        {
            todo!()
        }

        async fn listen_price_update<F, Fut>(&self, price_source: &Address, callback: F) -> eyre::Result<()>
        where
            F: Fn(IChainlinkAggregatorEvents) -> Fut + Send + 'static,
            Fut: Future<Output=eyre::Result<()>> + Send
        {
            todo!()
        }

        async fn get_reserve_configuration_data(&self, token: &Address) -> eyre::Result<f64> {
            todo!()
        }

        async fn get_user_reserve_data(&self, token: &Address, user: &Address) -> eyre::Result<UserReserveData> {
            todo!()
        }
    }


    struct DummyDataProvider(SharedDataProvider);

    impl DummyDataProvider {
        fn new() -> Self {
            Self(SharedDataProvider{})
        }
    }

    #[async_trait]
    impl DataProvider for DummyDataProvider {
        async fn get_all_reserves_tokens(&self) -> eyre::Result<Vec<TokenData>> {
            self.0.get_all_reserves_tokens().await
        }

        async fn get_source_of_asset(&self, token: &Address) -> eyre::Result<Address> {
            self.0.get_source_of_asset(token).await
        }

        async fn listen_events<F, Fut>(&self, callback: F) -> eyre::Result<()>
        where
            F: Fn(IL2PoolEvents) -> Fut + Send + 'static,
            Fut: Future<Output=eyre::Result<()>> + Send
        {
            todo!()
        }

        async fn listen_price_update<F, Fut>(&self, price_source: &Address, callback: F) -> eyre::Result<()>
        where
            F: Fn(IChainlinkAggregatorEvents) -> Fut + Send + 'static,
            Fut: Future<Output=eyre::Result<()>> + Send
        {
            todo!()
        }

        async fn get_reserve_configuration_data(&self, token: &Address) -> eyre::Result<f64> {
            self.0.get_reserve_configuration_data(token).await
        }

        async fn get_user_reserve_data(&self, token: &Address, user: &Address) -> eyre::Result<UserReserveData> {
            self.0.get_user_reserve_data(token, user).await
        }
    }

    #[tokio::test]
    async fn test_me() -> eyre::Result<()> {

        Ok(())
    }
}