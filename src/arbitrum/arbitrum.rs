use crate::arbitrum::arbitrum::IAaveOracle::IAaveOracleInstance;
use crate::arbitrum::arbitrum::IAaveProtocolDataProvider::{
    IAaveProtocolDataProviderInstance, TokenData, getUserReserveDataReturn,
};
use crate::arbitrum::arbitrum::IChainlinkAggregator::IChainlinkAggregatorEvents;
use crate::arbitrum::arbitrum::IL2Pool::IL2PoolEvents;
use crate::arbitrum::events::{
    answer_updated, borrow, liquidation_call, repay, reserve_data_updated,
    reserve_used_as_collateral_disabled, reserve_used_as_collateral_enabled, supply, withdraw,
};
use alloy::primitives::{Address, Log};
use alloy::providers::Provider;
use alloy::rpc::types::Filter;
use alloy::sol;
use alloy::sol_types::SolEventInterface;
use alloy_primitives::{I256, Sign, U256};
use async_trait::async_trait;
use bitvec::prelude::*;
use chrono::Utc;
use dashmap::DashMap;
use eyre::eyre;
use futures::future::try_join_all;
use ndarray::{Array1, Array2, Axis, concatenate};
use std::collections::HashMap;
use std::default::Default;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::{task, time};
use tracing::{debug, error};

pub const WS_URL: &str = "wss://arb-mainnet.g.alchemy.com/v2/9DDcCoPPxnq-aSjQ8k79vxfLvrhBAXjQ";
const L2_POOL_ADDRESS: &str = "0x794a61358D6845594F94dc1DB02A252b5b4814aD";
const AAVE_PROTOCOL_DATA_PROVIDER_ADDRESS: &str = "0x14496b405D62c24F91f04Cda1c69Dc526D56fDE5";
const AAVE_ORACLE_ADDRESS: &str = "0xb56c2F0B653B2e0b10C9b928C8580Ac5Df02C7C7";

sol! {
    #[sol(rpc)]
    interface IL2Pool {
        event Supply(
            address indexed reserve,
            address user,
            address indexed onBehalfOf,
            uint256 amount,
            uint16 indexed referralCode
        );

        event Withdraw(
            address indexed reserve,
            address indexed user,
            address indexed to,
            uint256 amount
        );

        event Borrow(
            address indexed reserve,
            address user,
            address indexed onBehalfOf,
            uint256 amount,
            uint8 interestRateMode,
            uint256 borrowRate,
            uint16 indexed referralCode
        );

        event Repay(
            address indexed reserve,
            address indexed user,
            address indexed repayer,
            uint256 amount,
            bool useATokens
        );

        event ReserveUsedAsCollateralEnabled(
            address indexed reserve,
            address indexed user
        );

        event ReserveUsedAsCollateralDisabled(
            address indexed reserve,
            address indexed user
        );

        event LiquidationCall(
            address indexed collateralAsset,
            address indexed debtAsset,
            address indexed user,
            uint256 debtToCover,
            uint256 liquidatedCollateralAmount,
            address liquidator,
            bool receiveAToken
        );

        event ReserveDataUpdated(
            address indexed reserve,
            uint256 liquidityRate,
            uint256 stableBorrowRate,
            uint256 variableBorrowRate,
            uint256 liquidityIndex,
            uint256 variableBorrowIndex
        );

        function getUserAccountData(
            address user
        )
        external
        view
        returns (
            uint256 totalCollateralBase,
            uint256 totalDebtBase,
            uint256 availableBorrowsBase,
            uint256 currentLiquidationThreshold,
            uint256 ltv,
            uint256 healthFactor
        );
    }

    #[sol(rpc)]
    interface IAaveProtocolDataProvider {
        struct TokenData {
            string symbol;
            address tokenAddress;
        }
        function getAllReservesTokens() external view returns (TokenData[] memory);

        function getUserReserveData(address asset, address user) external view returns (
            uint256 currentATokenBalance,
            uint256 currentStableDebt,
            uint256 currentVariableDebt,
            uint256 principalStableDebt,
            uint256 scaledVariableDebt,
            uint256 stableBorrowRate,
            uint256 liquidityRate,
            uint40 stableRateLastUpdated,
            bool usageAsCollateralEnabled
        );

        function getReserveConfigurationData(address asset) external view returns (
            uint256 decimals,
            uint256 ltv,
            uint256 liquidationThreshold,
            uint256 liquidationBonus,
            uint256 reserveFactor,
            bool usageAsCollateralEnabled,
            bool borrowingEnabled,
            bool stableBorrowRateEnabled,
            bool isActive,
            bool isFrozen
        );
    }

    #[sol(rpc)]
    interface IAaveOracle {
        function getSourceOfAsset(address asset) external view returns (address);
    }

    #[sol(rpc)]
    interface IChainlinkAggregator {
        event AnswerUpdated(int256 indexed current, uint256 indexed roundId, uint256 timestamp);
        function decimals() external view returns (uint8);
    }

    #[sol(rpc)]
    interface IERC20Metadata {
        function decimals() external view returns (uint8);
    }
}

#[async_trait]
pub trait DataProvider: Send + Sync {
    async fn get_all_reserves_tokens(&self) -> eyre::Result<Vec<TokenData>>;
    async fn get_source_of_asset(&self, token: &Address) -> eyre::Result<Address>;
    async fn listen_events<F, Fut>(&self, callback: F) -> eyre::Result<()>
    where
        F: Fn(IL2PoolEvents) -> Fut + Send + 'static,
        Fut: Future<Output = eyre::Result<()>> + Send;
    async fn listen_price_update<F, Fut>(
        &self,
        price_source: &Address,
        callback: F,
    ) -> eyre::Result<()>
    where
        F: Fn(IChainlinkAggregatorEvents) -> Fut + Send + 'static,
        Fut: Future<Output = eyre::Result<()>> + Send;
    async fn get_reserve_configuration_data(&self, token: &Address) -> eyre::Result<f64>;
    async fn get_user_reserve_data(
        &self,
        token: &Address,
        user: &Address,
    ) -> eyre::Result<UserReserveData>;
    async fn get_decimal(&self, token: &Address) -> eyre::Result<f64>;
    async fn get_price_decimal(&self, price_source: &Address) -> eyre::Result<f64>;
}

pub struct UserReserveData {
    pub usage_as_collateral_enabled: bool,
    pub current_atoken_balance: U256,
    pub current_variable_debt: U256,
}

impl UserReserveData {
    pub fn new(
        current_atoken_balance: U256,
        current_variable_debt: U256,
        usage_as_collateral_enabled: bool,
    ) -> Self {
        Self {
            current_atoken_balance,
            current_variable_debt,
            usage_as_collateral_enabled,
        }
    }
}

pub struct AaveDataProvider<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    pub(in crate::arbitrum) aave_protocol_data_provider: IAaveProtocolDataProviderInstance<P>,
    pub(in crate::arbitrum) aave_oracle: IAaveOracleInstance<P>,
    pub(in crate::arbitrum) provider: P,
}

impl<P> AaveDataProvider<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    pub fn new(provider: &P) -> eyre::Result<Self> {
        let aave_protocol_data_provider = IAaveProtocolDataProvider::new(
            AAVE_PROTOCOL_DATA_PROVIDER_ADDRESS.parse()?,
            provider.clone(),
        );

        let aave_oracle = IAaveOracle::new(AAVE_ORACLE_ADDRESS.parse()?, provider.clone());

        Ok(Self {
            aave_protocol_data_provider,
            aave_oracle,
            provider: provider.clone(),
        })
    }
}

#[async_trait]
impl<P> DataProvider for AaveDataProvider<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    async fn get_all_reserves_tokens(&self) -> eyre::Result<Vec<TokenData>> {
        let token_data = self
            .aave_protocol_data_provider
            .getAllReservesTokens()
            .call()
            .await?;

        Ok(token_data)
    }

    async fn get_source_of_asset(&self, token: &Address) -> eyre::Result<Address> {
        let asset_source = self
            .aave_oracle
            .getSourceOfAsset(token.clone())
            .call()
            .await?;

        Ok(asset_source)
    }

    async fn listen_events<F, Fut>(&self, callback: F) -> eyre::Result<()>
    where
        F: Fn(IL2PoolEvents) -> Fut + Send + 'static,
        Fut: Future<Output = eyre::Result<()>> + Send,
    {
        let l2_pool = IL2Pool::new(L2_POOL_ADDRESS.parse()?, self.provider.clone());
        let filter = Filter::new().address(l2_pool.address().clone());
        let mut stream = self.provider.subscribe_logs(&filter).await?;

        while let Ok(log) = stream.recv().await {
            if let Ok(Log { data, .. }) = IL2PoolEvents::decode_log(log.as_ref()) {
                callback(data).await?;
            }
        }

        Ok(())
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
        let filter = Filter::new().address(price_source.clone());
        let mut stream = self.provider.subscribe_logs(&filter).await?;

        while let Ok(log) = stream.recv().await {
            if let Ok(Log { data, .. }) = IChainlinkAggregatorEvents::decode_log(log.as_ref()) {
                callback(data).await?;
            }
        }

        Ok(())
    }

    async fn get_reserve_configuration_data(&self, token: &Address) -> eyre::Result<f64> {
        let data = self
            .aave_protocol_data_provider
            .getReserveConfigurationData(token.clone())
            .call()
            .await?;

        Ok(data.liquidationThreshold.as_f64(10_f64.powi(4)))
    }

    async fn get_user_reserve_data(
        &self,
        token: &Address,
        user: &Address,
    ) -> eyre::Result<UserReserveData> {
        let getUserReserveDataReturn {
            currentATokenBalance: current_atoken_balance,
            currentVariableDebt: current_variable_debt,
            usageAsCollateralEnabled: usage_as_collateral_enabled,
            ..
        } = self
            .aave_protocol_data_provider
            .getUserReserveData(token.clone(), user.clone())
            .call()
            .await?;

        Ok(UserReserveData::new(
            current_atoken_balance,
            current_variable_debt,
            bool::from(usage_as_collateral_enabled),
        ))
    }

    async fn get_decimal(&self, token: &Address) -> eyre::Result<f64> {
        let decimals = IERC20Metadata::new(token.clone(), &self.provider)
            .decimals()
            .call()
            .await?;

        Ok(10_f64.powi(decimals as i32))
    }

    async fn get_price_decimal(&self, price_source: &Address) -> eyre::Result<f64> {
        let decimals = IChainlinkAggregator::new(price_source.clone(), &self.provider);

        let decimals = IERC20Metadata::new(token.clone(), &self.provider)
            .decimals()
            .call()
            .await?;

        Ok(10_f64.powi(decimals as i32))
    }
}

pub(crate) type Tokens = HashMap<Address, TokenDetails>;

#[derive(Debug)]
pub(crate) struct TokenDetails {
    pub(crate) name: String,
    pub(crate) price_source: Address,
    pub(crate) order: usize,
    pub(crate) decimal: f64,
}

impl TokenDetails {
    pub(in crate::arbitrum) fn new(
        name: String,
        price_source: Address,
        order: usize,
        decimal: f64,
    ) -> Self {
        Self {
            name,
            price_source,
            order,
            decimal,
        }
    }
}

pub(crate) struct Token(pub(crate) Address);
pub(crate) struct RqDate(pub(crate) TimeStamp);

pub async fn start<P>(cache: Arc<Cache>, provider: Arc<P>) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    let tokens = Arc::new(setup(provider.clone()).await?);

    debug!("start: tokens = {:?}", tokens);

    cache.init(&tokens).await?;

    let (tx_events, mut rc_events) = channel::<AaveEvents>(1000_000);
    listen_events(provider.clone(), tx_events.clone()).await?;
    listen_price_update(provider.clone(), &tokens, tx_events).await?;

    let w_num = 4;
    let bound = 1000;
    let (mut sync_counter, sync_senders) = (0, listen_sync(cache.clone(), w_num, bound).await?);
    let (mut hf_counter, hf_senders) = (0, listen_hf_calc(cache.clone(), w_num, bound).await?);

    liquidation_threshold_update(
        cache.clone(),
        tokens.clone(),
        provider.clone(),
        hf_senders[hf_counter % w_num].clone(),
    )
    .await?;
    hf_counter = hf_counter.wrapping_add(1);

    let supply_txs = Cache::subscribe(
        cache.clone(),
        w_num,
        bound,
        provider.clone(),
        tokens.clone(),
        supply,
    )
    .await?;
    let withdraw_txs = Cache::subscribe(
        cache.clone(),
        w_num,
        bound,
        provider.clone(),
        tokens.clone(),
        withdraw,
    )
    .await?;
    let borrow_txs = Cache::subscribe(
        cache.clone(),
        w_num,
        bound,
        provider.clone(),
        tokens.clone(),
        borrow,
    )
    .await?;
    let repay_txs = Cache::subscribe(
        cache.clone(),
        w_num,
        bound,
        provider.clone(),
        tokens.clone(),
        repay,
    )
    .await?;
    let reserve_used_as_collateral_enabled_txs = Cache::subscribe(
        cache.clone(),
        w_num,
        bound,
        provider.clone(),
        tokens.clone(),
        reserve_used_as_collateral_enabled,
    )
    .await?;
    let reserve_used_as_collateral_disabled_txs = Cache::subscribe(
        cache.clone(),
        w_num,
        bound,
        provider.clone(),
        tokens.clone(),
        reserve_used_as_collateral_disabled,
    )
    .await?;
    let liquidation_call_txs = Cache::subscribe(
        cache.clone(),
        w_num,
        bound,
        provider.clone(),
        tokens.clone(),
        liquidation_call,
    )
    .await?;
    let reserve_data_updated_txs = Cache::subscribe(
        cache.clone(),
        w_num,
        bound,
        provider.clone(),
        tokens.clone(),
        reserve_data_updated,
    )
    .await?;
    let answer_updated_txs = Cache::subscribe(
        cache.clone(),
        w_num,
        bound,
        provider.clone(),
        tokens.clone(),
        answer_updated,
    )
    .await?;

    #[derive(Default)]
    struct EventCounter {
        supply: usize,
        withdraw: usize,
        borrow: usize,
        repay: usize,
        reserve_used_as_collateral_enabled: usize,
        reserve_used_as_collateral_disabled: usize,
        liquidation_call: usize,
        reserve_data_updated: usize,
        answer_updated: usize,
    }
    let mut counters = EventCounter::default();
    while let Some(event) = rc_events.recv().await {
        match event {
            AaveEvents::IL2PoolEvents(event, rq_date) => match event {
                IL2PoolEvents::Supply(ev) => {
                    debug!("start: supply");
                    supply_txs[counters.supply % w_num]
                        .send((
                            ev,
                            sync_senders[sync_counter % w_num].clone(),
                            hf_senders[hf_counter % w_num].clone(),
                            RqDate(rq_date),
                        ))
                        .await?;
                    counters.supply = counters.supply.wrapping_add(1);
                    sync_counter = sync_counter.wrapping_add(1);
                }
                IL2PoolEvents::Withdraw(ev) => {
                    debug!("start: withdraw");
                    withdraw_txs[counters.withdraw % w_num]
                        .send((
                            ev,
                            sync_senders[sync_counter % w_num].clone(),
                            hf_senders[hf_counter % w_num].clone(),
                            RqDate(rq_date),
                        ))
                        .await?;
                    counters.withdraw = counters.withdraw.wrapping_add(1);
                    sync_counter = sync_counter.wrapping_add(1);
                }
                IL2PoolEvents::Borrow(ev) => {
                    debug!("start: borrow");
                    borrow_txs[counters.borrow % w_num]
                        .send((
                            ev,
                            sync_senders[sync_counter % w_num].clone(),
                            hf_senders[hf_counter % w_num].clone(),
                            RqDate(rq_date),
                        ))
                        .await?;
                    counters.borrow = counters.borrow.wrapping_add(1);
                    sync_counter = sync_counter.wrapping_add(1);
                }
                IL2PoolEvents::Repay(ev) => {
                    debug!("start: repay");
                    repay_txs[counters.repay % w_num]
                        .send((
                            ev,
                            sync_senders[sync_counter % w_num].clone(),
                            hf_senders[hf_counter % w_num].clone(),
                            RqDate(rq_date),
                        ))
                        .await?;
                    counters.repay = counters.repay.wrapping_add(1);
                    sync_counter = sync_counter.wrapping_add(1);
                }
                IL2PoolEvents::ReserveUsedAsCollateralEnabled(ev) => {
                    debug!("start: enable as collateral");
                    reserve_used_as_collateral_enabled_txs
                        [counters.reserve_used_as_collateral_enabled % w_num]
                        .send((
                            ev,
                            sync_senders[sync_counter % w_num].clone(),
                            hf_senders[hf_counter % w_num].clone(),
                            RqDate(rq_date),
                        ))
                        .await?;
                    counters.reserve_used_as_collateral_enabled =
                        counters.reserve_used_as_collateral_enabled.wrapping_add(1);
                    sync_counter = sync_counter.wrapping_add(1);
                }
                IL2PoolEvents::ReserveUsedAsCollateralDisabled(ev) => {
                    debug!("start: disable as collateral");
                    reserve_used_as_collateral_disabled_txs
                        [counters.reserve_used_as_collateral_disabled % w_num]
                        .send((
                            ev,
                            sync_senders[sync_counter % w_num].clone(),
                            hf_senders[hf_counter % w_num].clone(),
                            RqDate(rq_date),
                        ))
                        .await?;
                    counters.reserve_used_as_collateral_disabled =
                        counters.reserve_used_as_collateral_disabled.wrapping_add(1);
                    sync_counter = sync_counter.wrapping_add(1);
                }
                IL2PoolEvents::LiquidationCall(ev) => {
                    debug!("start: liquidation call");
                    liquidation_call_txs[counters.liquidation_call % w_num]
                        .send((
                            ev,
                            sync_senders[sync_counter % w_num].clone(),
                            hf_senders[hf_counter % w_num].clone(),
                            RqDate(rq_date),
                        ))
                        .await?;
                    counters.liquidation_call = counters.liquidation_call.wrapping_add(1);
                    sync_counter = sync_counter.wrapping_add(1);
                }
                IL2PoolEvents::ReserveDataUpdated(ev) => {
                    debug!("start: reserve data updated");
                    reserve_data_updated_txs[counters.reserve_data_updated % w_num]
                        .send((ev, hf_senders[hf_counter % w_num].clone(), RqDate(rq_date)))
                        .await?;
                    counters.reserve_data_updated = counters.reserve_data_updated.wrapping_add(1);
                }
            },
            AaveEvents::IChainlinkAggregatorEvents(event, token, rq_date) => match event {
                IChainlinkAggregatorEvents::AnswerUpdated(ev) => {
                    debug!("start: answer updated");
                    answer_updated_txs[counters.answer_updated % w_num]
                        .send((
                            ev,
                            Token(token),
                            hf_senders[hf_counter % w_num].clone(),
                            RqDate(rq_date),
                        ))
                        .await?;
                    counters.answer_updated = counters.answer_updated.wrapping_add(1);
                }
            },
        }
        hf_counter = hf_counter.wrapping_add(1);
    }

    Ok(())
}

pub(crate) async fn setup<P>(provider: Arc<P>) -> eyre::Result<Tokens>
where
    P: DataProvider,
{
    let token_data = provider.get_all_reserves_tokens().await?;

    let mut tokens = HashMap::new();
    let mut order = 0;
    for token in token_data {
        debug!(
            "setup: token symbol = {}, token address = {}",
            token.symbol, token.tokenAddress
        );

        let asset_source = provider.get_source_of_asset(&token.tokenAddress).await?;
        let decimal = provider.get_decimal(&token.tokenAddress).await?;

        debug!("setup: token asset_address = {}", asset_source);

        tokens.insert(
            token.tokenAddress,
            TokenDetails::new(token.symbol, asset_source, order, decimal),
        );
        order += 1;
    }

    Ok(tokens)
}

pub type TimeStamp = i64;

pub(crate) enum AaveEvents {
    IL2PoolEvents(IL2PoolEvents, TimeStamp),
    IChainlinkAggregatorEvents(IChainlinkAggregatorEvents, Address, TimeStamp),
}

pub(crate) async fn listen_events<P>(provider: Arc<P>, tx: Sender<AaveEvents>) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    task::spawn(async move {
        loop {
            debug!("listen_events: created thread");

            let tx = tx.clone();
            match provider
                .listen_events(move |data| {
                    let tx = tx.clone();
                    async move {
                        match data {
                            IL2PoolEvents::Supply(_) => debug!("listen_events: supply event"),
                            IL2PoolEvents::Withdraw(_) => debug!("listen_events: withdraw event"),
                            IL2PoolEvents::Borrow(_) => debug!("listen_events: borrow event"),
                            IL2PoolEvents::Repay(_) => debug!("listen_events: repay event"),
                            IL2PoolEvents::ReserveUsedAsCollateralEnabled(_) => {
                                debug!("listen_events: enable collateral event")
                            }
                            IL2PoolEvents::ReserveUsedAsCollateralDisabled(_) => {
                                debug!("listen_events: disable collateral event")
                            }
                            IL2PoolEvents::LiquidationCall(_) => {
                                debug!("listen_events: liquidation event")
                            }
                            IL2PoolEvents::ReserveDataUpdated(_) => {
                                debug!("listen_events: reserve data updated event")
                            }
                        }

                        tx.send(AaveEvents::IL2PoolEvents(
                            data,
                            Utc::now().timestamp_micros(),
                        ))
                        .await?;

                        Ok(())
                    }
                })
                .await
            {
                Ok(_) => debug!("listen_events: Ok"),
                Err(e) => debug!("listen_events: error = {:?}", e),
            }
        }
    });

    Ok(())
}

pub(crate) async fn listen_price_update<P>(
    provider: Arc<P>,
    tokens: &Tokens,
    tx: Sender<AaveEvents>,
) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    for (token, TokenDetails { price_source, .. }) in tokens {
        let (token, price_source, provider, tx) = (
            token.clone(),
            price_source.clone(),
            provider.clone(),
            tx.clone(),
        );
        task::spawn(async move {
            loop {
                debug!("listen_price_update: created thread");

                let tx = tx.clone();
                match provider
                    .listen_price_update(&price_source, move |data| {
                        let tx = tx.clone();
                        async move {
                            match data {
                                IChainlinkAggregatorEvents::AnswerUpdated(_) => {
                                    debug!("listen_price_update: answer updated event")
                                }
                            }

                            tx.send(AaveEvents::IChainlinkAggregatorEvents(
                                data,
                                token,
                                Utc::now().timestamp_micros(),
                            ))
                            .await?;

                            Ok(())
                        }
                    })
                    .await
                {
                    Ok(_) => debug!("listen_price_update: Ok"),
                    Err(e) => debug!("listen_price_update: error = {:?}", e),
                }
            }
        });
    }

    Ok(())
}

pub(crate) async fn liquidation_threshold_update<P>(
    cache: Arc<Cache>,
    tokens: Arc<Tokens>,
    provider: Arc<P>,
    hf_tx: Sender<HFRequest>,
) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    task::spawn(async move {
        debug!("liquidation_threshold_update: created thread");

        let mut interval = time::interval(Duration::from_secs(600));
        loop {
            interval.tick().await;

            match liquidation_threshold_update_handler(&cache, &tokens, provider.as_ref(), &hf_tx)
                .await
            {
                Ok(_) => debug!("liquidation_threshold_update: Ok"),
                Err(e) => debug!("liquidation_threshold_update: error = {:?}", e),
            }
        }
    });

    Ok(())
}

async fn liquidation_threshold_update_handler<P>(
    cache: &Cache,
    tokens: &Tokens,
    provider: &P,
    hf_tx: &Sender<HFRequest>,
) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    let mut data = vec![0.0; tokens.len()];
    let tasks = tokens.iter().map(
        |(token_address, TokenDetails { name, order, .. })| async move {
            Ok::<_, eyre::Error>((
                provider
                    .get_reserve_configuration_data(token_address)
                    .await?,
                name.clone(),
                order.clone(),
            ))
        },
    );

    let rc_data = try_join_all(tasks).await?;

    for (lt, name, order) in rc_data {
        debug!(
            "liquidation_threshold_update: name = {}, liquidation_threshold = {}",
            name, lt
        );

        data[order] = lt;
    }

    let d = Array1::from_vec(data);
    let lt_modified = {
        let (lt, _) = &*cache.liquidation_threshold.read().await;
        !d.iter().zip(lt).all(|(a, b)| (a - b).abs() < 1e-8)
    };

    if lt_modified {
        let rq_date = Utc::now().timestamp_micros();
        *cache.liquidation_threshold.write().await = (d, rq_date);
        hf_tx.send(HFRequest::Full(rq_date)).await?;
    }

    Ok(())
}

#[derive(Debug, PartialEq)]
pub(crate) enum SyncTarget {
    Row(usize),
    Cell(usize, usize),
}

#[derive(Debug, PartialEq)]
pub(crate) enum SyncRequest {
    Collateral(SyncTarget, TimeStamp),
    Borrowed(SyncTarget, TimeStamp),
    Both(SyncTarget, TimeStamp),
}

pub(crate) async fn listen_sync(
    cache: Arc<Cache>,
    workers: usize,
    bound: usize,
) -> eyre::Result<Vec<Sender<SyncRequest>>> {
    let mut senders = Vec::with_capacity(workers);
    for _ in 0..workers {
        let (tx, mut rc) = channel::<SyncRequest>(bound);
        let cache = cache.clone();
        task::spawn(async move {
            loop {
                debug!("listen_sync: created thread");

                match listen_sync_handler(&cache, &mut rc).await {
                    Ok(_) => debug!("listen_sync: Ok"),
                    Err(e) => debug!("listen_sync: error = {:?}", e),
                }
            }
        });
        senders.push(tx);
    }

    Ok(senders)
}

async fn listen_sync_handler(cache: &Cache, rc: &mut Receiver<SyncRequest>) -> eyre::Result<()> {
    while let Some(sync_rq) = rc.recv().await {
        match sync_rq {
            SyncRequest::Collateral(target, rq_date) => {
                let _ = cache.sync_collateral(&target, rq_date).await;
            }
            SyncRequest::Borrowed(target, rq_date) => {
                let _ = cache.sync_borrowed(&target, rq_date).await;
            }
            SyncRequest::Both(target, rq_date) => {
                let _ = cache.sync_data(&target, rq_date).await;
            }
        }
    }

    Ok(())
}

#[derive(Debug, PartialEq)]
pub(crate) enum HFRequest {
    User(Address, TimeStamp),
    Full(TimeStamp),
}

pub(crate) async fn listen_hf_calc(
    cache: Arc<Cache>,
    workers: usize,
    bound: usize,
) -> eyre::Result<Vec<Sender<HFRequest>>> {
    let mut senders = Vec::with_capacity(workers);
    for _ in 0..workers {
        let (tx, mut rc) = channel::<HFRequest>(bound);
        let cache = cache.clone();
        task::spawn(async move {
            loop {
                debug!("listen_hf_calc: created thread");

                match listen_hf_calc_handler(&cache, &mut rc).await {
                    Ok(_) => debug!("listen_hf_calc: Ok"),
                    Err(e) => debug!("listen_hf_calc: error = {:?}", e),
                }
            }
        });
        senders.push(tx);
    }

    Ok(senders)
}

async fn listen_hf_calc_handler(cache: &Cache, rc: &mut Receiver<HFRequest>) -> eyre::Result<()> {
    while let Some(hf_rq) = rc.recv().await {
        match hf_rq {
            HFRequest::User(user, rq_date) => match cache.calc_hf(Some(&user), rq_date).await {
                Ok(_) => {
                    let (hf, _) = &*cache.health_factors.read().await;
                    debug!("listen_hf_calc: user = {}, hf = {}", user, hf);
                }
                Err(e) => {
                    error!("listen_hf_calc: user = {}, error = {:?}", user, e)
                }
            },
            HFRequest::Full(rq_date) => match cache.calc_hf(None, rq_date).await {
                Ok(_) => {
                    let (hf, _) = &*cache.health_factors.read().await;
                    debug!("listen_hf_calc: hf = {}", hf);
                }
                Err(e) => error!("listen_hf_calc: error = {:?}", e),
            },
        }
    }

    Ok(())
}

pub type UserDetails = DashMap<Address, UserSettings>;
pub type Array = RwLock<(Array1<f64>, TimeStamp)>;
pub type Arrays = RwLock<Vec<RwLock<(Array1<U256>, TimeStamp, TimeStamp)>>>;
pub type Matrix = RwLock<Array2<f64>>;

#[derive(Default, Debug, Clone)]
pub struct UserSettings {
    pub row_num: usize,
    pub use_as_collateral: BitVec<usize, Lsb0>,
}

impl UserSettings {
    pub fn new(row_num: usize, use_as_collateral: BitVec<usize, Lsb0>) -> Self {
        Self {
            row_num,
            use_as_collateral,
        }
    }
}

#[derive(Default, Debug)]
pub struct Cache {
    pub users: UserDetails,
    pub users_num: RwLock<usize>,
    pub decimal: Array,

    pub reserve: Arrays,
    pub collateral: Arrays,
    pub collateral_matrix: Matrix,

    pub borrowed: Arrays,
    pub borrowed_matrix: Matrix,

    pub liquidation_threshold: Array,
    pub prices: Array,
    pub health_factors: Array,
}

impl Cache {
    pub(crate) async fn init(&self, tokens: &Tokens) -> eyre::Result<()> {
        let token_num = tokens.len();
        *self.collateral_matrix.write().await = Array2::from_elem((0, token_num), 0.0);
        *self.borrowed_matrix.write().await = Array2::from_elem((0, token_num), 0.0);

        let now = Utc::now().timestamp_micros();

        let mut decimals = vec![0.0; token_num];
        for (_, TokenDetails { order, decimal, .. }) in tokens {
            decimals[*order] = *decimal;
        }
        *self.decimal.write().await = (Array1::from_vec(decimals), now);

        *self.prices.write().await = (Array1::from_elem(token_num, 0.0), now);
        *self.liquidation_threshold.write().await = (Array1::from_elem(token_num, 0.0), now);
        *self.health_factors.write().await = (Array1::from_elem(0, 0.0), now);

        Ok(())
    }

    pub(in crate::arbitrum) fn contains(&self, addr: &Address) -> bool {
        self.users.contains_key(addr)
    }

    pub(in crate::arbitrum) async fn sync_user<P>(
        &self,
        user: &Address,
        tokens: &Tokens,
        provider: Arc<P>,
    ) -> eyre::Result<()>
    where
        P: DataProvider + 'static,
    {
        let (collateral, reserve, borrowed) = self.get_user_data(provider, tokens, user).await?;
        let row_num = self
            .users
            .get(user)
            .ok_or_else(|| eyre!("user = {:?} not found", user))?
            .row_num;
        let now = Utc::now().timestamp_micros();

        {
            let collaterals = &*self.collateral.read().await;
            let (col, last_sync, last_modified) = &mut *collaterals
                .get(row_num)
                .ok_or_else(|| eyre!("can't get row = {} from collateral", row_num))?
                .write()
                .await;
            (*col, *last_sync, *last_modified) = (Array1::from(collateral), now, now);
            debug!("sync_user: new collateral = {:?}", col);
        }

        {
            let reserves = &*self.reserve.read().await;
            let (res, last_sync, last_modified) = &mut *reserves
                .get(row_num)
                .ok_or_else(|| eyre!("can't get row = {} from reserve", row_num))?
                .write()
                .await;
            (*res, *last_sync, *last_modified) = (Array1::from(reserve), now, now);
            debug!("sync_user: new reserve = {:?}", res);
        }

        {
            let debt = &*self.borrowed.read().await;
            let (bor, last_sync, last_modified) = &mut *debt
                .get(row_num)
                .ok_or_else(|| eyre!("can't get row = {} from borrowed", row_num))?
                .write()
                .await;
            (*bor, *last_sync, *last_modified) = (Array1::from(borrowed), now, now);
            debug!("sync_user: new borrowed = {:?}", bor);
        }

        Ok(())
    }

    pub(in crate::arbitrum) async fn init_user<P>(
        &self,
        user: &Address,
        tokens: &Tokens,
        provider: Arc<P>,
    ) -> eyre::Result<bool>
    where
        P: DataProvider + 'static,
    {
        if self.contains(user) {
            debug!("init_user: existing user = {}", user);
            return Ok(true);
        }

        {
            let mut user_num_lock = self.users_num.write().await;

            // if we have more than one thread in this fn
            if self.contains(user) {
                debug!("init_user: existing user = {} second attempt", user);
                return Ok(true);
            }

            debug!("init_user: new user = {}", user);

            self.users.insert(
                user.clone(),
                UserSettings::new(*user_num_lock, bitvec![usize, Lsb0; 0; tokens.len()]),
            );
            *user_num_lock += 1;
        }

        {
            let collaterals = &mut *self.collateral.write().await;
            collaterals.push(RwLock::new((
                Array1::from_vec(vec![U256::default(); tokens.len()]),
                0,
                0,
            )));
        }

        {
            let reserves = &mut *self.reserve.write().await;
            reserves.push(RwLock::new((
                Array1::from_vec(vec![U256::default(); tokens.len()]),
                0,
                0,
            )));
        }

        {
            let borrowed = &mut *self.borrowed.write().await;
            borrowed.push(RwLock::new((
                Array1::from_vec(vec![U256::default(); tokens.len()]),
                0,
                0,
            )));
        }

        {
            let (hf, last_modified) = &mut *self.health_factors.write().await;
            let mut hf_vec = hf.to_vec();
            hf_vec.push(0.0);
            (*hf, *last_modified) = (Array1::from_vec(hf_vec), 0);
        }

        self.sync_user(user, tokens, provider).await?;

        Ok(false)
    }

    pub(in crate::arbitrum) async fn get_user_data<P>(
        &self,
        provider: Arc<P>,
        tokens: &Tokens,
        user: &Address,
    ) -> eyre::Result<(Vec<U256>, Vec<U256>, Vec<U256>)>
    where
        P: DataProvider + 'static,
    {
        let tasks = tokens.iter().map(|(token_address, _)| {
            let provider = provider.clone();
            async move {
                Ok::<_, eyre::Error>((
                    provider.get_user_reserve_data(token_address, user).await?,
                    token_address.clone(),
                ))
            }
        });

        let user_reserve_data = try_join_all(tasks).await?;

        let (mut collateral, mut reserve, mut borrowed, mut user_settings) = (
            vec![U256::default(); tokens.len()],
            vec![U256::default(); tokens.len()],
            vec![U256::default(); tokens.len()],
            self.users
                .get(user)
                .ok_or_else(|| eyre!("user = {:?} not found", user))?
                .clone(),
        );

        for (
            UserReserveData {
                current_atoken_balance,
                current_variable_debt,
                usage_as_collateral_enabled,
            },
            token_address,
        ) in user_reserve_data
        {
            let idx = tokens
                .get(&token_address)
                .ok_or_else(|| eyre!("token = {} not found", token_address))?
                .order;
            if usage_as_collateral_enabled {
                collateral[idx] = current_atoken_balance;
                user_settings.use_as_collateral.set(idx, true);
            } else {
                reserve[idx] = current_atoken_balance;
            }
            borrowed[idx] = current_variable_debt;
        }

        self.users.insert(user.clone(), user_settings);

        Ok((collateral, reserve, borrowed))
    }

    pub(in crate::arbitrum) fn remove_user(&self, addr: &Address) {
        self.users.remove(addr);
    }

    pub(crate) async fn subscribe<P, T, F, Fut>(
        cache: Arc<Cache>,
        workers: usize,
        bound: usize,
        provider: Arc<P>,
        tokens: Arc<Tokens>,
        callback: F,
    ) -> eyre::Result<Vec<Sender<T>>>
    where
        P: DataProvider + 'static,
        T: Send + 'static,
        F: Fn(Arc<Cache>, Arc<P>, Arc<Tokens>, T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = eyre::Result<()>> + Send + 'static,
    {
        let callback = Arc::new(callback);
        let mut senders = vec![];
        for _ in 0..workers {
            let (tx, mut rc) = channel::<T>(bound);
            let (callback, cache, provider, tokens) = (
                callback.clone(),
                cache.clone(),
                provider.clone(),
                tokens.clone(),
            );
            task::spawn(async move {
                loop {
                    debug!("subscribe: created thread");

                    while let Some(msg) = rc.recv().await {
                        if let Err(e) =
                            callback(cache.clone(), provider.clone(), tokens.clone(), msg).await
                        {
                            error!("Error while calling event listener: {:?}", e);
                        }
                    }
                }
            });

            senders.push(tx);
        }

        Ok(senders)
    }

    pub(in crate::arbitrum) async fn sync_collateral(
        &self,
        target: &SyncTarget,
        rq_date: TimeStamp,
    ) -> eyre::Result<()> {
        let col_lock = self.collateral.read().await;
        let mut col_matrix_lock = self.collateral_matrix.write().await;
        let (decimals, _) = &*self.decimal.read().await;

        debug!(
            "sync_collateral: sync target = {:?}, col_lock = {:?}",
            target, col_lock
        );

        let low_bound = col_matrix_lock.nrows().saturating_sub(1);
        while col_matrix_lock.nrows() < col_lock.len() {
            let row_lock = col_lock
                .get(col_matrix_lock.nrows())
                .ok_or_else(|| eyre!("row = {} not found in collateral", col_matrix_lock.nrows()))?
                .read()
                .await;
            let row = Array1::from_iter(
                row_lock
                    .0
                    .iter()
                    .enumerate()
                    .map(|(idx, v)| v.as_f64(decimals[idx])),
            );
            col_matrix_lock.push_row(row.view())?;
        }

        debug!(
            "sync_collateral: after row check col_matrix_lock = {:?}",
            col_matrix_lock
        );

        let (row_num, col_num) = match target {
            SyncTarget::Row(row_num) => (*row_num, None),
            SyncTarget::Cell(row_num, col_num) => (*row_num, Some(*col_num)),
        };

        if row_num > low_bound {
            debug!("{}", {
                let received = Utc::now().timestamp_micros();
                format!(
                    "sync_collateral: we sync collateral, rq_date = {}, \
                         received = {}, delta = {} μs",
                    rq_date,
                    received,
                    received - rq_date
                )
            });
            return Ok(());
        }

        if let Some(col_num) = col_num {
            col_matrix_lock[(row_num, col_num)] = {
                let (col, _, _) = &*col_lock
                    .get(row_num)
                    .ok_or_else(|| eyre!("row = {} not found in collateral", row_num))?
                    .read()
                    .await;
                col.get(col_num)
                    .ok_or_else(|| eyre!("column = {} not found in collateral", col_num))?
                    .as_f64(decimals[col_num])
            };
        } else {
            let row = col_lock
                .get(row_num)
                .ok_or_else(|| eyre!("row = {} not found in collateral", row_num))?
                .read()
                .await;

            let row = Array1::from_iter(
                row.0
                    .iter()
                    .enumerate()
                    .map(|(idx, v)| v.as_f64(decimals[idx])),
            );
            col_matrix_lock.row_mut(row_num).assign(&row);
        }

        debug!("{}", {
            let received = Utc::now().timestamp_micros();
            format!(
                "sync_collateral: after row insert col_matrix_lock = {:?}, rq_date = {}, \
                     received = {}, delta = {} μs",
                col_matrix_lock,
                rq_date,
                received,
                received - rq_date
            )
        });

        Ok(())
    }

    pub(in crate::arbitrum) async fn sync_borrowed(
        &self,
        target: &SyncTarget,
        rq_date: TimeStamp,
    ) -> eyre::Result<()> {
        let bor_lock = self.borrowed.read().await;
        let mut bor_matrix_lock = self.borrowed_matrix.write().await;
        let (decimals, _) = &*self.decimal.read().await;

        debug!(
            "sync_borrowed: sync target = {:?}, bor_lock = {:?}",
            target, bor_lock
        );

        let low_bound = bor_matrix_lock.nrows().saturating_sub(1);
        while bor_matrix_lock.nrows() < bor_lock.len() {
            let row_lock = bor_lock
                .get(bor_matrix_lock.nrows())
                .ok_or_else(|| eyre!("row = {} not found in borrowed", bor_matrix_lock.nrows()))?
                .read()
                .await;

            let row = Array1::from_iter(
                row_lock
                    .0
                    .iter()
                    .enumerate()
                    .map(|(idx, v)| v.as_f64(decimals[idx])),
            );
            bor_matrix_lock.push_row(row.view())?;
        }

        debug!(
            "sync_borrowed: after row check bor_matrix_lock = {:?}",
            bor_matrix_lock
        );

        let (row_num, col_num) = match target {
            SyncTarget::Row(row_num) => (*row_num, None),
            SyncTarget::Cell(row_num, col_num) => (*row_num, Some(*col_num)),
        };

        if row_num > low_bound {
            debug!("{}", {
                let received = Utc::now().timestamp_micros();
                format!(
                    "sync_borrowed: we sync borrowed, rq_date = {}, \
                         received = {}, delta = {} μs",
                    rq_date,
                    received,
                    received - rq_date
                )
            });
            return Ok(());
        }

        if let Some(col_num) = col_num {
            bor_matrix_lock[(row_num, col_num)] = {
                let (bor, _, _) = &*bor_lock
                    .get(row_num)
                    .ok_or_else(|| eyre!("row = {} not found in borrowed", row_num))?
                    .read()
                    .await;
                bor.get(col_num)
                    .ok_or_else(|| eyre!("column = {} not found in borrowed", col_num))?
                    .as_f64(decimals[col_num])
            };
        } else {
            let row = bor_lock
                .get(row_num)
                .ok_or_else(|| eyre!("row = {} not found in borrowed", row_num))?
                .read()
                .await;

            let row = Array1::from_iter(
                row.0
                    .iter()
                    .enumerate()
                    .map(|(idx, v)| v.as_f64(decimals[idx])),
            );
            bor_matrix_lock.row_mut(row_num).assign(&row);
        }

        debug!("{}", {
            let received = Utc::now().timestamp_micros();
            format!(
                "sync_borrowed: after row insert bor_matrix_lock = {:?}, rq_date = {}, \
                     received = {}, delta = {} μs",
                bor_matrix_lock,
                rq_date,
                received,
                received - rq_date
            )
        });

        Ok(())
    }

    pub(in crate::arbitrum) async fn sync_data(
        &self,
        target: &SyncTarget,
        rq_date: TimeStamp,
    ) -> eyre::Result<()> {
        self.sync_collateral(target, rq_date).await?;
        self.sync_borrowed(target, rq_date).await?;

        Ok(())
    }

    pub(in crate::arbitrum) async fn calc_hf(
        &self,
        user: Option<&Address>,
        rq_date: TimeStamp,
    ) -> eyre::Result<()> {
        let lt = {
            let (lt, _) = &*self.liquidation_threshold.read().await;
            lt.view().to_owned()
        };
        let price = {
            let (price, _) = &*self.prices.read().await;
            price.view().to_owned()
        };
        let ltp = &lt * &price;

        if let Some(user) = user {
            let row_num = self
                .users
                .get(user)
                .ok_or_else(|| eyre!("user = {:?} not found", user))?
                .row_num;
            let (decimals, _) = &*self.decimal.read().await;

            let col_eff = {
                let collateral = &*self.collateral.read().await;
                let col_row_lock = Array1::from_iter(
                    collateral
                        .get(row_num)
                        .ok_or_else(|| eyre!("row = {} not found in collateral", row_num))?
                        .read()
                        .await
                        .0
                        .iter()
                        .enumerate()
                        .map(|(idx, v)| v.as_f64(decimals[idx])),
                );
                col_row_lock.dot(&ltp)
            };

            let bor_eff = {
                let borrowed = &*self.borrowed.read().await;
                let bor_row_lock = Array1::from_iter(
                    borrowed
                        .get(row_num)
                        .ok_or_else(|| eyre!("row = {} not found in borrowed", row_num))?
                        .read()
                        .await
                        .0
                        .iter()
                        .enumerate()
                        .map(|(idx, v)| v.as_f64(decimals[idx])),
                );
                bor_row_lock.dot(&price)
            };

            let mut hf_lock = self.health_factors.write().await;
            if row_num > hf_lock.0.len() - 1 {
                hf_lock.0 = concatenate(
                    Axis(0),
                    &[
                        hf_lock.0.view(),
                        Array1::from_elem(row_num + 1 - hf_lock.0.len(), 0.0).view(),
                    ],
                )?;
            }
            (hf_lock.0[row_num], hf_lock.1) = (col_eff / bor_eff, Utc::now().timestamp_micros());

            debug!("{}", {
                let received = Utc::now().timestamp_micros();
                format!(
                    "calc_hf: user = {:?}, hf = {:?}, rq_date = {}, \
                     received = {}, delta = {} μs",
                    user,
                    hf_lock,
                    rq_date,
                    received,
                    received - rq_date
                )
            });

            return Ok(());
        }

        let col_eff = {
            let collateral_lock = self.collateral_matrix.read().await;
            collateral_lock.dot(&ltp)
        };

        let bor_eff = {
            let borrowed_lock = self.borrowed_matrix.read().await;
            borrowed_lock.dot(&price)
        };

        let mut hf_lock = self.health_factors.write().await;
        (hf_lock.0, hf_lock.1) = (col_eff / bor_eff, Utc::now().timestamp_micros());

        debug!("{}", {
            let received = Utc::now().timestamp_micros();
            format!(
                "calc_hf: hf = {:?}, rq_date = {}, \
                     received = {}, delta = {} μs",
                hf_lock,
                rq_date,
                received,
                received - rq_date
            )
        });

        Ok(())
    }
}

pub(crate) trait F64Converter {
    fn as_f64(&self, divisor: f64) -> f64;
}

const POW64: [f64; 4] = [
    1.0,                                                          // 2^0
    18446744073709551616.0,                                       // 2^64
    340282366920938463463374607431768211456.0,                    // 2^128
    6277101735386680763835789423207666416102355444464034512896.0, // 2^192
];

impl F64Converter for I256 {
    #[inline(always)]
    fn as_f64(&self, divisor: f64) -> f64 {
        let (sign, limbs) = self.into_sign_and_abs();

        let mut result = limbs
            .into_limbs()
            .iter()
            .enumerate()
            .map(|(idx, &v)| v as f64 * POW64[idx])
            .sum::<f64>();

        if sign == Sign::Negative {
            result = -result;
        }

        result / divisor
    }
}

impl F64Converter for U256 {
    #[inline(always)]
    fn as_f64(&self, divisor: f64) -> f64 {
        let limbs = self.into_limbs();

        let mut result = 0.0;
        for i in 0..4 {
            result += limbs[i] as f64 * POW64[i];
        }
        result / divisor
    }
}
