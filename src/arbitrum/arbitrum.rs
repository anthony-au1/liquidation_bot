use crate::arbitrum::arbitrum::IAaveOracle::IAaveOracleInstance;
use crate::arbitrum::arbitrum::IAaveProtocolDataProvider::{
    IAaveProtocolDataProviderInstance, TokenData, getUserReserveDataReturn,
};
use crate::arbitrum::arbitrum::IChainlinkAggregator::{AnswerUpdated, IChainlinkAggregatorEvents};
use crate::arbitrum::arbitrum::IL2Pool::{
    Borrow, IL2PoolEvents, LiquidationCall, Repay, ReserveDataUpdated,
    ReserveUsedAsCollateralDisabled, ReserveUsedAsCollateralEnabled, Supply, Withdraw,
};
use alloy::primitives::{Address, Log};
use alloy::providers::Provider;
use alloy::rpc::types::Filter;
use alloy::sol;
use alloy::sol_types::SolEventInterface;
use async_trait::async_trait;
use bitvec::prelude::*;
use chrono::Utc;
use dashmap::DashMap;
use futures::future::join_all;
use ndarray::{Array1, Array2, Axis, concatenate};
use std::collections::HashMap;
use std::default::Default;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::{task, time};
use tracing::{debug, error, info};

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
}

pub struct UserReserveData {
    usage_as_collateral_enabled: bool,
    current_atoken_balance: f64,
    current_variable_debt: f64,
}

impl UserReserveData {
    fn new(
        current_atoken_balance: f64,
        current_variable_debt: f64,
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
    aave_protocol_data_provider: IAaveProtocolDataProviderInstance<P>,
    aave_oracle: IAaveOracleInstance<P>,
    provider: P,
}

impl<P> AaveDataProvider<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    pub fn new(provider: &P) -> Self {
        let aave_protocol_data_provider = IAaveProtocolDataProvider::new(
            AAVE_PROTOCOL_DATA_PROVIDER_ADDRESS.parse().unwrap(),
            provider.clone(),
        );

        let aave_oracle = IAaveOracle::new(AAVE_ORACLE_ADDRESS.parse().unwrap(), provider.clone());

        Self {
            aave_protocol_data_provider,
            aave_oracle,
            provider: provider.clone(),
        }
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

        Ok(f64::from(data.liquidationThreshold))
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
            f64::from(current_atoken_balance),
            f64::from(current_variable_debt),
            bool::from(usage_as_collateral_enabled),
        ))
    }
}

pub async fn start<P>(provider: Arc<P>) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    let tokens = Arc::new(setup(provider.clone()).await?);

    debug!("start: tokens = {:?}", tokens);

    let cache = Arc::new(Cache::default());

    {
        *cache.collateral_matrix.write().await = Array2::from_elem((1, tokens.len()), 0.);
        *cache.borrowed_matrix.write().await = Array2::from_elem((1, tokens.len()), 0.);

        let now = Utc::now().timestamp_micros();
        *cache.prices.write().await = (Array1::from_elem(tokens.len(), 0.), now);
        *cache.liquidation_threshold.write().await = (Array1::from_elem(tokens.len(), 0.), now);
        *cache.health_factors.write().await = (Array1::from_elem(1, 0.), now);
    }

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
                            rq_date,
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
                            rq_date,
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
                            rq_date,
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
                            rq_date,
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
                            rq_date,
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
                            rq_date,
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
                            rq_date,
                        ))
                        .await?;
                    counters.liquidation_call = counters.liquidation_call.wrapping_add(1);
                    sync_counter = sync_counter.wrapping_add(1);
                }
                IL2PoolEvents::ReserveDataUpdated(ev) => {
                    debug!("start: reserve data updated");
                    reserve_data_updated_txs[counters.reserve_data_updated % w_num]
                        .send((ev, hf_senders[hf_counter % w_num].clone(), rq_date))
                        .await?;
                    counters.reserve_data_updated = counters.reserve_data_updated.wrapping_add(1);
                }
            },
            AaveEvents::IChainlinkAggregatorEvents(event, token, rq_date) => match event {
                IChainlinkAggregatorEvents::AnswerUpdated(ev) => {
                    debug!("start: answer updated");
                    answer_updated_txs[counters.answer_updated % w_num]
                        .send((ev, token, hf_senders[hf_counter % w_num].clone(), rq_date))
                        .await?;
                    counters.answer_updated = counters.answer_updated.wrapping_add(1);
                }
            },
        }
        hf_counter = hf_counter.wrapping_add(1);
    }

    Ok(())
}

type Tokens = HashMap<Address, TokenDetails>;

#[derive(Debug)]
struct TokenDetails {
    name: String,
    price_source: Address,
    order: usize,
}

impl TokenDetails {
    pub fn new(name: String, price_source: Address, order: usize) -> Self {
        Self {
            name,
            price_source,
            order,
        }
    }
}

async fn setup<P>(provider: Arc<P>) -> eyre::Result<Tokens>
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

        debug!("setup: token asset_address = {}", asset_source);

        tokens.insert(
            token.tokenAddress,
            TokenDetails::new(token.symbol, asset_source, order),
        );
        order += 1;
    }

    Ok(tokens)
}

type TimeStamp = i64;

enum AaveEvents {
    IL2PoolEvents(IL2PoolEvents, TimeStamp),
    IChainlinkAggregatorEvents(IChainlinkAggregatorEvents, Address, TimeStamp),
}

async fn listen_events<P>(provider: Arc<P>, tx: Sender<AaveEvents>) -> eyre::Result<()>
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

async fn listen_price_update<P>(
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

async fn liquidation_threshold_update<P>(
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
            (
                provider
                    .get_reserve_configuration_data(token_address)
                    .await
                    .unwrap(),
                name.clone(),
                order,
            )
        },
    );

    for (lt, name, order) in join_all(tasks).await {
        debug!(
            "liquidation_threshold_update: name = {}, liquidation_threshold = {}",
            name, lt
        );

        data[order.clone()] = lt;
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
enum SyncRequest {
    Collateral(usize, TimeStamp),
    Borrowed(usize, TimeStamp),
    Both(usize, TimeStamp),
}

async fn listen_sync(
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
            SyncRequest::Collateral(row_num, rq_date) => {
                let _ = cache.sync_collateral(row_num, rq_date).await;
            }
            SyncRequest::Borrowed(row_num, rq_date) => {
                let _ = cache.sync_borrowed(row_num, rq_date).await;
            }
            SyncRequest::Both(row_num, rq_date) => {
                let _ = cache.sync_data(row_num, rq_date).await;
            }
        }
    }

    Ok(())
}

#[derive(Debug, PartialEq)]
enum HFRequest {
    User(Address, TimeStamp),
    Full(TimeStamp),
}

async fn listen_hf_calc(
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

type UserDetails = DashMap<Address, UserSettings>;
type Array = RwLock<(Array1<f64>, TimeStamp)>;
type Arrays = RwLock<(Vec<RwLock<Array1<f64>>>, TimeStamp, TimeStamp)>;
type Matrix = RwLock<Array2<f64>>;

#[derive(Default, Debug, Clone)]
struct UserSettings {
    row_num: usize,
    use_as_collateral: BitVec<usize, Lsb0>,
}

impl UserSettings {
    fn new(row_num: usize, use_as_collateral: BitVec<usize, Lsb0>) -> Self {
        Self {
            row_num,
            use_as_collateral,
        }
    }
}

#[derive(Default, Debug)]
struct Cache {
    users: UserDetails,
    users_num: RwLock<usize>,
    reserve: Arrays,
    collateral: Arrays,
    collateral_matrix: Matrix,
    borrowed: Arrays,
    borrowed_matrix: Matrix,
    liquidation_threshold: Array,
    prices: Array,
    health_factors: Array,
}

impl Cache {
    fn contains(&self, addr: &Address) -> bool {
        self.users.contains_key(addr)
    }

    async fn sync_user<P>(
        &self,
        user: &Address,
        tokens: &Tokens,
        provider: Arc<P>,
    ) -> eyre::Result<()>
    where
        P: DataProvider + 'static,
    {
        let (collateral, reserve, borrowed) = self.get_user_data(provider, tokens, user).await?;
        let row_num = self.users.get(user).unwrap().row_num;

        {
            let collaterals = self.collateral.write().await;
            let mut col = collaterals.0.get(row_num).unwrap().write().await;
            *col = Array1::from(collateral);

            debug!("sync_user: new collateral = {:?}", col);
        }

        {
            let reserves = self.reserve.write().await;
            let mut res = reserves.0.get(row_num).unwrap().write().await;
            *res = Array1::from(reserve);

            debug!("sync_user: new reserve = {:?}", res);
        }

        {
            let borroweds = self.borrowed.write().await;
            let mut bor = borroweds.0.get(row_num).unwrap().write().await;
            *bor = Array1::from(borrowed);

            debug!("sync_user: new borrowed = {:?}", bor);
        }

        Ok(())
    }

    async fn init_user<P>(
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

        self.sync_user(user, tokens, provider).await?;

        Ok(false)
    }

    async fn get_user_data<P>(
        &self,
        provider: Arc<P>,
        tokens: &Tokens,
        user: &Address,
    ) -> eyre::Result<(Vec<f64>, Vec<f64>, Vec<f64>)>
    where
        P: DataProvider + 'static,
    {
        let tasks = tokens.iter().map(|(token_address, _)| {
            let provider = provider.clone();
            async move {
                (
                    provider
                        .get_user_reserve_data(token_address, user)
                        .await
                        .unwrap(),
                    token_address.clone(),
                )
            }
        });

        let user_reserve_data = futures::future::join_all(tasks).await;

        let (mut collateral, mut reserve, mut user_settings, mut borrowed) = (
            vec![0.0; tokens.len()],
            vec![0.0; tokens.len()],
            self.users.get(user).unwrap().clone(),
            vec![0.0; tokens.len()],
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
            let idx = tokens.get(&token_address).unwrap().order;
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

    fn remove_user(&self, addr: &Address) {
        self.users.remove(addr);
    }

    async fn subscribe<P, T, F, Fut>(
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

    async fn sync_collateral(&self, row_num: usize, rq_date: TimeStamp) -> eyre::Result<()> {
        let col_lock = self.collateral.read().await;
        let mut col_matrix_lock = self.collateral_matrix.write().await;

        debug!(
            "sync_collateral: row_num = {}, col_lock = {:?}",
            row_num, col_lock
        );

        let low_bound = col_matrix_lock.nrows() - 1;
        while col_matrix_lock.nrows() < col_lock.0.len() {
            let row_lock = col_lock
                .0
                .get(col_matrix_lock.nrows())
                .unwrap()
                .read()
                .await;
            col_matrix_lock.push_row(row_lock.view())?;
        }

        debug!(
            "sync_collateral: after row check col_matrix_lock = {:?}",
            col_matrix_lock
        );

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

        let row = col_lock.0.get(row_num).unwrap().read().await;
        col_matrix_lock.row_mut(row_num).assign(&row);

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

    async fn sync_borrowed(&self, row_num: usize, rq_date: TimeStamp) -> eyre::Result<()> {
        let bor_lock = self.borrowed.read().await;
        let mut bor_matrix_lock = self.borrowed_matrix.write().await;

        debug!(
            "sync_borrowed: row_num = {}, bor_lock = {:?}",
            row_num, bor_lock
        );

        let low_bound = bor_matrix_lock.nrows() - 1;
        while bor_matrix_lock.nrows() < bor_lock.0.len() {
            let row_lock = bor_lock
                .0
                .get(bor_matrix_lock.nrows())
                .unwrap()
                .read()
                .await;
            bor_matrix_lock.push_row(row_lock.view())?;
        }

        debug!(
            "sync_borrowed: after row check bor_matrix_lock = {:?}",
            bor_matrix_lock
        );

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

        let row = bor_lock.0.get(row_num).unwrap().read().await;
        bor_matrix_lock.row_mut(row_num).assign(&row);

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

    async fn sync_data(&self, row_num: usize, rq_date: TimeStamp) -> eyre::Result<()> {
        self.sync_collateral(row_num, rq_date).await?;
        self.sync_borrowed(row_num, rq_date).await?;

        Ok(())
    }

    async fn calc_hf(&self, user: Option<&Address>, rq_date: TimeStamp) -> eyre::Result<()> {
        let lt = {
            let lt_lock = self.liquidation_threshold.read().await;
            (&(&*lt_lock).0.view()).to_owned()
        };
        let price = {
            let price_lock = self.prices.read().await;
            (&(&*price_lock).0.view()).to_owned()
        };
        let ltp = &lt * &price;

        if let Some(user) = user {
            let row_num = self.users.get(user).unwrap().row_num;

            let col_eff = {
                let collateral_lock = self.collateral.read().await;
                let col_row_lock = collateral_lock.0.get(row_num).unwrap().read().await;
                col_row_lock.dot(&ltp)
            };
            let bor_eff = {
                let borrowed_lock = self.borrowed.read().await;
                let bor_row_lock = borrowed_lock.0.get(row_num).unwrap().read().await;
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

async fn create_user<P>(
    rq_date: TimeStamp,
    cache: &Cache,
    provider: Arc<P>,
    tokens: &Tokens,
    user: &Address,
    sync_tx: &Sender<SyncRequest>,
    hf_tx: &Sender<HFRequest>,
) -> eyre::Result<bool>
where
    P: DataProvider + 'static,
{
    match cache.init_user(user, tokens, provider.clone()).await {
        Ok(exist) => {
            if !exist {
                debug!("{}", {
                    let received = Utc::now().timestamp_micros();
                    format!(
                        "create_user: cache = {:?}, rq_date = {}, \
                             received = {}, delta = {} μs",
                        cache,
                        rq_date,
                        received,
                        received - rq_date
                    )
                });

                sync_tx
                    .send(SyncRequest::Both(
                        cache.users.get(user).unwrap().row_num,
                        rq_date,
                    ))
                    .await?;
                hf_tx.send(HFRequest::User(user.clone(), rq_date)).await?;
                return Ok(true);
            }
        }
        Err(e) => {
            debug!("create_user: error = {:?}", e);
            cache.remove_user(user);
            return Err(e);
        }
    }

    Ok(false)
}

async fn handle_event<F1, R1, F2, R2, F3, R3, F4, R4>(
    rq_date: TimeStamp,
    last_sync: TimeStamp,
    last_modified: TimeStamp,
    new_event: F1,
    skip_event: F2,
    sync_user: F3,
    unknown: F4,
) -> eyre::Result<()>
where
    F1: FnOnce() -> R1,
    R1: Future<Output = ()> + Send,
    F2: FnOnce() -> R2,
    R2: Future<Output = ()> + Send,
    F3: FnOnce() -> R3,
    R3: Future<Output = ()> + Send,
    F4: FnOnce() -> R4,
    R4: Future<Output = ()> + Send,
{
    match rq_date {
        t if t > last_modified => {
            // new event
            new_event().await;
        }
        t if t <= last_sync => {
            // skip event
            skip_event().await;
        }
        t if t > last_sync && t <= last_modified => {
            // remove user and add
            sync_user().await;
        }
        _ => {
            unknown().await;
        }
    }

    Ok(())
}

async fn supply<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (Supply, Sender<SyncRequest>, Sender<HFRequest>, TimeStamp),
) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    let (event, sync_tx, hf_tx, rq_date) = event;

    debug!("{}", {
        let received = Utc::now().timestamp_micros();
        format!(
            "supply: rq_date = {}, received = {}, delta = {} μs",
            rq_date,
            received,
            received - rq_date
        )
    });

    if create_user(
        rq_date,
        &cache,
        provider.clone(),
        &tokens,
        &event.onBehalfOf,
        &sync_tx,
        &hf_tx,
    )
    .await?
    {
        debug!(
            "supplied: new user created = {}, cache = {:?}",
            event.onBehalfOf, cache
        );

        return Ok(());
    }

    let user_settings = cache.users.get(&event.onBehalfOf).unwrap().clone();
    let row_num = user_settings.row_num;
    let idx = tokens.get(&event.reserve).unwrap().order;
    let now = Utc::now().timestamp_micros();

    let c = cache.clone();
    let sync_user = async move || {
        debug!("supply: sync_user user = {}", event.onBehalfOf);

        c.sync_user(&event.onBehalfOf, &tokens, provider)
            .await
            .unwrap();

        debug!("{}", {
            let received = Utc::now().timestamp_micros();
            format!(
                "sync_user: cache = {:?}, rq_date = {}, \
                             received = {}, delta = {} μs",
                c,
                rq_date,
                received,
                received - rq_date
            )
        });
    };

    if user_settings.use_as_collateral[idx] {
        let (last_sync, last_modified) = {
            let collateral_lock = cache.collateral.read().await;
            (collateral_lock.1, collateral_lock.2)
        };

        let cache = cache.clone();
        let new_event = async move || {
            debug!("supply: collateral new event user = {}", event.onBehalfOf);

            let mut collateral_lock = cache.collateral.write().await;
            (collateral_lock.1, collateral_lock.2) = (now, now);
            let mut row_lock = collateral_lock.0[row_num].write().await;
            row_lock[idx] += f64::from(event.amount);
            sync_tx
                .send(SyncRequest::Collateral(row_num, rq_date))
                .await
                .unwrap();
            hf_tx
                .send(HFRequest::User(event.onBehalfOf, rq_date))
                .await
                .unwrap();
        };
        let skip_event = async move || {
            debug!(
                "event dated before sync:\
                     event = supply, user = {}, rq_date = {}, collateral sync = {}, collateral rq_date = {}",
                event.onBehalfOf, rq_date, last_sync, last_modified,
            );
        };
        let unknown = async move || {
            info!(
                "detected unknown case:\
                     event = supply, user = {}, rq_date = {}, collateral sync = {}, collateral rq_date = {}",
                event.onBehalfOf, rq_date, last_sync, last_modified
            );
        };

        handle_event(
            rq_date,
            last_sync,
            last_modified,
            new_event,
            skip_event,
            sync_user,
            unknown,
        )
        .await?;
    } else {
        let (last_sync, last_modified) = {
            let reserve_lock = cache.reserve.read().await;
            (reserve_lock.1, reserve_lock.2)
        };

        let cache = cache.clone();
        let new_event = async move || {
            debug!("supply: borrowed new event user = {}", event.onBehalfOf);

            let mut reserve_lock = cache.reserve.write().await;
            (reserve_lock.1, reserve_lock.2) = (now, now);
            let mut row_lock = reserve_lock.0[row_num].write().await;
            row_lock[idx] += f64::from(event.amount);
        };
        let skip_event = async move || {
            debug!(
                "event dated before sync:\
                     event = supply, user = {}, rq_date = {}, reserve sync = {}, reserve rq_date = {}",
                event.onBehalfOf, rq_date, last_sync, last_modified,
            );
        };
        let unknown = async move || {
            info!(
                "detected unknown case:\
                     event = supply, user = {}, rq_date = {}, reserve sync = {}, reserve rq_date = {}",
                event.onBehalfOf, rq_date, last_sync, last_modified
            );
        };

        handle_event(
            rq_date,
            last_sync,
            last_modified,
            new_event,
            skip_event,
            sync_user,
            unknown,
        )
        .await?;
    }

    debug!("{}", {
        let received = Utc::now().timestamp_micros();
        format!(
            "supplied: cache = {:?}, rq_date = {}, \
                     received = {}, delta = {} μs",
            cache,
            rq_date,
            received,
            received - rq_date
        )
    });

    Ok(())
}

async fn withdraw<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (Withdraw, Sender<SyncRequest>, Sender<HFRequest>, TimeStamp),
) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    debug!("withdraw: called");
    // let (event, sync_tx, hf_tx, rq_date) = event;
    // cache
    //     .init_user(&event.user, &tokens, provider.clone())
    //     .await?;

    Ok(())
}

async fn borrow<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (Borrow, Sender<SyncRequest>, Sender<HFRequest>, TimeStamp),
) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    debug!("borrow: called");
    // let (event, sync_tx, hf_tx, rq_date) = event;
    // cache
    //     .init_user(&event.user, &tokens, provider.clone())
    //     .await?;

    Ok(())
}

async fn repay<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (Repay, Sender<SyncRequest>, Sender<HFRequest>, TimeStamp),
) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    debug!("repay: called");
    // let (event, sync_tx, hf_tx, rq_date) = event;
    // cache
    //     .init_user(&event.user, &tokens, provider.clone())
    //     .await?;

    Ok(())
}

async fn reserve_used_as_collateral_enabled<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (
        ReserveUsedAsCollateralEnabled,
        Sender<SyncRequest>,
        Sender<HFRequest>,
        TimeStamp,
    ),
) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    debug!("reserve_used_as_collateral_enabled: called");
    // let (event, sync_tx, hf_tx, rq_date) = event;
    // cache
    //     .init_user(&event.user, &tokens, provider.clone())
    //     .await?;

    Ok(())
}

async fn reserve_used_as_collateral_disabled<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (
        ReserveUsedAsCollateralDisabled,
        Sender<SyncRequest>,
        Sender<HFRequest>,
        TimeStamp,
    ),
) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    debug!("reserve_used_as_collateral_disabled: called");
    // let (event, sync_tx, hf_tx, rq_date) = event;
    // cache
    //     .init_user(&event.user, &tokens, provider.clone())
    //     .await?;

    Ok(())
}

async fn liquidation_call<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (
        LiquidationCall,
        Sender<SyncRequest>,
        Sender<HFRequest>,
        TimeStamp,
    ),
) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    debug!("liquidation_call: called");
    // let (event, sync_tx, hf_tx, rq_date) = event;
    // cache
    //     .init_user(&event.user, &tokens, provider.clone())
    //     .await?;

    Ok(())
}

async fn reserve_data_updated<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (ReserveDataUpdated, Sender<HFRequest>, TimeStamp),
) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    debug!("reserve_data_updated: called");
    // let (event, hf_tx, rq_date) = event;
    Ok(())
}

async fn answer_updated<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (AnswerUpdated, Address, Sender<HFRequest>, TimeStamp),
) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    debug!("answer_updated: called");
    // let (event, token, sync_tx, rq_date) = event;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Days;
    use std::str::FromStr;

    struct DummyDataProvider;

    impl DummyDataProvider {
        fn new() -> Self {
            Self {}
        }
    }

    #[async_trait]
    impl DataProvider for DummyDataProvider {
        async fn get_all_reserves_tokens(&self) -> eyre::Result<Vec<TokenData>> {
            todo!()
        }

        async fn get_source_of_asset(&self, token: &Address) -> eyre::Result<Address> {
            todo!()
        }

        async fn listen_events<F, Fut>(&self, callback: F) -> eyre::Result<()>
        where
            F: Fn(IL2PoolEvents) -> Fut + Send + 'static,
            Fut: Future<Output = eyre::Result<()>> + Send,
        {
            todo!()
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
            todo!()
        }

        async fn get_reserve_configuration_data(&self, token: &Address) -> eyre::Result<f64> {
            todo!()
        }

        async fn get_user_reserve_data(
            &self,
            token_address: &Address,
            _: &Address,
        ) -> eyre::Result<UserReserveData> {
            let urd = match token_address {
                t if *t == Address::from_str("0x1Ac54C113cefD1792CbFcF41B711824d657eb61D")? => {
                    UserReserveData::new(1.0, 1.0, false)
                }
                t if *t == Address::from_str("0x1Af54C113cefD1792CbFcF41B711834d657ea61D")? => {
                    UserReserveData::new(2.0, 2.0, true)
                }
                t if *t == Address::from_str("0x1Af54C113cefD1792CbFcF41B711824d657eb61D")? => {
                    UserReserveData::new(3.0, 3.0, false)
                }
                _ => UserReserveData::new(4.0, 4.0, true),
            };

            Ok(urd)
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
            ),
        );
        tokens.insert(
            Address::from_str("0x1Af54C113cefD1792CbFcF41B711834d657ea61D")?,
            TokenDetails::new(
                String::from("USDC"),
                Address::from_str("0xaf88d065e77c8cC2239327C5EDb3A432268e5831")?,
                1,
            ),
        );
        tokens.insert(
            Address::from_str("0x1Af54C113cefD1792CbFcF41B711824d657eb61D")?,
            TokenDetails::new(
                String::from("DAI"),
                Address::from_str("0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1")?,
                2,
            ),
        );

        if user_num > 0 {
            let mut user_addr = Address::from_str("0x1Af54C553cefD1792CbFcF41B711834d657ea61D")?;
            for i in 0..user_num {
                cache.users.insert(
                    user_addr,
                    UserSettings::new(i, BitVec::<usize, Lsb0>::from_iter([true, false, false])),
                );
                user_addr = user_addr.create((i + 1) as u64);
            }
        }

        {
            let (collaterals, _, _) = &mut *cache.collateral.write().await;
            collaterals.push(RwLock::new(Array1::from_vec(vec![0.0; 3])));

            let (reserves, _, _) = &mut *cache.reserve.write().await;
            reserves.push(RwLock::new(Array1::from_vec(vec![0.0; 3])));

            let (borroweds, _, _) = &mut *cache.borrowed.write().await;
            borroweds.push(RwLock::new(Array1::from_vec(vec![0.0; 3])));
        }

        Ok((cache, tokens))
    }

    async fn get_all_user_data(
        cache: &Cache,
        user_row_num: usize,
    ) -> eyre::Result<(Vec<f64>, Vec<f64>, Vec<f64>)> {
        let (collateral, reserve, borrowed) = {
            let (collaterals, _, _) = &*cache.collateral.read().await;
            let collateral = &*collaterals.get(user_row_num).unwrap().read().await;

            let (reserves, _, _) = &*cache.reserve.read().await;
            let reserve = &*reserves.get(user_row_num).unwrap().read().await;

            let (borroweds, _, _) = &*cache.borrowed.read().await;
            let borrowed = &*borroweds.get(user_row_num).unwrap().read().await;

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
        let row = vec![1.0, 2.0, 3.0];
        let row_len = row.len();
        {
            let now = Utc::now().timestamp_micros();
            *cache.collateral.write().await =
                (vec![RwLock::new(Array1::from_vec(row.clone()))], now, now);
            *cache.collateral_matrix.write().await = Array2::from_elem((1, row_len), 0.);
        }

        cache.sync_collateral(0, rq_date).await?;

        let mut expected = Array2::from_shape_vec((1, row_len), row)?;
        {
            let collateral_matrix = &*cache.collateral_matrix.read().await;
            assert_eq!(collateral_matrix, expected);
        }

        let row = vec![4.0, 5.0, 6.0];
        {
            let (collateral, _, _) = &mut *cache.collateral.write().await;
            collateral.push(RwLock::new(Array1::from_vec(row.clone())));
        }

        cache.sync_collateral(1, rq_date).await?;

        expected.push_row(Array1::from(row).view())?;
        {
            let collateral_matrix = &*cache.collateral_matrix.read().await;
            assert_eq!(collateral_matrix, expected);
        }

        let row = vec![7.0, 8.0, 9.0];
        {
            let (collateral, _, _) = &mut *cache.collateral.write().await;
            collateral.push(RwLock::new(Array1::from_vec(row.clone())));
        }

        cache.sync_collateral(2, rq_date).await?;

        expected.push_row(Array1::from(row).view())?;
        {
            let collateral_matrix = &*cache.collateral_matrix.read().await;
            assert_eq!(collateral_matrix, expected);
        }

        {
            let (collateral, _, _) = &mut *cache.collateral.write().await;
            let mut col_row = collateral.get(1).unwrap().write().await;
            col_row[0] = 40.0;
            col_row[1] = 50.0;
            col_row[2] = 60.0;
        }

        cache.sync_collateral(1, rq_date).await?;

        expected[(1, 0)] = 40.0;
        expected[(1, 1)] = 50.0;
        expected[(1, 2)] = 60.0;

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
        let row = vec![1.0, 2.0, 3.0];
        let row_len = row.len();
        {
            let now = Utc::now().timestamp_micros();
            *cache.borrowed.write().await =
                (vec![RwLock::new(Array1::from_vec(row.clone()))], now, now);
            *cache.borrowed_matrix.write().await = Array2::from_elem((1, row_len), 0.);
        }

        cache.sync_borrowed(0, rq_date).await?;

        let mut expected = Array2::from_shape_vec((1, row_len), row)?;
        {
            let borrowed_matrix = &*cache.borrowed_matrix.read().await;
            assert_eq!(borrowed_matrix, expected);
        }

        let row = vec![4.0, 5.0, 6.0];
        {
            let (borrowed, _, _) = &mut *cache.borrowed.write().await;
            borrowed.push(RwLock::new(Array1::from_vec(row.clone())));
        }

        cache.sync_borrowed(1, rq_date).await?;

        expected.push_row(Array1::from(row).view())?;
        {
            let borrowed_matrix = &*cache.borrowed_matrix.read().await;
            assert_eq!(borrowed_matrix, expected);
        }

        let row = vec![7.0, 8.0, 9.0];
        {
            let (borrowed, _, _) = &mut *cache.borrowed.write().await;
            borrowed.push(RwLock::new(Array1::from_vec(row.clone())));
        }

        cache.sync_borrowed(2, rq_date).await?;

        expected.push_row(Array1::from(row).view())?;
        {
            let borrowed_matrix = &*cache.borrowed_matrix.read().await;
            assert_eq!(borrowed_matrix, expected);
        }

        {
            let (borrowed, _, _) = &mut *cache.borrowed.write().await;
            let mut bor_row = borrowed.get(1).unwrap().write().await;
            bor_row[0] = 40.0;
            bor_row[1] = 50.0;
            bor_row[2] = 60.0;
        }

        cache.sync_borrowed(1, rq_date).await?;

        expected[(1, 0)] = 40.0;
        expected[(1, 1)] = 50.0;
        expected[(1, 2)] = 60.0;

        {
            let borrowed_matrix = &*cache.borrowed_matrix.read().await;
            assert_eq!(borrowed_matrix, expected);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_supply() -> eyre::Result<()> {
        let cache = Arc::new(Cache::default());
        let dummy_data_provider = Arc::new(DummyDataProvider::new());
        let token = Address::from_str("0x1Af54C263cefD1792CbFcF41B711834d657ea61D")?;
        let user = Address::from_str("0x1Af54C263cefD1792CbFcF41B722834d657ea61D")?;

        {
            cache.users.insert(
                user.clone(),
                UserSettings::new(0, BitVec::<usize, Lsb0>::from_iter([true, false, false])),
            );
            let (collaterals, _, _) = &mut *cache.collateral.write().await;
            collaterals.push(RwLock::new(Array1::from_vec(vec![0.0; 3])));
        }

        let mut tokens = HashMap::new();
        tokens.insert(
            token.clone(),
            TokenDetails::new(
                String::from("AAVE"),
                Address::from_str("0xba5DdD1f9d7F570dc94a51479a000E3BCE967196")?,
                0,
            ),
        );
        tokens.insert(
            Address::from_str("0x1Af54C113cefD1792CbFcF41B711834d657ea61D")?,
            TokenDetails::new(
                String::from("USDC"),
                Address::from_str("0xaf88d065e77c8cC2239327C5EDb3A432268e5831")?,
                1,
            ),
        );
        tokens.insert(
            Address::from_str("0x1Af54C113cefD1792CbFcF41B711824d657eb61D")?,
            TokenDetails::new(
                String::from("DAI"),
                Address::from_str("0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1")?,
                2,
            ),
        );
        let tokens = Arc::new(tokens);

        let event = Supply {
            reserve: token.clone(),
            user: user.clone(),
            onBehalfOf: user.clone(),
            amount: alloy_primitives::U256::from(10.0),
            referralCode: 0,
        };
        let (sync_tx, mut sync_rc) = channel::<SyncRequest>(1);
        let (hf_tx, mut hf_rc) = channel::<HFRequest>(1);
        let rq_date = Utc::now().timestamp_micros();

        let sync_handler = task::spawn(async move {
            while let Some(msg) = sync_rc.recv().await {
                assert_eq!(SyncRequest::Collateral(0, rq_date), msg);
            }
        });

        let hf_handler = task::spawn(async move {
            while let Some(msg) = hf_rc.recv().await {
                assert_eq!(HFRequest::User(user.clone(), rq_date), msg);
            }
        });

        // 1 case: new message containing collateral

        supply(
            cache.clone(),
            dummy_data_provider.clone(),
            tokens.clone(),
            (event, sync_tx, hf_tx, rq_date),
        )
        .await?;
        sync_handler.await?;
        hf_handler.await?;

        {
            let (collaterals, _, _) = &*cache.collateral.read().await;
            let collateral = &*collaterals.get(0).unwrap().read().await;

            assert_eq!(collateral, Array1::from_vec(vec![10.0, 0.0, 0.0]));
        }

        // 2 case: new message containing reserve

        let token = Address::from_str("0x1Af54C113cefD1792CbFcF41B711834d657ea61D")?;
        let user = Address::from_str("0x1Af54C553cefD1792CbFcF41B711834d657ea61D")?;

        {
            cache.users.insert(
                user.clone(),
                UserSettings::new(1, BitVec::<usize, Lsb0>::from_iter([false, false, false])),
            );
            let (reserves, _, _) = &mut *cache.reserve.write().await;
            reserves.push(RwLock::new(Array1::from_vec(vec![0.0; 3])));
            reserves.push(RwLock::new(Array1::from_vec(vec![0.0; 3])));
        }

        let event = Supply {
            reserve: token.clone(),
            user: user.clone(),
            onBehalfOf: user.clone(),
            amount: alloy_primitives::U256::from(3.0),
            referralCode: 0,
        };

        let (sync_tx, _) = channel::<SyncRequest>(1);
        let (hf_tx, _) = channel::<HFRequest>(1);

        supply(
            cache.clone(),
            dummy_data_provider.clone(),
            tokens.clone(),
            (event, sync_tx, hf_tx, rq_date),
        )
        .await?;

        {
            let (reserves, _, _) = &*cache.reserve.read().await;
            let reserve = &*reserves.get(1).unwrap().read().await;

            assert_eq!(reserve, Array1::from_vec(vec![0.0, 3.0, 0.0]));
        }

        // 3 case: new message skip event

        let token = Address::from_str("0x1Af54C113cefD1792CbFcF41B711824d657eb61D")?;
        let user = Address::from_str("0x1Af54C263cefD1792CbFcF41B722834d611ea61D")?;

        {
            cache.users.insert(
                user.clone(),
                UserSettings::new(2, BitVec::<usize, Lsb0>::from_iter([false, false, true])),
            );
            let (collaterals, last_sync, _) = &mut *cache.collateral.write().await;
            collaterals.push(RwLock::new(Array1::from_vec(vec![0.0; 3])));
            collaterals.push(RwLock::new(Array1::from_vec(vec![0.0; 3])));

            *last_sync = Utc::now().timestamp_micros();
        }

        let event = Supply {
            reserve: token.clone(),
            user: user.clone(),
            onBehalfOf: user.clone(),
            amount: alloy_primitives::U256::from(3.0),
            referralCode: 0,
        };

        let (sync_tx, _) = channel::<SyncRequest>(1);
        let (hf_tx, _) = channel::<HFRequest>(1);

        supply(
            cache.clone(),
            dummy_data_provider.clone(),
            tokens.clone(),
            (event, sync_tx, hf_tx, rq_date),
        )
        .await?;

        {
            let (collaterals, _, _) = &*cache.collateral.read().await;
            let collateral = &*collaterals.get(1).unwrap().read().await;

            assert_eq!(collateral, Array1::from_vec(vec![0.0, 0.0, 0.0]));
        }

        // 4 case: new message sync user

        let rq_date = Utc::now()
            .checked_sub_days(Days::new(1))
            .unwrap()
            .timestamp_micros();
        {
            cache.users.insert(
                user.clone(),
                UserSettings::new(2, BitVec::<usize, Lsb0>::from_iter([false, false, true])),
            );

            let (reserves, _, _) = &mut *cache.reserve.write().await;
            reserves.push(RwLock::new(Array1::from_vec(vec![0.0; 3])));

            let (borrowed, _, _) = &mut *cache.borrowed.write().await;
            borrowed.push(RwLock::new(Array1::from_vec(vec![0.0; 3])));
            borrowed.push(RwLock::new(Array1::from_vec(vec![0.0; 3])));
            borrowed.push(RwLock::new(Array1::from_vec(vec![0.0; 3])));

            let (_, last_sync, last_modified) = &mut *cache.collateral.write().await;
            *last_sync = Utc::now()
                .checked_sub_days(Days::new(2))
                .unwrap()
                .timestamp_micros();
            *last_modified = Utc::now().timestamp_micros();
        }

        let event = Supply {
            reserve: token.clone(),
            user: user.clone(),
            onBehalfOf: user.clone(),
            amount: alloy_primitives::U256::from(3.0),
            referralCode: 0,
        };

        let (sync_tx, mut sync_rc) = channel::<SyncRequest>(1);
        let (hf_tx, mut hf_rc) = channel::<HFRequest>(1);

        let sync_handler = task::spawn(async move {
            while let Some(msg) = sync_rc.recv().await {
                assert_eq!(SyncRequest::Both(2, rq_date), msg);
            }
        });

        let hf_handler = task::spawn(async move {
            while let Some(msg) = hf_rc.recv().await {
                assert_eq!(HFRequest::User(user.clone(), rq_date), msg);
            }
        });

        supply(
            cache.clone(),
            dummy_data_provider.clone(),
            tokens.clone(),
            (event, sync_tx, hf_tx, rq_date),
        )
        .await?;
        sync_handler.await?;
        hf_handler.await?;

        let (collateral, reserve, borrowed) = {
            let (collaterals, _, _) = &*cache.collateral.read().await;
            let col_futs = collaterals.iter().map(|c| async {
                let row = c.read().await;
                Array1::to_vec(&*row)
            });
            let col = join_all(col_futs).await;

            let (reserves, _, _) = &*cache.reserve.read().await;
            let res_futs = reserves.iter().map(|c| async {
                let row = c.read().await;
                Array1::to_vec(&*row)
            });
            let res = join_all(res_futs).await;

            let (borroweds, _, _) = &*cache.borrowed.read().await;
            let bor_futs = borroweds.iter().map(|c| async {
                let row = c.read().await;
                Array1::to_vec(&*row)
            });
            let bor = join_all(bor_futs).await;

            (col, res, bor)
        };

        println!("collateral: {:?}", collateral);
        println!("reserve: {:?}", reserve);
        println!("borrowed: {:?}", borrowed);

        assert_eq!(cache.contains(&user), true);

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_user() -> eyre::Result<()> {
        let dummy_data_provider = Arc::new(DummyDataProvider::new());
        let (cache, tokens) = generate_cache_and_tokens(1).await?;
        let user = cache.users.iter().next().unwrap().key().clone();

        cache
            .sync_user(&user, &tokens, dummy_data_provider.clone())
            .await?;

        assert_eq!(cache.contains(&user), true);

        let (collateral, reserve, borrowed) = get_all_user_data(&cache, 0).await?;

        assert_eq!(collateral, vec![0.0, 2.0, 0.0]);
        assert_eq!(reserve, vec![1.0, 0.0, 3.0]);
        assert_eq!(borrowed, vec![1.0, 2.0, 3.0]);

        Ok(())
    }

    #[tokio::test]
    async fn test_init_user() -> eyre::Result<()> {
        // 1 case - cache has this user

        let dummy_data_provider = Arc::new(DummyDataProvider::new());
        let (cache, tokens) = generate_cache_and_tokens(1).await?;
        let user = cache.users.iter().next().unwrap().key().clone();

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

        assert_eq!(collateral, vec![0.0, 2.0, 0.0]);
        assert_eq!(reserve, vec![1.0, 0.0, 3.0]);
        assert_eq!(borrowed, vec![1.0, 2.0, 3.0]);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_user_data() -> eyre::Result<()> {
        let dummy_data_provider = Arc::new(DummyDataProvider::new());
        let (cache, tokens) = generate_cache_and_tokens(1).await?;
        let user = cache.users.iter().next().unwrap().key().clone();

        let (collateral, reserve, borrowed) = cache
            .get_user_data(dummy_data_provider, &tokens, &user)
            .await?;

        assert_eq!(cache.contains(&user), true);
        assert_eq!(cache.users.len(), 1);

        assert_eq!(collateral, vec![0.0, 2.0, 0.0]);
        assert_eq!(reserve, vec![1.0, 0.0, 3.0]);
        assert_eq!(borrowed, vec![1.0, 2.0, 3.0]);

        Ok(())
    }

    #[tokio::test]
    async fn test_contains() -> eyre::Result<()> {
        let (cache, _) = generate_cache_and_tokens(1).await?;
        let user = cache.users.iter().next().unwrap().key().clone();

        assert_eq!(cache.contains(&user), true);

        Ok(())
    }

    #[tokio::test]
    async fn test_remove_user() -> eyre::Result<()> {
        let (cache, _) = generate_cache_and_tokens(1).await?;
        let user = cache.users.iter().next().unwrap().key().clone();
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
        let cb = move |c: Arc<Cache>, p, t, Message(text)| {
            let test_message = test_message.clone();
            async move {
                assert_eq!(text, test_message);

                let (collaterals, _, _) = &*c.collateral.write().await;
                let mut col = collaterals.get(0).unwrap().write().await;
                *col = Array1::from(vec![7.0, 7.0, 7.0]);

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

        senders.get(0).unwrap().send(Message(msg)).await?;

        tokio::time::sleep(Duration::from_secs(1)).await;

        let (collaterals, _, _) = &*cache.collateral.read().await;
        let col = collaterals.get(0).unwrap().write().await;
        assert_eq!(*col, Array1::from(vec![7.0, 7.0, 7.0]));

        Ok(())
    }

    #[tokio::test]
    async fn test_calc_hf() -> eyre::Result<()> {
        let dummy_data_provider = Arc::new(DummyDataProvider::new());
        let (cache, tokens) = generate_cache_and_tokens(1).await?;
        let user = cache.users.iter().next().unwrap().key().clone();

        Ok(())
    }
}
