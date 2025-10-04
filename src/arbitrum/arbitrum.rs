use crate::arbitrum::arbitrum::IAaveOracle::IAaveOracleInstance;
use crate::arbitrum::arbitrum::IAaveProtocolDataProvider::{
    IAaveProtocolDataProviderInstance, TokenData, getReserveDataReturn, getUserReserveDataReturn,
};
use crate::arbitrum::arbitrum::IL2Pool::{
    IL2PoolEvents, IL2PoolInstance, getUserAccountDataReturn,
};
use crate::arbitrum::events::{
    borrow, liquidation_call, repay, reserve_data_updated, reserve_used_as_collateral_disabled,
    reserve_used_as_collateral_enabled, supply, withdraw,
};
use alloy::primitives::{Address, Log};
use alloy::providers::Provider;
use alloy::rpc::types::Filter;
use alloy::sol;
use alloy::sol_types::SolEventInterface;
use alloy_primitives::aliases::U40;
use alloy_primitives::{I256, Sign, U256, U512};
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
use tokio::{task, time, try_join};
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

        function getReserveData(address asset) external view override returns (
            uint256 unbacked,
            uint256 accruedToTreasuryScaled,
            uint256 totalAToken,
            uint256 totalStableDebt,
            uint256 totalVariableDebt,
            uint256 liquidityRate,
            uint256 variableBorrowRate,
            uint256 stableBorrowRate,
            uint256 averageStableBorrowRate,
            uint256 liquidityIndex,
            uint256 variableBorrowIndex,
            uint40 lastUpdateTimestamp
        );
    }

    #[sol(rpc)]
    interface IAaveOracle {
        function getSourceOfAsset(address asset) external view returns (address);
        function getAssetsPrices(address[] calldata assets) external view override returns (uint256[] memory);
        function getAssetPrice(address asset) public view override returns (uint256);
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
    async fn get_reserve_configuration_data(&self, token: &Address) -> eyre::Result<f64>;
    async fn get_user_reserve_data(
        &self,
        token: &Address,
        user: &Address,
    ) -> eyre::Result<UserReserveData>;
    async fn get_reserve_data(&self, token: &Address) -> eyre::Result<ReserveData>;
    async fn get_decimals(&self, token: &Address) -> eyre::Result<f64>;
    async fn get_price_decimals(&self, price_source: &Address) -> eyre::Result<f64>;
    async fn get_asset_prices(&self, tokens: Vec<Address>) -> eyre::Result<Vec<U256>>;
    async fn get_asset_price(&self, token: &Address) -> eyre::Result<U256>;
    async fn listen_prices_update<F, Fut>(
        &self,
        tokens: &Vec<Address>,
        price_decimals: &Vec<f64>,
        callback: F,
    ) -> eyre::Result<()>
    where
        F: Fn(Vec<f64>) -> Fut + Send + 'static,
        Fut: Future<Output = eyre::Result<()>> + Send;
    async fn get_user_account_data(&self, user: &Address) -> eyre::Result<UserAccountData>;
}

pub struct UserAccountData {
    pub total_collateral_base: U256,
    pub total_debt_base: U256,
    pub available_borrows_base: U256,
    pub current_liquidation_threshold: U256,
    pub ltv: U256,
    pub health_factor: U256,
}

impl UserAccountData {
    pub fn new(
        total_collateral_base: U256,
        total_debt_base: U256,
        available_borrows_base: U256,
        current_liquidation_threshold: U256,
        ltv: U256,
        health_factor: U256,
    ) -> Self {
        Self {
            total_collateral_base,
            total_debt_base,
            available_borrows_base,
            current_liquidation_threshold,
            ltv,
            health_factor,
        }
    }
}

pub struct ReserveData {
    pub liquidity_rate: U256,
    pub variable_borrow_rate: U256,
    pub liquidity_index: U256,
    pub variable_borrow_index: U256,
    pub last_update_timestamp: U40,
}

impl ReserveData {
    pub fn new(
        liquidity_rate: U256,
        variable_borrow_rate: U256,
        liquidity_index: U256,
        variable_borrow_index: U256,
        last_update_timestamp: U40,
    ) -> Self {
        Self {
            liquidity_rate,
            variable_borrow_rate,
            liquidity_index,
            variable_borrow_index,
            last_update_timestamp,
        }
    }
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
    pub(in crate::arbitrum) aave_l2_pool: IL2PoolInstance<P>,
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

        let aave_l2_pool = IL2Pool::new(L2_POOL_ADDRESS.parse()?, provider.clone());

        Ok(Self {
            aave_protocol_data_provider,
            aave_oracle,
            aave_l2_pool,
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

    async fn get_reserve_data(&self, token: &Address) -> eyre::Result<ReserveData> {
        let getReserveDataReturn {
            liquidityRate: liquidity_rate,
            variableBorrowRate: variable_borrow_rate,
            liquidityIndex: liquidity_index,
            variableBorrowIndex: variable_borrow_index,
            lastUpdateTimestamp: last_update_timestamp,
            ..
        } = self
            .aave_protocol_data_provider
            .getReserveData(token.clone())
            .call()
            .await?;

        Ok(ReserveData::new(
            liquidity_rate,
            variable_borrow_rate,
            liquidity_index,
            variable_borrow_index,
            last_update_timestamp,
        ))
    }

    async fn get_decimals(&self, token: &Address) -> eyre::Result<f64> {
        let decimals = IERC20Metadata::new(token.clone(), &self.provider)
            .decimals()
            .call()
            .await?;

        Ok(10_f64.powi(decimals as i32))
    }

    async fn get_price_decimals(&self, price_source: &Address) -> eyre::Result<f64> {
        let decimals = IChainlinkAggregator::new(price_source.clone(), &self.provider)
            .decimals()
            .call()
            .await?;

        Ok(10_f64.powi(decimals as i32))
    }

    async fn get_asset_prices(&self, tokens: Vec<Address>) -> eyre::Result<Vec<U256>> {
        let prices = self.aave_oracle.getAssetsPrices(tokens).call().await?;

        Ok(prices)
    }

    async fn get_asset_price(&self, token: &Address) -> eyre::Result<U256> {
        let price = self.aave_oracle.getAssetPrice(token.clone()).call().await?;

        Ok(price)
    }

    async fn listen_prices_update<F, Fut>(
        &self,
        tokens: &Vec<Address>,
        price_decimals: &Vec<f64>,
        callback: F,
    ) -> eyre::Result<()>
    where
        F: Fn(Vec<f64>) -> Fut + Send + 'static,
        Fut: Future<Output = eyre::Result<()>> + Send,
    {
        let mut block_stream = self.provider.subscribe_blocks().await?;
        while let Ok(_) = block_stream.recv().await {
            let prices = self
                .get_asset_prices(tokens.clone())
                .await?
                .iter()
                .enumerate()
                .map(|(idx, price)| price.as_f64(price_decimals[idx]))
                .collect();
            callback(prices).await?;
        }

        Ok(())
    }

    async fn get_user_account_data(&self, user: &Address) -> eyre::Result<UserAccountData> {
        let getUserAccountDataReturn {
            totalCollateralBase: total_collateral_base,
            totalDebtBase: total_debt_base,
            availableBorrowsBase: available_borrows_base,
            currentLiquidationThreshold: current_liquidation_threshold,
            ltv,
            healthFactor: health_factor,
        } = self
            .aave_l2_pool
            .getUserAccountData(user.clone())
            .call()
            .await?;

        Ok(UserAccountData::new(
            total_collateral_base,
            total_debt_base,
            available_borrows_base,
            current_liquidation_threshold,
            ltv,
            health_factor,
        ))
    }
}

pub(crate) type Tokens = HashMap<Address, TokenDetails>;

#[derive(Debug)]
pub(crate) struct TokenDetails {
    pub(crate) name: String,
    pub(crate) price_source: Address,
    pub(crate) order: usize,
    pub(crate) decimals: f64,
    pub(crate) price_decimals: f64,
}

impl TokenDetails {
    pub(in crate::arbitrum) fn new(
        name: String,
        price_source: Address,
        order: usize,
        decimals: f64,
        price_decimals: f64,
    ) -> Self {
        Self {
            name,
            price_source,
            order,
            decimals,
            price_decimals,
        }
    }
}

pub(crate) struct RqDate(pub(crate) TimeStamp);

pub async fn start<P>(cache: Arc<Cache>, provider: Arc<P>) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    let tokens = Arc::new(setup(provider.clone()).await?);

    debug!("start: tokens = {:?}", tokens);

    cache.init(provider.clone(), &tokens).await?;

    let (tx_events, mut rc_events) = channel::<AaveEvents>(1000_000);
    listen_events(provider.clone(), tx_events.clone()).await?;

    let w_num = 4;
    let bound = 1000;
    let lq_lookup_tx = liquidation_lookup(
        cache.clone(),
        liquidation(cache.clone(), w_num, bound).await?,
        bound,
    )
    .await?;
    let (mut sync_counter, sync_senders) = (0, listen_sync(cache.clone(), w_num, bound).await?);
    let (mut hf_counter, hf_senders) = (
        0,
        listen_hf_calc(cache.clone(), lq_lookup_tx, w_num, bound).await?,
    );

    liquidation_threshold_update(
        cache.clone(),
        tokens.clone(),
        provider.clone(),
        hf_senders[hf_counter % w_num].clone(),
    )
    .await?;
    hf_counter = hf_counter.wrapping_add(1);

    listen_prices_update(
        provider.clone(),
        cache.clone(),
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
        let (asset_source, decimals) = try_join!(
            provider.get_source_of_asset(&token.tokenAddress),
            provider.get_decimals(&token.tokenAddress),
        )?;

        let price_decimals = provider.get_price_decimals(&asset_source).await?;

        debug!(
            "setup: token symbol = {}, token address = {}, decimals = {}, price_source = {}, price_decimals = {}",
            token.symbol, token.tokenAddress, decimals, asset_source, price_decimals
        );

        tokens.insert(
            token.tokenAddress,
            TokenDetails::new(token.symbol, asset_source, order, decimals, price_decimals),
        );
        order += 1;
    }

    Ok(tokens)
}

pub type TimeStamp = i64;

pub(crate) enum AaveEvents {
    IL2PoolEvents(IL2PoolEvents, TimeStamp),
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
                        if let Err(e) = tx
                            .send(AaveEvents::IL2PoolEvents(
                                data,
                                Utc::now().timestamp_micros(),
                            ))
                            .await
                        {
                            error!("listen_events: failed to send to event channel: {:?}", e);
                        }

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

pub(crate) async fn listen_prices_update<P>(
    provider: Arc<P>,
    cache: Arc<Cache>,
    hf_tx: Sender<HFRequest>,
) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    task::spawn(async move {
        debug!("listen_prices_update: created thread");

        let tokens = {
            let (tokens, _) = &*cache.tokens.read().await;
            &tokens.to_vec()
        };
        let price_decimals = {
            let (price_decimals, _) = &*cache.price_decimals.read().await;
            &price_decimals.to_vec()
        };

        loop {
            let hf_tx = hf_tx.clone();
            let cache = cache.clone();
            match provider
                .listen_prices_update(&tokens, &price_decimals, move |prices| {
                    let hf_tx = hf_tx.clone();
                    let cache = cache.clone();
                    async move {
                        let now = Utc::now().timestamp_micros();

                        {
                            let (prices_current, _) = &mut *cache.prices.write().await;
                            let prices = Array1::from_vec(prices);

                            if *prices_current != prices {
                                *prices_current = prices;

                                if let Err(e) = hf_tx.send(HFRequest::Full(now)).await {
                                        error!(
                                        "listen_prices_update: failed to send to hf calculation channel: {:?}",
                                        e
                                    );
                                }
                            }
                        }

                        Ok(())
                    }
                })
                .await
            {
                Ok(_) => debug!("listen_prices_update: Ok"),
                Err(e) => debug!("listen_prices_update: error = {:?}", e),
            }
        }
    });

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

        if let Err(e) = hf_tx.send(HFRequest::Full(rq_date)).await {
            error!(
                "liquidation_threshold_update: failed to send to hf calculation channel: {:?}",
                e
            );
        }
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
    for worker in 0..workers {
        let (tx, mut rc) = channel::<SyncRequest>(bound);
        let cache = cache.clone();
        task::spawn(async move {
            debug!("listen_sync (worker = {}): created thread", worker);

            match listen_sync_handler(&cache, &mut rc).await {
                Ok(_) => debug!("listen_sync (worker = {}): Ok", worker),
                Err(e) => debug!("listen_sync (worker = {}): error = {:?}", worker, e),
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
    lq_lookup_tx: Sender<()>,
    workers: usize,
    bound: usize,
) -> eyre::Result<Vec<Sender<HFRequest>>> {
    let mut senders = Vec::with_capacity(workers);
    for worker in 0..workers {
        let (tx, mut rc) = channel::<HFRequest>(bound);
        let (cache, lq_lookup_tx) = (cache.clone(), lq_lookup_tx.clone());
        task::spawn(async move {
            debug!("listen_hf_calc (worker = {}): created thread", worker);

            match listen_hf_calc_handler(&cache, &mut rc, &lq_lookup_tx).await {
                Ok(_) => {
                    debug!("listen_hf_calc (worker = {}): Ok", worker);
                }
                Err(e) => error!("listen_hf_calc (worker = {}): error = {:?}", worker, e),
            }
        });
        senders.push(tx);
    }

    Ok(senders)
}

async fn listen_hf_calc_handler(
    cache: &Cache,
    rc: &mut Receiver<HFRequest>,
    lq_lookup_tx: &Sender<()>,
) -> eyre::Result<()> {
    while let Some(hf_rq) = rc.recv().await {
        match hf_rq {
            HFRequest::User(user, rq_date) => cache.calc_hf(Some(&user), rq_date).await?,
            HFRequest::Full(rq_date) => cache.calc_hf(None, rq_date).await?,
        }

        if let Err(e) = lq_lookup_tx.send(()).await {
            error!(
                "listen_hf_calc_handler: failed to send to liquidation lookup channel: {:?}",
                e
            );
        }
    }

    Ok(())
}

pub(crate) async fn liquidation_lookup(
    cache: Arc<Cache>,
    lq_txs: Vec<Sender<usize>>,
    bound: usize,
) -> eyre::Result<Sender<()>> {
    let (lq_lookup_tx, mut lq_lookup_rc) = channel::<()>(bound);
    task::spawn(async move {
        debug!("liquidation_lookup: check if we have any liquidation opportunities");

        let (workers, mut counter) = (lq_txs.len(), 0);
        while let Some(_) = lq_lookup_rc.recv().await {
            let (hf, _) = &*cache.health_factors.read().await;
            for (i, &v) in hf.iter().enumerate() {
                if v < 1.0 {
                    let worker = counter % workers;
                    let lq_tx = lq_txs[worker].clone();
                    if let Err(e) = lq_tx.send(i).await {
                        error!(
                            "liquidation_lookup: failed to send index {} by worker = {}: {:?}",
                            i, worker, e
                        );
                    }
                    counter = counter.wrapping_add(1);
                }
            }
        }
    });

    Ok(lq_lookup_tx)
}

pub(crate) async fn liquidation(
    cache: Arc<Cache>,
    workers: usize,
    bound: usize,
) -> eyre::Result<Vec<Sender<usize>>> {
    let mut senders = Vec::with_capacity(workers);

    for worker in 0..workers {
        let (lq_tx, mut lq_rc) = channel::<usize>(bound);
        let cache = cache.clone();
        task::spawn(async move {
            debug!("liquidation (worker = {}): waiting for liquidation", worker);

            while let Some(i) = lq_rc.recv().await {
                let (hf, _) = &*cache.health_factors.read().await;
                info!(
                    "liquidation (worker = {}): index {}, hf = {}",
                    worker, i, hf[i]
                );
            }

            debug!("liquidation (worker = {}): channel closed", worker);
        });
        senders.push(lq_tx);
    }

    Ok(senders)
}

pub type UserDetails = DashMap<Address, UserSettings>;
pub type Array = RwLock<(Array1<f64>, TimeStamp)>;
pub type Arrays = RwLock<Vec<RwLock<(Array1<U256>, TimeStamp, TimeStamp)>>>;
pub type Matrix = RwLock<Array2<f64>>;
pub type Indexes = RwLock<(Array1<Index>, TimeStamp)>;
pub type Addresses = RwLock<(Array1<Address>, TimeStamp)>;

#[derive(Default, Debug, Clone)]
pub struct Index {
    pub index: U256,
    pub rate: U256,
    pub last_update: TimeStamp,
}

impl Index {
    pub fn new(index: U256, rate: U256, last_update: TimeStamp) -> Self {
        Self {
            index,
            rate,
            last_update,
        }
    }
}

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

#[derive(Debug)]
pub(in crate::arbitrum) struct UserData {
    pub(in crate::arbitrum) reserve_scaled: Vec<U256>,
    pub(in crate::arbitrum) collateral_scaled: Vec<U256>,
    pub(in crate::arbitrum) borrowed_scaled: Vec<U256>,
    pub(in crate::arbitrum) liquidity_indexes: Vec<U256>,
    pub(in crate::arbitrum) liquidity_rates: Vec<U256>,
    pub(in crate::arbitrum) variable_borrow_indexes: Vec<U256>,
    pub(in crate::arbitrum) variable_borrow_rates: Vec<U256>,
    pub(in crate::arbitrum) last_update_timestamps: Vec<U40>,
    pub(in crate::arbitrum) user_settings: UserSettings,
}

impl UserData {
    pub fn new(
        reserve_scaled: Vec<U256>,
        collateral_scaled: Vec<U256>,
        borrowed_scaled: Vec<U256>,
        liquidity_indexes: Vec<U256>,
        liquidity_rates: Vec<U256>,
        variable_borrow_indexes: Vec<U256>,
        variable_borrow_rates: Vec<U256>,
        last_update_timestamps: Vec<U40>,
        user_settings: UserSettings,
    ) -> Self {
        Self {
            reserve_scaled,
            collateral_scaled,
            borrowed_scaled,
            liquidity_indexes,
            liquidity_rates,
            variable_borrow_indexes,
            variable_borrow_rates,
            last_update_timestamps,
            user_settings,
        }
    }
}

#[derive(Default, Debug)]
pub struct Cache {
    pub users: UserDetails,
    pub users_num: RwLock<usize>,
    pub decimals: Array,

    pub tokens: Addresses,
    pub price_decimals: Array,

    pub reserve: Arrays,
    pub collateral: Arrays,
    pub collateral_matrix: Matrix,

    pub borrowed: Arrays,
    pub borrowed_matrix: Matrix,

    pub liquidity: Indexes,
    pub liquidity_index: Array,
    pub variable_borrow: Indexes,
    pub variable_borrow_index: Array,

    pub liquidation_threshold: Array,
    pub prices: Array,
    pub health_factors: Array,
}

impl Cache {
    pub(crate) async fn init<P>(&self, provider: Arc<P>, tokens: &Tokens) -> eyre::Result<()>
    where
        P: DataProvider + 'static,
    {
        let token_num = tokens.len();
        *self.collateral_matrix.write().await = Array2::from_elem((0, token_num), 0.0);
        *self.borrowed_matrix.write().await = Array2::from_elem((0, token_num), 0.0);

        let now = Utc::now().timestamp_micros();

        let (mut decimals, mut price_decimals, mut token_addresses) = (
            vec![0.0; token_num],
            vec![0.0; token_num],
            vec![Address::default(); token_num],
        );
        for (
            token_address,
            TokenDetails {
                order,
                decimals: dec,
                price_decimals: pd,
                ..
            },
        ) in tokens
        {
            decimals[*order] = *dec;
            price_decimals[*order] = *pd;
            token_addresses[*order] = token_address.clone();
        }
        *self.decimals.write().await = (Array1::from_vec(decimals), now);

        *self.tokens.write().await = (Array1::from_vec(token_addresses.clone()), now);
        *self.price_decimals.write().await = (Array1::from_vec(price_decimals.clone()), now);

        *self.liquidity.write().await = (Array1::from_elem(token_num, Index::default()), now);
        *self.liquidity_index.write().await = (Array1::from_elem(token_num, 0.0), now);
        *self.variable_borrow.write().await = (Array1::from_elem(token_num, Index::default()), now);
        *self.variable_borrow_index.write().await = (Array1::from_elem(token_num, 0.0), now);

        *self.liquidation_threshold.write().await = (Array1::from_elem(token_num, 0.0), now);
        *self.health_factors.write().await = (Array1::from_elem(0, 0.0), now);

        let prices = provider
            .get_asset_prices(token_addresses)
            .await?
            .iter()
            .enumerate()
            .map(|(idx, price)| price.as_f64(price_decimals[idx]))
            .collect();
        *self.prices.write().await = (Array1::from_vec(prices), now);

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
        } = self.get_user_data(provider, tokens, user).await?;
        let row_num = self
            .users
            .get(user)
            .ok_or_else(|| eyre!("sync_user: user = {:?} not found", user))?
            .row_num;
        let now = Utc::now().timestamp_micros();

        self.users.insert(user.clone(), user_settings);

        {
            let collaterals = &*self.collateral.read().await;
            let (col, last_sync, last_modified) = &mut *collaterals
                .get(row_num)
                .ok_or_else(|| {
                    eyre!(
                        "sync_user (user = {}): can't get row = {} from collateral",
                        user,
                        row_num
                    )
                })?
                .write()
                .await;
            (*col, *last_sync, *last_modified) = (Array1::from(collateral_scaled), now, now);
            debug!(
                "sync_user (user = {}): new collateral scaled = {:?}",
                user, col
            );
        }

        {
            let reserves = &*self.reserve.read().await;
            let (res, last_sync, last_modified) = &mut *reserves
                .get(row_num)
                .ok_or_else(|| {
                    eyre!(
                        "sync_user (user = {}): can't get row = {} from reserve",
                        user,
                        row_num
                    )
                })?
                .write()
                .await;
            (*res, *last_sync, *last_modified) = (Array1::from(reserve_scaled), now, now);
            debug!(
                "sync_user (user = {}): new reserve scaled = {:?}",
                user, res
            );
        }

        {
            let debt = &*self.borrowed.read().await;
            let (bor, last_sync, last_modified) = &mut *debt
                .get(row_num)
                .ok_or_else(|| {
                    eyre!(
                        "sync_user (user = {}): can't get row = {} from borrowed",
                        user,
                        row_num
                    )
                })?
                .write()
                .await;
            (*bor, *last_sync, *last_modified) = (Array1::from(borrowed_scaled), now, now);
            debug!(
                "sync_user (user = {}): new borrowed scaled = {:?}",
                user, bor
            );
        }

        {
            let (indexes, last_modified) = &mut *self.liquidity.write().await;
            let idx = liquidity_indexes
                .iter()
                .zip(liquidity_rates.iter())
                .zip(last_update_timestamps.iter())
                .map(|((li, lr), lu)| {
                    Index::new(li.clone(), lr.clone(), lu.to::<i64>() * 1_000_000)
                })
                .collect::<Vec<_>>();
            (*indexes, *last_modified) = (Array1::from(idx), now);
            debug!("sync_user (user = {}): new liquidity = {:?}", user, indexes);
        }

        {
            let (indexes, last_modified) = &mut *self.liquidity_index.write().await;
            (*indexes, *last_modified) = (
                Array1::from_iter(liquidity_indexes.iter().map(F64Converter::as_f64_ray)),
                now,
            );
            debug!(
                "sync_user (user = {}): new liquidity index = {:?}",
                user, indexes
            );
        }

        {
            let (indexes, last_modified) = &mut *self.variable_borrow.write().await;
            let idx = variable_borrow_indexes
                .iter()
                .zip(variable_borrow_rates.iter())
                .zip(last_update_timestamps.iter())
                .map(|((vbi, vbr), lu)| {
                    Index::new(vbi.clone(), vbr.clone(), lu.to::<i64>() * 1_000_000)
                })
                .collect::<Vec<_>>();
            (*indexes, *last_modified) = (Array1::from(idx), now);
            debug!(
                "sync_user (user = {}): new variable borrow = {:?}",
                user, indexes
            );
        }

        {
            let (indexes, last_modified) = &mut *self.variable_borrow_index.write().await;
            (*indexes, *last_modified) = (
                Array1::from_iter(variable_borrow_indexes.iter().map(F64Converter::as_f64_ray)),
                now,
            );
            debug!(
                "sync_user (user = {}): new variable borrow index = {:?}",
                user, indexes
            );
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
    ) -> eyre::Result<UserData>
    where
        P: DataProvider + 'static,
    {
        let tasks = tokens.iter().map(|(token_address, _)| {
            let provider = provider.clone();
            async move {
                let (reserve_data, user_reserve_data) = try_join!(
                    provider.get_reserve_data(token_address),
                    provider.get_user_reserve_data(token_address, user)
                )?;

                Ok::<_, eyre::Error>((reserve_data, user_reserve_data, token_address.clone()))
            }
        });

        let task_results = try_join_all(tasks).await?;
        let (decimals, _) = &*self.decimals.read().await;

        let (
            mut reserve_scaled,
            mut collateral_scaled,
            mut borrowed_scaled,
            mut liquidity_indexes,
            mut liquidity_rates,
            mut variable_borrow_indexes,
            mut variable_borrow_rates,
            mut last_update_timestamps,
            mut user_settings,
        ) = (
            vec![U256::default(); tokens.len()],
            vec![U256::default(); tokens.len()],
            vec![U256::default(); tokens.len()],
            vec![U256::default(); tokens.len()],
            vec![U256::default(); tokens.len()],
            vec![U256::default(); tokens.len()],
            vec![U256::default(); tokens.len()],
            vec![U40::default(); tokens.len()],
            self.users
                .get(user)
                .ok_or_else(|| eyre!("get_user_data: user = {:?} not found", user))?
                .clone(),
        );

        for (
            ReserveData {
                liquidity_rate,
                variable_borrow_rate,
                liquidity_index,
                variable_borrow_index,
                last_update_timestamp,
            },
            UserReserveData {
                current_atoken_balance,
                current_variable_debt,
                usage_as_collateral_enabled,
            },
            token_address,
        ) in task_results
        {
            let idx = tokens
                .get(&token_address)
                .ok_or_else(|| {
                    eyre!(
                        "get_user_data (user = {}): token = {} not found",
                        user,
                        token_address
                    )
                })?
                .order;

            liquidity_indexes[idx] = liquidity_index;
            liquidity_rates[idx] = liquidity_rate;
            variable_borrow_indexes[idx] = variable_borrow_index;
            variable_borrow_rates[idx] = variable_borrow_rate;
            last_update_timestamps[idx] = last_update_timestamp;

            if usage_as_collateral_enabled {
                collateral_scaled[idx] = current_atoken_balance
                    .to_ray(decimals[idx])
                    .to_scaled(liquidity_index);
                user_settings.use_as_collateral.set(idx, true);
            } else {
                reserve_scaled[idx] = current_atoken_balance
                    .to_ray(decimals[idx])
                    .to_scaled(liquidity_index);
                user_settings.use_as_collateral.set(idx, false);
            }
            borrowed_scaled[idx] = current_variable_debt
                .to_ray(decimals[idx])
                .to_scaled(variable_borrow_index);
        }

        Ok(UserData::new(
            reserve_scaled,
            collateral_scaled,
            borrowed_scaled,
            liquidity_indexes,
            liquidity_rates,
            variable_borrow_indexes,
            variable_borrow_rates,
            last_update_timestamps,
            user_settings,
        ))
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
        for worker in 0..workers {
            let (tx, mut rc) = channel::<T>(bound);
            let (callback, cache, provider, tokens) = (
                callback.clone(),
                cache.clone(),
                provider.clone(),
                tokens.clone(),
            );
            task::spawn(async move {
                loop {
                    debug!("subscribe (worker = {}): created thread", worker);

                    while let Some(msg) = rc.recv().await {
                        if let Err(e) =
                            callback(cache.clone(), provider.clone(), tokens.clone(), msg).await
                        {
                            error!(
                                "subscribe (worker = {}): Error while calling event listener: {:?}",
                                worker, e
                            );
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

        debug!(
            "sync_collateral: sync target = {:?}, col lock len = {}",
            target,
            col_lock.len()
        );

        let low_bound = col_matrix_lock.nrows().saturating_sub(1);
        while col_matrix_lock.nrows() < col_lock.len() {
            let row_lock = col_lock
                .get(col_matrix_lock.nrows())
                .ok_or_else(|| {
                    eyre!(
                        "sync_collateral: row = {} not found in collateral",
                        col_matrix_lock.nrows()
                    )
                })?
                .read()
                .await;
            let row = Array1::from_iter(row_lock.0.iter().map(F64Converter::as_f64_ray));
            col_matrix_lock.push_row(row.view())?;
        }

        debug!(
            "sync_collateral: after row check col matrix rows = {}",
            col_matrix_lock.nrows()
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
                    .ok_or_else(|| {
                        eyre!("sync_collateral: row = {} not found in collateral", row_num)
                    })?
                    .read()
                    .await;
                col.get(col_num)
                    .ok_or_else(|| {
                        eyre!(
                            "sync_collateral: column = {} not found in collateral",
                            col_num
                        )
                    })?
                    .as_f64_ray()
            };
        } else {
            let row = col_lock
                .get(row_num)
                .ok_or_else(|| eyre!("sync_collateral: row = {} not found in collateral", row_num))?
                .read()
                .await;

            let row = Array1::from_iter(row.0.iter().map(F64Converter::as_f64_ray));
            col_matrix_lock.row_mut(row_num).assign(&row);
        }

        debug!("{}", {
            let received = Utc::now().timestamp_micros();
            format!(
                "sync_collateral: after row insert col matrix rows = {}, rq_date = {}, \
                     received = {}, delta = {} μs",
                col_matrix_lock.nrows(),
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

        debug!(
            "sync_borrowed: sync target = {:?}, bor lock len = {:?}",
            target,
            bor_lock.len()
        );

        let low_bound = bor_matrix_lock.nrows().saturating_sub(1);
        while bor_matrix_lock.nrows() < bor_lock.len() {
            let row_lock = bor_lock
                .get(bor_matrix_lock.nrows())
                .ok_or_else(|| {
                    eyre!(
                        "sync_borrowed: row = {} not found in borrowed",
                        bor_matrix_lock.nrows()
                    )
                })?
                .read()
                .await;

            let row = Array1::from_iter(row_lock.0.iter().map(F64Converter::as_f64_ray));
            bor_matrix_lock.push_row(row.view())?;
        }

        debug!(
            "sync_borrowed: after row check bor matrix rows = {}",
            bor_matrix_lock.nrows()
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
                    .ok_or_else(|| eyre!("sync_borrowed: row = {} not found in borrowed", row_num))?
                    .read()
                    .await;
                bor.get(col_num)
                    .ok_or_else(|| {
                        eyre!("sync_borrowed: column = {} not found in borrowed", col_num)
                    })?
                    .as_f64_ray()
            };
        } else {
            let row = bor_lock
                .get(row_num)
                .ok_or_else(|| eyre!("sync_borrowed: row = {} not found in borrowed", row_num))?
                .read()
                .await;

            let row = Array1::from_iter(row.0.iter().map(F64Converter::as_f64_ray));
            bor_matrix_lock.row_mut(row_num).assign(&row);
        }

        debug!("{}", {
            let received = Utc::now().timestamp_micros();
            format!(
                "sync_borrowed: after row insert bor matrix rows = {}, rq_date = {}, \
                     received = {}, delta = {} μs",
                bor_matrix_lock.nrows(),
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
                .ok_or_else(|| eyre!("calc_hf: user = {:?} not found", user))?
                .row_num;

            let col_eff = {
                let collateral = &*self.collateral.read().await;
                let (li, _) = &*self.liquidity.read().await;
                let col_row_lock = Array1::from_iter(
                    collateral
                        .get(row_num)
                        .ok_or_else(|| {
                            eyre!(
                                "calc_hf (user = {}): row = {} not found in collateral",
                                user,
                                row_num
                            )
                        })?
                        .read()
                        .await
                        .0
                        .iter()
                        .enumerate()
                        .map(|(idx, v)| v.to_current(li[idx].index).as_f64_ray()),
                );
                col_row_lock.dot(&ltp)
            };

            let bor_eff = {
                let borrowed = &*self.borrowed.read().await;
                let (vbi, _) = &*self.variable_borrow.read().await;
                let bor_row_lock = Array1::from_iter(
                    borrowed
                        .get(row_num)
                        .ok_or_else(|| {
                            eyre!(
                                "calc_hf (user = {}): row = {} not found in borrowed",
                                user,
                                row_num
                            )
                        })?
                        .read()
                        .await
                        .0
                        .iter()
                        .enumerate()
                        .map(|(idx, v)| v.to_current(vbi[idx].index).as_f64_ray()),
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
                    "calc_hf (user = {}): hf = {}, rq_date = {}, \
                     received = {}, delta = {} μs",
                    user,
                    hf_lock.0[row_num],
                    rq_date,
                    received,
                    received - rq_date
                )
            });

            return Ok(());
        }

        let col_eff = {
            let collateral = &*self.collateral_matrix.read().await;
            let (li, _) = &*self.liquidity_index.read().await;
            let scaled = collateral
                * &li
                    .broadcast((collateral.nrows(), li.len()))
                    .ok_or_else(|| eyre!("calc_hf: scaling collateral failed"))?
                    .to_owned();
            scaled.dot(&ltp)
        };

        let bor_eff = {
            let borrowed = &*self.borrowed_matrix.read().await;
            let (vbi, _) = &*self.variable_borrow_index.read().await;
            let scaled = borrowed
                * &vbi
                    .broadcast((borrowed.nrows(), vbi.len()))
                    .ok_or_else(|| eyre!("calc_hf: scaling borrowed failed"))?
                    .to_owned();
            scaled.dot(&price)
        };

        let mut hf_lock = self.health_factors.write().await;
        (hf_lock.0, hf_lock.1) = (col_eff / bor_eff, Utc::now().timestamp_micros());

        debug!("{}", {
            let received = Utc::now().timestamp_micros();
            format!(
                "calc_hf: hf len = {}, rq_date = {}, \
                     received = {}, delta = {} μs",
                hf_lock.0.len(),
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
    fn as_f64_ray(&self) -> f64;
    fn as_f64_wad(&self) -> f64;
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

    #[inline(always)]
    fn as_f64_ray(&self) -> f64 {
        self.as_f64(1e27)
    }

    #[inline(always)]
    fn as_f64_wad(&self) -> f64 {
        self.as_f64(1e18)
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

    #[inline(always)]
    fn as_f64_ray(&self) -> f64 {
        self.as_f64(1e27)
    }

    #[inline(always)]
    fn as_f64_wad(&self) -> f64 {
        self.as_f64(1e18)
    }
}

pub(crate) trait RayOperations {
    fn ray_mul(self, b: U256) -> U256;
    fn ray_div(self, b: U256) -> U256;
    fn to_ray(self, decimals: f64) -> U256;
}

pub(in crate::arbitrum) const RAY: u128 = 1_000_000_000_000_000_000_000_000_000; // 1e27
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

pub(crate) trait Scaler {
    fn to_scaled(self, index: U256) -> U256;
    fn to_current(self, index: U256) -> U256;
}

impl Scaler for U256 {
    fn to_scaled(self, index: U256) -> U256 {
        self.ray_div(index) // (self * RAY) / index
    }

    fn to_current(self, index: U256) -> U256 {
        self.ray_mul(index) // (self * index) / RAY
    }
}
