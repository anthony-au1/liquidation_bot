use crate::arbitrum::arbitrum::IChainlinkAggregator::{AnswerUpdated, IChainlinkAggregatorEvents};
use crate::arbitrum::arbitrum::IL2Pool::{
    Borrow, IL2PoolEvents, LiquidationCall, Repay, ReserveDataUpdated,
    ReserveUsedAsCollateralDisabled, ReserveUsedAsCollateralEnabled, Supply, Withdraw,
};
use alloy::eips::{BlockId, BlockNumberOrTag};
use alloy::primitives::{Address, BlockNumber, Log};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Header};
use alloy::sol;
use alloy::sol_types::SolEventInterface;
use ndarray::{Array1, Array2, ArrayView, array};
use std::collections::HashMap;
use std::default::Default;
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;
use tokio::sync::Mutex;
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

pub async fn get_block_number(provider: &dyn Provider) -> eyre::Result<BlockNumber> {
    let block_number = provider.get_block_number().await?;
    debug!("block number: {}", block_number);

    Ok(block_number)
}

pub async fn get_block(provider: &dyn Provider, block: BlockNumber) -> eyre::Result<()> {
    let block_id = BlockId::from(block as u64);
    let block = provider.get_block(block_id).await?;
    if let Some(block) = block {
        debug!("block: {:?}", block);
    }

    Ok(())
}

pub async fn get_logs(provider: &dyn Provider) -> eyre::Result<()> {
    let filter = Filter::new()
        .from_block(BlockNumberOrTag::Latest)
        .to_block(BlockNumberOrTag::Latest);
    let logs = provider.get_logs(&filter).await?;
    for log in logs {
        debug!("Log: {:?}", log);
        debug!("Block: {:?}", log.block_number);
        debug!("Data: {:?}", log.inner.data);
    }

    Ok(())
}

pub async fn get_headers(provider: &dyn Provider) -> eyre::Result<()> {
    let mut stream = provider.subscribe_blocks().await?;

    let (tx, rc) = mpsc::sync_channel::<Header>(100);

    task::spawn(async move {
        while let Ok(header) = stream.recv().await {
            if tx.send(header).is_err() {
                info!("Main thread dropped receiver, exiting background task.");
                break;
            }
        }
    });

    while let Ok(header) = rc.recv() {
        info!("header: {:?}", header);

        time::sleep(Duration::from_secs(5)).await;
    }

    Ok(())
}

pub async fn get_borrows<P>(provider: Box<P>) -> eyre::Result<()>
where
    P: Provider + Clone,
{
    let l2_pool = IL2Pool::new(L2_POOL_ADDRESS.parse()?, provider.clone());
    let latest = provider.get_block_number().await?;
    let filter = l2_pool
        .Borrow_filter()
        .from_block(BlockNumberOrTag::Number(latest - 100u64))
        .to_block(BlockNumberOrTag::Latest)
        .filter;

    let mut stream = provider.subscribe_logs(&filter).await?;
    let (tx, rc) = mpsc::sync_channel::<Borrow>(100);

    task::spawn(async move {
        while let Ok(log) = stream.recv().await {
            if let Ok(Log {
                data: IL2PoolEvents::Borrow(event),
                ..
            }) = IL2PoolEvents::decode_log(log.as_ref())
            {
                if tx.send(event).is_err() {
                    info!("Main thread dropped receiver, exiting background task.");
                    break;
                }
            }
        }
    });

    while let Ok(event) = rc.recv() {
        info!(
            "event: reserve = {}, user = {}, onBehalfOf = {}, amount = {}, interestRateMode = {}, borrowRate = {}, referralCode = {}",
            event.reserve,
            event.user,
            event.onBehalfOf,
            event.amount,
            event.interestRateMode,
            event.borrowRate,
            event.referralCode
        );

        let user_account_data = l2_pool.getUserAccountData(event.user).call().await?;

        info!(
            "user data: totalCollateralBase = {}, totalDebtBase = {}, availableBorrowsBase = {}, currentLiquidationThreshold = {}, ltv = {}, healthFactor = {}",
            user_account_data.totalCollateralBase,
            user_account_data.totalDebtBase,
            user_account_data.availableBorrowsBase,
            user_account_data.currentLiquidationThreshold,
            user_account_data.ltv,
            user_account_data.healthFactor
        );

        info!("******************************************************************************");

        time::sleep(Duration::from_secs(5)).await;
    }

    Ok(())
}

pub async fn test_me() -> eyre::Result<()> {
    let coll = array![[0.1, 0.5, 5_f64], [0.3, 1_f64, 100_f64]];
    let bor = array![[0.5, 0.1, 10_f64], [0.8, 0_f64, 0_f64]];
    let lt = array![0.8, 0.73, 0.85];
    let prices = array![110_000_f64, 3500_f64, 200_f64];

    let ltp = &lt * &prices;
    info!("ltp: {:?}", ltp);

    let coll_eff = coll.dot(&ltp);
    info!("coll_eff: {:?}", coll_eff);

    let bor_eff = bor.dot(&prices);
    info!("bor_eff: {:?}", bor_eff);

    let hf = &coll_eff / &bor_eff;
    info!("hf: {:?}", hf);

    Ok(())
}

pub async fn start<P>(provider: Arc<P>) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let tokens = Arc::new(setup(provider.clone()).await?);
    let cache = Cache::default();
    cache.init_lt(tokens.clone()).await;

    let (tx_events, rc_events) = mpsc::sync_channel::<AaveEvents>(1000_000);
    listen_events(provider.clone(), tx_events.clone()).await?;
    listen_price_update(provider.clone(), tokens.clone(), tx_events.clone()).await?;

    let w_num = 4;
    let bound = 1000;
    let cache = Arc::new(Mutex::new(cache));
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
    while let Ok(event) = rc_events.recv() {
        match event {
            AaveEvents::IL2PoolEvents(event) => match event {
                IL2PoolEvents::Supply(ev) => {
                    supply_txs[counters.supply % w_num].send(ev)?;
                    counters.supply = counters.supply.wrapping_add(1);
                }
                IL2PoolEvents::Withdraw(ev) => {
                    withdraw_txs[counters.withdraw % w_num].send(ev)?;
                    counters.withdraw = counters.withdraw.wrapping_add(1);
                }
                IL2PoolEvents::Borrow(ev) => {
                    borrow_txs[counters.borrow % w_num].send(ev)?;
                    counters.borrow = counters.borrow.wrapping_add(1);
                }
                IL2PoolEvents::Repay(ev) => {
                    repay_txs[counters.repay % w_num].send(ev)?;
                    counters.repay = counters.repay.wrapping_add(1);
                }
                IL2PoolEvents::ReserveUsedAsCollateralEnabled(ev) => {
                    reserve_used_as_collateral_enabled_txs
                        [counters.reserve_used_as_collateral_enabled % w_num]
                        .send(ev)?;
                    counters.reserve_used_as_collateral_enabled =
                        counters.reserve_used_as_collateral_enabled.wrapping_add(1);
                }
                IL2PoolEvents::ReserveUsedAsCollateralDisabled(ev) => {
                    reserve_used_as_collateral_disabled_txs
                        [counters.reserve_used_as_collateral_disabled % w_num]
                        .send(ev)?;
                    counters.reserve_used_as_collateral_disabled =
                        counters.reserve_used_as_collateral_disabled.wrapping_add(1);
                }
                IL2PoolEvents::LiquidationCall(ev) => {
                    liquidation_call_txs[counters.liquidation_call % w_num].send(ev)?;
                    counters.liquidation_call = counters.liquidation_call.wrapping_add(1);
                }
                IL2PoolEvents::ReserveDataUpdated(ev) => {
                    reserve_data_updated_txs[counters.reserve_data_updated % w_num].send(ev)?;
                    counters.reserve_data_updated = counters.reserve_data_updated.wrapping_add(1);
                }
            },
            AaveEvents::IChainlinkAggregatorEvents(event) => match event {
                IChainlinkAggregatorEvents::AnswerUpdated(ev) => {
                    answer_updated_txs[counters.answer_updated % w_num].send(ev)?;
                    counters.answer_updated = counters.answer_updated.wrapping_add(1);
                }
            },
        }
    }

    Ok(())
}

struct TokenDetails {
    token: Address,
    price_source: Address,
    order: usize,
    liquidation_threshold: f64,
}

impl TokenDetails {
    pub fn new(
        token: Address,
        price_source: Address,
        order: usize,
        liquidation_threshold: f64,
    ) -> Self {
        Self {
            token,
            price_source,
            order,
            liquidation_threshold,
        }
    }
}

async fn setup<P>(provider: Arc<P>) -> eyre::Result<HashMap<String, TokenDetails>>
where
    P: Provider + Clone,
{
    let aave_protocol_data_provider = IAaveProtocolDataProvider::new(
        AAVE_PROTOCOL_DATA_PROVIDER_ADDRESS.parse()?,
        provider.clone(),
    );

    let aave_oracle_provider = IAaveOracle::new(AAVE_ORACLE_ADDRESS.parse()?, provider.clone());

    let token_data = aave_protocol_data_provider
        .getAllReservesTokens()
        .call()
        .await?;

    let mut tokens = HashMap::new();
    let mut order = 0;
    for token in token_data {
        debug!("token: {:?}", token
            "Token: symbol = {}, address = {}",
            token.symbol, token.tokenAddress
        );

        let asset_source = aave_oracle_provider
            .getSourceOfAsset(token.tokenAddress)
            .call()
            .await?;

        debug!("Token: asset_address = {}", asset_source);

        let reserve_configuration_data = aave_protocol_data_provider
            .getReserveConfigurationData(token.tokenAddress)
            .call()
            .await?;

        debug!(
            "Token: liquidation_threshold = {}",
            reserve_configuration_data.liquidationThreshold
        );

        tokens.insert(
            token.symbol,
            TokenDetails::new(
                token.tokenAddress,
                asset_source,
                order,
                f64::from(reserve_configuration_data.liquidationThreshold),
            ),
        );
        order += 1;
    }

    Ok(tokens)
}

enum AaveEvents {
    IL2PoolEvents(IL2PoolEvents),
    IChainlinkAggregatorEvents(IChainlinkAggregatorEvents),
}

async fn listen_events<P>(provider: Arc<P>, tx: SyncSender<AaveEvents>) -> eyre::Result<()>
where
    P: Provider + Clone,
{
    let l2_pool = IL2Pool::new(L2_POOL_ADDRESS.parse()?, provider.clone());
    let filter = Filter::new().address(l2_pool.address().clone());
    let mut stream = provider.subscribe_logs(&filter).await?;

    task::spawn(async move {
        while let Ok(log) = stream.recv().await {
            if let Ok(Log { data, .. }) = IL2PoolEvents::decode_log(log.as_ref()) {
                if tx.send(AaveEvents::IL2PoolEvents(data)).is_err() {
                    info!("Main thread dropped receiver for pool events, exiting background task.");
                    break;
                }
            }
        }
    });

    Ok(())
}

async fn listen_price_update<P>(
    provider: Arc<P>,
    tokens: Arc<HashMap<String, TokenDetails>>,
    tx: SyncSender<AaveEvents>,
) -> eyre::Result<()>
where
    P: Provider + Clone,
{
    for (_, TokenDetails { price_source, .. }) in tokens.iter() {
        let filter = Filter::new().address(price_source.clone());
        let mut stream = provider.clone().subscribe_logs(&filter).await?;

        let t = tx.clone();
        task::spawn(async move {
            while let Ok(log) = stream.recv().await {
                if let Ok(Log { data, .. }) = IChainlinkAggregatorEvents::decode_log(log.as_ref()) {
                    if t.send(AaveEvents::IChainlinkAggregatorEvents(data))
                        .is_err()
                    {
                        info!(
                            "Main thread dropped receiver for chainlink events, exiting background task."
                        );
                        break;
                    }
                }
            }
        });
    }

    Ok(())
}

#[derive(Default)]
struct Cache {
    tokens: Mutex<Vec<String>>,
    users: Mutex<Vec<Address>>,
    reserve: Mutex<Array2<f64>>,
    collateral: Mutex<Array2<f64>>,
    borrowed: Mutex<Array2<f64>>,
    liquidation_threshold: Mutex<Array1<f64>>,
    prices: Mutex<Array1<f64>>,
}

impl Cache {
    async fn contains(&self, addr: &Address) -> bool {
        self.users.lock().await.contains(addr)
    }

    async fn init_user<P>(
        &mut self,
        user: &Address,
        tokens: &HashMap<String, TokenDetails>,
        provider: Arc<P>,
    ) -> eyre::Result<bool>
    where
        P: Provider + Clone,
    {
        if self.contains(user).await {
            return Ok(true);
        }

        let aave_protocol_data_provider = IAaveProtocolDataProvider::new(
            AAVE_PROTOCOL_DATA_PROVIDER_ADDRESS.parse()?,
            provider.clone(),
        );
        let tasks = tokens
            .iter()
            .map(|(token_name, TokenDetails { token, .. })| {
                let provider = aave_protocol_data_provider.clone();
                let user = user.clone();
                async move {
                    (
                        provider
                            .getUserReserveData(token.clone(), user)
                            .call()
                            .await
                            .unwrap(),
                        token_name.clone(),
                    )
                }
            });

        let (mut collateral, mut reserve, mut borrowed) = (
            vec![0.0; tokens.len()],
            vec![0.0; tokens.len()],
            vec![0.0; tokens.len()],
        );
        for (urd, token_name) in futures::future::join_all(tasks).await {
            let idx = tokens.get(&token_name).unwrap().order;
            if urd.usageAsCollateralEnabled {
                collateral[idx] = f64::from(urd.currentATokenBalance);
            } else {
                reserve[idx] = f64::from(urd.currentATokenBalance);
            }
            borrowed[idx] = f64::from(urd.currentVariableDebt);
        }

        let mut locked = self.collateral.lock().await;
        locked.push_row(ArrayView::from(&collateral))?;

        let mut locked = self.reserve.lock().await;
        locked.push_row(ArrayView::from(&reserve))?;

        let mut locked = self.borrowed.lock().await;
        locked.push_row(ArrayView::from(&borrowed))?;

        self.users.lock().await.push(user.clone());

        Ok(false)
    }

    async fn init_lt(&self, tokens: Arc<HashMap<String, TokenDetails>>) {
        let mut data = vec![0.0; tokens.len()];
        tokens.iter().for_each(
            |(
                _,
                TokenDetails {
                    order,
                    liquidation_threshold,
                    ..
                },
            )| {
                data[order.clone()] = liquidation_threshold.clone();
            },
        );
        *self.liquidation_threshold.lock().await = Array1::from(data);
    }

    async fn subscribe<P, T, F, Fut>(
        cache: Arc<Mutex<Cache>>,
        workers: usize,
        bound: usize,
        provider: Arc<P>,
        tokens: Arc<HashMap<String, TokenDetails>>,
        callback: F,
    ) -> eyre::Result<Vec<SyncSender<T>>>
    where
        P: Provider + Clone + 'static,
        T: Send + 'static,
        F: Fn(Arc<Mutex<Cache>>, Arc<P>, Arc<HashMap<String, TokenDetails>>, T) -> Fut
            + Send
            + Sync
            + 'static,
        Fut: Future<Output = eyre::Result<()>> + Send + 'static,
    {
        let callback = Arc::new(callback);
        let mut senders = vec![];
        for _ in 0..workers {
            let (tx, rc) = mpsc::sync_channel::<T>(bound);
            let (cb, c, p, t) = (
                callback.clone(),
                cache.clone(),
                provider.clone(),
                tokens.clone(),
            );
            thread::spawn(async move || {
                while let Ok(msg) = rc.recv() {
                    if let Err(e) = cb(c.clone(), p.clone(), t.clone(), msg).await {
                        error!("Error while calling event listener: {:?}", e);
                    }
                }
            });

            senders.push(tx);
        }

        Ok(senders)
    }
}

async fn supply<P>(
    cache: Arc<Mutex<Cache>>,
    provider: Arc<P>,
    tokens: Arc<HashMap<String, TokenDetails>>,
    event: Supply,
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let mut cache = cache.lock().await;
    cache
        .init_user(&event.user, &tokens, provider.clone())
        .await?;

    Ok(())
}

async fn withdraw<P>(
    cache: Arc<Mutex<Cache>>,
    provider: Arc<P>,
    tokens: Arc<HashMap<String, TokenDetails>>,
    event: Withdraw,
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let mut cache = cache.lock().await;
    cache
        .init_user(&event.user, &tokens, provider.clone())
        .await?;

    Ok(())
}

async fn borrow<P>(
    cache: Arc<Mutex<Cache>>,
    provider: Arc<P>,
    tokens: Arc<HashMap<String, TokenDetails>>,
    event: Borrow,
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let mut cache = cache.lock().await;
    cache
        .init_user(&event.user, &tokens, provider.clone())
        .await?;

    Ok(())
}

async fn repay<P>(
    cache: Arc<Mutex<Cache>>,
    provider: Arc<P>,
    tokens: Arc<HashMap<String, TokenDetails>>,
    event: Repay,
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let mut cache = cache.lock().await;
    cache
        .init_user(&event.user, &tokens, provider.clone())
        .await?;

    Ok(())
}

async fn reserve_used_as_collateral_enabled<P>(
    cache: Arc<Mutex<Cache>>,
    provider: Arc<P>,
    tokens: Arc<HashMap<String, TokenDetails>>,
    event: ReserveUsedAsCollateralEnabled,
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let mut cache = cache.lock().await;
    cache
        .init_user(&event.user, &tokens, provider.clone())
        .await?;

    Ok(())
}

async fn reserve_used_as_collateral_disabled<P>(
    cache: Arc<Mutex<Cache>>,
    provider: Arc<P>,
    tokens: Arc<HashMap<String, TokenDetails>>,
    event: ReserveUsedAsCollateralDisabled,
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let mut cache = cache.lock().await;
    cache
        .init_user(&event.user, &tokens, provider.clone())
        .await?;

    Ok(())
}

async fn liquidation_call<P>(
    cache: Arc<Mutex<Cache>>,
    provider: Arc<P>,
    tokens: Arc<HashMap<String, TokenDetails>>,
    event: LiquidationCall,
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let mut cache = cache.lock().await;
    cache
        .init_user(&event.user, &tokens, provider.clone())
        .await?;

    Ok(())
}

async fn reserve_data_updated<P>(
    cache: Arc<Mutex<Cache>>,
    provider: Arc<P>,
    tokens: Arc<HashMap<String, TokenDetails>>,
    event: ReserveDataUpdated,
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let mut cache = cache.lock().await;

    Ok(())
}

async fn answer_updated<P>(
    cache: Arc<Mutex<Cache>>,
    provider: Arc<P>,
    tokens: Arc<HashMap<String, TokenDetails>>,
    event: AnswerUpdated,
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let mut cache = cache.lock().await;

    Ok(())
}
