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
use bitvec::prelude::*;
use dashmap::DashMap;
use ndarray::{Array1, Array2, array};
use std::collections::HashMap;
use std::default::Default;
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::{task, time};
use tracing::{debug, error, info, warn};

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
    let tokens = setup(provider.clone()).await?;
    let cache = Cache::default();
    cache.init_lt(&tokens).await?;

    {
        let el_num = cache.liquidation_threshold.read().await.len();
        *cache.prices.write().await = Array1::from_vec(vec![0.0; el_num]);
        *cache.health_factors.write().await = Array1::from_vec(vec![0.0; el_num]);
    }

    let (tx_events, rc_events) = mpsc::sync_channel::<AaveEvents>(1000_000);
    listen_events(provider.clone(), tx_events.clone()).await?;
    listen_price_update(provider.clone(), &tokens, tx_events.clone()).await?;

    let w_num = 4;
    let bound = 1000;
    let cache = Arc::new(cache);
    let tokens = Arc::new(tokens);

    let mut sync_counter = 0;
    let sync_senders = listen_cache_sync(cache.clone(), w_num, bound).await?;

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
                    supply_txs[counters.supply % w_num]
                        .send((ev, sync_senders[sync_counter % w_num].clone()))?;
                    counters.supply = counters.supply.wrapping_add(1);
                }
                IL2PoolEvents::Withdraw(ev) => {
                    withdraw_txs[counters.withdraw % w_num]
                        .send((ev, sync_senders[sync_counter % w_num].clone()))?;
                    counters.withdraw = counters.withdraw.wrapping_add(1);
                }
                IL2PoolEvents::Borrow(ev) => {
                    borrow_txs[counters.borrow % w_num]
                        .send((ev, sync_senders[sync_counter % w_num].clone()))?;
                    counters.borrow = counters.borrow.wrapping_add(1);
                }
                IL2PoolEvents::Repay(ev) => {
                    repay_txs[counters.repay % w_num]
                        .send((ev, sync_senders[sync_counter % w_num].clone()))?;
                    counters.repay = counters.repay.wrapping_add(1);
                }
                IL2PoolEvents::ReserveUsedAsCollateralEnabled(ev) => {
                    reserve_used_as_collateral_enabled_txs
                        [counters.reserve_used_as_collateral_enabled % w_num]
                        .send((ev, sync_senders[sync_counter % w_num].clone()))?;
                    counters.reserve_used_as_collateral_enabled =
                        counters.reserve_used_as_collateral_enabled.wrapping_add(1);
                }
                IL2PoolEvents::ReserveUsedAsCollateralDisabled(ev) => {
                    reserve_used_as_collateral_disabled_txs
                        [counters.reserve_used_as_collateral_disabled % w_num]
                        .send((ev, sync_senders[sync_counter % w_num].clone()))?;
                    counters.reserve_used_as_collateral_disabled =
                        counters.reserve_used_as_collateral_disabled.wrapping_add(1);
                }
                IL2PoolEvents::LiquidationCall(ev) => {
                    liquidation_call_txs[counters.liquidation_call % w_num]
                        .send((ev, sync_senders[sync_counter % w_num].clone()))?;
                    counters.liquidation_call = counters.liquidation_call.wrapping_add(1);
                }
                IL2PoolEvents::ReserveDataUpdated(ev) => {
                    reserve_data_updated_txs[counters.reserve_data_updated % w_num]
                        .send((ev, sync_senders[sync_counter % w_num].clone()))?;
                    counters.reserve_data_updated = counters.reserve_data_updated.wrapping_add(1);
                }
            },
            AaveEvents::IChainlinkAggregatorEvents(event, token) => match event {
                IChainlinkAggregatorEvents::AnswerUpdated(ev) => {
                    answer_updated_txs[counters.answer_updated % w_num].send((
                        ev,
                        token,
                        sync_senders[sync_counter % w_num].clone(),
                    ))?;
                    counters.answer_updated = counters.answer_updated.wrapping_add(1);
                }
            },
        }
        sync_counter = sync_counter.wrapping_add(1);
    }

    Ok(())
}

type Tokens = HashMap<Address, TokenDetails>;
struct TokenDetails {
    name: String,
    price_source: Address,
    order: usize,
    liquidation_threshold: f64,
}

impl TokenDetails {
    pub fn new(
        name: String,
        price_source: Address,
        order: usize,
        liquidation_threshold: f64,
    ) -> Self {
        Self {
            name,
            price_source,
            order,
            liquidation_threshold,
        }
    }
}

async fn setup<P>(provider: Arc<P>) -> eyre::Result<Tokens>
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
        debug!(
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
            token.tokenAddress,
            TokenDetails::new(
                token.symbol,
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
    IChainlinkAggregatorEvents(IChainlinkAggregatorEvents, Address),
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
    tokens: &Tokens,
    tx: SyncSender<AaveEvents>,
) -> eyre::Result<()>
where
    P: Provider + Clone,
{
    for (token_name, TokenDetails { price_source, .. }) in tokens {
        let filter = Filter::new().address(price_source.clone());
        let mut stream = provider.clone().subscribe_logs(&filter).await?;

        let (t, name) = (tx.clone(), token_name.clone());
        task::spawn(async move {
            while let Ok(log) = stream.recv().await {
                if let Ok(Log { data, .. }) = IChainlinkAggregatorEvents::decode_log(log.as_ref()) {
                    if t.send(AaveEvents::IChainlinkAggregatorEvents(data, name))
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

enum SyncRequest {
    Collateral(usize),
    Borrowed(usize),
    Full,
}

async fn listen_cache_sync(
    cache: Arc<Cache>,
    workers: usize,
    bound: usize,
) -> eyre::Result<Vec<SyncSender<SyncRequest>>> {
    let mut senders = Vec::with_capacity(workers);
    for _ in 0..workers {
        let (tx, rc) = mpsc::sync_channel::<SyncRequest>(bound);
        let c = cache.clone();
        task::spawn(async move {
            while let Ok(sync_rq) = rc.recv() {
                //TODO sync here
            }
        });
        senders.push(tx);
    }

    Ok(senders)
}

type UserDetails = DashMap<Address, UserSettings>;
type Array = Array1<f64>;
type Arrays = RwLock<Vec<RwLock<Array>>>;
type Matrix = RwLock<Array2<f64>>;

#[derive(Default)]
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

#[derive(Default)]
struct Cache {
    users: UserDetails,
    users_num: RwLock<usize>,
    reserve: Arrays,
    collateral: Arrays,
    collateral_matrix: Matrix,
    borrowed: Arrays,
    borrowed_matrix: Matrix,
    liquidation_threshold: RwLock<Array>,
    prices: RwLock<Array>,
    health_factors: RwLock<Array>,
}

impl Cache {
    fn contains(&self, addr: &Address) -> bool {
        self.users.contains_key(addr)
    }

    async fn init_user<P>(
        &self,
        user: &Address,
        tokens: &Tokens,
        provider: Arc<P>,
    ) -> eyre::Result<bool>
    where
        P: Provider + Clone,
    {
        if self.contains(user) {
            return Ok(true);
        }

        {
            let mut lock = self.users_num.write().await;

            // if we have more than one thread in this fn
            if self.contains(user) {
                return Ok(true);
            }

            self.users.insert(
                user.clone(),
                UserSettings::new(*lock, bitvec![usize, Lsb0; 0; tokens.len()]),
            );
            *lock += 1;
        }

        let aave_protocol_data_provider = IAaveProtocolDataProvider::new(
            AAVE_PROTOCOL_DATA_PROVIDER_ADDRESS.parse()?,
            provider.clone(),
        );
        let tasks = tokens.iter().map(|(token_address, _)| {
            let provider = aave_protocol_data_provider.clone();
            let user = user.clone();
            async move {
                (
                    provider
                        .getUserReserveData(token_address.clone(), user)
                        .call()
                        .await
                        .unwrap(),
                    token_address.clone(),
                )
            }
        });

        let (mut collateral, mut reserve, use_as_collateral, mut borrowed) = (
            vec![0.0; tokens.len()],
            vec![0.0; tokens.len()],
            &mut self.users.get_mut(user).unwrap().use_as_collateral,
            vec![0.0; tokens.len()],
        );
        for (urd, token_address) in futures::future::join_all(tasks).await {
            let idx = tokens.get(&token_address).unwrap().order;
            if urd.usageAsCollateralEnabled {
                collateral[idx] = f64::from(urd.currentATokenBalance);
                use_as_collateral.set(idx, true);
            } else {
                reserve[idx] = f64::from(urd.currentATokenBalance);
            }
            borrowed[idx] = f64::from(urd.currentVariableDebt);
        }

        {
            let mut locked = self.collateral.write().await;
            locked.push(RwLock::new(Array1::from(collateral)));
        }

        {
            let mut locked = self.reserve.write().await;
            locked.push(RwLock::new(Array1::from(reserve)));
        }

        {
            let mut locked = self.borrowed.write().await;
            locked.push(RwLock::new(Array1::from(borrowed)));
        }

        Ok(false)
    }

    fn remove_user(&self, addr: &Address) {
        self.users.remove(addr);
    }

    async fn init_lt(&self, tokens: &Tokens) -> eyre::Result<()> {
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
        *self.liquidation_threshold.write().await = Array1::from(data);

        Ok(())
    }

    async fn subscribe<P, T, F, Fut>(
        cache: Arc<Cache>,
        workers: usize,
        bound: usize,
        provider: Arc<P>,
        tokens: Arc<Tokens>,
        callback: F,
    ) -> eyre::Result<Vec<SyncSender<T>>>
    where
        P: Provider + Clone + 'static,
        T: Send + 'static,
        F: Fn(Arc<Cache>, Arc<P>, Arc<Tokens>, T) -> Fut + Send + Sync + 'static,
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

    async fn calc_hf(&self, user: Option<&Address>) -> eyre::Result<()> {
        let lt_lock = self.liquidation_threshold.read().await;
        let price_lock = self.prices.read().await;
        let ltp = &*lt_lock * &*price_lock;

        if let Some(user) = user {
            let row_num = self.users.get(user).unwrap().row_num;

            let collateral_lock = self.collateral.read().await;
            let col_row_lock = collateral_lock.get(row_num).unwrap().read().await;

            let col_eff = col_row_lock.dot(&ltp);

            let borrowed_lock = self.borrowed.read().await;
            let bor_row_lock = borrowed_lock.get(row_num).unwrap().read().await;

            let bor_eff = bor_row_lock.dot(&*price_lock);

            let mut hf_lock = self.health_factors.write().await;
            hf_lock[row_num] = col_eff / bor_eff;

            return Ok(());
        }

        // TODO update all users

        // let coll = array![[0.1, 0.5, 5_f64], [0.3, 1_f64, 100_f64]];
        // let bor = array![[0.5, 0.1, 10_f64], [0.8, 0_f64, 0_f64]];
        // let lt = array![0.8, 0.73, 0.85];
        // let prices = array![110_000_f64, 3500_f64, 200_f64];
        //
        // let ltp = &lt * &prices;
        // info!("ltp: {:?}", ltp);
        //
        // let coll_eff = coll.dot(&ltp);
        // info!("coll_eff: {:?}", coll_eff);
        //
        // let bor_eff = bor.dot(&prices);
        // info!("bor_eff: {:?}", bor_eff);
        //
        // let hf = &coll_eff / &bor_eff;
        // info!("hf: {:?}", hf);

        Ok(())
    }
}

async fn supply<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (Supply, SyncSender<SyncRequest>),
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let (event, tx) = event;
    match cache
        .init_user(&event.onBehalfOf, &tokens, provider.clone())
        .await
    {
        Ok(exist) => {
            if !exist {
                tx.send(SyncRequest::Full)?;
                return Ok(());
            }
        }
        Err(e) => {
            warn!("supply failed while initializing user: {:?}", e);
            cache.remove_user(&event.onBehalfOf);
            return Ok(());
        }
    }

    let user_settings = cache.users.get(&event.onBehalfOf).unwrap();
    let row_num = user_settings.row_num;
    let idx = tokens.get(&event.reserve).unwrap().order;

    if user_settings.use_as_collateral[idx] {
        let collateral_lock = cache.collateral.write().await;
        let mut row_lock = collateral_lock[row_num].write().await;
        row_lock[idx] += f64::from(event.amount);

        tx.send(SyncRequest::Collateral(row_num))?;
    } else {
        let reserve_lock = cache.reserve.write().await;
        let mut row_lock = reserve_lock[row_num].write().await;
        row_lock[idx] += f64::from(event.amount);
    }

    Ok(())
}

async fn withdraw<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (Withdraw, SyncSender<SyncRequest>),
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let (event, tx) = event;
    cache
        .init_user(&event.user, &tokens, provider.clone())
        .await?;

    Ok(())
}

async fn borrow<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (Borrow, SyncSender<SyncRequest>),
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let (event, tx) = event;
    cache
        .init_user(&event.user, &tokens, provider.clone())
        .await?;

    Ok(())
}

async fn repay<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (Repay, SyncSender<SyncRequest>),
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let (event, tx) = event;
    cache
        .init_user(&event.user, &tokens, provider.clone())
        .await?;

    Ok(())
}

async fn reserve_used_as_collateral_enabled<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (ReserveUsedAsCollateralEnabled, SyncSender<SyncRequest>),
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let (event, tx) = event;
    cache
        .init_user(&event.user, &tokens, provider.clone())
        .await?;

    Ok(())
}

async fn reserve_used_as_collateral_disabled<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (ReserveUsedAsCollateralDisabled, SyncSender<SyncRequest>),
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let (event, tx) = event;
    cache
        .init_user(&event.user, &tokens, provider.clone())
        .await?;

    Ok(())
}

async fn liquidation_call<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (LiquidationCall, SyncSender<SyncRequest>),
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let (event, tx) = event;
    cache
        .init_user(&event.user, &tokens, provider.clone())
        .await?;

    Ok(())
}

async fn reserve_data_updated<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (ReserveDataUpdated, SyncSender<SyncRequest>),
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let (event, tx) = event;
    Ok(())
}

async fn answer_updated<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (AnswerUpdated, Address, SyncSender<SyncRequest>),
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let (event, token, tx) = event;
    Ok(())
}
