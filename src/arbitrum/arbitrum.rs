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
use chrono::Utc;
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
    let cache = Arc::new(Cache::default());

    {
        let now = Utc::now().timestamp_millis();
        *cache.prices.write().await =
            (Array1::from_vec(vec![f64::MIN_POSITIVE; tokens.len()]), now);
        *cache.health_factors.write().await = (Array1::from_vec(vec![0.0; tokens.len()]), now);
    }

    let (tx_events, rc_events) = mpsc::sync_channel::<AaveEvents>(1000_000);
    listen_events(provider.clone(), tx_events.clone()).await?;
    listen_price_update(provider.clone(), &tokens, tx_events.clone()).await?;

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
    while let Ok(event) = rc_events.recv() {
        match event {
            AaveEvents::IL2PoolEvents(event, timestamp) => match event {
                IL2PoolEvents::Supply(ev) => {
                    supply_txs[counters.supply % w_num].send((
                        ev,
                        sync_senders[sync_counter % w_num].clone(),
                        hf_senders[hf_counter % w_num].clone(),
                        timestamp,
                    ))?;
                    counters.supply = counters.supply.wrapping_add(1);
                    sync_counter = sync_counter.wrapping_add(1);
                }
                IL2PoolEvents::Withdraw(ev) => {
                    withdraw_txs[counters.withdraw % w_num].send((
                        ev,
                        sync_senders[sync_counter % w_num].clone(),
                        hf_senders[hf_counter % w_num].clone(),
                        timestamp,
                    ))?;
                    counters.withdraw = counters.withdraw.wrapping_add(1);
                    sync_counter = sync_counter.wrapping_add(1);
                }
                IL2PoolEvents::Borrow(ev) => {
                    borrow_txs[counters.borrow % w_num].send((
                        ev,
                        sync_senders[sync_counter % w_num].clone(),
                        hf_senders[hf_counter % w_num].clone(),
                        timestamp,
                    ))?;
                    counters.borrow = counters.borrow.wrapping_add(1);
                    sync_counter = sync_counter.wrapping_add(1);
                }
                IL2PoolEvents::Repay(ev) => {
                    repay_txs[counters.repay % w_num].send((
                        ev,
                        sync_senders[sync_counter % w_num].clone(),
                        hf_senders[hf_counter % w_num].clone(),
                        timestamp,
                    ))?;
                    counters.repay = counters.repay.wrapping_add(1);
                    sync_counter = sync_counter.wrapping_add(1);
                }
                IL2PoolEvents::ReserveUsedAsCollateralEnabled(ev) => {
                    reserve_used_as_collateral_enabled_txs
                        [counters.reserve_used_as_collateral_enabled % w_num]
                        .send((
                            ev,
                            sync_senders[sync_counter % w_num].clone(),
                            hf_senders[hf_counter % w_num].clone(),
                            timestamp,
                        ))?;
                    counters.reserve_used_as_collateral_enabled =
                        counters.reserve_used_as_collateral_enabled.wrapping_add(1);
                    sync_counter = sync_counter.wrapping_add(1);
                }
                IL2PoolEvents::ReserveUsedAsCollateralDisabled(ev) => {
                    reserve_used_as_collateral_disabled_txs
                        [counters.reserve_used_as_collateral_disabled % w_num]
                        .send((
                            ev,
                            sync_senders[sync_counter % w_num].clone(),
                            hf_senders[hf_counter % w_num].clone(),
                            timestamp,
                        ))?;
                    counters.reserve_used_as_collateral_disabled =
                        counters.reserve_used_as_collateral_disabled.wrapping_add(1);
                    sync_counter = sync_counter.wrapping_add(1);
                }
                IL2PoolEvents::LiquidationCall(ev) => {
                    liquidation_call_txs[counters.liquidation_call % w_num].send((
                        ev,
                        sync_senders[sync_counter % w_num].clone(),
                        hf_senders[hf_counter % w_num].clone(),
                        timestamp,
                    ))?;
                    counters.liquidation_call = counters.liquidation_call.wrapping_add(1);
                    sync_counter = sync_counter.wrapping_add(1);
                }
                IL2PoolEvents::ReserveDataUpdated(ev) => {
                    reserve_data_updated_txs[counters.reserve_data_updated % w_num].send((
                        ev,
                        hf_senders[hf_counter % w_num].clone(),
                        timestamp,
                    ))?;
                    counters.reserve_data_updated = counters.reserve_data_updated.wrapping_add(1);
                }
            },
            AaveEvents::IChainlinkAggregatorEvents(event, token, timestamp) => match event {
                IChainlinkAggregatorEvents::AnswerUpdated(ev) => {
                    answer_updated_txs[counters.answer_updated % w_num].send((
                        ev,
                        token,
                        hf_senders[hf_counter % w_num].clone(),
                        timestamp,
                    ))?;
                    counters.answer_updated = counters.answer_updated.wrapping_add(1);
                }
            },
        }
        hf_counter = hf_counter.wrapping_add(1);
    }

    Ok(())
}

type Tokens = HashMap<Address, TokenDetails>;
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

        tokens.insert(
            token.tokenAddress,
            TokenDetails::new(token.symbol, asset_source, order),
        );
        order += 1;
    }

    Ok(tokens)
}

enum AaveEvents {
    IL2PoolEvents(IL2PoolEvents, i64),
    IChainlinkAggregatorEvents(IChainlinkAggregatorEvents, Address, i64),
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
                if tx
                    .send(AaveEvents::IL2PoolEvents(
                        data,
                        Utc::now().timestamp_millis(),
                    ))
                    .is_err()
                {
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
                    if t.send(AaveEvents::IChainlinkAggregatorEvents(
                        data,
                        name,
                        Utc::now().timestamp_millis(),
                    ))
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

async fn liquidation_threshold_update<P>(
    cache: Arc<Cache>,
    tokens: Arc<Tokens>,
    provider: Arc<P>,
    tx: SyncSender<HFRequest>,
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let aave_protocol_data_provider = IAaveProtocolDataProvider::new(
        AAVE_PROTOCOL_DATA_PROVIDER_ADDRESS.parse()?,
        provider.clone(),
    );
    task::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(600));
        loop {
            let mut data = vec![0.0; tokens.len()];
            let tasks = tokens
                .iter()
                .map(|(token_address, TokenDetails { order, .. })| {
                    let provider = aave_protocol_data_provider.clone();
                    async move {
                        (
                            provider
                                .getReserveConfigurationData(token_address.clone())
                                .call()
                                .await
                                .unwrap(),
                            token_address.clone(),
                            order,
                        )
                    }
                });

            for (rs, token_address, order) in futures::future::join_all(tasks).await {
                debug!(
                    "Token: token_address = {}, liquidation_threshold = {}",
                    token_address, rs.liquidationThreshold
                );

                data[order.clone()] = f64::from(rs.liquidationThreshold);
            }

            let (lt, _) = &*cache.liquidation_threshold.read().await;
            let d = Array1::from_vec(data);
            if !d.iter().zip(lt).all(|(a, b)| (a - b).abs() < 1e-8) {
                *cache.liquidation_threshold.write().await = (d, Utc::now().timestamp_millis());
                tx.send(HFRequest::Full).unwrap();
            }

            interval.tick().await;
        }
    });

    Ok(())
}

enum SyncRequest {
    Collateral(usize),
    Borrowed(usize),
    Both(usize),
}

async fn listen_sync(
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
                match sync_rq {
                    SyncRequest::Collateral(row_num) => {
                        let _ = c.sync_collateral(row_num).await;
                    }
                    SyncRequest::Borrowed(row_num) => {
                        let _ = c.sync_borrowed(row_num).await;
                    }
                    SyncRequest::Both(row_num) => {
                        let _ = c.sync_data(row_num).await;
                    }
                }
            }
        });
        senders.push(tx);
    }

    Ok(senders)
}

enum HFRequest {
    User(Address),
    Full,
}

async fn listen_hf_calc(
    cache: Arc<Cache>,
    workers: usize,
    bound: usize,
) -> eyre::Result<Vec<SyncSender<HFRequest>>> {
    let mut senders = Vec::with_capacity(workers);
    for _ in 0..workers {
        let (tx, rc) = mpsc::sync_channel::<HFRequest>(bound);
        let c = cache.clone();
        task::spawn(async move {
            while let Ok(hf_rq) = rc.recv() {
                match hf_rq {
                    HFRequest::User(user) => c.calc_hf(Some(&user)).await.unwrap(),
                    HFRequest::Full => c.calc_hf(None).await.unwrap(),
                }
            }
        });
        senders.push(tx);
    }

    Ok(senders)
}

type UserDetails = DashMap<Address, UserSettings>;
type Array = RwLock<(Array1<f64>, i64)>;
type Arrays = RwLock<(Vec<RwLock<Array1<f64>>>, i64, i64)>;
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
    liquidation_threshold: Array,
    prices: Array,
    health_factors: Array,
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
            let mut user_num_lock = self.users_num.write().await;

            // if we have more than one thread in this fn
            if self.contains(user) {
                return Ok(true);
            }

            self.users.insert(
                user.clone(),
                UserSettings::new(*user_num_lock, bitvec![usize, Lsb0; 0; tokens.len()]),
            );
            *user_num_lock += 1;
        }

        let aave_protocol_data_provider = IAaveProtocolDataProvider::new(
            AAVE_PROTOCOL_DATA_PROVIDER_ADDRESS.parse()?,
            provider.clone(),
        );
        let tasks = tokens.iter().map(|(token_address, _)| {
            let provider = aave_protocol_data_provider.clone();
            let u = user.clone();
            async move {
                (
                    provider
                        .getUserReserveData(token_address.clone(), u)
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

        let now = Utc::now().timestamp();
        {
            let mut locked = self.collateral.write().await;
            locked.0.push(RwLock::new(Array1::from(collateral)));
            (locked.1, locked.2) = (now, now);
        }

        {
            let mut locked = self.reserve.write().await;
            locked.0.push(RwLock::new(Array1::from(reserve)));
            (locked.1, locked.2) = (now, now);
        }

        {
            let mut locked = self.borrowed.write().await;
            locked.0.push(RwLock::new(Array1::from(borrowed)));
            (locked.1, locked.2) = (now, now);
        }

        Ok(false)
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

    async fn sync_collateral(&self, row_num: usize) -> eyre::Result<()> {
        let col_lock = self.collateral.read().await;
        let mut col_matrix_lock = self.collateral_matrix.write().await;
        let low_bound = col_matrix_lock.len() - 1;
        while col_matrix_lock.len() < col_lock.0.len() {
            let row_lock = col_lock.0.get(col_matrix_lock.len()).unwrap().read().await;
            col_matrix_lock.push_row(row_lock.view())?;
        }

        if row_num > low_bound {
            return Ok(());
        }

        let row = col_lock.0.get(row_num).unwrap().read().await;
        col_matrix_lock.row_mut(row_num).assign(&row);

        Ok(())
    }

    async fn sync_borrowed(&self, row_num: usize) -> eyre::Result<()> {
        let bor_lock = self.borrowed.read().await;
        let mut bor_matrix_lock = self.borrowed_matrix.write().await;
        let low_bound = bor_matrix_lock.len() - 1;
        while bor_matrix_lock.len() < bor_lock.0.len() {
            let row_lock = bor_lock.0.get(bor_matrix_lock.len()).unwrap().read().await;
            bor_matrix_lock.push_row(row_lock.view())?;
        }

        if row_num > low_bound {
            return Ok(());
        }

        let row = bor_lock.0.get(row_num).unwrap().read().await;
        bor_matrix_lock.row_mut(row_num).assign(&row);

        Ok(())
    }

    async fn sync_data(&self, row_num: usize) -> eyre::Result<()> {
        self.sync_collateral(row_num).await?;
        self.sync_borrowed(row_num).await?;

        Ok(())
    }

    async fn calc_hf(&self, user: Option<&Address>) -> eyre::Result<()> {
        let lt_lock = self.liquidation_threshold.read().await;
        let price_lock = self.prices.read().await;
        let ltp = &(&*lt_lock).0 * &(&*price_lock).0;

        if let Some(user) = user {
            let row_num = self.users.get(user).unwrap().row_num;

            let collateral_lock = self.collateral.read().await;
            let col_row_lock = collateral_lock.0.get(row_num).unwrap().read().await;

            let col_eff = col_row_lock.dot(&ltp);

            let borrowed_lock = self.borrowed.read().await;
            let bor_row_lock = borrowed_lock.0.get(row_num).unwrap().read().await;

            let bor_eff = bor_row_lock.dot(&(&*price_lock).0);

            let mut hf_lock = self.health_factors.write().await;
            (hf_lock.0[row_num], hf_lock.1) = (col_eff / bor_eff, Utc::now().timestamp_millis());

            return Ok(());
        }

        let collateral_lock = self.collateral_matrix.read().await;
        let col_eff = collateral_lock.dot(&ltp);

        let borrowed_lock = self.borrowed_matrix.read().await;
        let bor_eff = borrowed_lock.dot(&(&*price_lock).0);

        let mut hf_lock = self.health_factors.write().await;
        (hf_lock.0, hf_lock.1) = (col_eff / bor_eff, Utc::now().timestamp_millis());

        Ok(())
    }
}

async fn create_user<P>(
    cache: &Cache,
    provider: Arc<P>,
    tokens: &Tokens,
    user: &Address,
    sync_tx: &SyncSender<SyncRequest>,
    hf_tx: &SyncSender<HFRequest>,
) -> eyre::Result<bool>
where
    P: Provider + Clone,
{
    match cache.init_user(user, tokens, provider.clone()).await {
        Ok(exist) => {
            if !exist {
                sync_tx.send(SyncRequest::Both(cache.users.get(user).unwrap().row_num))?;
                hf_tx.send(HFRequest::User(user.clone()))?;
                return Ok(true);
            }
        }
        Err(e) => {
            cache.remove_user(user);
            return Err(e);
        }
    }

    Ok(false)
}

async fn handle_event<F1, R1, F2, R2, F3, R3, F4, R4>(
    event_timestamp: i64,
    last_sync: i64,
    last_modified: i64,
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
    match event_timestamp {
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
    event: (Supply, SyncSender<SyncRequest>, SyncSender<HFRequest>, i64),
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let (event, sync_tx, hf_tx, timestamp) = event;
    create_user(
        &cache,
        provider.clone(),
        &tokens,
        &event.onBehalfOf,
        &sync_tx,
        &hf_tx,
    )
    .await?;

    let user_settings = cache.users.get(&event.onBehalfOf).unwrap();
    let row_num = user_settings.row_num;
    let idx = tokens.get(&event.reserve).unwrap().order;
    let now = Utc::now().timestamp_millis();

    let (c, sync_t, hf_t) = (cache.clone(), sync_tx.clone(), hf_tx.clone());
    let sync_user = async move || {
        c.remove_user(&event.onBehalfOf);
        c.init_user(&event.onBehalfOf, &tokens, provider.clone())
            .await
            .unwrap();
        sync_t
            .send(SyncRequest::Both(
                c.users.get(&event.onBehalfOf).unwrap().row_num,
            ))
            .unwrap();
        hf_t.send(HFRequest::User(event.onBehalfOf)).unwrap();
    };

    let (last_sync, last_modified);
    if user_settings.use_as_collateral[idx] {
        let mut collateral_lock = cache.collateral.write().await;
        (last_sync, last_modified) = (collateral_lock.1, collateral_lock.2);

        let new_event = async move || {
            (collateral_lock.1, collateral_lock.2) = (now, now);
            let mut row_lock = collateral_lock.0[row_num].write().await;
            row_lock[idx] += f64::from(event.amount);
            sync_tx.send(SyncRequest::Collateral(row_num)).unwrap();
            hf_tx.send(HFRequest::User(event.onBehalfOf)).unwrap();
        };
        let skip_event = async move || {
            debug!(
                "event dated before sync:\
                     event = supply, user = {}, timestamp = {}, collateral sync = {}, collateral timestamp = {}",
                event.onBehalfOf, timestamp, last_sync, last_modified,
            );
        };
        let unknown = async move || {
            info!(
                "detected unknown case:\
                     event = supply, user = {}, timestamp = {}, collateral sync = {}, collateral timestamp = {}",
                event.onBehalfOf, timestamp, last_sync, last_modified
            );
        };

        handle_event(
            timestamp,
            last_sync,
            last_modified,
            new_event,
            skip_event,
            sync_user,
            unknown,
        )
        .await?;
    } else {
        let mut reserve_lock = cache.reserve.write().await;
        (last_sync, last_modified) = (reserve_lock.1, reserve_lock.2);

        let new_event = async move || {
            (reserve_lock.1, reserve_lock.2) = (now, now);
            let mut row_lock = reserve_lock.0[row_num].write().await;
            row_lock[idx] += f64::from(event.amount);
        };
        let skip_event = async move || {
            debug!(
                "event dated before sync:\
                     event = supply, user = {}, timestamp = {}, reserve sync = {}, reserve timestamp = {}",
                event.onBehalfOf, timestamp, last_sync, last_modified,
            );
        };
        let unknown = async move || {
            info!(
                "detected unknown case:\
                     event = supply, user = {}, timestamp = {}, reserve sync = {}, reserve timestamp = {}",
                event.onBehalfOf, timestamp, last_sync, last_modified
            );
        };

        handle_event(
            timestamp,
            last_sync,
            last_modified,
            new_event,
            skip_event,
            sync_user,
            unknown,
        )
        .await?;
    }
    Ok(())
}

async fn withdraw<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (
        Withdraw,
        SyncSender<SyncRequest>,
        SyncSender<HFRequest>,
        i64,
    ),
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let (event, sync_tx, hf_tx, timestamp) = event;
    cache
        .init_user(&event.user, &tokens, provider.clone())
        .await?;

    Ok(())
}

async fn borrow<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (Borrow, SyncSender<SyncRequest>, SyncSender<HFRequest>, i64),
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let (event, sync_tx, hf_tx, timestamp) = event;
    cache
        .init_user(&event.user, &tokens, provider.clone())
        .await?;

    Ok(())
}

async fn repay<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (Repay, SyncSender<SyncRequest>, SyncSender<HFRequest>, i64),
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let (event, sync_tx, hf_tx, timestamp) = event;
    cache
        .init_user(&event.user, &tokens, provider.clone())
        .await?;

    Ok(())
}

async fn reserve_used_as_collateral_enabled<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (
        ReserveUsedAsCollateralEnabled,
        SyncSender<SyncRequest>,
        SyncSender<HFRequest>,
        i64,
    ),
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let (event, sync_tx, hf_tx, timestamp) = event;
    cache
        .init_user(&event.user, &tokens, provider.clone())
        .await?;

    Ok(())
}

async fn reserve_used_as_collateral_disabled<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (
        ReserveUsedAsCollateralDisabled,
        SyncSender<SyncRequest>,
        SyncSender<HFRequest>,
        i64,
    ),
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let (event, sync_tx, hf_tx, timestamp) = event;
    cache
        .init_user(&event.user, &tokens, provider.clone())
        .await?;

    Ok(())
}

async fn liquidation_call<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (
        LiquidationCall,
        SyncSender<SyncRequest>,
        SyncSender<HFRequest>,
        i64,
    ),
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let (event, sync_tx, hf_tx, timestamp) = event;
    cache
        .init_user(&event.user, &tokens, provider.clone())
        .await?;

    Ok(())
}

async fn reserve_data_updated<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (ReserveDataUpdated, SyncSender<HFRequest>, i64),
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let (event, hf_tx, timestamp) = event;
    Ok(())
}

async fn answer_updated<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (AnswerUpdated, Address, SyncSender<HFRequest>, i64),
) -> eyre::Result<()>
where
    P: Provider + Clone + 'static,
{
    let (event, token, sync_tx, timestamp) = event;
    Ok(())
}
