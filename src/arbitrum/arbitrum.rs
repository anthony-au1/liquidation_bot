use std::any::Any;
use crate::arbitrum::arbitrum::IChainlinkAggregator::IChainlinkAggregatorEvents;
use crate::arbitrum::arbitrum::IL2Pool::{Borrow, IL2PoolEvents};
use alloy::eips::{BlockId, BlockNumberOrTag};
use alloy::primitives::{Address, BlockNumber, Log};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Header};
use alloy::sol;
use alloy::sol_types::SolEventInterface;
use ndarray::{Array1, Array2, array};
use std::collections::HashMap;
use std::default::Default;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, SyncSender};
use std::thread;
use std::time::Duration;
use tokio::{task, time};
use tracing::{debug, info};

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

pub async fn start<P>(provider: Box<P>) -> eyre::Result<()>
where
    P: Provider + Clone,
{
    let tokens = setup(provider.clone()).await?;
    let cache = Cache::default();

    let (tx_events, rc_events) = mpsc::sync_channel::<AaveEvents>(1000_000);
    listen_events(provider.clone(), tx_events.clone()).await?;
    listen_price_update(provider.clone(), &tokens, tx_events.clone()).await?;

    let w_num = 4;
    let bound = 1000;
    let supply_txs = cache.subscribe(w_num, bound, |supply| {
        
    }).await?;
    let withdraw_txs = cache.subscribe(w_num, bound, |withdraw| {

    }).await?;

    #[derive(Default)]
    struct EventCounter {
        supply: usize,
        withdraw: usize,
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
                    // cache.init(&ev.user, &tx);
                }
                IL2PoolEvents::Repay(ev) => {
                    // cache.init(&ev.user, &tx);
                }
                IL2PoolEvents::ReserveUsedAsCollateralEnabled(ev) => {
                    // cache.init(&ev.user, &tx);
                }
                IL2PoolEvents::ReserveUsedAsCollateralDisabled(ev) => {
                    // cache.init(&ev.user, &tx);
                }
                IL2PoolEvents::LiquidationCall(ev) => {
                    // cache.init(&ev.user, &tx);
                }
                IL2PoolEvents::ReserveDataUpdated(ev) => {
                    // println!("{}", ev.reserve);
                }
            },
            AaveEvents::IChainlinkAggregatorEvents(event) => match event {
                IChainlinkAggregatorEvents::AnswerUpdated(ev) => {}
            },
        }
    }

    Ok(())
}

struct TokenAddress {
    token: Address,
    price_source: Address,
}

impl TokenAddress {
    pub fn new(token: Address, price_source: Address) -> Self {
        Self {
            token,
            price_source,
        }
    }
}

async fn setup<P>(provider: Box<P>) -> eyre::Result<HashMap<String, TokenAddress>>
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

        tokens.insert(
            token.symbol,
            TokenAddress::new(token.tokenAddress, asset_source),
        );
    }

    Ok(tokens)
}

enum AaveEvents {
    IL2PoolEvents(IL2PoolEvents),
    IChainlinkAggregatorEvents(IChainlinkAggregatorEvents),
}

async fn listen_events<P>(provider: Box<P>, tx: SyncSender<AaveEvents>) -> eyre::Result<()>
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
    provider: Box<P>,
    tokens: &HashMap<String, TokenAddress>,
    tx: SyncSender<AaveEvents>,
) -> eyre::Result<()>
where
    P: Provider + Clone,
{
    for (_, TokenAddress { price_source, .. }) in tokens {
        let filter = Filter::new().address(price_source.clone());
        let mut stream = provider.clone().subscribe_logs(&filter).await?;

        let tx = tx.clone();
        task::spawn(async move {
            while let Ok(log) = stream.recv().await {
                if let Ok(Log { data, .. }) = IChainlinkAggregatorEvents::decode_log(log.as_ref()) {
                    if tx
                        .send(AaveEvents::IChainlinkAggregatorEvents(data))
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
    users: Vec<Address>,
    reserve: Array2<f64>,
    collateral: Array2<f64>,
    borrowed: Array2<f64>,
    liquidation_threshold: Array1<f64>,
    prices: Array1<f64>,
}

impl Cache {
    fn contains(&self, addr: &Address) -> bool {
        self.users.contains(addr)
    }

    fn init(&mut self, addr: &Address, tx: &SyncSender<Address>) -> eyre::Result<bool> {
        if self.contains(addr) {
            return Ok(true);
        }

        tx.send(addr.clone())?;

        Ok(false)
    }
}

impl Cache {
    async fn subscribe<T, F>(
        &self,
        workers: usize,
        bound: usize,
        callback: F,
    ) -> eyre::Result<Vec<SyncSender<T>>>
    where
        T: Send + 'static,
        F: Fn(T) + Send + Sync + 'static,
    {
        let callback = std::sync::Arc::new(callback);
        let mut senders = vec![];
        for _ in 0..workers {
            let (tx, rc) = mpsc::sync_channel::<T>(bound);
            let cb = callback.clone();
            thread::spawn(move || {
                while let Ok(msg) = rc.recv() {
                    cb(msg);
                }
            });

            senders.push(tx);
        }

        Ok(senders)
    }
}
