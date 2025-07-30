use crate::arbitrum::arbitrum::IChainlinkAggregator::AnswerUpdated;
use crate::arbitrum::arbitrum::IL2Pool::{
    Borrow, LiquidationCall, Repay, ReserveDataUpdated, ReserveUsedAsCollateralDisabled,
    ReserveUsedAsCollateralEnabled, Supply, Withdraw,
};
use crate::arbitrum::arbitrum::{Cache, DataProvider, HFRequest, SyncRequest, TimeStamp, Tokens};
use alloy_primitives::Address;
use chrono::Utc;
use eyre::eyre;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tracing::{debug, info};

pub(in crate::arbitrum) async fn create_user<P>(
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
                        cache
                            .users
                            .get(user)
                            .ok_or_else(|| eyre!("user = {:?} not found", user))?
                            .row_num,
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
    R1: Future<Output = eyre::Result<()>> + Send,
    F2: FnOnce() -> R2,
    R2: Future<Output = eyre::Result<()>> + Send,
    F3: FnOnce() -> R3,
    R3: Future<Output = eyre::Result<()>> + Send,
    F4: FnOnce() -> R4,
    R4: Future<Output = eyre::Result<()>> + Send,
{
    match rq_date {
        t if t > last_modified => {
            // new event
            new_event().await?;
        }
        t if t <= last_sync => {
            // skip event
            skip_event().await?;
        }
        t if t > last_sync && t <= last_modified => {
            // remove user and add
            sync_user().await?;
        }
        _ => {
            unknown().await?;
        }
    }

    Ok(())
}

pub(in crate::arbitrum) async fn supply<P>(
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

    let user_settings = cache
        .users
        .get(&event.onBehalfOf)
        .ok_or_else(|| eyre!("user = {:?} not found", event.onBehalfOf))?
        .clone();
    let row_num = user_settings.row_num;
    let idx = tokens
        .get(&event.reserve)
        .ok_or_else(|| eyre!("token = {:?} not found", event.reserve))?
        .order;
    let now = Utc::now().timestamp_micros();

    let c = cache.clone();
    let sync_user = async move || {
        debug!("supply: sync_user user = {}", event.onBehalfOf);

        c.sync_user(&event.onBehalfOf, &tokens, provider).await?;

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

        Ok(())
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
                .await?;
            hf_tx
                .send(HFRequest::User(event.onBehalfOf, rq_date))
                .await?;

            Ok(())
        };
        let skip_event = async move || {
            debug!(
                "event dated before sync:\
                     event = supply, user = {}, rq_date = {}, collateral sync = {}, collateral rq_date = {}",
                event.onBehalfOf, rq_date, last_sync, last_modified,
            );

            Ok(())
        };
        let unknown = async move || {
            info!(
                "detected unknown case:\
                     event = supply, user = {}, rq_date = {}, collateral sync = {}, collateral rq_date = {}",
                event.onBehalfOf, rq_date, last_sync, last_modified
            );

            Ok(())
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

            Ok(())
        };
        let skip_event = async move || {
            debug!(
                "event dated before sync:\
                     event = supply, user = {}, rq_date = {}, reserve sync = {}, reserve rq_date = {}",
                event.onBehalfOf, rq_date, last_sync, last_modified,
            );

            Ok(())
        };
        let unknown = async move || {
            info!(
                "detected unknown case:\
                     event = supply, user = {}, rq_date = {}, reserve sync = {}, reserve rq_date = {}",
                event.onBehalfOf, rq_date, last_sync, last_modified
            );

            Ok(())
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

pub(in crate::arbitrum) async fn withdraw<P>(
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

pub(in crate::arbitrum) async fn borrow<P>(
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

pub(in crate::arbitrum) async fn repay<P>(
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

pub(in crate::arbitrum) async fn reserve_used_as_collateral_enabled<P>(
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

pub(in crate::arbitrum) async fn reserve_used_as_collateral_disabled<P>(
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

pub(in crate::arbitrum) async fn liquidation_call<P>(
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

pub(in crate::arbitrum) async fn reserve_data_updated<P>(
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

pub(in crate::arbitrum) async fn answer_updated<P>(
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
