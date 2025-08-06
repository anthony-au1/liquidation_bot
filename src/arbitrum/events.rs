use crate::arbitrum::arbitrum::IChainlinkAggregator::AnswerUpdated;
use crate::arbitrum::arbitrum::IL2Pool::{
    Borrow, LiquidationCall, Repay, ReserveDataUpdated, ReserveUsedAsCollateralDisabled,
    ReserveUsedAsCollateralEnabled, Supply, Withdraw,
};
use crate::arbitrum::arbitrum::{Cache, DataProvider, HFRequest, SyncRequest, TimeStamp, Tokens};
use alloy_primitives::{Address, I256};
use chrono::Utc;
use eyre::eyre;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tracing::debug;

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

async fn handle_event<P, F1, R1, F2, R2>(
    user: &Address,
    cache: &Cache,
    tokens: &Tokens,
    provider: Arc<P>,
    sync_tx: &Sender<SyncRequest>,
    hf_tx: &Sender<HFRequest>,
    rq_date: TimeStamp,
    last_sync: TimeStamp,
    last_modified: TimeStamp,
    new_event: F1,
    skip_event: F2,
) -> eyre::Result<()>
where
    P: DataProvider + 'static,
    F1: FnOnce() -> R1,
    R1: Future<Output = eyre::Result<()>> + Send,
    F2: FnOnce() -> R2,
    R2: Future<Output = eyre::Result<()>> + Send,
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
            // sync user
            debug!("supply: sync_user user = {:?}", user);
            cache.sync_user(&user, &tokens, provider).await?;
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

            debug!("{}", {
                let received = Utc::now().timestamp_micros();
                format!(
                    "sync_user: cache = {:?}, rq_date = {}, \
                             received = {}, delta = {} μs",
                    cache,
                    rq_date,
                    received,
                    received - rq_date
                )
            });
        }
        _ => {
            unreachable!(
                "rq_date = {}, last_sync = {}, last_modified = {}",
                rq_date, last_sync, last_modified
            );
        }
    }

    Ok(())
}

pub(crate) async fn supply<P>(
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

    if user_settings.use_as_collateral[idx] {
        let (last_sync, last_modified) = {
            let collateral_lock = cache.collateral.read().await;
            (collateral_lock.1, collateral_lock.2)
        };

        let (c, s_tx, h_tx) = (cache.clone(), sync_tx.clone(), hf_tx.clone());
        let new_event = async move || {
            debug!("supply: collateral new event user = {}", event.onBehalfOf);

            let mut collateral_lock = c.collateral.write().await;
            (collateral_lock.1, collateral_lock.2) = (now, now);
            let mut row_lock = collateral_lock.0[row_num].write().await;
            row_lock[idx] += f64::from(event.amount);
            s_tx.send(SyncRequest::Collateral(row_num, rq_date)).await?;
            h_tx.send(HFRequest::User(event.onBehalfOf, rq_date))
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

        handle_event(
            &event.onBehalfOf,
            &cache,
            &tokens,
            provider.clone(),
            &sync_tx,
            &hf_tx,
            rq_date,
            last_sync,
            last_modified,
            new_event,
            skip_event,
        )
        .await?;
    } else {
        let (last_sync, last_modified) = {
            let reserve_lock = cache.reserve.read().await;
            (reserve_lock.1, reserve_lock.2)
        };

        let c = cache.clone();
        let new_event = async move || {
            debug!("supply: borrowed new event user = {}", event.onBehalfOf);

            let mut reserve_lock = c.reserve.write().await;
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

        handle_event(
            &event.onBehalfOf,
            &cache,
            &tokens,
            provider.clone(),
            &sync_tx,
            &hf_tx,
            rq_date,
            last_sync,
            last_modified,
            new_event,
            skip_event,
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

pub(crate) async fn withdraw<P>(
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

pub(crate) async fn borrow<P>(
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

pub(crate) async fn repay<P>(
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

pub(crate) async fn reserve_used_as_collateral_enabled<P>(
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

pub(crate) async fn reserve_used_as_collateral_disabled<P>(
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

pub(crate) async fn liquidation_call<P>(
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

pub(crate) async fn reserve_data_updated<P>(
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

pub(crate) async fn answer_updated<P>(
    cache: Arc<Cache>,
    _: Arc<P>,
    tokens: Arc<Tokens>,
    event: (AnswerUpdated, Address, Sender<HFRequest>, TimeStamp),
) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    let (AnswerUpdated { current, .. }, token, hf_tx, rq_date) = event;
    let token_details = tokens
        .get(&token)
        .ok_or_else(|| eyre::eyre!("token not found: {}", token))?;

    debug!("{}", {
        let received = Utc::now().timestamp_micros();
        format!(
            "answer_updated ({}): rq_date = {}, received = {}, delta = {} μs",
            token_details.name.clone(),
            rq_date,
            received,
            received - rq_date
        )
    });

    {
        let (prices, last_modified) = &mut *cache.prices.write().await;
        prices[token_details.order] = current.as_u64() as f64 / 100_000_000.0;
        *last_modified = rq_date;
    }

    hf_tx.send(HFRequest::Full(rq_date)).await?;

    Ok(())
}
