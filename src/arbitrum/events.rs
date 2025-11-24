use crate::arbitrum::arbitrum::IL2Pool::{
    Borrow, LiquidationCall, Repay, ReserveDataUpdated, ReserveUsedAsCollateralDisabled,
    ReserveUsedAsCollateralEnabled, Supply, Withdraw,
};
use crate::arbitrum::arbitrum::{
    Cache, DataProvider, F64Converter, HFRequest, RAY, RayOperations, RqDate, SECONDS_PER_YEAR,
    Scaler, SyncRequest, SyncTarget, TimeStamp, TokenDetails, Tokens, get_latest_liquidity_index,
    get_latest_variable_borrow_index,
};
use alloy_primitives::{Address, U256};
use chrono::Utc;
use eyre::eyre;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tracing::{debug, error};

pub(in crate::arbitrum) async fn create_user<P>(
    rq_date: TimeStamp,
    cache: &Cache,
    provider: Arc<P>,
    tokens: &Tokens,
    user: &Address,
    sync_tx: &Sender<SyncRequest>,
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
                        "create_user (user = {}): rq_date = {}, \
                             received = {}, delta = {} μs",
                        user,
                        rq_date,
                        received,
                        received - rq_date
                    )
                });

                sync_tx
                    .send(SyncRequest::Both(
                        SyncTarget::Row(
                            cache
                                .users
                                .get(user)
                                .ok_or_else(|| eyre!("create_user: user = {} not found", user))?
                                .row_num,
                        ),
                        rq_date,
                    ))
                    .await?;
                return Ok(true);
            }
        }
        Err(e) => {
            debug!("create_user (user = {}): error = {:?}", user, e);
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
    let sync_requested = Utc::now().timestamp_micros() - last_sync > 86_400_000_000;
    match rq_date {
        t if !sync_requested && t > last_modified => {
            // new event
            new_event().await?;
        }
        t if !sync_requested && t <= last_sync => {
            debug!("handle_event (user = {}): skip", user);
            skip_event().await?;
        }
        t if sync_requested || (t > last_sync && t <= last_modified) => {
            debug!("handle_event (user = {}): sync", user);
            cache.sync_user(&user, &tokens, provider).await?;

            sync_tx
                .send(SyncRequest::Both(
                    SyncTarget::Row(
                        cache
                            .users
                            .get(user)
                            .ok_or_else(|| {
                                eyre!("handle_event: user = {} not found for sync rq", user)
                            })?
                            .row_num,
                    ),
                    rq_date,
                ))
                .await?;

            debug!("{}", {
                let received = Utc::now().timestamp_micros();
                format!(
                    "handle_event (user = {}): rq_date = {}, \
                             received = {}, delta = {} μs for sync",
                    user,
                    rq_date,
                    received,
                    received - rq_date
                )
            });
        }
        _ => {
            unreachable!(
                "handle_event (user = {}): rq_date = {}, last_sync = {}, last_modified = {} for unknown case",
                user, rq_date, last_sync, last_modified
            );
        }
    }

    Ok(())
}

pub(crate) async fn supply<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (Supply, Sender<SyncRequest>, Sender<HFRequest>, RqDate),
) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    let (event, sync_tx, _, RqDate(rq_date)) = event;

    debug!("{}", {
        let received = Utc::now().timestamp_micros();
        format!(
            "supply (user = {}): amount = {}, reserve = {}, rq_date = {}, received = {}, delta = {} μs",
            event.onBehalfOf,
            event.amount,
            event.reserve,
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
    )
    .await?
    {
        debug!("supply: new user created = {}", event.onBehalfOf);
        return Ok(());
    }

    let user_settings = cache
        .users
        .get(&event.onBehalfOf)
        .ok_or_else(|| eyre!("supply: user = {} not found", event.onBehalfOf))?
        .clone();
    let row_num = user_settings.row_num;
    let idx = tokens
        .get(&event.reserve)
        .ok_or_else(|| {
            eyre!(
                "supply (user = {}): token = {} not found",
                event.onBehalfOf,
                event.reserve
            )
        })?
        .order;
    let (decimals, _) = &*cache.decimals.read().await;
    let now = Utc::now().timestamp_micros();

    if user_settings.use_as_collateral[idx] {
        let (last_sync, last_modified) = {
            let collateral = cache.collateral.read().await.to_vec();
            let (_, last_sync, last_modified) = &*collateral
                .get(row_num)
                .ok_or_else(|| {
                    eyre!(
                        "supply (user = {}): can't get row = {} from collateral",
                        event.onBehalfOf,
                        row_num
                    )
                })?
                .read()
                .await;

            (*last_sync, *last_modified)
        };

        let (c, s_tx) = (cache.clone(), sync_tx.clone());
        let new_event = async move || {
            let collateral = c.collateral.read().await.to_vec();
            let (col, _, last_modified) = &mut *collateral
                .get(row_num)
                .ok_or_else(|| {
                    eyre!(
                        "supply (user = {}): can't get row = {} from collateral",
                        event.onBehalfOf,
                        row_num
                    )
                })?
                .write()
                .await;

            {
                let (liquidity, _) = &*c.liquidity.read().await;
                let liquidity_index = get_latest_liquidity_index(&liquidity[idx], now)?;
                col[idx] += event
                    .amount
                    .to_ray(decimals[idx])
                    .to_scaled(liquidity_index);

                debug!(
                    "supply (user = {}): collateral amount = {}, col = {}, liquidity = {:?}, liquidity_index_updated = {}",
                    event.onBehalfOf, event.amount, col, liquidity[idx], liquidity_index
                );
            }
            *last_modified = now;

            s_tx.send(SyncRequest::Collateral(
                SyncTarget::Cell(row_num, idx),
                rq_date,
            ))
            .await?;

            Ok(())
        };
        let skip_event = async move || {
            debug!(
                "supply (user = {}): event dated before sync:\
                     event = supply, rq_date = {}, collateral sync = {}, collateral modified = {}",
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
            rq_date,
            last_sync,
            last_modified,
            new_event,
            skip_event,
        )
        .await?;
    } else {
        let (last_sync, last_modified) = {
            let reserve = cache.reserve.read().await.to_vec();
            let (_, last_sync, last_modified) = &*reserve
                .get(row_num)
                .ok_or_else(|| {
                    eyre!(
                        "supply (user = {}): can't get row = {} from reserve",
                        event.onBehalfOf,
                        row_num
                    )
                })?
                .read()
                .await;

            (*last_sync, *last_modified)
        };

        let c = cache.clone();
        let new_event = async move || {
            let reserve = c.reserve.read().await.to_vec();
            let (res, _, last_modified) = &mut *reserve
                .get(row_num)
                .ok_or_else(|| {
                    eyre!(
                        "supply (user = {}): can't get row = {} from reserve",
                        event.onBehalfOf,
                        row_num
                    )
                })?
                .write()
                .await;

            {
                let (liquidity, _) = &*c.liquidity.read().await;
                let liquidity_index = get_latest_liquidity_index(&liquidity[idx], now)?;
                res[idx] += event
                    .amount
                    .to_ray(decimals[idx])
                    .to_scaled(liquidity_index);

                debug!(
                    "supply (user = {}): reserve amount = {}, res = {}, liquidity = {:?}, liquidity_index_updated = {}",
                    event.onBehalfOf, event.amount, res, liquidity[idx], liquidity_index
                );
            }
            *last_modified = now;

            Ok(())
        };
        let skip_event = async move || {
            debug!(
                "supply (user = {}): event dated before sync:\
                     event = supply, rq_date = {}, reserve sync = {}, reserve modified = {}",
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
            "supply (user = {}): rq_date = {}, \
                     received = {}, delta = {} μs",
            event.onBehalfOf,
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
    event: (Withdraw, Sender<SyncRequest>, Sender<HFRequest>, RqDate),
) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    let (event, sync_tx, _, RqDate(rq_date)) = event;

    debug!("{}", {
        let received = Utc::now().timestamp_micros();
        format!(
            "withdraw (user = {}): amount = {}, reserve = {}, rq_date = {}, received = {}, delta = {} μs",
            event.user,
            event.amount,
            event.reserve,
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
        &event.user,
        &sync_tx,
    )
    .await?
    {
        debug!("withdraw: new user created = {}", event.user);
        return Ok(());
    }

    let user_settings = cache
        .users
        .get(&event.user)
        .ok_or_else(|| eyre!("withdraw: user = {} not found", event.user))?
        .clone();
    let row_num = user_settings.row_num;
    let idx = tokens
        .get(&event.reserve)
        .ok_or_else(|| {
            eyre!(
                "withdraw (user = {}): token = {} not found",
                event.user,
                event.reserve
            )
        })?
        .order;
    let (decimals, _) = &*cache.decimals.read().await;
    let now = Utc::now().timestamp_micros();

    if user_settings.use_as_collateral[idx] {
        let (last_sync, last_modified) = {
            let collateral = cache.collateral.read().await.to_vec();
            let (_, last_sync, last_modified) = &*collateral
                .get(row_num)
                .ok_or_else(|| {
                    eyre!(
                        "withdraw (user = {}): can't get row = {} from collateral",
                        event.user,
                        row_num
                    )
                })?
                .read()
                .await;

            (*last_sync, *last_modified)
        };

        let (c, s_tx) = (cache.clone(), sync_tx.clone());
        let new_event = async move || {
            let collateral = c.collateral.read().await.to_vec();
            let (col, _, last_modified) = &mut *collateral
                .get(row_num)
                .ok_or_else(|| {
                    eyre!(
                        "withdraw (user = {}): can't get row = {} from collateral",
                        event.user,
                        row_num
                    )
                })?
                .write()
                .await;

            {
                let (liquidity, _) = &*c.liquidity.read().await;
                let liquidity_index = get_latest_liquidity_index(&liquidity[idx], now)?;
                col[idx] = col[idx].saturating_sub(
                    event
                        .amount
                        .to_ray(decimals[idx])
                        .to_scaled(liquidity_index),
                );

                debug!(
                    "withdraw (user = {}): collateral amount = {}, col = {}, liquidity = {:?}, liquidity_index_updated = {}",
                    event.user, event.amount, col, liquidity[idx], liquidity_index
                );
            }
            *last_modified = now;

            s_tx.send(SyncRequest::Collateral(
                SyncTarget::Cell(row_num, idx),
                rq_date,
            ))
            .await?;

            Ok(())
        };
        let skip_event = async move || {
            debug!(
                "withdraw (user = {}): event dated before sync:\
                     event = withdraw, rq_date = {}, collateral sync = {}, collateral modified = {}",
                event.user, rq_date, last_sync, last_modified,
            );

            Ok(())
        };

        handle_event(
            &event.user,
            &cache,
            &tokens,
            provider.clone(),
            &sync_tx,
            rq_date,
            last_sync,
            last_modified,
            new_event,
            skip_event,
        )
        .await?;
    } else {
        let (last_sync, last_modified) = {
            let reserve = cache.reserve.read().await.to_vec();
            let (_, last_sync, last_modified) = &*reserve
                .get(row_num)
                .ok_or_else(|| {
                    eyre!(
                        "withdraw (user = {}): can't get row = {} from reserve",
                        event.user,
                        row_num
                    )
                })?
                .read()
                .await;

            (*last_sync, *last_modified)
        };

        let c = cache.clone();
        let new_event = async move || {
            let reserve = c.reserve.read().await.to_vec();
            let (res, _, last_modified) = &mut *reserve
                .get(row_num)
                .ok_or_else(|| {
                    eyre!(
                        "withdraw (user = {}): can't get row = {} from reserve",
                        event.user,
                        row_num
                    )
                })?
                .write()
                .await;

            {
                let (liquidity, _) = &*c.liquidity.read().await;
                let liquidity_index = get_latest_liquidity_index(&liquidity[idx], now)?;
                res[idx] = res[idx].saturating_sub(
                    event
                        .amount
                        .to_ray(decimals[idx])
                        .to_scaled(liquidity_index),
                );

                debug!(
                    "withdraw (user = {}): reserve amount = {}, res = {}, liquidity = {:?}, liquidity_index_updated = {}",
                    event.user, event.amount, res, liquidity[idx], liquidity_index
                );
            }
            *last_modified = now;

            Ok(())
        };
        let skip_event = async move || {
            debug!(
                "withdraw (user = {}): event dated before sync:\
                     event = withdraw, rq_date = {}, reserve sync = {}, reserve modified = {}",
                event.user, rq_date, last_sync, last_modified,
            );

            Ok(())
        };

        handle_event(
            &event.user,
            &cache,
            &tokens,
            provider.clone(),
            &sync_tx,
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
            "withdraw (user = {}): rq_date = {}, \
                     received = {}, delta = {} μs",
            event.user,
            rq_date,
            received,
            received - rq_date
        )
    });

    Ok(())
}

pub(crate) async fn borrow<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (Borrow, Sender<SyncRequest>, Sender<HFRequest>, RqDate),
) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    let (event, sync_tx, _, RqDate(rq_date)) = event;

    debug!("{}", {
        let received = Utc::now().timestamp_micros();
        format!(
            "borrow (user = {}): amount = {}, reserve = {}, rq_date = {}, received = {}, delta = {} μs",
            event.onBehalfOf,
            event.amount,
            event.reserve,
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
    )
    .await?
    {
        debug!("borrow: new user created = {}", event.onBehalfOf);

        return Ok(());
    }

    let user_settings = cache
        .users
        .get(&event.onBehalfOf)
        .ok_or_else(|| eyre!("borrow: user = {} not found", event.onBehalfOf))?
        .clone();
    let row_num = user_settings.row_num;
    let idx = tokens
        .get(&event.reserve)
        .ok_or_else(|| {
            eyre!(
                "borrow (user = {}): token = {} not found",
                event.onBehalfOf,
                event.reserve
            )
        })?
        .order;
    let (decimals, _) = &*cache.decimals.read().await;
    let now = Utc::now().timestamp_micros();

    let (last_sync, last_modified) = {
        let borrowed = cache.borrowed.read().await.to_vec();
        let (_, last_sync, last_modified) = &*borrowed
            .get(row_num)
            .ok_or_else(|| {
                eyre!(
                    "borrow (user = {}): can't get row = {} from borrowed",
                    event.onBehalfOf,
                    row_num
                )
            })?
            .read()
            .await;

        (*last_sync, *last_modified)
    };

    let (c, s_tx) = (cache.clone(), sync_tx.clone());
    let new_event = async move || {
        let borrowed = c.borrowed.read().await.to_vec();
        let (bor, _, last_modified) = &mut *borrowed
            .get(row_num)
            .ok_or_else(|| {
                eyre!(
                    "borrow (user = {}): can't get row = {} from borrowed",
                    event.onBehalfOf,
                    row_num
                )
            })?
            .write()
            .await;

        {
            let (variable_borrow, _) = &*c.variable_borrow.read().await;
            let variable_borrow_index =
                get_latest_variable_borrow_index(&variable_borrow[idx], now)?;
            bor[idx] += event
                .amount
                .to_ray(decimals[idx])
                .to_scaled(variable_borrow_index);

            debug!(
                "borrow (user = {}): borrowed amount = {}, bor = {}, \
                variable_borrow = {:?}, variable_borrow_index_updated = {}",
                event.onBehalfOf, event.amount, bor, variable_borrow, variable_borrow_index
            );
        }
        *last_modified = now;

        s_tx.send(SyncRequest::Borrowed(
            SyncTarget::Cell(row_num, idx),
            rq_date,
        ))
        .await?;

        Ok(())
    };
    let skip_event = async move || {
        debug!(
            "borrow (user = {}): event dated before sync:\
                     event = borrow, rq_date = {}, borrowed sync = {}, borrowed modified = {}",
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
        rq_date,
        last_sync,
        last_modified,
        new_event,
        skip_event,
    )
    .await?;

    debug!("{}", {
        let received = Utc::now().timestamp_micros();
        format!(
            "borrow (user = {}): rq_date = {}, \
                     received = {}, delta = {} μs",
            event.onBehalfOf,
            rq_date,
            received,
            received - rq_date
        )
    });

    Ok(())
}

pub(crate) async fn repay<P>(
    cache: Arc<Cache>,
    provider: Arc<P>,
    tokens: Arc<Tokens>,
    event: (Repay, Sender<SyncRequest>, Sender<HFRequest>, RqDate),
) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    let (event, sync_tx, _, RqDate(rq_date)) = event;

    debug!("{}", {
        let received = Utc::now().timestamp_micros();
        format!(
            "repay (user = {}): amount = {}, reserve = {}, rq_date = {}, received = {}, delta = {} μs",
            event.user,
            event.amount,
            event.reserve,
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
        &event.user,
        &sync_tx,
    )
    .await?
    {
        debug!("repay: new user created = {}", event.user);

        return Ok(());
    }

    let user_settings = cache
        .users
        .get(&event.user)
        .ok_or_else(|| eyre!("repay: user = {} not found", event.user))?
        .clone();
    let row_num = user_settings.row_num;
    let idx = tokens
        .get(&event.reserve)
        .ok_or_else(|| {
            eyre!(
                "repay (user = {}): token = {} not found",
                event.user,
                event.reserve
            )
        })?
        .order;
    let (decimals, _) = &*cache.decimals.read().await;
    let now = Utc::now().timestamp_micros();

    let (last_sync, last_modified) = {
        let borrowed = cache.borrowed.read().await.to_vec();
        let (_, last_sync, last_modified) = &*borrowed
            .get(row_num)
            .ok_or_else(|| {
                eyre!(
                    "repay (user = {}): can't get row = {} from borrowed",
                    event.user,
                    row_num
                )
            })?
            .read()
            .await;

        (*last_sync, *last_modified)
    };

    let (c, s_tx) = (cache.clone(), sync_tx.clone());
    let new_event = async move || {
        let borrowed = c.borrowed.read().await.to_vec();
        let (bor, _, last_modified) = &mut *borrowed
            .get(row_num)
            .ok_or_else(|| {
                eyre!(
                    "repay (user = {}): can't get row = {} from borrowed",
                    event.user,
                    row_num
                )
            })?
            .write()
            .await;

        {
            let (variable_borrow, _) = &*c.variable_borrow.read().await;
            let variable_borrow_index =
                get_latest_variable_borrow_index(&variable_borrow[idx], now)?;
            bor[idx] = bor[idx].saturating_sub(
                event
                    .amount
                    .to_ray(decimals[idx])
                    .to_scaled(variable_borrow_index),
            );

            debug!(
                "repay (user = {}): borrowed amount = {}, bor = {}, variable_borrow = {:?}, variable_borrow_index_updated = {}",
                event.user, event.amount, bor, variable_borrow, variable_borrow_index
            );
        }
        *last_modified = now;

        s_tx.send(SyncRequest::Borrowed(
            SyncTarget::Cell(row_num, idx),
            rq_date,
        ))
        .await?;

        Ok(())
    };
    let skip_event = async move || {
        debug!(
            "repay (user = {}): event dated before sync:\
                     event = repay, rq_date = {}, borrowed sync = {}, borrowed modified = {}",
            event.user, rq_date, last_sync, last_modified,
        );

        Ok(())
    };

    handle_event(
        &event.user,
        &cache,
        &tokens,
        provider.clone(),
        &sync_tx,
        rq_date,
        last_sync,
        last_modified,
        new_event,
        skip_event,
    )
    .await?;

    debug!("{}", {
        let received = Utc::now().timestamp_micros();
        format!(
            "repay (user = {}): rq_date = {}, \
                     received = {}, delta = {} μs",
            event.user,
            rq_date,
            received,
            received - rq_date
        )
    });

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
        RqDate,
    ),
) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    let (event, sync_tx, _, RqDate(rq_date)) = event;

    debug!("{}", {
        let received = Utc::now().timestamp_micros();
        format!(
            "reserve_used_as_collateral_enabled (user = {}): reserve = {}, rq_date = {}, received = {}, delta = {} μs",
            event.user,
            event.reserve,
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
        &event.user,
        &sync_tx,
    )
    .await?
    {
        debug!(
            "reserve_used_as_collateral_enabled: new user created = {}",
            event.user
        );

        return Ok(());
    }

    let user_settings = cache
        .users
        .get(&event.user)
        .ok_or_else(|| {
            eyre!(
                "reserve_used_as_collateral_enabled: user = {} not found",
                event.user
            )
        })?
        .clone();
    let row_num = user_settings.row_num;
    let idx = tokens
        .get(&event.reserve)
        .ok_or_else(|| {
            eyre!(
                "reserve_used_as_collateral_enabled (user = {}): token = {:?} not found",
                event.user,
                event.reserve
            )
        })?
        .order;
    let now = Utc::now().timestamp_micros();

    let (last_sync, last_modified) = {
        let reserve = cache.reserve.read().await.to_vec();
        let (_, last_sync, last_modified) = &*reserve
            .get(row_num)
            .ok_or_else(|| eyre!("reserve_used_as_collateral_enabled (user = {}): can't get row = {} from reserve", event.user, row_num))?
            .read()
            .await;

        (*last_sync, *last_modified)
    };

    let (c, s_tx) = (cache.clone(), sync_tx.clone());
    let new_event = async move || {
        c.users
            .get_mut(&event.user)
            .ok_or_else(|| {
                eyre!(
                    "reserve_used_as_collateral_enabled: user = {} not found",
                    event.user
                )
            })?
            .use_as_collateral
            .set(idx, true);

        let reserve = c.reserve.read().await.to_vec();
        let (res, _, last_modified) = &mut *reserve
            .get(row_num)
            .ok_or_else(|| eyre!("reserve_used_as_collateral_enabled (user = {}): can't get row = {} from reserve", event.user, row_num))?
            .write()
            .await;
        *last_modified = now;

        let collateral = c.collateral.read().await.to_vec();
        let (col, _, last_modified) = &mut *collateral
            .get(row_num)
            .ok_or_else(|| eyre!("reserve_used_as_collateral_enabled (user = {}): can't get row = {} from collateral", event.user, row_num))?
            .write()
            .await;
        *last_modified = now;

        col[idx] = res[idx];
        res[idx] = U256::default();

        debug!(
            "reserve_used_as_collateral_enabled (user = {}): col = {}, res = {}",
            event.user, col, res
        );

        s_tx.send(SyncRequest::Collateral(
            SyncTarget::Cell(row_num, idx),
            rq_date,
        ))
        .await?;

        Ok(())
    };
    let skip_event = async move || {
        debug!(
            "reserve_used_as_collateral_enabled (user = {}): event dated before sync:\
                     event = reserve_used_as_collateral_enabled, rq_date = {}, reserve sync = {}, reserve modified = {}",
            event.user, rq_date, last_sync, last_modified,
        );

        Ok(())
    };

    handle_event(
        &event.user,
        &cache,
        &tokens,
        provider.clone(),
        &sync_tx,
        rq_date,
        last_sync,
        last_modified,
        new_event,
        skip_event,
    )
    .await?;

    debug!("{}", {
        let received = Utc::now().timestamp_micros();
        format!(
            "reserve_used_as_collateral_enabled (user = {}): rq_date = {}, \
                     received = {}, delta = {} μs",
            event.user,
            rq_date,
            received,
            received - rq_date
        )
    });

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
        RqDate,
    ),
) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    let (event, sync_tx, _, RqDate(rq_date)) = event;

    debug!("{}", {
        let received = Utc::now().timestamp_micros();
        format!(
            "reserve_used_as_collateral_disabled (user = {}): reserve = {}, rq_date = {}, received = {}, delta = {} μs",
            event.user,
            event.reserve,
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
        &event.user,
        &sync_tx,
    )
    .await?
    {
        debug!(
            "reserve_used_as_collateral_disabled: new user created = {}",
            event.user
        );

        return Ok(());
    }

    let user_settings = cache
        .users
        .get(&event.user)
        .ok_or_else(|| {
            eyre!(
                "reserve_used_as_collateral_disabled: user = {} not found",
                event.user
            )
        })?
        .clone();
    let row_num = user_settings.row_num;
    let idx = tokens
        .get(&event.reserve)
        .ok_or_else(|| {
            eyre!(
                "reserve_used_as_collateral_disabled (user = {}): token = {} not found",
                event.user,
                event.reserve
            )
        })?
        .order;
    let now = Utc::now().timestamp_micros();

    let (last_sync, last_modified) = {
        let reserve = cache.reserve.read().await.to_vec();
        let (_, last_sync, last_modified) = &*reserve
            .get(row_num)
            .ok_or_else(|| eyre!("reserve_used_as_collateral_disabled (user = {}): can't get row = {} from reserve", event.user, row_num))?
            .read()
            .await;

        (*last_sync, *last_modified)
    };

    let (c, s_tx) = (cache.clone(), sync_tx.clone());
    let new_event = async move || {
        c.users
            .get_mut(&event.user)
            .ok_or_else(|| {
                eyre!(
                    "reserve_used_as_collateral_disabled: user = {} not found",
                    event.user
                )
            })?
            .use_as_collateral
            .set(idx, false);

        let reserve = c.reserve.read().await.to_vec();
        let (res, _, last_modified) = &mut *reserve
            .get(row_num)
            .ok_or_else(|| eyre!("reserve_used_as_collateral_disabled (user = {}): can't get row = {} from reserve", event.user, row_num))?
            .write()
            .await;
        *last_modified = now;

        let collateral = c.collateral.read().await.to_vec();
        let (col, _, last_modified) = &mut *collateral
            .get(row_num)
            .ok_or_else(|| eyre!("reserve_used_as_collateral_disabled (user = {}): can't get row = {} from collateral", event.user, row_num))?
            .write()
            .await;
        *last_modified = now;

        res[idx] = col[idx];
        col[idx] = U256::default();

        debug!(
            "reserve_used_as_collateral_disabled (user = {}): res = {}, col = {}",
            event.user, res, col
        );

        s_tx.send(SyncRequest::Collateral(
            SyncTarget::Cell(row_num, idx),
            rq_date,
        ))
        .await?;

        Ok(())
    };
    let skip_event = async move || {
        debug!(
            "reserve_used_as_collateral_disabled (user = {}): event dated before sync:\
                     event = reserve_used_as_collateral_disabled, rq_date = {}, reserve sync = {}, reserve modified = {}",
            event.user, rq_date, last_sync, last_modified,
        );

        Ok(())
    };

    handle_event(
        &event.user,
        &cache,
        &tokens,
        provider.clone(),
        &sync_tx,
        rq_date,
        last_sync,
        last_modified,
        new_event,
        skip_event,
    )
    .await?;

    debug!("{}", {
        let received = Utc::now().timestamp_micros();
        format!(
            "reserve_used_as_collateral_disabled (user = {}): rq_date = {}, \
                     received = {}, delta = {} μs",
            event.user,
            rq_date,
            received,
            received - rq_date
        )
    });

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
        RqDate,
    ),
) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    let (event, sync_tx, _, RqDate(rq_date)) = event;

    debug!("{}", {
        let received = Utc::now().timestamp_micros();
        format!(
            "liquidation_call (user = {}): debtToCover = {}, debtReserve = {}, \
            liquidatedCollateralAmount = {}, liquidatedCollateralReserve = {}, rq_date = {}, received = {}, delta = {} μs",
            event.user,
            event.debtToCover,
            event.debtAsset,
            event.liquidatedCollateralAmount,
            event.collateralAsset,
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
        &event.user,
        &sync_tx,
    )
    .await?
    {
        debug!("liquidation_call: new user created = {}", event.user);

        return Ok(());
    }

    let user_settings = cache
        .users
        .get(&event.user)
        .ok_or_else(|| eyre!("liquidation_call: user = {} not found", event.user))?
        .clone();
    let row_num = user_settings.row_num;
    let (decimals, _) = &*cache.decimals.read().await;
    let now = Utc::now().timestamp_micros();

    let bor_idx = tokens
        .get(&event.debtAsset)
        .ok_or_else(|| {
            eyre!(
                "liquidation_call (user = {}): token = {} not found",
                event.user,
                event.debtAsset
            )
        })?
        .order;

    let col_idx = tokens
        .get(&event.collateralAsset)
        .ok_or_else(|| {
            eyre!(
                "liquidation_call (user = {}): token = {} not found",
                event.user,
                event.collateralAsset
            )
        })?
        .order;

    let (last_sync, last_modified) = {
        let borrowed = cache.borrowed.read().await.to_vec();
        let (_, last_sync, last_modified) = &*borrowed
            .get(row_num)
            .ok_or_else(|| {
                eyre!(
                    "liquidation_call (user = {}): can't get row = {} from borrowed",
                    event.user,
                    row_num
                )
            })?
            .read()
            .await;

        (*last_sync, *last_modified)
    };

    let (c, s_tx) = (cache.clone(), sync_tx.clone());
    let new_event = async move || {
        let collateral = c.collateral.read().await.to_vec();
        let (col, _, last_modified) = &mut *collateral
            .get(row_num)
            .ok_or_else(|| {
                eyre!(
                    "liquidation_call (user = {}): can't get row = {} from collateral",
                    event.user,
                    row_num
                )
            })?
            .write()
            .await;
        *last_modified = now;

        let borrowed = c.borrowed.read().await.to_vec();
        let (bor, _, last_modified) = &mut *borrowed
            .get(row_num)
            .ok_or_else(|| {
                eyre!(
                    "liquidation_call (user = {}): can't get row = {} from borrowed",
                    event.user,
                    row_num
                )
            })?
            .write()
            .await;
        *last_modified = now;

        {
            let (variable_borrow, _) = &*c.variable_borrow.read().await;
            let variable_borrow_index =
                get_latest_variable_borrow_index(&variable_borrow[bor_idx], now)?;
            bor[bor_idx] = bor[bor_idx].saturating_sub(
                event
                    .debtToCover
                    .to_ray(decimals[bor_idx])
                    .to_scaled(variable_borrow_index),
            );

            // let (liquidity, _) = &*c.liquidity.read().await;
            // let liquidity_index = get_latest_liquidity_index(&liquidity[col_idx], now)?;
            // col[col_idx] = col[col_idx].saturating_sub(
            //     event
            //         .liquidatedCollateralAmount
            //         .to_ray(decimals[col_idx])
            //         .to_scaled(liquidity_index),
            // );

            // debug!(
            //     "liquidation_call (user = {}): borrowed repay amount = {}, bor = {},\
            //  collateral liquidated amount = {}, col = {}, variable_borrow = {:?}, variable_borrow_index_updated = {}, \
            //  liquidity = {:?}, liquidity_index_updated = {}",
            //     event.user,
            //     event.debtToCover,
            //     bor,
            //     event.liquidatedCollateralAmount,
            //     col,
            //     variable_borrow,
            //     variable_borrow_index,
            //     liquidity,
            //     liquidity_index
            // );

            debug!(
                "liquidation_call (user = {}): borrowed repay amount = {}, bor = {},\
             collateral liquidated amount = {}, col = {}, variable_borrow = {:?}, variable_borrow_index_updated = {}",
                event.user,
                event.debtToCover,
                bor,
                event.liquidatedCollateralAmount,
                col,
                variable_borrow,
                variable_borrow_index,
            );
        }

        // s_tx.send(SyncRequest::Collateral(
        //     SyncTarget::Cell(row_num, col_idx),
        //     rq_date,
        // ))
        // .await?;
        s_tx.send(SyncRequest::Borrowed(
            SyncTarget::Cell(row_num, bor_idx),
            rq_date,
        ))
        .await?;

        Ok(())
    };
    let skip_event = async move || {
        debug!(
            "liquidation_call (user = {}): event dated before sync:\
                     event = liquidation_call, rq_date = {}, borrowed sync = {}, borrowed modified = {}",
            event.user, rq_date, last_sync, last_modified,
        );

        Ok(())
    };

    handle_event(
        &event.user,
        &cache,
        &tokens,
        provider.clone(),
        &sync_tx,
        rq_date,
        last_sync,
        last_modified,
        new_event,
        skip_event,
    )
    .await?;

    debug!("{}", {
        let received = Utc::now().timestamp_micros();
        format!(
            "liquidation_call (user = {}): rq_date = {}, \
                     received = {}, delta = {} μs",
            event.user,
            rq_date,
            received,
            received - rq_date
        )
    });

    Ok(())
}

pub(crate) async fn reserve_data_updated<P>(
    cache: Arc<Cache>,
    _: Arc<P>,
    tokens: Arc<Tokens>,
    event: (ReserveDataUpdated, Sender<HFRequest>, RqDate),
) -> eyre::Result<()>
where
    P: DataProvider + 'static,
{
    let (event, hf_tx, RqDate(rq_date)) = event;

    debug!("{}", {
        let received = Utc::now().timestamp_micros();
        format!(
            "reserve_data_updated (reserve = {}): rq_date = {}, received = {}, delta = {} μs",
            event.reserve,
            rq_date,
            received,
            received - rq_date
        )
    });

    let now = Utc::now().timestamp_micros();

    let idx = tokens
        .get(&event.reserve)
        .ok_or_else(|| eyre::eyre!("reserve_data_updated: {} token not found", event.reserve))?
        .order;

    {
        let (li, li_last_modified) = &mut *cache.liquidity.write().await;
        (li[idx].index, li[idx].rate, li[idx].last_update) =
            (event.liquidityIndex, event.liquidityRate, now);

        let (lii, lii_last_modified) = &mut *cache.liquidity_index.write().await;
        lii[idx] = event.liquidityIndex.as_f64_ray();

        let (vbi, vbi_last_modified) = &mut *cache.variable_borrow.write().await;
        (vbi[idx].index, vbi[idx].rate, vbi[idx].last_update) =
            (event.variableBorrowIndex, event.variableBorrowRate, now);

        let (vbii, vbii_last_modified) = &mut *cache.variable_borrow_index.write().await;
        vbii[idx] = event.variableBorrowIndex.as_f64_ray();

        for (_, TokenDetails { order, .. }) in tokens.iter() {
            let idx2 = order.clone();
            if idx == idx2 {
                continue;
            }

            let li_new = get_latest_liquidity_index(&li[idx2], now)?;
            (li[idx2].index, li[idx2].last_update) = (li_new, now);
            lii[idx2] = li_new.as_f64_ray();

            let vbi_new = get_latest_variable_borrow_index(&vbi[idx2], now)?;
            (vbi[idx2].index, vbi[idx2].last_update) = (vbi_new, now);
            vbii[idx2] = vbi_new.as_f64_ray();
        }

        (
            *li_last_modified,
            *lii_last_modified,
            *vbi_last_modified,
            *vbii_last_modified,
        ) = (now, now, now, now);

        debug!(
            "reserve_data_updated (token = {}): li = {:?}, lii = {}, vbi = {:?}, vbii = {}",
            event.reserve, li, lii, vbi, vbii
        );
    }

    if let Err(e) = hf_tx.send(HFRequest::Full(rq_date)).await {
        error!(
            "reserve_data_updated: failed to send to hf channel: {:?}",
            e
        );
    }

    Ok(())
}
