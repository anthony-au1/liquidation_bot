use crate::arbitrum::arbitrum::IAaveProtocolDataProvider::TokenData;
use crate::arbitrum::arbitrum::IChainlinkAggregator::IChainlinkAggregatorEvents;
use crate::arbitrum::arbitrum::IL2Pool::{IL2PoolEvents, Supply};
use crate::arbitrum::arbitrum::{
    Cache, DataProvider, HFRequest, SyncRequest, TokenDetails, UserReserveData, UserSettings,
    create_user, supply,
};
use alloy_primitives::Address;
use async_trait::async_trait;
use bitvec::order::Lsb0;
use bitvec::prelude::BitVec;
use chrono::{Days, Utc};
use ndarray::{Array1, Array2};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::sync::mpsc::channel;
use tokio::task;

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

    async fn get_source_of_asset(&self, _: &Address) -> eyre::Result<Address> {
        todo!()
    }

    async fn listen_events<F, Fut>(&self, _: F) -> eyre::Result<()>
    where
        F: Fn(IL2PoolEvents) -> Fut + Send + 'static,
        Fut: Future<Output = eyre::Result<()>> + Send,
    {
        todo!()
    }

    async fn listen_price_update<F, Fut>(&self, _: &Address, _: F) -> eyre::Result<()>
    where
        F: Fn(IChainlinkAggregatorEvents) -> Fut + Send + 'static,
        Fut: Future<Output = eyre::Result<()>> + Send,
    {
        todo!()
    }

    async fn get_reserve_configuration_data(&self, _: &Address) -> eyre::Result<f64> {
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

            let (collaterals, _, _) = &mut *cache.collateral.write().await;
            collaterals.push(RwLock::new(Array1::from_vec(vec![0.0; 3])));

            let (reserves, _, _) = &mut *cache.reserve.write().await;
            reserves.push(RwLock::new(Array1::from_vec(vec![0.0; 3])));

            let (borroweds, _, _) = &mut *cache.borrowed.write().await;
            borroweds.push(RwLock::new(Array1::from_vec(vec![0.0; 3])));
        }
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
        let msg = sync_rc.recv().await.unwrap();
        assert_eq!(SyncRequest::Collateral(0, rq_date), msg);
    });

    let hf_handler = task::spawn(async move {
        let msg = hf_rc.recv().await.unwrap();
        assert_eq!(HFRequest::User(user.clone(), rq_date), msg);
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
        let msg = sync_rc.recv().await.unwrap();
        assert_eq!(SyncRequest::Both(2, rq_date), msg);
    });

    let hf_handler = task::spawn(async move {
        let msg = hf_rc.recv().await.unwrap();
        assert_eq!(HFRequest::User(user.clone(), rq_date), msg);
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
    let cb = move |c: Arc<Cache>, _, _, Message(text)| {
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
    // 1 case - hf calculation for 1 user

    let (cache, _) = generate_cache_and_tokens(1).await?;
    let user = cache.users.iter().next().unwrap().key().clone();

    {
        let (lt, _) = &mut *cache.liquidation_threshold.write().await;
        *lt = Array1::from_vec(vec![0.0, 7500.0, 8000.0]);

        let (price, _) = &mut *cache.prices.write().await;
        *price = Array1::from_vec(vec![120_000.0, 4000.0, 200.0]);

        let (hf, _) = &mut *cache.health_factors.write().await;
        *hf = Array1::from_vec(vec![0.0]);

        let (collaterals, _, _) = &mut *cache.collateral.write().await;
        let mut col_row = collaterals.get(0).unwrap().write().await;
        *col_row = Array1::from_vec(vec![2.0, 10.0, 200.0]);

        let (borroweds, _, _) = &mut *cache.borrowed.write().await;
        let mut bor_row = borroweds.get(0).unwrap().write().await;
        *bor_row = Array1::from_vec(vec![1.5, 15.0, 0.0]);
    }

    cache
        .calc_hf(Some(&user), Utc::now().timestamp_millis())
        .await?;

    let hf = {
        let (hf, _) = &*cache.health_factors.read().await;
        hf.to_vec()
    };

    assert_eq!(hf, vec![0.25833333333333336]);

    // 2 case - hf calculation for all users

    let (cache, _) = generate_cache_and_tokens(3).await?;

    {
        let (lt, _) = &mut *cache.liquidation_threshold.write().await;
        *lt = Array1::from_vec(vec![0.0, 7500.0, 8000.0]);

        let (price, _) = &mut *cache.prices.write().await;
        *price = Array1::from_vec(vec![120_000.0, 4000.0, 200.0]);

        let (hf, _) = &mut *cache.health_factors.write().await;
        *hf = Array1::from_vec(vec![0.0; 3]);

        let (collaterals, _, _) = &mut *cache.collateral.write().await;
        collaterals.push(RwLock::new(Array1::from_vec(vec![2.0, 10.0, 200.0])));
        collaterals.push(RwLock::new(Array1::from_vec(vec![2.0, 10.0, 200.0])));
        let mut col_row = collaterals.get(0).unwrap().write().await;
        *col_row = Array1::from_vec(vec![2.0, 10.0, 200.0]);

        let (borroweds, _, _) = &mut *cache.borrowed.write().await;
        borroweds.push(RwLock::new(Array1::from_vec(vec![1.5, 15.0, 0.0])));
        borroweds.push(RwLock::new(Array1::from_vec(vec![1.5, 15.0, 0.0])));
        let mut bor_row = borroweds.get(0).unwrap().write().await;
        *bor_row = Array1::from_vec(vec![1.5, 15.0, 0.0]);

        *cache.collateral_matrix.write().await = Array2::from_elem((0, 3), 0.);
        let col_matrix = &mut *cache.collateral_matrix.write().await;
        col_matrix.push_row(Array1::from_vec(vec![2.0, 10.0, 200.0]).view())?;
        col_matrix.push_row(Array1::from_vec(vec![2.0, 10.0, 200.0]).view())?;
        col_matrix.push_row(Array1::from_vec(vec![2.0, 10.0, 200.0]).view())?;

        *cache.borrowed_matrix.write().await = Array2::from_elem((0, 3), 0.);
        let bor_matrix = &mut *cache.borrowed_matrix.write().await;
        bor_matrix.push_row(Array1::from_vec(vec![1.5, 15.0, 0.0]).view())?;
        bor_matrix.push_row(Array1::from_vec(vec![1.5, 15.0, 0.0]).view())?;
        bor_matrix.push_row(Array1::from_vec(vec![1.5, 15.0, 0.0]).view())?;
    }

    cache.calc_hf(None, Utc::now().timestamp_millis()).await?;

    let hf = {
        let (hf, _) = &*cache.health_factors.read().await;
        hf.to_vec()
    };

    assert_eq!(
        hf,
        vec![
            0.25833333333333336,
            0.25833333333333336,
            0.25833333333333336
        ]
    );

    Ok(())
}

struct CreateUserDataProvider;

impl CreateUserDataProvider {
    fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl DataProvider for CreateUserDataProvider {
    async fn get_all_reserves_tokens(&self) -> eyre::Result<Vec<TokenData>> {
        todo!()
    }

    async fn get_source_of_asset(&self, _: &Address) -> eyre::Result<Address> {
        todo!()
    }

    async fn listen_events<F, Fut>(&self, _: F) -> eyre::Result<()>
    where
        F: Fn(IL2PoolEvents) -> Fut + Send + 'static,
        Fut: Future<Output = eyre::Result<()>> + Send,
    {
        todo!()
    }

    async fn listen_price_update<F, Fut>(&self, _: &Address, _: F) -> eyre::Result<()>
    where
        F: Fn(IChainlinkAggregatorEvents) -> Fut + Send + 'static,
        Fut: Future<Output = eyre::Result<()>> + Send,
    {
        todo!()
    }

    async fn get_reserve_configuration_data(&self, _: &Address) -> eyre::Result<f64> {
        todo!()
    }

    async fn get_user_reserve_data(
        &self,
        token_address: &Address,
        _: &Address,
    ) -> eyre::Result<UserReserveData> {
        Err(eyre::eyre!("mock error"))
    }
}

#[tokio::test]
async fn test_create_user() -> eyre::Result<()> {
    // 1 case - user exists

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(1).await?;
    let user = cache.users.iter().next().unwrap().key().clone();
    let rq_date = Utc::now().timestamp_micros();

    let (sync_tx, _) = channel::<SyncRequest>(1);
    let (hf_tx, _) = channel::<HFRequest>(1);

    assert_eq!(cache.contains(&user), true);

    let created = create_user(
        rq_date,
        &cache,
        dummy_data_provider,
        &tokens,
        &user,
        &sync_tx,
        &hf_tx,
    )
    .await?;

    assert_eq!(cache.contains(&user), true);
    assert_eq!(created, false);

    // 2 case - create new user

    let dummy_data_provider = Arc::new(DummyDataProvider::new());
    let (cache, tokens) = generate_cache_and_tokens(0).await?;
    let rq_date = Utc::now().timestamp_micros();
    let user = Address::from_str("0x1Af54C553cefD1792CbFcF41B711834d657ea61D")?;

    let (sync_tx, mut sync_rc) = channel::<SyncRequest>(1);
    let (hf_tx, mut hf_rc) = channel::<HFRequest>(1);

    let sync_handler = task::spawn(async move {
        let msg = sync_rc.recv().await.unwrap();
        assert_eq!(SyncRequest::Both(0, rq_date), msg);
    });

    let hf_handler = task::spawn(async move {
        let msg = hf_rc.recv().await.unwrap();
        assert_eq!(HFRequest::User(user.clone(), rq_date), msg);
    });

    let created = create_user(
        rq_date,
        &cache,
        dummy_data_provider.clone(),
        &tokens,
        &user,
        &sync_tx,
        &hf_tx,
    )
    .await?;
    sync_handler.await?;
    hf_handler.await?;

    assert_eq!(created, true);

    let (collateral, reserve, borrowed) = cache
        .get_user_data(dummy_data_provider, &tokens, &user)
        .await?;

    assert_eq!(cache.contains(&user), true);
    assert_eq!(cache.users.len(), 1);

    assert_eq!(collateral, vec![0.0, 2.0, 0.0]);
    assert_eq!(reserve, vec![1.0, 0.0, 3.0]);
    assert_eq!(borrowed, vec![1.0, 2.0, 3.0]);

    // 3 case - error

    let create_user_data_provider = Arc::new(CreateUserDataProvider::new());

    let err = create_user(
        rq_date,
        &cache,
        create_user_data_provider,
        &tokens,
        &user,
        &sync_tx,
        &hf_tx,
    )
    .await;

    assert_eq!(err.is_err(), true);
    let e = err.unwrap_err();

    assert_eq!(e.to_string(), "mock error");

    Ok(())
}
