use crate::{
    dag, db,
    embed::{self, do_init, types::RebaseOpts, validate_rebase, OpenTransactionError, Transaction},
    kv::memstore::MemStore,
    util::rlog,
};
use crate::{embed::Rpc, sync};
use crate::{kv::idbstore::IdbStore, util::to_debug};
use crate::{kv::Store, util::rlog::LogContext};
use std::sync::{atomic::Ordering, Once};
use std::{
    cell::RefCell,
    collections::HashMap,
    sync::{atomic::AtomicU32, Arc},
};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsValue;

#[cfg(not(default))]
pub async fn new_idbstore(name: String) -> Option<Box<dyn Store>> {
    init_panic_hook();
    match IdbStore::new(&name).await {
        Ok(store) => Some(Box::new(store)),
        _ => None,
    }
}

#[wasm_bindgen]
pub async fn dispatch(db_name: String, rpc: u8, args: JsValue) -> Result<JsValue, JsValue> {
    init_panic_hook();
    let rpc = Rpc::from_u8(rpc)
        .ok_or_else(|| JsValue::from(js_sys::Error::new(&format!("Invalid RPC: {:?}", rpc))))?;
    embed::dispatch(db_name, rpc, args).await
}

static INIT: Once = Once::new();

pub fn init_console_log() {
    INIT.call_once(|| {
        if let Err(e) = console_log::init_with_level(log::Level::Info) {
            web_sys::console::error_1(&format!("Error registering console_log: {}", e).into());
        }
    });
}

fn init_panic_hook() {
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
    init_console_log();
}

thread_local! {
    static DAG_STORES: RefCell<HashMap<String, Arc<dag::Store>>> = Default::default();
}

fn insert_dag_store(k: String, v: Arc<dag::Store>) {
    DAG_STORES.with(|dag_stores| {
        dag_stores.borrow_mut().insert(k, v);
    });
}

fn get_dag_store(k: &str) -> Option<Arc<dag::Store>> {
    DAG_STORES.with(|dag_stores| return dag_stores.borrow().get(k).map(|s| s.clone()))
}

fn remove_dag_store(k: &str) {
    DAG_STORES.with(|dag_stores| {
        dag_stores.borrow_mut().remove(k);
    });
}

lazy_static! {
    static ref TRANSACTION_COUNTER: AtomicU32 = AtomicU32::new(1);
}

thread_local! {
    static TXNS: RefCell<HashMap<u32, Transaction<'static>>> = Default::default();
}

fn insert_transaction(txn_id: u32, txn: Transaction<'static>) -> () {
    // todo!()
    TXNS.with(|txns| async {
        let mut txns = txns.borrow_mut();
        txns.insert(txn_id, txn);
    });
}

#[wasm_bindgen]
pub async fn open(db_name: String, use_memstore: bool) -> Result<String, js_sys::Error> {
    if db_name.is_empty() {
        return Err(js_sys::Error::new("db_name must be non-empty"));
    }

    if get_dag_store(&db_name).is_some() {
        return Err(js_sys::Error::new(&format!(
            "Database \"{}\" has already been opened. Please close it before opening it again",
            db_name
        )));
    }

    let kv: Box<dyn Store> = if use_memstore {
        Box::new(MemStore::new())
    } else {
        match IdbStore::new(&db_name).await {
            Err(e) => {
                return Err(js_sys::Error::new(&format!(
                    "Failed to open \"{}\": {}",
                    db_name, e
                )))
            }
            Ok(store) => Box::new(store),
        }
    };

    let lc = LogContext::new();
    let client_id = sync::client_id::init(kv.as_ref(), lc.clone())
        .await
        .map_err(to_debug)
        .map_err(|s| js_sys::Error::new(&s))?;

    // insert_kv_store(db_name.clone(), kv.clone());

    let store = Arc::new(dag::Store::new(kv));

    do_init(store.as_ref(), lc.clone())
        .await
        .map_err(|e| js_sys::Error::new(&to_debug(e)))?;
    insert_dag_store(db_name, store);
    Ok(client_id)
}

#[wasm_bindgen]
pub async fn open_transaction(
    db_name: String,
    name: Option<String>,
    mutator_args: Option<String>,
    basis: Option<String>,
    original_hash: Option<String>,
) -> Result<u32, js_sys::Error> {
    use OpenTransactionError::*;

    let rebase_opts = match (basis, original_hash) {
        (None, None) => None,
        (None, Some(_)) | (Some(_), None) => panic!("Invalid arguments"),
        (Some(basis), Some(original_hash)) => Some(RebaseOpts {
            basis,
            original_hash,
        }),
    };

    let lc = LogContext::new();

    let store = match get_dag_store(&db_name) {
        None => return Err(js_sys::Error::new("Database is not open")),
        Some(store) => store,
    };

    // let x: &'static dag::Store = store.as_ref();

    let txn = match name {
        Some(mutator_name) => {
            let mutator_args = mutator_args.ok_or(ArgsRequired)?;

            let lock_timer = rlog::Timer::new();
            debug!(lc, "Waiting for write lock...");
            let dag_write = store.write(lc.clone()).await.map_err(DagWriteError)?;

            debug!(
                lc,
                "...Write lock acquired in {}ms",
                lock_timer.elapsed_ms()
            );
            let (whence, original_hash) = match rebase_opts {
                None => (db::Whence::Head(db::DEFAULT_HEAD_NAME.to_string()), None),
                Some(opts) => {
                    validate_rebase(&opts, dag_write.read(), &mutator_name, &mutator_args).await?;
                    (db::Whence::Hash(opts.basis), Some(opts.original_hash))
                }
            };

            let write =
                db::Write::new_local(whence, mutator_name, mutator_args, original_hash, dag_write)
                    .await
                    .map_err(DBWriteError)?;
            Transaction::Write(write)
        }
        None => {
            let x2 = store.clone();
            let dag_read = x2.read(lc.clone()).await.map_err(DagReadError)?;

            let read = db::OwnedRead::from_whence(
                db::Whence::Head(db::DEFAULT_HEAD_NAME.to_string()),
                dag_read,
            )
            .await
            .map_err(DBReadError)?;
            Transaction::Read(read)
        }
    };

    let txn_id = TRANSACTION_COUNTER.fetch_add(1, Ordering::SeqCst);
    insert_transaction(txn_id, txn);
    // txns.write().await.insert(txn_id, RwLock::new(txn));
    Ok(txn_id)
}

#[wasm_bindgen]
pub async fn close(db_name: String) -> Result<(), js_sys::Error> {
    let kv = get_dag_store(&db_name);
    if let Some(kv) = kv {
        remove_dag_store(&db_name);
        kv.close().await;
    }
    Ok(())
}

#[wasm_bindgen]
pub async fn get_root(db_name: String) -> Result<String, js_sys::Error> {
    // use GetRootError::*;
    let store = get_dag_store(&db_name);
    if store.is_none() {
        return Err(js_sys::Error::new("Database is not open"));
    }
    let store = store.unwrap();

    // let head_name = match head_name {
    //     Some(name) => name,
    //     None => db::DEFAULT_HEAD_NAME.to_string(),
    // };
    db::get_root(&store, db::DEFAULT_HEAD_NAME, LogContext::new())
        .await
        .map_err(to_debug)
        .map_err(|s| js_sys::Error::new(&s))
}
