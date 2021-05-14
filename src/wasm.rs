use crate::{
    dag,
    db::{self, GetRootError},
    embed,
    kv::memstore::MemStore,
};
use crate::{embed::Rpc, sync};
use crate::{kv::idbstore::IdbStore, util::to_debug};
use crate::{kv::Store, util::rlog::LogContext};
use std::{cell::RefCell, sync::Arc};
use std::{collections::HashMap, sync::Once};
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
    let rpc = Rpc::from_u8(rpc).ok_or_else(|| JsValue::from(format!("Invalid RPC: {:?}", rpc)))?;
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

// thread_local! {
//     static KV_STORES: HashMap<String, Box<dyn Store>> = Default::default();
// }

thread_local! {
    pub static KV_STORES: RefCell<HashMap<String, Arc<dyn Store>>> = Default::default();
}

fn insert_kv_store(k: String, v: Arc<dyn Store>) {
    KV_STORES.with(|kv_stores| {
        kv_stores.borrow_mut().insert(k, v);
    });
}

fn get_kv_store(k: &str) -> Option<Arc<dyn Store>> {
    KV_STORES.with(|kv_stores| kv_stores.borrow().get(k).map(|s| s.clone()))
}

fn remove_kv_store(k: &str) {
    KV_STORES.with(|kv_stores| {
        kv_stores.borrow_mut().remove(k);
    });
}

// fn has_kv_store(k: &str) -> bool {
//     KV_STORES.with(|kv_stores| kv_stores.borrow().contains_key(k))
// }

#[wasm_bindgen]
pub async fn open(db_name: String, use_memstore: bool) -> Result<String, js_sys::Error> {
    if db_name.is_empty() {
        return Err(js_sys::Error::new("db_name must be non-empty"));
    }

    let kv: Arc<dyn Store> = if use_memstore {
        Arc::new(MemStore::new())
    } else {
        match IdbStore::new(&db_name).await {
            Err(e) => {
                return Err(js_sys::Error::new(&format!(
                    "Failed to open \"{}\": {}",
                    db_name, e
                )))
            }
            Ok(store) => Arc::new(store),
        }
    };

    let lc = LogContext::new();
    let client_id = sync::client_id::init(kv.as_ref(), lc)
        .await
        .map_err(to_debug)
        .map_err(|s| js_sys::Error::new(&s))?;

    insert_kv_store(db_name, kv);

    Ok(client_id)
}

#[wasm_bindgen]
pub async fn close(db_name: String) -> Result<(), js_sys::Error> {
    let kv = get_kv_store(&db_name);
    if let Some(kv) = kv {
        remove_kv_store(&db_name);
        kv.close().await;
    }
    Ok(())
}

#[wasm_bindgen]
pub async fn get_root(db_name: String, head_name: Option<String>) -> Result<String, js_sys::Error> {
    // use GetRootError::*;
    let kv = get_kv_store(&db_name);
    if kv.is_none() {
        return Err(js_sys::Error::new("Database is not open"));
    }
    let kv = kv.unwrap();

    let store = dag::Store::new(kv);

    let head_name = match head_name {
        Some(name) => name,
        None => db::DEFAULT_HEAD_NAME.to_string(),
    };
    db::get_root(kv, head_name.as_str(), LogContext::new())
        .await
        .map_err(to_debug)
        .map_err(|s| js_sys::Error::new(&s))
}
