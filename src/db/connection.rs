use crate::dag;
use crate::kv;
use crate::util::rlog::LogContext;
use crate::util::uuid::uuid;
use std::collections::HashMap;
use std::marker::Send;
use std::sync::Mutex;

pub struct Connection {
    store: dag::Store,
    client_id: String,
}

unsafe impl Send for Connection {}

type ConnMap = HashMap<String, Connection>;

lazy_static! {
    static ref CONNECTIONS: Mutex<ConnMap> = Mutex::new(HashMap::new());
}

#[derive(Debug)]
pub enum OpenError {
    DBNameMustBeNonEmpty,
    InitClientIDError(InitClientIdError),
    LockPoisoned,
    NoKVStoreCreated,
    OpenKVStoreError(kv::StoreError),
}

pub async fn open_database(db_name: String, lc: LogContext) -> Result<(), OpenError> {
    use OpenError::*;

    if db_name.is_empty() {
        return Err(DBNameMustBeNonEmpty);
    }

    let mut lock = CONNECTIONS.lock().map_err(|_| LockPoisoned)?;
    if lock.contains_key(&db_name) {
        return Ok(());
    }

    let kv: Box<dyn kv::Store> = match db_name.as_str() {
        #[cfg(not(target_arch = "wasm32"))]
        "mem" => Box::new(kv::memstore::MemStore::new()),
        _ => Box::new(
            kv::idbstore::IdbStore::new(&db_name)
                .await
                .map_err(OpenKVStoreError)?
                .ok_or(NoKVStoreCreated)?,
        ),
    };

    let client_id = init_client_id(kv.as_ref(), lc)
        .await
        .map_err(InitClientIDError)?;
    let store = dag::Store::new(kv);

    lock.insert(db_name, Connection { client_id, store });
    Ok(())
}

#[derive(Debug)]
pub enum InitClientIdError {
    CommitErr(kv::StoreError),
    GetErr(kv::StoreError),
    InvalidUtf8(std::string::FromUtf8Error),
    OpenErr(kv::StoreError),
    PutClientIdErr(kv::StoreError),
}

async fn init_client_id(s: &dyn kv::Store, lc: LogContext) -> Result<String, InitClientIdError> {
    use InitClientIdError::*;
    const CID_KEY: &str = "sys/cid";
    let cid = s.get(CID_KEY).await.map_err(GetErr)?;
    if let Some(cid) = cid {
        let s = String::from_utf8(cid).map_err(InvalidUtf8)?;
        return Ok(s);
    }
    let wt = s.write(lc).await.map_err(OpenErr)?;
    let uuid = uuid();
    wt.put(CID_KEY, &uuid.as_bytes())
        .await
        .map_err(PutClientIdErr)?;
    wt.commit().await.map_err(CommitErr)?;
    Ok(uuid)
}
