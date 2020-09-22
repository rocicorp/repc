use crate::dag;
use crate::db;
use crate::embed::types::{OpenTransactionRequest, OpenTransactionResponse};
use crate::embed::{validate_rebase, OpenTransactionError};
use crate::kv;
use crate::util::rlog;
use crate::util::rlog::LogContext;
use crate::util::uuid::uuid;
use async_std::sync::{Mutex, RwLock};
use std::collections::HashMap;
use std::marker::Send;
use std::sync::atomic::{AtomicU32, Ordering};

type ConnMap = HashMap<String, Connection>;
type TxnMap = HashMap<u32, RwLock<Transaction>>;

lazy_static! {
    static ref CONNECTIONS: Mutex<ConnMap> = Mutex::new(HashMap::new());
    static ref TRANSACTION_COUNTER: AtomicU32 = AtomicU32::new(1);
}

struct Connection {
    store: dag::Store,
    client_id: String,
    transactions: RwLock<TxnMap>,
}

unsafe impl Send for Connection {}

impl Connection {
    pub async fn open_transaction(
        db_name: &str,
        req: OpenTransactionRequest,
        lc: &LogContext,
    ) -> Result<OpenTransactionResponse, OpenTransactionError> {
        use OpenTransactionError::*;

        let guard = CONNECTIONS.lock().await;
        let conn = guard.get(db_name).ok_or(DatabaseNotOpen)?;

        let txn = match req.name {
            Some(mutator_name) => {
                let OpenTransactionRequest {
                    name: _,
                    args: mutator_args,
                    rebase_opts,
                } = req;
                let mutator_args = mutator_args.ok_or(ArgsRequired)?;
                let lock_timer = rlog::Timer::new().map_err(InternalTimerError)?;
                debug!(lc, "Waiting for write lock...");
                let dag_write = conn.store.write(lc.clone()).await.map_err(DagWriteError)?;
                debug!(
                    lc,
                    "...Write lock acquired in {}ms",
                    lock_timer.elapsed_ms()
                );
                let (whence, original_hash) = match rebase_opts {
                    None => (db::Whence::Head(db::DEFAULT_HEAD_NAME.to_string()), None),
                    Some(opts) => {
                        validate_rebase(&opts, dag_write.read(), &mutator_name, &mutator_args)
                            .await?;
                        (db::Whence::Hash(opts.basis), Some(opts.original_hash))
                    }
                };

                let write = db::Write::new_local(
                    whence,
                    mutator_name,
                    mutator_args,
                    original_hash,
                    dag_write,
                )
                .await
                .map_err(DBWriteError)?;
                Transaction::Write(write)
            }
            None => {
                let dag_read = conn.store.read(lc.clone()).await.map_err(DagReadError)?;
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
        /*
        conn.transactions
            .write()
            .await
            .insert(txn_id, RwLock::new(txn));
            */
        Ok(OpenTransactionResponse {
            transaction_id: txn_id,
        })
    }
}

pub enum Transaction {
    Read(db::OwnedRead<'static>),
    Write(db::Write<'static>),
}

impl Transaction {
    fn as_read(&self) -> db::Read {
        match self {
            Transaction::Read(r) => r.as_read(),
            Transaction::Write(w) => w.as_read(),
        }
    }
}

#[derive(Debug)]
pub enum OpenError {
    DBNameMustBeNonEmpty,
    InitClientIDError(InitClientIdError),
    NoKVStoreCreated,
    OpenKVStoreError(kv::StoreError),
}

pub async fn open_database(db_name: String, lc: LogContext) -> Result<(), OpenError> {
    use OpenError::*;

    if db_name.is_empty() {
        return Err(DBNameMustBeNonEmpty);
    }

    let mut lock = CONNECTIONS.lock().await;
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

    lock.insert(
        db_name,
        Connection {
            client_id,
            store,
            transactions: RwLock::new(TxnMap::new()),
        },
    );
    Ok(())
}

pub async fn close_database(db_name: &str) {
    let mut lock = CONNECTIONS.lock().await;
    lock.remove(db_name);
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
