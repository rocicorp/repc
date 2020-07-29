#![allow(clippy::redundant_pattern_matching)] // For derive(DeJson).

use crate::dag;
use crate::db;
use crate::kv::idbstore::IdbStore;
use async_fn::AsyncFn2;
use async_std::sync::{channel, Receiver, RwLock, Sender};
use log::warn;
use nanoserde::{DeJson, DeJsonErr, SerJson};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Mutex;
use wasm_bindgen_futures::spawn_local;

struct Request {
    db_name: String,
    rpc: String,
    data: String,
    response: Sender<Response>,
}

type Response = Result<String, String>;

lazy_static! {
    static ref SENDER: Mutex<Sender::<Request>> = {
        let (tx, rx) = channel::<Request>(1);
        spawn_local(dispatch_loop(rx));
        Mutex::new(tx)
    };
}

async fn dispatch_loop(rx: Receiver<Request>) {
    let mut dispatcher = Dispatcher {
        connections: HashMap::new(),
    };

    loop {
        match rx.recv().await {
            Err(why) => warn!("Dispatch loop recv failed: {}", why),
            Ok(req) => {
                log::error!("Dispatch processing {}", req.rpc);
                let response = match req.rpc.as_str() {
                    "open" => Some(dispatcher.open(&req).await),
                    "close" => Some(dispatcher.close(&req).await),
                    "debug" => Some(dispatcher.debug(&req).await),
                    _ => None,
                };
                if let Some(response) = response {
                    req.response.send(response).await;
                    continue;
                }
                match dispatcher.connections.get(&req.db_name[..]) {
                    Some(tx) => tx.send(req).await,
                    None => {
                        req.response
                            .send(Err(format!("\"{}\" not open", req.db_name)))
                            .await;
                        continue;
                    }
                };
                /*
                match req.rpc.as_str() {
                    "has" => {
                        spawn_local(execute(Dispatcher::has, db.clone(), req));
                    }
                    "get" => {
                        spawn_local(execute(Dispatcher::get, db.clone(), req));
                    }
                    "put" => {
                        spawn_local(execute(Dispatcher::put, db.clone(), req));
                    }
                    _ => {
                        req.response.send(Err("Unsupported rpc name".into())).await;
                    }
                };
                */
            }
        }
    }
}

async fn connection_loop(store: dag::Store, rx: Receiver<Request>) {
    let store = Rc::new(store); // NOCOMMIT: Maybe remove?
    let h = HashMap::new();
    /*
    let mut dispatcher = Dispatcher {
        connections: HashMap::new(),
    };
    */
    loop {
        log::error!("Connection loop awaiting");
        match rx.recv().await {
            Err(why) => warn!("Dispatch loop recv failed: {}", why),
            Ok(req) => match req.rpc.as_str() {
                "has" => {
                    spawn_local(execute(Dispatcher::has, &h, req));
                }
                /*
                "get" => {
                    spawn_local(execute(Dispatcher::get, h.clone(), req));
                }
                "put" => {
                    spawn_local(execute(Dispatcher::put, &h, req));
                }
                */
                "openTransaction" => {
                    use NewOpenTransactionError::*;
                    let dag_write = store.write().await.map_err(DagWriteError).unwrap();
                    let write = db::Write::new_from_head("main", dag_write)
                        .await
                        .map_err(DBWriteError)
                        .unwrap();
                    //let transaction_id = TRANSACTION_COUNTER.fetch_add(1, Ordering::SeqCst);
                    let transaction_id: u32 = 13;
                    h.insert(transaction_id, RwLock::new(write));
                    req.response.send(Ok("Opened transaction!".into())).await;
                }
                "close" => {
                    //spawn_local(execute(Dispatcher::put, store.clone(), req));
                    log::error!("Connection loop got close!");
                    req.response.send(Ok("Closed!".into())).await;
                    break;
                }
                _ => {
                    req.response.send(Err("Unsupported rpc name".into())).await;
                }
            },
        }
    }
}

async fn execute<T, E, F>(func: F, txns: &HashMap<u32, RwLock<db::Write<'_>>>, req: Request)
where
    T: DeJson + TransactionRequest,
    E: std::fmt::Debug,
    F: for<'r, 's> AsyncFn2<&'r RwLock<db::Write<'r>>, T, Output = Result<String, E>>,
{
    let request: T = match DeJson::deserialize_json(&req.data) {
        //.map_err(InvalidJson) {
        Ok(v) => v,
        Err(e) => {
            req.response.send(Err(format!("InvalidJson({})", e))).await;
            return;
        }
    };

    //let write = match Dispatcher::open_transaction(&store).await {
    let txn_id = request.transaction_id();
    //let guard = txns.read().await;
    let txn = match txns.get(&txn_id) {
        Some(v) => v,
        None => {
            req.response
                .send(Err(format!("No such transaction {}", txn_id)))
                .await;
            return;
        }
    };

    /*
    let mut write = match Dispatcher::open_transaction(&store).await {
        Ok(v) => v,
        Err(e) => {
            req.response
                .send(Err("Open transaction error".into()))
                .await;
            return;
        }
    };
    */
    //.map_err(OpenTransactionError)?;
    /* NOCOMMIT
    let response = func
        .call(&txn, request)
        .await
        .map_err(|e| format!("{:?}", e));
    req.response.send(response).await;
    */
}

trait TransactionRequest {
    fn transaction_id(&self) -> u32;
}

#[derive(DeJson)]
struct GetRequest {
    transaction_id: u32,
    key: String,
}

impl TransactionRequest for GetRequest {
    fn transaction_id(&self) -> u32 {
        return self.transaction_id;
    }
}

#[derive(SerJson)]
struct GetResponse {
    value: Option<String>,
    has: bool, // Second to avoid trailing comma if value == None.
}

#[derive(DeJson)]
struct PutRequest {
    transaction_id: u32,
    key: String,
    value: String,
}

impl TransactionRequest for PutRequest {
    fn transaction_id(&self) -> u32 {
        return self.transaction_id;
    }
}

struct Dispatcher {
    connections: HashMap<String, Sender<Request>>,
}

impl Dispatcher {
    async fn open(&mut self, req: &Request) -> Response {
        if req.db_name.is_empty() {
            return Err("db_name must be non-empty".into());
        }
        if self.connections.contains_key(&req.db_name[..]) {
            return Ok("".into());
        }
        match IdbStore::new(&req.db_name[..]).await {
            Err(e) => {
                return Err(format!("Failed to open \"{}\": {}", req.db_name, e));
            }
            Ok(v) => {
                if let Some(kv) = v {
                    let (tx, rx) = channel::<Request>(1);
                    spawn_local(connection_loop(dag::Store::new(Box::new(kv)), rx));
                    self.connections.insert(req.db_name.clone(), tx);

                    //self.connections
                    //    .insert(req.db_name.clone(), Rc::new(dag::Store::new(Box::new(kv))));
                }
            }
        }
        Ok("".into())
    }

    async fn close(&mut self, req: &Request) -> Response {
        log::error!("In close 1");
        match self.connections.get(&req.db_name[..]) {
            Some(tx) => {
                let (tx2, rx2) = channel::<Response>(1);
                tx.send(Request {
                    db_name: req.db_name.clone(),
                    rpc: "close".into(),
                    data: "".into(),
                    response: tx2,
                })
                .await;
                match rx2.recv().await {
                    Err(e) => {
                        log::error!("Error response from close");
                    }
                    Ok(v) => {
                        log::error!("Got response from close");
                    }
                };
            }
            None => {
                return Ok("".into());
            }
        }
        log::error!("In close 2");
        self.connections.remove(&req.db_name);
        log::error!("In close 3");

        Ok("".into())
    }

    // TODO: Everything around databases and transaction management needs thought:
    // - We need to actually implement the transactions
    // - Should there be an err, DB, type in the in the db package, or should we continue to construct them on each tx? If so, it could do some of below.
    // - We will def need some kind of "connection" struct, analagous to the corresponding one in Go, that keeps track of the transactions by ID
    // - read/get need to use read txs
    async fn open_transaction<'a>(
        ds: &'_ dag::Store,
    ) -> Result<db::Write<'_>, OpenTransactionError> {
        use OpenTransactionError::*;
        let dag_write = ds.write().await.map_err(DagWriteError)?;
        let write = db::Write::new_from_head("main", dag_write)
            .await
            .map_err(DBWriteError)?;
        Ok(write)
    }

    async fn has(
        txn: &RwLock<db::Write<'_>>,
        /*txns: &RwLock<*/ req: GetRequest,
    ) -> Result<String, HasError> {
        //let req: GetRequest = DeJson::deserialize_json(data).map_err(InvalidJson)?;

        /*
        let write = Dispatcher::open_transaction(ds)
            .await
            .map_err(OpenTransactionError)?;
        */
        /*
         */
        Ok(SerJson::serialize_json(&GetResponse {
            has: txn.read().await.has(req.key.as_bytes()),
            value: None,
        }))
    }
    /*

    async fn get(txn: &RwLock<db::Write<'_>>, req: GetRequest) -> Result<String, GetError> {
        use GetError::*;
        //let req: GetRequest = DeJson::deserialize_json(data).map_err(InvalidJson)?;

        #[cfg(not(default))] // Not enabled in production.
        if req.key.starts_with("sleep") {
            use async_std::task::sleep;
            use core::time::Duration;

            match req.key[5..].parse::<u64>() {
                Ok(ms) => sleep(Duration::from_millis(ms)).await,
                Err(_) => log::error!("No sleep"),
            }
        }

        /*
        let write = Dispatcher::open_transaction(ds)
            .await
            .map_err(OpenTransactionError)?;
        */
        let got = txn.read().await.get(req.key.as_bytes());
        let got = got.map(|buf| String::from_utf8(buf.to_vec()));
        if let Some(Err(e)) = got {
            return Err(InvalidUtf8(e));
        }
        let got = got.map(|r| r.unwrap());
        Ok(SerJson::serialize_json(&GetResponse {
            has: got.is_some(),
            value: got,
        }))
    }
    */

    async fn put(txn: &RwLock<db::Write<'_>>, req: PutRequest) -> Result<String, PutError> {
        //use PutError::*;
        /*let req: PutRequest = DeJson::deserialize_json(data).map_err(InvalidJson)?;
        let mut write = Dispatcher::open_transaction(ds)
            .await
            .map_err(OpenTransactionError)?;
        */
        txn.write()
            .await
            .put(req.key.as_bytes().to_vec(), req.value.into_bytes());
        /*
        txn.commit(
            "main",
            "local-create-date",
            "checksum",
            42,
            "foo",
            b"bar",
            None,
        )
        .await
        .map_err(CommitError)?;*/
        Ok("{}".into())
    }

    async fn debug(&self, req: &Request) -> Response {
        match req.data.as_str() {
            "open_dbs" => Ok(format!("{:?}", self.connections.keys())),
            _ => Err("Debug command not defined".into()),
        }
    }
}

pub async fn dispatch(db_name: String, rpc: String, data: String) -> Response {
    let (tx, rx) = channel::<Response>(1);
    let request = Request {
        db_name,
        rpc,
        data,
        response: tx,
    };
    match SENDER.lock() {
        Ok(v) => v.send(request).await,
        Err(e) => return Err(e.to_string()),
    }
    match rx.recv().await {
        Err(e) => Err(e.to_string()),
        Ok(v) => v,
    }
}

#[derive(Debug)]
enum OpenTransactionError {
    DagWriteError(dag::Error),
    DBWriteError(db::NewError),
}
#[derive(Debug)]
enum HasError {
    InvalidJson(DeJsonErr),
    OpenTransactionError(OpenTransactionError),
}
#[derive(Debug)]
enum GetError {
    InvalidJson(DeJsonErr),
    OpenTransactionError(OpenTransactionError),
    InvalidUtf8(std::string::FromUtf8Error),
}
#[derive(Debug)]
enum PutError {
    InvalidJson(DeJsonErr),
    OpenTransactionError(OpenTransactionError),
    CommitError(db::CommitError),
}

#[derive(Debug)]
enum NewOpenTransactionError {
    // NOCOMMIT
    InvalidJson(DeJsonErr),
    DagWriteError(dag::Error),
    DBWriteError(db::NewError),
    //OpenTransactionError(OpenTransactionError),
    //CommitError(db::CommitError),
}
