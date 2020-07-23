#![allow(clippy::redundant_pattern_matching)] // For derive(DeJson).

use crate::dag;
use crate::db;
use crate::kv;
use crate::kv::idbstore::IdbStore;
use crate::uuid::uuid;

use async_std::sync::{channel, Receiver, Sender};
use log::warn;
use nanoserde::{DeJson, DeJsonErr, SerJson};
use std::collections::HashMap;
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
                let db = match dispatcher.connections.get_mut(&req.db_name[..]) {
                    Some(v) => v,
                    None => {
                        req.response
                            .send(Err(format!("\"{}\" not open", req.db_name)))
                            .await;
                        continue;
                    }
                };
                let response = match req.rpc.as_str() {
                    "has" => stringlify(Dispatcher::has(&mut **db, &req.data).await),
                    "get" => stringlify(Dispatcher::get(&mut **db, &req.data).await),
                    "put" => stringlify(Dispatcher::put(&mut **db, &req.data).await),
                    _ => Err("Unsupported rpc name".into()),
                };
                req.response.send(response).await;
            }
        }
    }
}

#[derive(DeJson)]
struct GetRequest {
    key: String,
}

fn stringlify<R, E: std::fmt::Debug>(result: Result<R, E>) -> Result<R, String> {
    result.map_err(|e| format!("{:?}", e))
}

#[derive(SerJson)]
struct GetResponse {
    value: Option<String>,
    has: bool, // Second to avoid trailing comma if value == None.
}

#[derive(DeJson)]
struct PutRequest {
    key: String,
    value: String,
}

struct Dispatcher {
    connections: HashMap<String, Box<dag::Store>>,
}

const CID_KEY: &str = "sys/cid";

impl Dispatcher {
    async fn open(&mut self, req: &Request) -> Response {
        if req.db_name.is_empty() {
            return Err("db_name must be non-empty".into());
        }
        if self.connections.contains_key(&req.db_name[..]) {
            return Ok("".into());
        }
        let idb_store = match IdbStore::new(&req.db_name[..]).await {
            Err(e) => return Err(format!("Failed to open \"{}\": {}", req.db_name, e)),
            Ok(None) => return Err(format!("Failed to open \"{}\"", req.db_name)),
            Ok(Some(idb_store)) => {
                let _client_id = match init_client_id(&idb_store).await {
                    Ok(v) => v,
                    Err(why) => {
                        return Err(format!("Failed to read client ID: {:?}", why));
                    }
                };
                idb_store
            }
        };

        self.connections.insert(
            req.db_name.clone(),
            Box::new(dag::Store::new(Box::new(idb_store))),
        );

        // TODO(arv): If we get None in IdbStore::new that should be an error.
        Ok("".into())
    }

    async fn close(&mut self, req: &Request) -> Response {
        if !self.connections.contains_key(&req.db_name[..]) {
            return Ok("".into());
        }
        self.connections.remove(&req.db_name);

        Ok("".into())
    }

    // TODO: Everything around databases and transaction management needs thought:
    // - We need to actually implement the transactions
    // - Should there be an err, DB, type in the in the db package, or should we continue to construct them on each tx? If so, it could do some of below.
    // - We will def need some kind of "connection" struct, analagous to the corresponding one in Go, that keeps track of the transactions by ID
    // - read/get need to use read txs
    async fn open_transaction<'a>(
        ds: &'_ mut dag::Store,
    ) -> Result<db::Write<'_>, OpenTransactionError> {
        use OpenTransactionError::*;
        let dag_write = ds.write().await.map_err(DagWriteError)?;
        let write = db::Write::new_from_head("main", dag_write)
            .await
            .map_err(DBWriteError)?;
        Ok(write)
    }

    async fn has(ds: &mut dag::Store, data: &str) -> Result<String, HasError> {
        use HasError::*;
        let req: GetRequest = DeJson::deserialize_json(data).map_err(InvalidJson)?;
        let write = Dispatcher::open_transaction(ds)
            .await
            .map_err(OpenTransactionError)?;
        Ok(SerJson::serialize_json(&GetResponse {
            has: write.has(req.key.as_bytes()),
            value: None,
        }))
    }

    async fn get(ds: &mut dag::Store, data: &str) -> Result<String, GetError> {
        use GetError::*;
        let req: GetRequest = DeJson::deserialize_json(data).map_err(InvalidJson)?;
        let write = Dispatcher::open_transaction(ds)
            .await
            .map_err(OpenTransactionError)?;
        let got = write.get(req.key.as_bytes());
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

    async fn put(ds: &mut dag::Store, data: &str) -> Result<String, PutError> {
        use PutError::*;
        let req: PutRequest = DeJson::deserialize_json(data).map_err(InvalidJson)?;
        let mut write = Dispatcher::open_transaction(ds)
            .await
            .map_err(OpenTransactionError)?;
        write.put(req.key.as_bytes().to_vec(), req.value.into_bytes());
        write
            .commit(
                "main",
                "local-create-date",
                "checksum",
                42,
                "foo",
                b"bar",
                None,
            )
            .await
            .map_err(CommitError)?;
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
enum InitClientIdError {
    GetErr(kv::StoreError),
    OpenErr(kv::StoreError),
    InvalidUtf8(std::string::FromUtf8Error),
    PutClientIdErr(kv::StoreError),
    CommitErr(kv::StoreError),
}

async fn init_client_id(s: &dyn kv::Store) -> Result<String, InitClientIdError> {
    use InitClientIdError::*;

    let cid = s.get(CID_KEY).await.map_err(GetErr)?;
    if let Some(cid) = cid {
        let s = String::from_utf8(cid).map_err(InvalidUtf8)?;
        return Ok(s);
    }
    let wt = s.write().await.map_err(OpenErr)?;
    let uuid = uuid();
    wt.put(CID_KEY, &uuid.as_bytes())
        .await
        .map_err(PutClientIdErr)?;
    wt.commit().await.map_err(CommitErr)?;
    Ok(uuid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::memstore::MemStore;
    use regex::Regex;

    #[async_std::test]
    async fn test_init_client_id() {
        let kv = MemStore::new();
        let cid = init_client_id(&kv).await.unwrap();

        let re =
            Regex::new(r"^[0-9:A-z]{8}-[0-9:A-z]{4}-4[0-9:A-z]{3}-[0-9:A-z]{4}-[0-9:A-z]{12}$")
                .unwrap();
        assert!(re.is_match(&cid));

        let cid2 = init_client_id(&kv).await.unwrap();
        assert_eq!(cid, cid2);

        let kv = MemStore::new();
        let cid3 = init_client_id(&kv).await.unwrap();
        assert_ne!(cid, cid3);
    }
}
