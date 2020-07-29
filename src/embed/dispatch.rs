#![allow(clippy::redundant_pattern_matching)] // For derive(DeJson).

use super::Connection;
use crate::dag;
use crate::kv::idbstore::IdbStore;
use async_std::sync::{channel, Receiver, RwLock, Sender};
use log::warn;
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
                let conn = match dispatcher.connections.get(&req.db_name[..]) {
                    Some(v) => v,
                    None => {
                        req.response
                            .send(Err(format!("\"{}\" not open", req.db_name)))
                            .await;
                        continue;
                    }
                };
                spawn_local(execute(req, conn.clone()));
            }
        }
    }
}

async fn execute(req: Request, conn: Rc<RwLock<Connection<'_>>>) {
    let response = match req.rpc.as_str() {
        "has" => conn
            .read()
            .await
            .has(&req.data)
            .await
            .map_err(|e| format!("{:?}", e)),
        "get" => conn
            .read()
            .await
            .get(&req.data)
            .await
            .map_err(|e| format!("{:?}", e)),
        "put" => conn
            .write()
            .await
            .put(&req.data)
            .await
            .map_err(|e| format!("{:?}", e)),
        _ => Err("Unsupported rpc name".into()),
    };
    req.response.send(response).await;
}

struct Dispatcher<'a> {
    // TODO: Given that we *have* to lock Connection way up here
    // there doesn't seem any point to additionally lock it the
    // same way in kv impl. Remove that, mark the mutating methods
    // appropriately, and rely on Rust to enforce for us!
    connections: HashMap<String, Rc<RwLock<Connection<'a>>>>,
}

impl<'a> Dispatcher<'a> {
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
                    self.connections.insert(
                        req.db_name.clone(),
                        Rc::new(RwLock::new(Connection::new(dag::Store::new(Box::new(kv))))),
                    );
                }
            }
        }
        Ok("".into())
    }

    async fn close(&mut self, req: &Request) -> Response {
        if !self.connections.contains_key(&req.db_name[..]) {
            return Ok("".into());
        }
        self.connections.remove(&req.db_name);

        Ok("".into())
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
