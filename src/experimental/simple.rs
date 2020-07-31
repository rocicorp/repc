use async_std::sync::{channel, Receiver, Sender};
use async_std::sync::{RwLock, RwLockWriteGuard};
use log::warn;
use std::collections::HashMap;
use std::sync::Mutex;
use wasm_bindgen_futures::spawn_local;

struct Store {
    map: RwLock<HashMap<String, bool>>,
}

impl Store {
    async fn new() -> Box<Store> {
        Box::new(Store {
            map: RwLock::new(HashMap::new()),
        })
    }
    async fn write(&mut self) -> WriteTx<'_> {
        let guard = self.map.write().await;
        WriteTx {
            store: guard,
            v: Box::new(false),
        }
    }
}

struct WriteTx<'a> {
    store: RwLockWriteGuard<'a, HashMap<String, bool>>,
    v: Box<bool>,
}

impl WriteTx<'_> {
    async fn set(&mut self, v: bool) {
        let val = &mut (*self.v);
        *val = v;
    }
    async fn commit(&mut self) {
        self.store.insert("key".into(), *(self.v));
        // TODO drop self
    }
}

struct Connection<'a> {
    store: Box<Store>,
    txs: Vec<WriteTx<'a>>,
}

struct Dispatcher<'a> {
    connections: HashMap<String, Connection<'a>>,
}

impl Dispatcher<'_> {
    async fn open(&mut self, req: &Request) -> Response {
        if req.db_name.is_empty() {
            return "db_name must be non-empty".into();
        }
        if self.connections.contains_key(&req.db_name[..]) {
            return "".into();
        }
        let store = Store::new().await;
        self.connections
            .insert(req.db_name.clone(), Connection { store, txs: vec![] });
        "".into()
    }

    async fn close(&mut self, req: &Request) -> Response {
        if !self.connections.contains_key(&req.db_name[..]) {
            return "".into();
        }
        self.connections.remove(&req.db_name);
        // TODO how to know all tx closed?

        "".into()
    }

    //async fn open_transaction(&mut self, req: &Request) ->
}

struct Request {
    db_name: String,
    rpc: String,
    response: Sender<Response>,
}

type Response = String;

lazy_static! {
    static ref SENDER: Mutex<Sender::<Request>> = {
        let (tx, rx) = channel::<Request>(1);
        spawn_local(dispatch_loop(rx));
        Mutex::new(tx)
    };
}

async fn dispatch_loop(rx: Receiver<Request>) {
    let mut stores = HashMap::new();
    //let mut txs = HashMap<String, WriteTx>::new();
    // let mut dispatcher = Dispatcher {
    //     connections: HashMap::new(),
    // };

    loop {
        match rx.recv().await {
            Err(why) => warn!("Dispatch loop recv failed: {}", why),
            Ok(req) => {
                let response: Option<String> = match req.rpc.as_str() {
                    "open" => {
                        // if stores.contains_key(&req.db_name[..]) {
                        //     Some("".into())
                        // }
                        // TODO check like ^^ if already exists
                        let store = Store::new().await;
                        stores.insert(req.db_name.clone(), Connection { store, txs: vec![] });
                        //Some(dispatcher.open(&req).await),
                        Some("".into())
                    }
                    //"close" => Some(dispatcher.close(&req).await),
                    _ => None,
                };
                if let Some(response) = response {
                    req.response.send(response).await;
                    continue;
                }
                let conn = stores.get_mut(&req.db_name[..]).unwrap();
                match req.rpc.as_str() {
                    "open_transaction" => {
                        let write_tx = conn.store.write().await;

                        conn.txs.push(write_tx);
                        // spawn_local()
                        //spawn_local(execute(Dispatcher::has, db.clone(), req));
                    }
                    // "get" => {
                    //     spawn_local(execute(Dispatcher::get, db.clone(), req));
                    // }
                    // "put" => {
                    //     spawn_local(execute(Dispatcher::put, db.clone(), req));
                    // }
                    _ => {
                        req.response.send("Unsupported rpc name".into()).await;
                    }
                };
            }
        }
    }
}

pub async fn dispatch(db_name: String, rpc: String) -> Response {
    let (tx, rx) = channel::<Response>(1);
    let request = Request {
        db_name,
        rpc,
        response: tx,
    };
    match SENDER.lock() {
        Ok(v) => v.send(request).await,
        Err(e) => return e.to_string(),
    }
    match rx.recv().await {
        Err(e) => e.to_string(),
        Ok(v) => v,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[async_std::test]
    pub async fn test_store() {
        let x = Store::new();
    }
}
