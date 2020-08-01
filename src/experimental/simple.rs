use async_std::sync::{channel, Receiver, Sender};
use async_std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};
use log::warn;
use std::collections::HashMap;
use wasm_bindgen_futures::spawn_local;

struct Store {
    pub map: RwLock<HashMap<String, bool>>,
}

impl Store {
    async fn new() -> Store {
        Store {
            map: RwLock::new(HashMap::new()),
        }
    }
    // async fn write<'b: 'a, 'a>(&'a mut self) -> WriteTx<'b> {
    //     let guard = self.map.write().await;
    //     WriteTx {
    //         store: guard,
    //         v: Box::new(false),
    //     }
    // }
}

async fn write<'t, 's: 't>(store: &'s mut Store) -> WriteTx<'t> {
    let guard = store.map.write().await;
    WriteTx {
        store: guard,
        v: Box::new(false),
    }
}

struct WriteTx<'a> {
    store: RwLockWriteGuard<'a, HashMap<String, bool>>,
    v: Box<bool>,
}

impl<'a> WriteTx<'a> {
    async fn set(&mut self, v: bool) {
        let val = &mut (*self.v);
        *val = v;
    }
    async fn commit(&mut self) {
        //       self.store.insert("key".into(), *(self.v));
        // TODO drop self
    }
}

// struct Connection {
//     store: Box<Store>,
//     txs: HashMap<i32, WriteTx>,
// }

// struct Dispatcher {
//     connections: HashMap<String, Connection>,
// }

// impl Dispatcher {
//     async fn open(&mut self, req: &Request) -> Response {
//         if req.db_name.is_empty() {
//             return "db_name must be non-empty".into();
//         }
//         if self.connections.contains_key(&req.db_name[..]) {
//             return "".into();
//         }
//         let store = Store::new().await;
//         // self.connections.insert(
//         //     req.db_name.clone(),
//         //     Connection {
//         //         store,
//         //         txs: HashMap::new(),
//         //     },
//         // );
//         "".into()
//     }

//     async fn close(&mut self, req: &Request) -> Response {
//         if !self.connections.contains_key(&req.db_name[..]) {
//             return "".into();
//         }
//         self.connections.remove(&req.db_name);
//         // TODO how to know all tx closed?

//         "".into()
//     }

//     //async fn open_transaction(&mut self, req: &Request) ->
// }

async fn open_store<'c>(stores: &'c mut HashMap<String, Store>, req: &Request) -> Response {
    if req.db_name.is_empty() {
        return "db_name must be non-empty".into();
    }
    if stores.contains_key(&req.db_name[..]) {
        return "".into();
    }
    let store = Store::new().await;
    stores.insert(req.db_name.clone(), store);
    "".into()
}

#[derive(Clone, Debug)]

struct Request {
    db_name: String,
    rpc: String,
    opt_tx_id: String,
    response: Sender<Response>,
}

struct Stores<'s> {
    stores: HashMap<String, Store>,
    txs: HashMap<i32, Arc<WriteTx<'s>>>,
    next_tx_id: i32,
}

impl<'s> Stores<'s> {
    async fn open<'a>(self: &'a mut Stores<'s>, req: &Request) -> Response {
        if req.db_name.is_empty() {
            return "db_name must be non-empty".into();
        }
        if self.stores.contains_key(&req.db_name[..]) {
            return "".into();
        }
        let store = Store::new().await;
        self.stores.insert(req.db_name.clone(), store);
        "".into()
    }

    async fn open_write_tx(self: &mut Stores<'s>, req: &Request) -> Response {
        let store = self.stores.get_mut(&req.db_name[..]).unwrap();
        let w = write(store).await;
        self.txs.insert(self.next_tx_id, Arc::new(w)); // this can fail
        self.next_tx_id += 1;
        format!("{}", self.next_tx_id).into()
    }
}

type Response = String;

lazy_static! {
    // TODO use async mutex?
    static ref SENDER: std::sync::Mutex<Sender::<Request>> = {
        let (tx, rx) = channel::<Request>(1);
        spawn_local(dispatch_loop(rx));
        std::sync::Mutex::new(tx)
    };
}

async fn dispatch_loop(rx: Receiver<Request>) {
    // //let mut stores = HashMap::new();
    // //let mut txs = HashMap<String, WriteTx>::new();
    // let dispatcher = Arc::new(Mutex::new(Dispatcher {
    //     connections: HashMap::new(),
    // }));

    //let mut stores: HashMap<String, Store> = HashMap::new();
    // TODO all txs in one pile for now
    // TODO need locking outside of writtx?
    //let mut txs: HashMap<i32, Arc<WriteTx>> = HashMap::new();
    //let mut next_tx_id = 0;
    let mut stores = Stores {
        stores: HashMap::new(),
        txs: HashMap::new(),
        next_tx_id: 0,
    };

    loop {
        match rx.recv().await {
            Err(why) => warn!("Dispatch loop recv failed: {}", why),
            Ok(req) => {
                let response: Option<Response> = match req.rpc.as_str() {
                    "open" => {
                        // let dc = Arc::clone(&dispatcher);
                        // let mut d = dc.lock().await;
                        // let r = d.open(&req).await;
                        // Some(r.into()
                        // Some(open_store(&mut stores, &req).await.into())
                        Some(stores.open(&req).await.into())
                        //Some("".into())
                    }
                    //"close" => Some(dispatcher.close(&req).await),
                    _ => None,
                };
                if let Some(response) = response {
                    req.response.send(response).await;
                    continue;
                }

                // TODO handle error here
                let x = match req.rpc.as_str() {
                    "open_transaction" => {
                        {
                            stores.open_write_tx(&req);

                            // let s = stores.get_mut(&req.db_name[..]).unwrap();
                            // let w = write(s).await;
                            // txs.insert(next_tx_id, Arc::new(w)); // this can fail
                            // req.response.send(format!("{}", next_tx_id)).await;
                            // next_tx_id += 1;

                            // let xdc = Arc::clone(&dispatcher);
                            // let mut z = xdc.lock().await;
                            // let conns = &mut z.connections;
                            // let write =
                            //     conns.get_mut(&req.db_name[..]).unwrap().store.write().await;
                            // conns
                            //     .get_mut(&req.db_name[..])
                            //     .unwrap()
                            //     .txs
                            //     .insert(32, write);
                        };
                        // let x1 = xdc.lock();
                        // let xd = x1.await;
                        // let write = xd
                        //     .connections
                        //     .get_mut(&req.db_name[..])
                        //     .unwrap()
                        //     .store
                        //     .write()
                        //     .await;
                        // xd.connections
                        //     .get_mut(&req.db_name[..])
                        //     .unwrap()
                        //     .txs
                        //     .insert(32, write);
                        //                        drop(xd);
                        //                      drop(xdc);

                        // let xdc = Arc::clone(&dispatcher);
                        // let mut xd = xdc.lock().await;
                        // let xdd = &mut *xd;
                        // let conn = xdd.connections.get_mut(&req.db_name[..]).unwrap();
                        // let write = conn.store.write().await;
                        // let txs = &mut conn.txs;
                        // txs.insert(32, write);
                        // drop(xd);
                        // drop(xdc);
                        // spawn_local()
                        //spawn_local(execute(Dispatcher::has, db.clone(), req));
                    }
                    // "get" => {
                    //     spawn_local(execute(Dispatcher::get, db.clone(), req));
                    // }
                    "put" => {
                        // //spawn_local(execute(Dispatcher::put, db.clone(), req));
                        // let tx_id = req.opt_tx_id.parse::<i32>().unwrap();
                        // let tx = txs.get(&tx_id).unwrap();
                        // let tx_ref = Arc::clone(tx);
                        // // TODO handle not found
                        // let req_clone = req.clone();
                        // let put_closure = put(tx_ref, req_clone);
                        // spawn_local(put_closure);
                    }
                    _ => {
                        req.response.send("Unsupported rpc name".into()).await;
                    }
                };
            }
        }
    }
}

async fn put(write_tx: Arc<WriteTx<'_>>, req: Request) {}

// async fn open_transaction<'b, 'c>(d: &mut Dispatcher<'b>, db_name: &str) -> Response {
//     let write = d.connections.get_mut(db_name).unwrap().store.write().await;
//    let _ = d.connections
//         .get_mut(db_name)
//         .unwrap()
//         .txs
//         .insert(32, write);

//     "foo".into()
// }

// async fn open_transaction(d: Arc<Mutex<Dispatcher<'_>>>, db_name: &str) -> Response {
//     let d2 = Arc::clone(&d);
//     let x1 = d2.lock();
//     {
//         let xd = x1.await;
//         let write = xd.connections.get_mut(db_name).unwrap().store.write().await;
//         xd.connections
//             .get_mut(db_name)
//             .unwrap()
//             .txs
//             .insert(32, write);
//     }

//     "foo".into()
// }

pub async fn dispatch(db_name: String, rpc: String) -> Response {
    let (tx, rx) = channel::<Response>(1);
    let request = Request {
        db_name,
        rpc,
        opt_tx_id: "0".to_string(), // TODO
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
