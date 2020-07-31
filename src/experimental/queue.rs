use async_std::sync::TryRecvError;
use async_std::sync::{channel, Receiver, Sender};
use log::warn;
use std::collections::HashMap;
use std::collections::VecDeque;
use wasm_bindgen_futures::spawn_local;

// BIG PICTURE
//
// Took aaron's idea: serialize processing and stack-allocate our
// book-keeping so that ownership is clear and we can rely on
// rust's compile-time borrow checking instead of runtime checking
// and locks.
// 
// Dispatch happens in two layers. dispatch_loop is the first layer, 
// responsible for opening and managing connections to databases. Each 
// database ("store" in the code here) has its own connection_loop.
// The dispatch_loop sends requests for a given db to its connection_loop.
//
// The connection_loop keeps a queue of incoming requests. It keeps at most
// one write tx open at a time and when a write tx is open no other txs may run.
// In order not to deadlock the connection_loop scans the queue for requests
// for the open write tx if any, and runs them ahead of the rest of the queue.
// 
// Caveats:
// - the database just stores a bool
// - I inlined a bunch of stuff into Request (tx id, key, value, etc) for
//   convenience to make this toy easier to write, ditto simplified return types.
// - there is no del, rollback, etc only open, close, open_tx, get, put, commit.
// - no concept of a read tx
// - no parallelism

struct Dispatcher {
    // ch to send requests to the dispatch_loop.
    req_ch_snd: Sender<Request>,
    // ch to tell the dispatch_loop to exit.
    // TODO this quit channel to dispatch loop is currently unused.
    // Should we have a "quit" rpc that exits everything out?
    // How do we wait for it to exit?
    quit_ch_snd: Sender<()>,
}

lazy_static! {
    static ref SENDER: std::sync::Mutex<Dispatcher> = {
        // Channels to dispatch_loop. The dispatch_loop creates
        // Connections and channels to them inside of it.
        let (req_ch_snd, req_ch_rec) = channel::<Request>(1);
        let (quit_ch_snd, quit_ch_rec) = channel::<()>(1);
        spawn_local(dispatch_loop(req_ch_rec, quit_ch_rec));
        let dispatcher = Dispatcher{
            req_ch_snd,
            quit_ch_snd,
        };
        // TODO async mutex?
        std::sync::Mutex::new(dispatcher)
    };
}

struct Request {
    db_name: String,
    rpc: String,
    key: String,
    value: bool,
    tx_id: String,
    response_sender: Sender<Response>,
}

type Response = String;

struct Store {
    // Box to mimic the real thing which i think is a dyn trait.
    pub map: Box<HashMap<String, bool>>,
}

struct WriteTx {
    id: u64,
    buffer: HashMap<String, bool>,
}

pub async fn dispatch(
    db_name: String,
    rpc: String,
    key: String,
    value: bool,
    tx_id: String,
) -> Response {
    let (ch_in, ch_out) = channel::<Response>(1);
    let request = Request {
        db_name,
        rpc,
        key,
        value,
        tx_id: tx_id.clone(),
        response_sender: ch_in,
    };
    match SENDER.lock() {
        Ok(d) => d.req_ch_snd.send(request).await,
        Err(e) => return format!("error locking :{}", e.to_string()),
    }
    match ch_out.recv().await {
        Err(e) => format!("error receiving: {}", e.to_string()),
        Ok(v) => v,
    }
}

struct Connection {
    // Requests come in on this ch. Better name requested.
    query_ch: Sender<Request>,
    // Signal to quit from dispatch_loop.
    quit_ch: Sender<()>,
}

async fn dispatch_loop(req_ch_rec: Receiver<Request>, quit_ch_rec: Receiver<()>) {
    let mut conns: HashMap<String, Connection> = HashMap::new();

    loop {
        match quit_ch_rec.try_recv() {
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => {
                // TODO log an error
                return;
            }
            Ok(_) => {
                // TODO iterate all quit_chs and send them the
                // close signal and wait for them to complete.
                // Unclear yet whether we need to wait for them to
                // signal that they have exited and if so how we do
                // that. If we had a closure we could wait on it, or
                // maybe we have something like a refcounted () we
                // pass to each and when it Drops we can exit?
                // Bet there is a more standard solution.
                return;
            }
        }

        match req_ch_rec.recv().await {
            // TODO should probably log an error and bail on Err here.
            Err(why) => warn!("Dispatch loop recv failed: {}", why),
            Ok(req) => {
                match req.rpc.as_str() {
                    "open" => {
                        if req.db_name.is_empty() {
                            req.response_sender
                                .send("db_name must be non-empty".into())
                                .await;
                            continue;
                        }
                        if conns.contains_key(&req.db_name[..]) {
                            req.response_sender.send("ok".into()).await;
                            continue;
                        }

                        // Create a new Connection and connection_loop.
                        let (query_req_ch_snd, query_req_ch_rec) = channel::<Request>(1);
                        let (query_quit_ch_snd, query_quit_ch_rec) = channel::<()>(1);
                        let c = Connection {
                            query_ch: query_req_ch_snd,
                            quit_ch: query_quit_ch_snd,
                        };
                        conns.insert(req.db_name.clone(), c);
                        // TODO do something real to open the db.
                        let store = Store {
                            map: Box::new(HashMap::new()),
                        };
                        spawn_local(connection_loop(query_req_ch_rec, query_quit_ch_rec, store));
                        req.response_sender.send("ok".into()).await;
                        continue;
                    }
                    "close" => {
                        let msg = if req.db_name.is_empty() {
                            "db_name must be non-empty"
                        } else {
                            match conns.get(&req.db_name[..]) {
                                Some(c) => {
                                    c.quit_ch.send(()).await;
                                    // TODO maybe wait for the quit to complete?
                                    conns.remove(&req.db_name[..]);
                                    "ok"
                                }
                                None => "db not open",
                            }
                        };
                        req.response_sender.send(msg.into()).await;
                        continue;
                    }
                    _ => match conns.get(&req.db_name[..]) {
                        Some(c) => {
                            c.query_ch.send(req).await;
                        }
                        None => {
                            req.response_sender.send("no such db".into()).await;
                        }
                    },
                }
            }
        }
    }
}

// One per connection / database. Handles requests for this db.
async fn connection_loop(req_ch_rec: Receiver<Request>, quit_ch_rec: Receiver<()>, mut store: Store) {
    // We buffer Requests to the db so we can allow Requests to the current
    // write tx (if any) to jump the queue.
    let mut queue = VecDeque::new();

    // If write_tx is None we are executing reads.
    let mut write_tx: Option<WriteTx> = None;
    let mut next_tx_id = 0u64;

    loop {
        // Check if we have received the quit signal.
        match quit_ch_rec.try_recv() {
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => {
                // TODO log an error
                return;
            }
            Ok(_) => return,
        }

        // We want to wait asynchronously for a new request (and not spin) if
        // we have nothing to do. We have nothing to do if:
        //   A) the queue is empty or
        //   B) we have an open write tx but zero Requests for it in the queue
        let r_write_tx = write_tx.as_ref();
        if queue.is_empty()
            || (r_write_tx.is_some()
                && !queue
                    .iter()
                    .any(|r: &Request| r.tx_id == r_write_tx.unwrap().id.to_string()))
        {
            let received = req_ch_rec.recv().await;
            match received {
                Err(why) => warn!("Dispatch loop recv failed: {}", why),
                Ok(req) => queue.push_back(req),
            }
        }

        // Now we likely have work to do: either the queue was not empty or
        // it was and we received a new request. (The case where the queue
        // was not empty and we received an error so have no work to do is
        // handled by the pop step below.) We drain anything in the channel
        // in a nonblocking fashion. This enables sender to proceed if it
        // is blocked and maximizes the chance that we see requests for the
        // current write tx, if any (see below).
        let mut r = req_ch_rec.try_recv();
        while !r.is_err() {
            queue.push_back(r.unwrap());
            r = req_ch_rec.try_recv();
        }
        match r {
            Err(TryRecvError::Disconnected) => {
                // TODO log
                return;
            }
            _ => {}
        };

        // Now we likely have at least one request in the queue. However
        // if we are in a write transaction we can't just pop it off and
        // execute it: it might be a read tx or a new write tx. Instead,
        // if we are in a write tx we have to scan the queue for any pending
        // requests for the current write tx and if so take the first one.
        let mut next_req = None;
        let r_write_tx = write_tx.as_ref();
        if r_write_tx.is_some() {
            match queue
                .iter()
                .position(|r| r.tx_id == r_write_tx.unwrap().id.to_string())
            {
                Some(i) => {
                    let removed = queue.remove(i);
                    if removed.is_some() {
                        next_req = removed;
                    } else {
                        panic!("not possible");
                    }
                }
                None => {
                    // Nothing in the queue for the current write tx. Go back
                    // and wait for something to arrive.
                    continue;
                }
            }
        }
        // If we are not in a write tx or we are in a write tx and didn't find a queued request
        // for the current tx, pop the next thing off the queue.
        if next_req.is_none() {
            next_req = queue.pop_front();
        }
        // We need to check here that we actually got something. For example if the queue was empty
        // and we received an error no request will have been enqueued.
        if next_req.is_none() {
            continue;
        }

        let req = next_req.unwrap();
        match req.rpc.as_str() {
            "get" => {
                let mut msg: Option<String> = None;
                if r_write_tx.is_some() {
                    if req.tx_id != r_write_tx.unwrap().id.to_string() {
                        panic!("phritz messed up");
                    } else {
                        msg = match r_write_tx.unwrap().buffer.get(&req.key) {
                            Some(v) => Some(format!("{}", v)),
                            None => None,
                        }
                    }
                };
                if msg.is_none() {
                    msg = match store.map.get(&req.key) {
                        Some(v) => Some(format!("{}", v)),
                        None => Some("no such key".into()),
                    }
                }
                req.response_sender.send(msg.unwrap()).await;
            }
            "open_transaction" => {
                if write_tx.is_some() {
                    panic!("phritz messed up");
                }
                write_tx = Some(WriteTx {
                    id: next_tx_id,
                    buffer: HashMap::new(),
                });
                next_tx_id += 1;
                let resp = format!("{}", write_tx.as_ref().unwrap().id);
                req.response_sender.send(resp).await;
            }
            "put" => match write_tx {
                Some(ref mut tx) => {
                    if req.tx_id != tx.id.to_string() {
                        req.response_sender.send("Wrong tx id".into()).await;
                    } else {
                        tx.buffer.insert(req.key, req.value);
                        req.response_sender.send("ok".into()).await;
                    }
                }
                None => {
                    req.response_sender
                        .send("Can't put: no open write tx".into())
                        .await;
                }
            },
            "commit" => match write_tx {
                Some(ref mut tx) => {
                    if req.tx_id != tx.id.to_string() {
                        req.response_sender.send("Wrong tx id".into()).await;
                    } else {
                        for (k, v) in tx.buffer.drain() {
                            store.map.insert(k, v);
                        }
                        drop(tx);
                        write_tx = None;
                        req.response_sender.send("ok".into()).await;
                    }
                }
                None => {
                    req.response_sender
                        .send("Can't put: no open write tx".into())
                        .await;
                }
            },
            _ => {
                req.response_sender
                    .send("Unsupported rpc name".into())
                    .await;
            }
        };
    }
}
