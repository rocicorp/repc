use super::dispatch::Request;
use super::types::*;
use crate::dag;
use crate::db;
use crate::fetch;
use crate::sync;
use crate::util::nanoserde::any;
use crate::util::rlog;
use crate::util::rlog::LogContext;
use async_fn::{AsyncFn2, AsyncFn3};
use async_std::stream::StreamExt;
use async_std::sync::{Receiver, RecvError, RwLock};
use futures::stream::futures_unordered::FuturesUnordered;
use log::warn;
use nanoserde::{DeJson, SerJson};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

lazy_static! {
    static ref TRANSACTION_COUNTER: AtomicU32 = AtomicU32::new(1);
}

enum Transaction<'a> {
    Read(db::OwnedRead<'a>),
    Write(db::Write<'a>),
}

impl<'a> Transaction<'a> {
    fn as_read(&self) -> db::Read {
        match self {
            Transaction::Read(r) => r.as_read(),
            Transaction::Write(w) => w.as_read(),
        }
    }
}

type TxnMap<'a> = RwLock<HashMap<u32, RwLock<Transaction<'a>>>>;

fn deserialize<T: DeJson>(data: &str) -> Result<T, String> {
    match DeJson::deserialize_json(data) {
        Ok(v) => Ok(v),
        Err(e) => Err(format!("InvalidJson({})", e)),
    }
}

enum UnorderedResult {
    Request(Result<Request, RecvError>),
    Stop(),
    None(),
}

async fn connection_future<'a, 'b>(
    rx: &Receiver<Request>,
    ctx: Context<'a, 'b>,
    request: Option<Request>,
) -> UnorderedResult {
    let req = match request {
        None => return UnorderedResult::Request(rx.recv().await),
        Some(v) => v,
    };
    match req.rpc.as_str() {
        "getRoot" => execute(ctx, do_get_root, req).await,
        "has" => execute_in_txn(do_has, ctx.txns, req).await,
        "get" => execute_in_txn(do_get, ctx.txns, req).await,
        "scan" => execute_in_txn(do_scan, ctx.txns, req).await,
        "put" => execute_in_txn(do_put, ctx.txns, req).await,
        "del" => execute_in_txn(do_del, ctx.txns, req).await,
        "openTransaction" => execute(ctx, do_open_transaction, req).await,
        "commitTransaction" => execute(ctx, do_commit, req).await,
        "closeTransaction" => execute(ctx, do_close_transaction, req).await,
        "beginSync" => execute(ctx, do_begin_sync, req).await,
        "maybeEndSync" => execute(ctx, do_maybe_end_sync, req).await,
        "close" => {
            ctx.store.close().await;
            req.response.send(Ok("".into())).await;
            return UnorderedResult::Stop();
        }
        _ => {
            req.response
                .send(Err(format!("Unsupported rpc name {}", req.rpc)))
                .await
        }
    };
    UnorderedResult::None()
}

pub async fn process(store: dag::Store, rx: Receiver<Request>, client_id: String, lc: LogContext) {
    if let Err(err) = do_init(&store, lc.clone()).await {
        error!(lc, "Could not initialize db: {:?}", err);
        return;
    }

    let txns = RwLock::new(HashMap::new());
    let mut futures = FuturesUnordered::new();
    let mut recv = true;

    futures.push(connection_future(
        &rx,
        Context::new(&store, &txns, client_id.clone(), LogContext::new()),
        None,
    ));
    while let Some(value) = futures.next().await {
        if recv {
            futures.push(connection_future(
                &rx,
                Context::new(&store, &txns, client_id.clone(), LogContext::new()),
                None,
            ));
        }
        match value {
            UnorderedResult::Request(value) => match value {
                // TODO turn this into lc.info() it is expected and not a problem or
                // turn it into an lc.error() otherwise.
                Err(why) => warn!("Connection loop recv failed: {}", why),
                Ok(req) => {
                    futures.push(connection_future(
                        &rx,
                        Context::new(
                            &store,
                            &txns,
                            client_id.clone(),
                            LogContext::new_from_context(req.lc.clone()),
                        ),
                        Some(req),
                    ));
                }
            },
            UnorderedResult::Stop() => recv = false,
            UnorderedResult::None() => {}
        }
    }
}

async fn execute_in_txn<T, S, F>(func: F, txns: &TxnMap<'_>, req: Request)
where
    T: DeJson + TransactionRequest,
    S: SerJson,
    F: for<'r, 's> AsyncFn3<&'r RwLock<Transaction<'s>>, T, LogContext, Output = Result<S, String>>,
{
    let request: T = match deserialize(&req.data) {
        Ok(v) => v,
        Err(e) => return req.response.send(Err(e)).await,
    };

    let txn_id = request.transaction_id();
    let mut lc = LogContext::new_from_context(req.lc.clone());
    let txn_id_string = txn_id.to_string();
    lc.add_context("txid", &txn_id_string);
    let txns = txns.read().await;
    let txn = match txns.get(&txn_id) {
        Some(v) => v,
        None => {
            return req
                .response
                .send(Err(format!("No transaction {}", txn_id)))
                .await
        }
    };

    req.response
        .send(
            func.call(txn, request, lc)
                .await
                .map(|v| SerJson::serialize_json(&v)),
        )
        .await;
}

struct Context<'a, 'b> {
    store: &'a dag::Store,
    txns: &'b TxnMap<'a>,
    client_id: String,
    lc: LogContext,
}

impl<'a, 'b> Context<'a, 'b> {
    fn new(
        store: &'a dag::Store,
        txns: &'b TxnMap<'a>,
        client_id: String,
        lc: LogContext,
    ) -> Context<'a, 'b> {
        Context {
            store,
            txns,
            client_id,
            lc,
        }
    }
}

async fn execute<'a, 'b, T, S, F, E>(ctx: Context<'a, 'b>, func: F, req: Request)
where
    T: DeJson,
    S: SerJson,
    E: std::fmt::Debug,
    F: AsyncFn2<Context<'a, 'b>, T, Output = Result<S, E>>,
{
    let request: T = match deserialize(&req.data) {
        Ok(v) => v,
        Err(e) => return req.response.send(Err(e)).await,
    };

    let result = func
        .call(ctx, request)
        .await
        .map(|v| SerJson::serialize_json(&v))
        .map_err(|e| format!("{:?}", e));

    req.response.send(result).await
}

#[derive(Debug)]
pub enum DoInitError {
    WriteError(dag::Error),
    GetHeadError(dag::Error),
    InitDBError(db::InitDBError),
}

async fn do_init(store: &dag::Store, lc: LogContext) -> Result<(), DoInitError> {
    use DoInitError::*;
    let dw = store.write(lc).await.map_err(WriteError)?;
    if dw
        .read()
        .get_head(db::DEFAULT_HEAD_NAME)
        .await
        .map_err(GetHeadError)?
        .is_none()
    {
        db::init_db(dw, db::DEFAULT_HEAD_NAME, "local_create_date")
            .await
            .map_err(InitDBError)?;
    }
    Ok(())
}

async fn do_open_transaction<'a, 'b>(
    ctx: Context<'a, 'b>,
    req: OpenTransactionRequest,
) -> Result<OpenTransactionResponse, OpenTransactionError> {
    use OpenTransactionError::*;

    let txn = match req.name {
        Some(mutator_name) => {
            let OpenTransactionRequest {
                name: _,
                args: mutator_args,
                rebase_opts,
            } = req;
            let mutator_args = mutator_args.ok_or(ArgsRequired)?;

            let lock_timer = rlog::Timer::new().map_err(InternalTimerError)?;
            debug!(ctx.lc, "Waiting for write lock...");
            let dag_write = ctx
                .store
                .write(ctx.lc.clone())
                .await
                .map_err(DagWriteError)?;
            debug!(
                ctx.lc,
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
            let dag_read = ctx.store.read(ctx.lc.clone()).await.map_err(DagReadError)?;
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
    ctx.txns.write().await.insert(txn_id, RwLock::new(txn));
    Ok(OpenTransactionResponse {
        transaction_id: txn_id,
    })
}

async fn validate_rebase<'a>(
    opts: &'a RebaseOpts,
    dag_read: dag::Read<'_>,
    mutator_name: &'a str,
    _args: &'a any::Any,
) -> Result<(), OpenTransactionError> {
    use OpenTransactionError::*;

    // Ensure the rebase commit is going on top of the current sync head.
    let sync_head_hash = dag_read
        .get_head(sync::SYNC_HEAD_NAME)
        .await
        .map_err(GetHeadError)?;
    if sync_head_hash.as_ref() != Some(&opts.basis) {
        return Err(WrongSyncHeadJSLogInfo(format!(
            "sync head is {:?}, transaction basis is {:?}",
            sync_head_hash, opts.basis
        )));
    }

    // Ensure rebase and original commit mutator names match.
    let (_, original, _) = db::read_commit(db::Whence::Hash(opts.original_hash.clone()), &dag_read)
        .await
        .map_err(NoSuchOriginal)?;
    match original.meta().typed() {
        db::MetaTyped::Local(lm) => {
            if lm.mutator_name() != mutator_name {
                return Err(InconsistentMutator(format!(
                    "original: {}, request: {}",
                    lm.mutator_name(),
                    mutator_name
                )));
            }
        }
        _ => {
            return Err(InternalProgrammerError(
                "Commit is not a local commit".to_string(),
            ))
        }
    };

    // Ensure rebase and original commit mutation ids names match.
    let (_, basis, _) = db::read_commit(db::Whence::Hash(opts.basis.clone()), &dag_read)
        .await
        .map_err(NoSuchBasis)?;
    if basis.next_mutation_id() != original.mutation_id() {
        return Err(InconsistentMutationId(format!(
            "original: {}, next: {}",
            original.mutation_id(),
            basis.next_mutation_id(),
        )));
    }

    // TODO: temporarily skipping check that args are the same.
    // https://github.com/rocicorp/repc/issues/151

    Ok(())
}

async fn do_commit<'a, 'b>(
    ctx: Context<'a, 'b>,
    req: CommitTransactionRequest,
) -> Result<CommitTransactionResponse, CommitTransactionError> {
    use CommitTransactionError::*;
    let txn_id = req.transaction_id;
    let mut txns = ctx.txns.write().await;
    let txn = txns.remove(&txn_id).ok_or(UnknownTransaction)?;
    let txn = match txn.into_inner() {
        Transaction::Write(w) => Ok(w),
        Transaction::Read(_) => Err(TransactionIsReadOnly),
    }?;
    let head_name = if txn.is_rebase() {
        sync::SYNC_HEAD_NAME
    } else {
        db::DEFAULT_HEAD_NAME
    };
    let hash = txn
        .commit(head_name, "local-create-date")
        .await
        .map_err(CommitError)?;
    Ok(CommitTransactionResponse {
        hash,
        retry_commit: false,
    })
}

async fn do_close_transaction<'a, 'b>(
    ctx: Context<'a, 'b>,
    request: CloseTransactionRequest,
) -> Result<CloseTransactionResponse, CloseTransactionError> {
    use CloseTransactionError::*;
    let txn_id = request.transaction_id;
    ctx.txns
        .write()
        .await
        .remove(&txn_id)
        .ok_or(UnknownTransaction)?;
    Ok(CloseTransactionResponse {})
}

async fn do_get_root<'a, 'b>(
    ctx: Context<'a, 'b>,
    req: GetRootRequest,
) -> Result<GetRootResponse, GetRootError> {
    let head_name = match req.head_name {
        Some(name) => name,
        None => db::DEFAULT_HEAD_NAME.to_string(),
    };
    Ok(GetRootResponse {
        root: db::get_root(ctx.store, head_name.as_str(), ctx.lc.clone())
            .await
            .map_err(GetRootError::DBError)?,
    })
}

async fn do_has(
    txn: &RwLock<Transaction<'_>>,
    req: HasRequest,
    _: LogContext,
) -> Result<HasResponse, String> {
    Ok(HasResponse {
        has: txn.read().await.as_read().has(req.key.as_bytes()),
    })
}

async fn do_get(
    txn: &RwLock<Transaction<'_>>,
    req: GetRequest,
    _: LogContext,
) -> Result<GetResponse, String> {
    #[cfg(not(default))] // Not enabled in production.
    if req.key.starts_with("sleep") {
        use async_std::task::sleep;
        use core::time::Duration;

        match req.key[5..].parse::<u64>() {
            Ok(ms) => {
                sleep(Duration::from_millis(ms)).await;
            }
            Err(_) => error!("", "No sleep time"),
        }
    }

    let got = txn
        .read()
        .await
        .as_read()
        .get(req.key.as_bytes())
        .map(|buf| String::from_utf8(buf.to_vec()));
    if let Some(Err(e)) = got {
        return Err(format!("{:?}", e));
    }
    let got = got.map(|r| r.unwrap());
    Ok(GetResponse {
        has: got.is_some(),
        value: got,
    })
}

async fn do_scan(
    txn: &RwLock<Transaction<'_>>,
    req: ScanRequest,
    _: LogContext,
) -> Result<ScanResponse, String> {
    use std::convert::TryFrom;
    let mut res = Vec::<ScanItem>::new();
    for pe in txn.read().await.as_read().scan((&req.opts).into()) {
        res.push(ScanItem::try_from(pe).map_err(|e| format!("{:?}", e))?);
    }
    Ok(ScanResponse { items: res })
}

async fn do_put(
    txn: &RwLock<Transaction<'_>>,
    req: PutRequest,
    _: LogContext,
) -> Result<PutResponse, String> {
    let mut guard = txn.write().await;
    let write = match &mut *guard {
        Transaction::Write(w) => Ok(w),
        Transaction::Read(_) => Err("Specified transaction is read-only".to_string()),
    }?;
    write.put(req.key.as_bytes().to_vec(), req.value.into_bytes());
    Ok(PutResponse {})
}

async fn do_del(
    txn: &RwLock<Transaction<'_>>,
    req: DelRequest,
    _: LogContext,
) -> Result<DelResponse, String> {
    let mut guard = txn.write().await;
    let write = match &mut *guard {
        Transaction::Write(w) => Ok(w),
        Transaction::Read(_) => Err("Specified transaction is read-only".to_string()),
    }?;
    let had = write.as_read().has(req.key.as_bytes());
    write.del(req.key.as_bytes().to_vec());
    Ok(DelResponse { had })
}

async fn do_begin_sync<'a, 'b>(
    ctx: Context<'a, 'b>,
    req: sync::BeginSyncRequest,
) -> Result<sync::BeginSyncResponse, sync::BeginSyncError> {
    // TODO move client, puller, pusher up to process() or into a lazy static so we can share.
    let fetch_client = fetch::client::Client::new();
    let pusher = sync::push::FetchPusher::new(&fetch_client);
    let puller = sync::FetchPuller::new(&fetch_client);
    let begin_sync_response = sync::begin_sync(
        ctx.store,
        ctx.lc.clone(),
        &pusher,
        &puller,
        req,
        ctx.client_id,
    )
    .await?;
    Ok(begin_sync_response)
}

async fn do_maybe_end_sync<'a, 'b>(
    ctx: Context<'a, 'b>,
    req: sync::MaybeEndSyncRequest,
) -> Result<sync::MaybeEndSyncResponse, sync::MaybeEndSyncError> {
    let maybe_end_sync_response = sync::maybe_end_sync(ctx.store, ctx.lc.clone(), req).await?;
    Ok(maybe_end_sync_response)
}

#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
enum GetRootError {
    DBError(db::GetRootError),
}

#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
enum OpenTransactionError {
    ArgsRequired,
    DagWriteError(dag::Error),
    DagReadError(dag::Error),
    DBWriteError(db::ReadCommitError),
    DBReadError(db::ReadCommitError),
    GetHeadError(dag::Error),
    InconsistentMutationId(String),
    InconsistentMutator(String),
    InternalProgrammerError(String),
    InternalTimerError(rlog::TimerError),
    NoSuchBasis(db::ReadCommitError),
    NoSuchOriginal(db::ReadCommitError),
    WrongSyncHeadJSLogInfo(String), // "JSLogInfo" is a signal to bindings to not log this alarmingly.
}

#[derive(Debug)]
enum CommitTransactionError {
    CommitError(db::CommitError),
    TransactionIsReadOnly,
    UnknownTransaction,
}

#[derive(Debug)]
enum CloseTransactionError {
    UnknownTransaction,
}

trait TransactionRequest {
    fn transaction_id(&self) -> u32;
}

macro_rules! impl_transaction_request {
    ($type_name:ident) => {
        impl TransactionRequest for $type_name {
            fn transaction_id(&self) -> u32 {
                self.transaction_id
            }
        }
    };
}

impl_transaction_request!(HasRequest);
impl_transaction_request!(GetRequest);
impl_transaction_request!(ScanRequest);
impl_transaction_request!(PutRequest);
impl_transaction_request!(DelRequest);

// Note: dispatch is mostly tested in tests/wasm.rs.
// TODO those tests should move here and *also* be run from there so we have
// coverage in both rust using memstore and in wasm using idbstore.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_helpers::*;
    use crate::kv::memstore::MemStore;
    use crate::sync::test_helpers::*;
    use crate::util::nanoserde::any::Any;
    use crate::util::rlog::LogContext;
    use str_macro::str;

    #[async_std::test]
    async fn test_open_transaction_rebase_opts() {
        // Note: store needs to outlive txns.
        let store = dag::Store::new(Box::new(MemStore::new()));
        {
            let txns = RwLock::new(HashMap::new());
            let mut main_chain: Chain = vec![];
            add_genesis(&mut main_chain, &store).await;
            add_local(&mut main_chain, &store).await;
            let sync_chain =
                add_sync_snapshot(&mut main_chain, &store, LogContext::new(), false).await;
            let original = &main_chain[1];
            let meta = original.meta();
            let (original_hash, original_name, original_args) = match meta.typed() {
                db::MetaTyped::Local(lm) => (
                    str!(original.chunk().hash()),
                    str!(lm.mutator_name()),
                    Any::deserialize_json(std::str::from_utf8(lm.mutator_args_json()).unwrap())
                        .unwrap(),
                ),
                _ => panic!("not local"),
            };
            drop(meta);
            drop(original);

            // Error: rebase commit's basis must be sync head.
            let result = do_open_transaction(
                Context::new(&store, &txns, str!("client_id"), LogContext::new()),
                OpenTransactionRequest {
                    name: Some(original_name.clone()),
                    args: Some(original_args.clone()),
                    rebase_opts: Some(RebaseOpts {
                        basis: original_hash.clone(), // <-- not the sync head
                        original_hash: original_hash.clone(),
                    }),
                },
            )
            .await;
            assert!(format!("{:?}", result.unwrap_err()).contains("WrongSyncHeadJSLogInfo"));

            // Error: rebase commit's name should not change.
            let result = do_open_transaction(
                Context::new(&store, &txns, str!("client_id"), LogContext::new()),
                OpenTransactionRequest {
                    name: Some(str!("different!")),
                    args: Some(original_args.clone()),
                    rebase_opts: Some(RebaseOpts {
                        basis: str!(sync_chain[0].chunk().hash()),
                        original_hash: original_hash.clone(),
                    }),
                },
            )
            .await;
            assert!(format!("{:?}", result.unwrap_err()).contains("InconsistentMutator"));

            // TODO test error: rebase commit's args should not change.
            // https://github.com/rocicorp/repc/issues/151

            // Ensure it doesn't let us rebase with a different mutation id.
            add_local(&mut main_chain, &store).await;
            let new_local = &main_chain[main_chain.len() - 1];
            let meta = new_local.meta();
            let (new_local_hash, new_local_name, new_local_args) = match meta.typed() {
                db::MetaTyped::Local(lm) => (
                    str!(new_local.chunk().hash()),
                    str!(lm.mutator_name()),
                    Any::deserialize_json(std::str::from_utf8(lm.mutator_args_json()).unwrap())
                        .unwrap(),
                ),
                _ => panic!("not local"),
            };
            let result = do_open_transaction(
                Context::new(&store, &txns, str!("client_id"), LogContext::new()),
                OpenTransactionRequest {
                    name: Some(new_local_name),
                    args: Some(new_local_args),
                    rebase_opts: Some(RebaseOpts {
                        basis: str!(sync_chain[0].chunk().hash()),
                        original_hash: new_local_hash, // <-- has different mutation id
                    }),
                },
            )
            .await;
            let err = result.unwrap_err();
            print!("{:?}", err);
            //assert!(format!("{:?}", result.unwrap_err()).contains("InconsistentMutationId"));
            assert!(format!("{:?}", err).contains("InconsistentMutationId"));

            // Correct rebase_opt (test this last because it affects the chain).
            let otr = do_open_transaction(
                Context::new(&store, &txns, str!("client_id"), LogContext::new()),
                OpenTransactionRequest {
                    name: Some(original_name.clone()),
                    args: Some(original_args.clone()),
                    rebase_opts: Some(RebaseOpts {
                        basis: str!(sync_chain[0].chunk().hash()),
                        original_hash: original_hash.clone(),
                    }),
                },
            )
            .await
            .unwrap();
            let ctr = do_commit(
                Context::new(&store, &txns, str!("client_id"), LogContext::new()),
                CommitTransactionRequest {
                    transaction_id: otr.transaction_id,
                },
            )
            .await
            .unwrap();
            let w = store.write(LogContext::new()).await.unwrap();
            let sync_head_hash = w
                .read()
                .get_head(sync::SYNC_HEAD_NAME)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(ctr.hash, sync_head_hash);
        }
    }
}
