use crate::dag;
use crate::db;
use nanoserde::{DeJson, DeJsonErr, SerJson};
use std::collections::HashMap;

pub struct Connection<'a> {
    next_tx_id: u32,
    store: dag::Store,

    // TODO: Support read-only transactions too
    transactions: HashMap<u32, db::Write<'a>>,
}

#[allow(dead_code)]
impl<'a> Connection<'a> {
    pub fn new(store: dag::Store) -> Connection<'a> {
        Connection {
            next_tx_id: 1,
            store,
            transactions: HashMap::new(),
        }
    }

    pub async fn open_transaction(&'a mut self) -> Result<u32, OpenTransactionError> {
        use OpenTransactionError::*;

        let tx_id = self.next_tx_id;
        self.next_tx_id = tx_id + 1;

        let dag_write = self.store.write().await.map_err(DagWriteOpenFailed)?;
        let db_write = db::Write::new_from_head("main", dag_write)
            .await
            .map_err(DbWriteNewFailed)?;
        self.transactions.insert(tx_id, db_write);
        Ok(tx_id)
    }

    pub async fn abort_transaction(&mut self, tx_id: u32) -> Result<(), AbortTransactionError> {
        self.transactions
            .remove(&tx_id)
            .and(().into())
            .ok_or(AbortTransactionError::UnknownTransactionID)
    }

    pub async fn commit_transaction(
        &mut self,
        tx_id: u32,
    ) -> Result<String, CommitTransactionError> {
        use CommitTransactionError::*;
        let tx = self
            .transactions
            .remove(&tx_id)
            .ok_or(UnknownTransactionID)?;
        // TODO: Store values from JSON in db::Write.
        // TODO: Only commit if there were changes.
        tx.commit(
            "main",
            "date",
            "checksum",
            1,
            "mutator_name",
            b"mutator_args_json",
            None,
        )
        .await
        .map_err(DBCommitError)?;
        // TODO: Return correct thing per protocol.
        Ok("{}".to_string())
    }

    pub async fn has(&self, data: &str) -> Result<String, HasError> {
        use HasError::*;
        let req: HasRequest = DeJson::deserialize_json(&data).map_err(InvalidJson)?;
        let write = self
            .transactions
            .get(&req.transaction_id)
            .ok_or(UnknownTransactionID)?;
        Ok(SerJson::serialize_json(&HasResponse {
            has: write.has(req.key.as_bytes()),
        }))
    }

    pub async fn get(&self, data: &str) -> Result<String, GetError> {
        use GetError::*;
        let req: GetRequest = DeJson::deserialize_json(&data).map_err(InvalidJson)?;

        #[cfg(not(default))] // Not enabled in production.
        if req.key.starts_with("sleep") {
            use async_std::task::sleep;
            use core::time::Duration;

            match req.key[5..].parse::<u64>() {
                Ok(ms) => sleep(Duration::from_millis(ms)).await,
                Err(_) => log::error!("No sleep"),
            }
        }

        let write = self
            .transactions
            .get(&req.transaction_id)
            .ok_or(UnknownTransactionID)?;

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

    pub async fn put(&mut self, data: &str) -> Result<String, PutError> {
        use PutError::*;
        let req: PutRequest = DeJson::deserialize_json(&data).map_err(InvalidJson)?;
        let write = self
            .transactions
            .get_mut(&req.transaction_id)
            .ok_or(UnknownTransactionID)?;
        write.put(req.key.as_bytes().to_vec(), req.value.into_bytes());
        Ok("{}".into())
    }
}

#[derive(Debug)]
pub enum OpenTransactionError {
    DagWriteOpenFailed(dag::Error),
    DbWriteNewFailed(db::NewError),
}

#[derive(Debug)]
pub enum AbortTransactionError {
    UnknownTransactionID,
}

#[derive(Debug)]
pub enum CommitTransactionError {
    UnknownTransactionID,
    DBCommitError(db::CommitError),
}

#[derive(DeJson)]
struct HasRequest {
    transaction_id: u32,
    key: String,
}

#[derive(SerJson)]
struct HasResponse {
    has: bool,
}

#[derive(Debug)]
pub enum HasError {
    InvalidJson(DeJsonErr),
    UnknownTransactionID,
}

#[derive(DeJson)]
struct GetRequest {
    transaction_id: u32,
    key: String,
}

#[derive(SerJson)]
struct GetResponse {
    value: Option<String>,
    has: bool, // Second to avoid trailing comma if value == None.
}
#[derive(Debug)]
pub enum GetError {
    InvalidJson(DeJsonErr),
    UnknownTransactionID,
    InvalidUtf8(std::string::FromUtf8Error),
}
#[derive(DeJson)]
struct PutRequest {
    transaction_id: u32,
    key: String,
    value: String,
}
#[derive(Debug)]
pub enum PutError {
    InvalidJson(DeJsonErr),
    UnknownTransactionID,
}
