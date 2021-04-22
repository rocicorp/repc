use async_std::sync::RwLock;

use super::ScanOptions;
use super::{
    commit::{Commit, FromHashError},
    index,
};
use crate::dag;
use crate::prolly;
use std::{
    collections::{hash_map::HashMap, BTreeSet},
    sync::Arc,
};
use std::{convert::TryInto, string::FromUtf8Error};

#[derive(Debug)]
pub enum Whence {
    Head(String),
    #[allow(dead_code)]
    Hash(String),
}

pub enum SubscriptionInnerError {
    InvalidUtf8(std::string::FromUtf8Error),
}

pub struct SubscriptionInner {
    key_set: BTreeSet<Vec<u8>>,
    scans: Vec<ScanOptions>,
}

impl SubscriptionInner {
    pub fn new() -> Self {
        SubscriptionInner {
            key_set: BTreeSet::new(),
            scans: Vec::new(),
        }
    }

    pub fn record_get(&mut self, key: Vec<u8>) {
        self.key_set.insert(key);
    }

    pub fn record_has(&mut self, key: Vec<u8>) {
        self.key_set.insert(key);
    }

    pub fn record_scan(&mut self, scan_options: ScanOptions) {
        self.scans.push(scan_options);
    }

    // TODO(arv): Just make the error String::FromUtf8Error
    pub fn touched_keys(&self) -> Result<Vec<String>, SubscriptionInnerError> {
        // TODO: Consider draining the vec. We might not need this data more
        // than once depending on usage.
        use SubscriptionInnerError::*;

        self.key_set
            .iter()
            .map(|b| String::from_utf8(b.to_vec()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(InvalidUtf8)
    }

    pub fn touched_scans(&self) -> Vec<ScanOptions> {
        // TODO: Consider draining the vec. We might not need this data more
        // than once depending on usage.
        self.scans.clone()
    }
}

pub type Subscription = Arc<RwLock<SubscriptionInner>>;

pub struct OwnedRead<'a> {
    dag_read: dag::OwnedRead<'a>,
    map: prolly::Map,
    indexes: HashMap<String, index::Index>,
    pub subscription: Option<Subscription>,
}

#[derive(Debug)]
pub enum ReadCommitError {
    CommitFromHeadError(FromHashError),
    GetHeadError(dag::Error),
    MapLoadError(prolly::LoadError),
    UnknownHead(String),
}

impl<'a> OwnedRead<'a> {
    pub async fn from_whence(
        whence: Whence,
        dag_read: dag::OwnedRead<'a>,
        subscription: Option<Subscription>,
    ) -> Result<OwnedRead<'a>, ReadCommitError> {
        let (_, basis, map) = read_commit(whence, &dag_read.read()).await?;
        let indexes = read_indexes(&basis);
        Ok(OwnedRead {
            dag_read,
            map,
            indexes,
            subscription: subscription.clone(),
        })
    }

    pub fn as_read(&'a self) -> Read<'a> {
        Read::new(
            self.dag_read.read(),
            &self.map,
            &self.indexes,
            self.subscription.clone(),
        )
    }
}

pub async fn read_commit(
    whence: Whence,
    read: &dag::Read<'_>,
) -> Result<(String, Commit, prolly::Map), ReadCommitError> {
    use ReadCommitError::*;
    let hash = match whence {
        Whence::Hash(s) => s,
        Whence::Head(s) => read
            .get_head(&s)
            .await
            .map_err(GetHeadError)?
            .ok_or_else(|| UnknownHead(s.to_string()))?,
    };
    let commit = Commit::from_hash(&hash, read)
        .await
        .map_err(CommitFromHeadError)?;
    let map = prolly::Map::load(commit.value_hash(), read)
        .await
        .map_err(MapLoadError)?;
    Ok((hash, commit, map))
}

pub fn read_indexes(commit: &Commit) -> HashMap<String, index::Index> {
    commit
        .indexes()
        .iter()
        .map(|meta| {
            (
                meta.definition.name.clone(),
                index::Index::new(meta.clone(), None),
            )
        })
        .collect()
}

pub struct Read<'a> {
    #[allow(dead_code)]
    dag_read: dag::Read<'a>,
    map: &'a prolly::Map,
    indexes: &'a HashMap<String, index::Index>,
    subscription: Option<Subscription>,
}

impl<'a> Read<'a> {
    pub fn new(
        dag_read: dag::Read<'a>,
        map: &'a prolly::Map,
        indexes: &'a HashMap<String, index::Index>,
        subscription: Option<Subscription>,
    ) -> Read<'a> {
        Read {
            dag_read,
            map,
            indexes,
            subscription,
        }
    }

    pub async fn has(&self, key: &[u8]) -> bool {
        if let Some(sub) = self.subscription.clone() {
            let mut subscription = sub.write().await;
            subscription.record_has(key.to_vec());
        }
        self.map.has(key)
    }

    pub async fn get(&self, key: &[u8]) -> Option<&[u8]> {
        if let Some(sub) = self.subscription.clone() {
            let mut subscription = sub.write().await;
            subscription.record_get(key.to_vec());
        }
        self.map.get(key)
    }

    pub async fn scan(
        &'a self,
        opts: super::ScanOptions,
        callback: impl Fn(super::scan::ScanResult<'_>),
    ) -> Result<(), ScanError> {
        use ScanError::*;

        if let Some(subscription) = self.subscription.clone() {
            let mut subscription = subscription.write().await;
            subscription.record_scan(opts.clone());
        }

        let opts_internal: super::scan::ScanOptionsInternal =
            opts.try_into().map_err(ScanError::ScanOptionsError)?;

        match &opts_internal.index_name {
            Some(name) => {
                let idx = self
                    .indexes
                    .get(name)
                    .ok_or_else(|| UnknownIndexName(name.to_string()))?;
                let guard = idx.get_map(&self.dag_read).await.map_err(GetMapError)?;
                super::scan::scan(guard.get_map(), opts_internal).for_each(callback)
            }
            None => super::scan::scan(&self.map, opts_internal).for_each(callback),
        };
        Ok(())
    }
}

impl From<SubscriptionInnerError> for ScanError {
    fn from(e: SubscriptionInnerError) -> Self {
        match e {
            SubscriptionInnerError::InvalidUtf8(e) => ScanError::InvalidUtf8(e),
        }
    }
}

#[derive(Debug)]
pub enum ScanError {
    GetMapError(index::GetMapError),
    InvalidUtf8(FromUtf8Error),
    ScanOptionsError(super::scan::ScanOptionsError),
    UnknownIndexName(String),
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use super::*;
    use crate::dag;
    use crate::db;
    use crate::db::write::init_db;
    use crate::kv::memstore::MemStore;
    use crate::util::rlog::LogContext;
    use str_macro::str;

    #[async_std::test]
    async fn basics() {
        let ds = dag::Store::new(Box::new(MemStore::new()));
        init_db(
            ds.write(LogContext::new()).await.unwrap(),
            db::DEFAULT_HEAD_NAME,
        )
        .await
        .unwrap();
        let mut w = write::Write::new_local(
            Whence::Head(str!(db::DEFAULT_HEAD_NAME)),
            str!("mutator_name"),
            serde_json::Value::Array(vec![]).to_string(),
            None,
            ds.write(LogContext::new()).await.unwrap(),
        )
        .await
        .unwrap();
        w.put(
            LogContext::new(),
            "foo".as_bytes().to_vec(),
            "bar".as_bytes().to_vec(),
        )
        .await
        .unwrap();
        w.commit(db::DEFAULT_HEAD_NAME, false).await.unwrap();

        let dr = ds.read(LogContext::new()).await.unwrap();
        let r = OwnedRead::from_whence(Whence::Head(str!(db::DEFAULT_HEAD_NAME)), dr, None)
            .await
            .unwrap();
        let rr = r.as_read();
        let val = rr.get("foo".as_bytes()).await;
        assert_eq!(Some("bar".as_bytes()), val);
    }
}
