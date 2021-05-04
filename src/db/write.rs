use super::index::GetMapError;
use super::{commit, index, read, scan, ReadCommitError, Whence};
use crate::dag;
use crate::prolly;
use crate::util::rlog;
use std::{collections::HashMap, string::FromUtf8Error};
use str_macro::str;

#[allow(dead_code)]
enum Meta {
    IndexChange(IndexChangeMeta),
    Local(LocalMeta),
    Snapshot(SnapshotMeta),
}

struct IndexChangeMeta {
    last_mutation_id: u64,
}

struct LocalMeta {
    mutator_name: String,
    mutator_args: String,
    mutation_id: u64,
    original_hash: Option<String>,
}

struct SnapshotMeta {
    last_mutation_id: u64,
    cookie: serde_json::Value,
}

pub struct Write<'a> {
    dag_write: dag::Write<'a>,
    map: prolly::Map,
    basis: Option<commit::Commit>,
    meta: Meta,
    indexes: HashMap<String, index::Index>,
}

#[derive(Debug)]
pub enum InitDBError {
    CommitError(CommitError),
}

// Return value is the hash of the commit.
#[allow(dead_code)]
pub async fn init_db(dag_write: dag::Write<'_>, head_name: &str) -> Result<String, InitDBError> {
    use InitDBError::*;
    let w = Write {
        dag_write,
        map: prolly::Map::new(),
        basis: None,
        meta: Meta::Snapshot(SnapshotMeta {
            last_mutation_id: 0,
            cookie: serde_json::Value::default(), // Value::Null()
        }),
        indexes: HashMap::new(),
    };
    w.commit(head_name).await.map_err(CommitError)
}

// The ChangedKeysMap is used to describe a map of changed keys. The key in the
// map is the name of the index. The primary index uses `""` in this map. The
// value of the map is the keys that changed in the last pull/mutations.
pub type ChangedKeysMap = HashMap<String, Vec<Vec<u8>>>;

pub type ChangedKeysMapRpc = HashMap<String, Vec<String>>;

pub fn changed_keys_map_to_rpc(map: ChangedKeysMap) -> Result<ChangedKeysMapRpc, FromUtf8Error> {
    map.into_iter()
        .map(|(k, v)| {
            let v = v
                .into_iter()
                .map(|v| String::from_utf8(v))
                .collect::<Result<_, _>>()?;
            Ok((k, v))
        })
        .collect::<Result<_, _>>()
}

#[allow(dead_code)]
impl<'a> Write<'a> {
    pub async fn new_local(
        whence: Whence,
        mutator_name: String,
        mutator_args: String,
        original_hash: Option<String>,
        dag_write: dag::Write<'a>,
    ) -> Result<Write<'a>, ReadCommitError> {
        let (_, basis, map) = read::read_commit(whence, &dag_write.read()).await?;
        let mutation_id = basis.next_mutation_id();
        let indexes = read::read_indexes(&basis);
        Ok(Write {
            basis: basis.into(),
            dag_write,
            map,
            meta: Meta::Local(LocalMeta {
                mutator_name,
                mutator_args,
                mutation_id,
                original_hash,
            }),
            indexes,
        })
    }

    pub async fn new_snapshot(
        whence: Whence,
        last_mutation_id: u64,
        cookie: serde_json::Value,
        dag_write: dag::Write<'a>,
        indexes: HashMap<String, index::Index>,
    ) -> Result<Write<'a>, ReadCommitError> {
        let (_, basis, map) = read::read_commit(whence, &dag_write.read()).await?;
        Ok(Write {
            basis: basis.into(),
            dag_write,
            map,
            meta: Meta::Snapshot(SnapshotMeta {
                last_mutation_id,
                cookie,
            }),
            indexes,
        })
    }

    pub async fn new_index_change(
        whence: Whence,
        dag_write: dag::Write<'a>,
    ) -> Result<Write<'a>, ReadCommitError> {
        let (_, basis, map) = read::read_commit(whence, &dag_write.read()).await?;
        let last_mutation_id = basis.mutation_id();
        let indexes = read::read_indexes(&basis);
        Ok(Write {
            basis: basis.into(),
            dag_write,
            map,
            meta: Meta::IndexChange(IndexChangeMeta { last_mutation_id }),
            indexes,
        })
    }

    pub fn as_read(&'a self) -> super::Read<'a> {
        super::Read::new(self.dag_write.read(), &self.map, &self.indexes)
    }

    pub fn is_rebase(&self) -> bool {
        match &self.meta {
            Meta::Local(lm) => lm.original_hash.is_some(),
            _ => false,
        }
    }

    pub async fn put(
        &mut self,
        lc: rlog::LogContext,
        key: Vec<u8>,
        val: Vec<u8>,
    ) -> Result<(), PutError> {
        use PutError::*;
        match &self.meta {
            Meta::Local(_) | Meta::Snapshot(_) => {}
            _ => return Err(NotAllowed),
        }

        let old_val = self.map.get(&key);
        if let Some(old_val) = old_val {
            Self::update_indexes(
                lc.clone(),
                &self.indexes,
                &self.dag_write,
                index::IndexOperation::Remove,
                &key,
                &old_val,
            )
            .await
            .map_err(RemoveOldIndexEntriesError)?;
        }
        Self::update_indexes(
            lc,
            &self.indexes,
            &self.dag_write,
            index::IndexOperation::Add,
            &key,
            &val,
        )
        .await
        .map_err(AddNewIndexEntriesError)?;
        self.map.put(key, val);
        Ok(())
    }

    pub async fn del(&mut self, lc: rlog::LogContext, key: Vec<u8>) -> Result<(), DelError> {
        use DelError::*;
        match &self.meta {
            Meta::Local(_) | Meta::Snapshot(_) => {}
            _ => return Err(NotAllowed),
        }

        let old_val = self.map.get(&key);
        match old_val {
            None => {}
            Some(old_val) => {
                Self::update_indexes(
                    lc,
                    &self.indexes,
                    &self.dag_write,
                    index::IndexOperation::Remove,
                    &key,
                    &old_val,
                )
                .await
                .map_err(UpdateIndexesError)?;
            }
        };
        self.map.del(key);
        Ok(())
    }

    async fn update_indexes(
        lc: rlog::LogContext,
        indexes: &HashMap<String, index::Index>,
        dag_write: &dag::Write<'a>,
        op: index::IndexOperation,
        key: &[u8],
        val: &[u8],
    ) -> Result<(), UpdateIndexesError> {
        use UpdateIndexesError::*;
        for idx in indexes.values() {
            if key.starts_with(&idx.meta.definition.key_prefix) {
                let mut guard = idx
                    .get_map_mut(&dag_write.read())
                    .await
                    .map_err(GetMapError)?;
                // TODO: use outer guard to avoid unwrap. But it doesn't work.
                // See comment in that struct.
                let map = guard.guard.as_mut().unwrap();
                // Right now all the errors that index_value() returns are customers dev
                // problems: either the value is not json, the pointer is into nowhere, etc.
                // So we ignore them.
                index::index_value(map, op, key, val, &idx.meta.definition.json_pointer)
                    .unwrap_or_else(|e| {
                        info!(
                            lc,
                            "Not indexing value '{:?}': {:?}",
                            String::from_utf8(val.into()).unwrap_or_else(|_| str!("<unparsable>")),
                            e
                        )
                    });
            }
        }
        Ok(())
    }

    pub async fn clear(&mut self) -> Result<(), ClearError> {
        use ClearError::*;
        match &self.meta {
            Meta::Local(_) | Meta::Snapshot(_) => {}
            _ => return Err(NotAllowed),
        }

        self.map = prolly::Map::new();
        for (_, idx) in self.indexes.iter() {
            let mut guard = idx
                .get_map_mut(&self.dag_write.read())
                .await
                .map_err(GetMapError)?
                .guard;
            *guard = Some(prolly::Map::new());
        }
        Ok(())
    }

    pub async fn create_index(
        &mut self,
        lc: rlog::LogContext,
        name: String,
        key_prefix: &[u8],
        json_pointer: &str,
    ) -> Result<(), CreateIndexError> {
        use CreateIndexError::*;
        match &self.meta {
            // Snapshots also need to manipulate indexes.
            Meta::Snapshot(_) | Meta::IndexChange(_) => {}
            _ => return Err(NotAllowed),
        }

        let definition = commit::IndexDefinition {
            name: name.clone(),
            key_prefix: key_prefix.to_vec(),
            json_pointer: json_pointer.to_string(),
        };

        // Check to see if the index already exists.
        if let Some(index) = self.indexes.get(&name) {
            if index.meta.definition == definition {
                return Ok(());
            }
            return Err(IndexExistsWithDifferentDefinition);
        }

        let mut index_map = prolly::Map::new();
        for entry in scan::scan_raw(
            &self.map,
            scan::ScanOptionsInternal {
                prefix: Some(key_prefix.into()),
                limit: None,
                start_key: None,
                index_name: None,
            },
        ) {
            // All the index_value errors because of customer-supplied data: malformed
            // json, json path pointing to nowhere, etc. We ignore them.
            index::index_value(
                &mut index_map,
                index::IndexOperation::Add,
                entry.key,
                entry.val,
                json_pointer,
            )
            .unwrap_or_else(|e| {
                info!(
                    lc,
                    "Not indexing value '{:?}': {:?}",
                    String::from_utf8(entry.val.to_vec()).unwrap_or_else(|_| str!("<unparsable>")),
                    e
                );
            });
        }

        self.indexes.insert(
            name,
            index::Index::new(
                commit::IndexRecord {
                    definition,
                    value_hash: str!(""),
                },
                Some(index_map),
            ),
        );

        Ok(())
    }

    pub async fn drop_index(&mut self, name: &str) -> Result<(), DropIndexError> {
        use DropIndexError::*;
        match &self.meta {
            // Snapshots also need to manipulate indexes.
            Meta::Snapshot(_) | Meta::IndexChange(_) => {}
            _ => return Err(NotAllowed),
        }

        match self.indexes.remove(name) {
            None => Err(NoSuchIndexError(name.to_string())),
            Some(_) => Ok(()),
        }
    }

    // Return value is the hash of the new commit.
    pub async fn commit(self, head_name: &str) -> Result<String, CommitError> {
        self.commit_with_changed_keys(head_name, false)
            .await
            .map(|(hash, _)| hash)
    }

    // Return value is the hash of the new commit and the diff compared to before the commit.
    pub async fn commit_with_changed_keys(
        mut self,
        head_name: &str,
        generate_changed_keys: bool,
    ) -> Result<(String, ChangedKeysMap), CommitError> {
        use CommitError::*;
        let value_changed_keys = if generate_changed_keys {
            self.map.pending_changed_keys()
        } else {
            Vec::new()
        };
        let value_hash = self
            .map
            .flush(&mut self.dag_write)
            .await
            .map_err(FlushError)?;
        let mut index_metas = Vec::new();
        let mut key_changes = ChangedKeysMap::new();
        if !value_changed_keys.is_empty() {
            key_changes.insert(str!(""), value_changed_keys);
        }
        for (name, index) in self.indexes.into_iter() {
            {
                let guard = index
                    .get_map(&self.dag_write.read())
                    .await
                    .map_err(GetMapError)?;
                let map = guard.get_map();
                let index_changed_keys = map.pending_changed_keys();
                if !index_changed_keys.is_empty() {
                    key_changes.insert(name, index_changed_keys);
                }
            }
            let value_hash = index
                .flush(&mut self.dag_write)
                .await
                .map_err(IndexFlushError)?;
            let index::Index { mut meta, .. } = index;
            meta.value_hash = value_hash;
            index_metas.push(meta);
        }
        let basis_hash = self.basis.as_ref().map(|b| b.chunk().hash().to_string());
        let commit = match &self.meta {
            Meta::Local(meta) => {
                let LocalMeta {
                    mutation_id,
                    mutator_name,
                    mutator_args,
                    original_hash,
                } = meta;

                commit::Commit::new_local(
                    basis_hash.as_deref(),
                    *mutation_id,
                    mutator_name,
                    mutator_args.as_bytes(),
                    original_hash.as_deref(),
                    &value_hash,
                    &index_metas,
                )
            }

            Meta::Snapshot(meta) => {
                let SnapshotMeta {
                    last_mutation_id,
                    cookie,
                } = meta;

                commit::Commit::new_snapshot(
                    basis_hash.as_deref(),
                    *last_mutation_id,
                    &serde_json::to_vec(cookie).map_err(SerializeCookieError)?,
                    &value_hash,
                    &index_metas,
                )
            }

            Meta::IndexChange(meta) => {
                let IndexChangeMeta { last_mutation_id } = meta;

                if let Some(basis) = &self.basis {
                    if &basis.mutation_id() != last_mutation_id {
                        return Err(IndexChangeMustNotChangeMutationID);
                    }
                    if basis.value_hash() != value_hash {
                        return Err(IndexChangeMustNotChangeValueHash);
                    }
                }

                commit::Commit::new_index_change(
                    basis_hash.as_deref(),
                    *last_mutation_id,
                    &value_hash,
                    &index_metas,
                )
            }
        };

        // TODO: Below two writes can be done in parallel
        self.dag_write
            .put_chunk(commit.chunk())
            .await
            .map_err(DagPutChunkError)?;
        self.dag_write
            .set_head(head_name, Some(commit.chunk().hash()))
            .await
            .map_err(DagSetHeadError)?;

        self.dag_write.commit().await.map_err(DagCommitError)?;

        Ok((commit.chunk().hash().to_string(), key_changes))
    }
}

#[derive(Debug, PartialEq)]
pub enum CreateIndexError {
    FlushError(prolly::FlushError),
    IndexError((String, Vec<u8>, String, index::IndexValueError)),
    IndexExistsWithDifferentDefinition,
    NotAllowed,
}

#[derive(Debug, PartialEq)]
pub enum DropIndexError {
    NoSuchIndexError(String),
    NotAllowed,
}

#[derive(Debug)]
pub enum CommitError {
    DagPutChunkError(dag::Error),
    DagSetHeadError(dag::Error),
    DagCommitError(dag::Error),
    FlushError(prolly::FlushError),
    GetMapError(GetMapError),
    IndexChangeMustNotChangeMutationID,
    IndexChangeMustNotChangeValueHash,
    IndexFlushError(index::IndexFlushError),
    SerializeArgsError(serde_json::error::Error),
    SerializeCookieError(serde_json::error::Error),
}

#[derive(Debug, PartialEq)]
pub enum PutError {
    AddNewIndexEntriesError(UpdateIndexesError),
    NotAllowed,
    RemoveOldIndexEntriesError(UpdateIndexesError),
}

#[derive(Debug, PartialEq)]
pub enum DelError {
    NotAllowed,
    UpdateIndexesError(UpdateIndexesError),
}

#[derive(Debug, PartialEq)]
pub enum UpdateIndexesError {
    GetMapError(index::GetMapError),
    IndexValueError(index::IndexValueError),
}

#[derive(Debug)]
pub enum ClearError {
    GetMapError(index::GetMapError),
    NotAllowed,
}

#[cfg(test)]
mod tests {
    use super::super::index;
    use super::super::read;
    use super::*;
    use crate::dag;
    use crate::db;
    use crate::kv::memstore::MemStore;
    use crate::util::rlog::LogContext;
    use serde_json::json;

    #[async_std::test]
    async fn basics() {
        let ds = dag::Store::new(Box::new(MemStore::new()));
        init_db(
            ds.write(LogContext::new()).await.unwrap(),
            db::DEFAULT_HEAD_NAME,
        )
        .await
        .unwrap();

        // Put.
        let mut w = Write::new_local(
            Whence::Head(str!(db::DEFAULT_HEAD_NAME)),
            str!("mutator_name"),
            serde_json::Value::Array(vec![]).to_string(),
            None,
            ds.write(LogContext::new()).await.unwrap(),
        )
        .await
        .unwrap();
        w.put(rlog::LogContext::new(), b"foo".to_vec(), b"bar".to_vec())
            .await
            .unwrap();
        // Assert we can read the same value from within this transaction.
        let r = w.as_read();
        let val = r.get(b"foo");
        assert_eq!(Some(&(b"bar"[..])), val);
        w.commit(db::DEFAULT_HEAD_NAME).await.unwrap();

        // As well as after it has committed.
        let w = Write::new_local(
            Whence::Head(str!(db::DEFAULT_HEAD_NAME)),
            str!("mutator_name"),
            serde_json::Value::Null.to_string(),
            None,
            ds.write(LogContext::new()).await.unwrap(),
        )
        .await
        .unwrap();
        let r = w.as_read();
        let val = r.get(b"foo");
        assert_eq!(Some(&(b"bar"[..])), val);
        drop(w);

        // Del.
        let mut w = Write::new_local(
            Whence::Head(str!(db::DEFAULT_HEAD_NAME)),
            str!("mutator_name"),
            serde_json::Value::Array(vec![]).to_string(),
            None,
            ds.write(LogContext::new()).await.unwrap(),
        )
        .await
        .unwrap();
        w.del(rlog::LogContext::new(), b"foo".to_vec())
            .await
            .unwrap();
        // Assert it is gone while still within this transaction.
        let r = w.as_read();
        let val = r.get(b"foo");
        assert!(val.is_none());
        w.commit(db::DEFAULT_HEAD_NAME).await.unwrap();

        // As well as after it has committed.
        let w = Write::new_local(
            Whence::Head(str!(db::DEFAULT_HEAD_NAME)),
            str!("mutator_name"),
            serde_json::Value::Null.to_string(),
            None,
            ds.write(LogContext::new()).await.unwrap(),
        )
        .await
        .unwrap();
        let r = w.as_read();
        let val = r.get(b"foo");
        assert!(val.is_none());
    }

    #[async_std::test]
    async fn index_commit_type_constraints() {
        let ds = dag::Store::new(Box::new(MemStore::new()));
        init_db(
            ds.write(LogContext::new()).await.unwrap(),
            db::DEFAULT_HEAD_NAME,
        )
        .await
        .unwrap();

        // Test that local changes cannot create or drop an index.
        let mut w = Write::new_local(
            Whence::Head(str!(db::DEFAULT_HEAD_NAME)),
            str!("mutator_name"),
            serde_json::Value::Array(vec![]).to_string(),
            None,
            ds.write(LogContext::new()).await.unwrap(),
        )
        .await
        .unwrap();
        let got_err = w
            .create_index(
                rlog::LogContext::new(),
                str!("foo"),
                &str!("").into_bytes(),
                "",
            )
            .await
            .unwrap_err();
        assert_eq!(CreateIndexError::NotAllowed, got_err);
        let got_err = w.drop_index("foo").await.unwrap_err();
        assert_eq!(DropIndexError::NotAllowed, got_err);
        drop(w);

        // Test that snapshot changes CAN create or drop an index. This is needed for sync,
        // which rebuilds indexes from scratch.
        let mut w = Write::new_snapshot(
            Whence::Head(str!(db::DEFAULT_HEAD_NAME)),
            1,
            json!("cookie"),
            ds.write(LogContext::new()).await.unwrap(),
            HashMap::new(),
        )
        .await
        .unwrap();
        w.create_index(
            rlog::LogContext::new(),
            str!("foo"),
            &str!("").into_bytes(),
            "",
        )
        .await
        .unwrap();
        w.drop_index("foo").await.unwrap();
        drop(w);

        // Test that index changes cannot put or del.
        let mut w = Write::new_index_change(
            Whence::Head(str!(db::DEFAULT_HEAD_NAME)),
            ds.write(LogContext::new()).await.unwrap(),
        )
        .await
        .unwrap();
        let got_err = w
            .put(rlog::LogContext::new(), vec![], vec![])
            .await
            .unwrap_err();
        assert_eq!(PutError::NotAllowed, got_err);
        let got_err = w.del(rlog::LogContext::new(), vec![]).await.unwrap_err();
        assert_eq!(DelError::NotAllowed, got_err);
        drop(w);

        // Test that index changes cannot change the last mutation id or value hash.
        let mut w = Write::new_index_change(
            Whence::Head(str!(db::DEFAULT_HEAD_NAME)),
            ds.write(LogContext::new()).await.unwrap(),
        )
        .await
        .unwrap();
        match &mut w.meta {
            Meta::IndexChange(ic) => ic.last_mutation_id = 1000,
            _ => assert!(false),
        }
        let got_err = w.commit("some head").await.unwrap_err();
        // Compare as a string because we can't make derive PartialEq for CommitError
        // (it wraps serde errors that are not PartialEq).
        assert_eq!(
            "IndexChangeMustNotChangeMutationID",
            format!("{:?}", got_err)
        );
        let mut w = Write::new_index_change(
            Whence::Head(str!(db::DEFAULT_HEAD_NAME)),
            ds.write(LogContext::new()).await.unwrap(),
        )
        .await
        .unwrap();
        let m = &mut w.map;
        m.put(vec![0x01, 0x02], vec![0x03]);
        drop(m);
        let got_err = w.commit("some head").await.unwrap_err();
        assert_eq!(
            "IndexChangeMustNotChangeValueHash",
            format!("{:?}", got_err)
        );
    }

    #[async_std::test]
    async fn test_clear() {
        let lc = rlog::LogContext::new();
        let ds = dag::Store::new(Box::new(MemStore::new()));
        init_db(
            ds.write(LogContext::new()).await.unwrap(),
            db::DEFAULT_HEAD_NAME,
        )
        .await
        .unwrap();
        let mut w = Write::new_local(
            Whence::Head(str!(db::DEFAULT_HEAD_NAME)),
            str!("mutator_name"),
            serde_json::Value::Array(vec![]).to_string(),
            None,
            ds.write(LogContext::new()).await.unwrap(),
        )
        .await
        .unwrap();
        w.put(lc.clone(), b"foo".to_vec(), b"\"bar\"".to_vec())
            .await
            .unwrap();
        w.commit(db::DEFAULT_HEAD_NAME).await.unwrap();
        let mut w = Write::new_index_change(
            Whence::Head(str!(db::DEFAULT_HEAD_NAME)),
            ds.write(LogContext::new()).await.unwrap(),
        )
        .await
        .unwrap();
        w.create_index(rlog::LogContext::new(), str!("idx"), b"", "")
            .await
            .unwrap();
        w.commit(db::DEFAULT_HEAD_NAME).await.unwrap();

        w = Write::new_local(
            Whence::Head(str!(db::DEFAULT_HEAD_NAME)),
            str!("mutator_name"),
            serde_json::Value::Array(vec![]).to_string(),
            None,
            ds.write(LogContext::new()).await.unwrap(),
        )
        .await
        .unwrap();
        w.put(lc.clone(), b"hot".to_vec(), b"\"dog\"".to_vec())
            .await
            .unwrap();
        assert_eq!(w.map.iter().count(), 2);
        assert_eq!(
            (&w.indexes["idx"])
                .get_map(&w.dag_write.read())
                .await
                .unwrap()
                .get_map()
                .iter()
                .count(),
            2
        );
        w.clear().await.unwrap();
        assert_eq!(w.map.iter().count(), 0);
        assert_eq!(
            (&w.indexes["idx"])
                .get_map(&w.dag_write.read())
                .await
                .unwrap()
                .get_map()
                .iter()
                .count(),
            0
        );
        w.commit(db::DEFAULT_HEAD_NAME).await.unwrap();

        let owned_read = ds.read(LogContext::new()).await.unwrap();
        let (_, c, m) = read::read_commit(
            Whence::Head(str!(db::DEFAULT_HEAD_NAME)),
            &owned_read.read(),
        )
        .await
        .unwrap();
        let indexes = read::read_indexes(&c);
        assert_eq!(0, m.iter().count());
        assert_eq!(
            (&indexes["idx"])
                .get_map(&owned_read.read())
                .await
                .unwrap()
                .get_map()
                .iter()
                .count(),
            0
        );
    }

    #[async_std::test]
    async fn test_create_and_drop_index() {
        async fn test(write_before_indexing: bool) {
            let ds = dag::Store::new(Box::new(MemStore::new()));
            init_db(
                ds.write(LogContext::new()).await.unwrap(),
                db::DEFAULT_HEAD_NAME,
            )
            .await
            .unwrap();

            if write_before_indexing {
                let mut w = Write::new_local(
                    Whence::Head(str!(db::DEFAULT_HEAD_NAME)),
                    str!("mutator_name"),
                    serde_json::Value::Array(vec![]).to_string(),
                    None,
                    ds.write(LogContext::new()).await.unwrap(),
                )
                .await
                .unwrap();
                for i in 0..3 {
                    w.put(
                        rlog::LogContext::new(),
                        format!("k{}", i).as_bytes().to_vec(),
                        json!({ "s": format!("s{}", i) })
                            .to_string()
                            .as_bytes()
                            .to_vec(),
                    )
                    .await
                    .unwrap();
                }
                w.commit(db::DEFAULT_HEAD_NAME).await.unwrap();
            }

            let mut w = Write::new_index_change(
                Whence::Head(str!(db::DEFAULT_HEAD_NAME)),
                ds.write(LogContext::new()).await.unwrap(),
            )
            .await
            .unwrap();
            let index_name = "i1";
            w.create_index(rlog::LogContext::new(), index_name.to_string(), b"", "/s")
                .await
                .unwrap();
            w.commit(db::DEFAULT_HEAD_NAME).await.unwrap();

            if !write_before_indexing {
                let mut w = Write::new_local(
                    Whence::Head(str!(db::DEFAULT_HEAD_NAME)),
                    str!("mutator_name"),
                    serde_json::Value::Array(vec![]).to_string(),
                    None,
                    ds.write(LogContext::new()).await.unwrap(),
                )
                .await
                .unwrap();
                for i in 0..3 {
                    w.put(
                        rlog::LogContext::new(),
                        format!("k{}", i).as_bytes().to_vec(),
                        json!({ "s": format!("s{}", i) })
                            .to_string()
                            .as_bytes()
                            .to_vec(),
                    )
                    .await
                    .unwrap();
                }
                w.commit(db::DEFAULT_HEAD_NAME).await.unwrap();
            }

            let owned_read = ds.read(LogContext::new()).await.unwrap();
            let (_, c, _) = read::read_commit(
                Whence::Head(str!(db::DEFAULT_HEAD_NAME)),
                &owned_read.read(),
            )
            .await
            .unwrap();
            let indexes = c.indexes();
            assert_eq!(indexes.len(), 1);
            let idx = &indexes[0];
            assert_eq!(idx.definition.name, index_name);
            assert!(idx.definition.key_prefix.is_empty());
            assert_eq!(idx.definition.json_pointer, "/s");
            let idx_map = prolly::Map::load(&idx.value_hash, &owned_read.read())
                .await
                .unwrap();
            let entries = idx_map.iter().collect::<Vec<prolly::Entry>>();
            assert_eq!(entries.len(), 3);
            for i in 0..3 {
                assert_eq!(
                    entries.get(i).unwrap().key,
                    index::encode_index_key(&index::IndexKey {
                        secondary: format!("s{}", i).as_bytes(),
                        primary: format!("k{}", i).as_bytes(),
                    })
                    .unwrap()
                    .as_slice()
                );
            }
            drop(owned_read);

            // Ensure drop works.
            w = Write::new_index_change(
                Whence::Head(str!(db::DEFAULT_HEAD_NAME)),
                ds.write(LogContext::new()).await.unwrap(),
            )
            .await
            .unwrap();
            w.drop_index(index_name).await.unwrap();
            w.commit(db::DEFAULT_HEAD_NAME).await.unwrap();
            let owned_read = ds.read(LogContext::new()).await.unwrap();
            let (_, c, _) = read::read_commit(
                Whence::Head(str!(db::DEFAULT_HEAD_NAME)),
                &owned_read.read(),
            )
            .await
            .unwrap();
            let indexes = c.indexes();
            assert_eq!(indexes.len(), 0);
        }

        test(true).await;
        test(false).await;
    }
}
