use super::{patch, ChangedKeysError, PullError, PushError};
use crate::{
    dag,
    db::{self, ChangedKeysMap},
    prolly,
    to_native::ToNativeValue,
};
use serde::{Deserialize, Serialize};
use wasm_bindgen::JsValue;

#[derive(Serialize, Deserialize)]
#[cfg_attr(test, derive(Clone, Debug, PartialEq))]
pub struct HttpRequestInfo {
    #[serde(rename = "httpStatusCode")]
    pub http_status_code: u16,
    #[serde(rename = "errorMessage")]
    pub error_message: String,
}

#[derive(Deserialize, Serialize)]
pub struct MaybeEndTryPullRequest {
    #[serde(rename = "requestID")]
    pub request_id: String,
    #[serde(rename = "syncHead")]
    pub sync_head: String,
}

// If replay_mutations is empty then there are no pending mutations to replay
// and pull is complete. If replay_mutations is not empty the returned mutations
// should be replayed and maybeEndPull invoked again.
#[derive(Debug, Serialize)]
pub struct MaybeEndTryPullResponse {
    #[serde(rename = "replayMutations")]
    pub replay_mutations: Vec<ReplayMutation>,
    #[serde(rename = "syncHead")]
    pub sync_head: String,
    // The changed_keys will only be filled if there are no replay_mutations. If
    // there are still pending mutations to replay the JS bindings does not care
    // about the changed_keys. It only cares about the changed_keys when the
    // pull operation is complete and at that point it wants the changed keys
    // between the state before and after the pull.
    #[serde(rename = "changedKeys")]
    pub changed_keys: ChangedKeysMap,
}

// ReplayMutation is returned in the MaybeEndPushResponse, not be confused with
// sync::push::Mutation, which is used in batch push.
#[derive(Debug, Serialize)]
pub struct ReplayMutation {
    pub id: u64,
    pub name: String,
    pub args: String,
    pub original: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BeginTryPullRequest {
    #[serde(rename = "pullURL")]
    pub pull_url: String,
    #[serde(rename = "pullAuth")]
    pub pull_auth: String,
    #[serde(rename = "schemaVersion")]
    pub schema_version: String,
}

#[derive(Serialize)]
#[cfg_attr(test, derive(Debug))]
pub struct BeginTryPullResponse {
    #[serde(rename = "httpRequestInfo")]
    pub http_request_info: HttpRequestInfo,
    #[serde(rename = "syncHead")]
    pub sync_head: String,
    #[serde(rename = "requestID")]
    pub request_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TryPushRequest {
    #[serde(rename = "pushURL")]
    pub push_url: String,
    #[serde(rename = "pushAuth")]
    pub push_auth: String,
    #[serde(rename = "schemaVersion")]
    pub schema_version: String,
}

#[derive(Serialize)]
pub struct TryPushResponse {
    #[serde(rename = "httpRequestInfo")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_request_info: Option<HttpRequestInfo>,
}

#[derive(Debug)]
pub enum TryPushError {
    GetHeadError(dag::Error),
    InternalGetPendingCommitsError(db::WalkChainError),
    InternalNoMainHeadError,
    InternalNonLocalPendingCommit,
    InvalidPusher(JsValue),
    PushFailed(PushError),
    ReadError(dag::Error),
}

impl ToNativeValue<JsValue> for TryPushError {
    fn to_native(&self) -> Option<&JsValue> {
        match self {
            TryPushError::GetHeadError(e) => e.to_native(),
            TryPushError::InternalGetPendingCommitsError(e) => e.to_native(),
            TryPushError::InternalNoMainHeadError => None,
            TryPushError::InternalNonLocalPendingCommit => None,
            TryPushError::PushFailed(e) => e.to_native(),
            TryPushError::ReadError(e) => e.to_native(),
            TryPushError::InvalidPusher(v) => Some(v),
        }
    }
}

#[derive(Debug)]
pub enum BeginTryPullError {
    CommitError(db::CommitError),
    GetHeadError(dag::Error),
    InternalGetChainError(db::WalkChainError),
    InternalInvalidChainError,
    InternalNoMainHeadError,
    InternalProgrammerError(db::InternalProgrammerError),
    InternalRebuildIndexError(db::CreateIndexError),
    InvalidBaseSnapshotCookie(serde_json::error::Error),
    InvalidPuller(JsValue),
    LockError(dag::Error),
    MainHeadDisappeared,
    NoBaseSnapshot(db::BaseSnapshotError),
    OverlappingSyncsJSLogInfo, // "JSLogInfo" is a signal to bindings to not log this alarmingly.
    PatchFailed(patch::PatchError),
    PullFailed(PullError),
    ReadCommitError(db::ReadCommitError),
    ReadError(dag::Error),
    TimeTravelProhibited(String),
}

impl ToNativeValue<JsValue> for BeginTryPullError {
    fn to_native(&self) -> Option<&JsValue> {
        match self {
            BeginTryPullError::CommitError(e) => e.to_native(),
            BeginTryPullError::GetHeadError(e) => e.to_native(),
            BeginTryPullError::InternalGetChainError(e) => e.to_native(),
            BeginTryPullError::InternalInvalidChainError => None,
            BeginTryPullError::InternalNoMainHeadError => None,
            BeginTryPullError::InternalProgrammerError(e) => e.to_native(),
            BeginTryPullError::InternalRebuildIndexError(e) => e.to_native(),
            BeginTryPullError::InvalidBaseSnapshotCookie(_) => None,
            BeginTryPullError::LockError(e) => e.to_native(),
            BeginTryPullError::MainHeadDisappeared => None,
            BeginTryPullError::NoBaseSnapshot(e) => e.to_native(),
            BeginTryPullError::OverlappingSyncsJSLogInfo => None,
            BeginTryPullError::PatchFailed(e) => e.to_native(),
            BeginTryPullError::PullFailed(e) => e.to_native(),
            BeginTryPullError::ReadCommitError(e) => e.to_native(),
            BeginTryPullError::ReadError(e) => e.to_native(),
            BeginTryPullError::TimeTravelProhibited(_) => None,
            BeginTryPullError::InvalidPuller(v) => Some(v),
        }
    }
}

#[derive(Debug)]
pub enum MaybeEndTryPullError {
    ChangedKeysError(ChangedKeysError),
    CommitError(dag::Error),
    GetMainHeadError(dag::Error),
    GetSyncHeadError(dag::Error),
    InternalArgsUtf8Error(std::string::FromUtf8Error),
    InternalProgrammerError(String),
    InvalidUtf8(std::string::FromUtf8Error),
    LoadHeadError(prolly::LoadError),
    LoadSyncHeadError(db::FromHashError),
    MissingMainHead,
    MissingSyncHead,
    NoBaseSnapshot(db::BaseSnapshotError),
    OpenWriteTxWriteError(dag::Error),
    OverlappingSyncsJSLogInfo, // "JSLogInfo" is a signal to bindings to not log this alarmingly.
    PendingError(db::WalkChainError),
    ReadCommitError(db::ReadCommitError),
    SyncSnapshotWithNoBasis,
    WriteDefaultHeadError(dag::Error),
    WriteSyncHeadError(dag::Error),
    WrongSyncHeadJSLogInfo, // "JSLogInfo" is a signal to bindings to not log this alarmingly.
}

impl ToNativeValue<JsValue> for MaybeEndTryPullError {
    fn to_native(&self) -> Option<&JsValue> {
        match self {
            MaybeEndTryPullError::ChangedKeysError(e) => e.to_native(),
            MaybeEndTryPullError::CommitError(e) => e.to_native(),
            MaybeEndTryPullError::GetMainHeadError(e) => e.to_native(),
            MaybeEndTryPullError::GetSyncHeadError(e) => e.to_native(),
            MaybeEndTryPullError::InternalArgsUtf8Error(_) => None,
            MaybeEndTryPullError::InternalProgrammerError(_) => None,
            MaybeEndTryPullError::InvalidUtf8(_) => None,
            MaybeEndTryPullError::LoadHeadError(e) => e.to_native(),
            MaybeEndTryPullError::LoadSyncHeadError(e) => e.to_native(),
            MaybeEndTryPullError::MissingMainHead => None,
            MaybeEndTryPullError::MissingSyncHead => None,
            MaybeEndTryPullError::NoBaseSnapshot(e) => e.to_native(),
            MaybeEndTryPullError::OpenWriteTxWriteError(e) => e.to_native(),
            MaybeEndTryPullError::OverlappingSyncsJSLogInfo => None,
            MaybeEndTryPullError::PendingError(e) => e.to_native(),
            MaybeEndTryPullError::ReadCommitError(e) => e.to_native(),
            MaybeEndTryPullError::SyncSnapshotWithNoBasis => None,
            MaybeEndTryPullError::WriteDefaultHeadError(e) => e.to_native(),
            MaybeEndTryPullError::WriteSyncHeadError(e) => e.to_native(),
            MaybeEndTryPullError::WrongSyncHeadJSLogInfo => None,
        }
    }
}
