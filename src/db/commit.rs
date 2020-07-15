use super::commit_generated::commit;
use flatbuffers::FlatBufferBuilder;

// Commit is a thin wrapper around the Commit flatbuffer that makes it
// easier to read and write them. Commit::load() does validation
// so that users don't have to worry about missing fields.
pub struct Commit {
    buffer: (Vec<u8>, usize),
}

#[allow(dead_code)]
impl Commit {
    #![allow(clippy::too_many_arguments)]
    pub fn new_local(
        local_create_date: &str,
        basis_hash: &str,
        checksum: &str,
        mutation_id: u64,
        mutator_name: &str,
        mutator_args_json: &[u8],
        original_hash: Option<&str>,
        value_hash: &str,
    ) -> Commit {
        let mut builder = FlatBufferBuilder::default();
        let local_meta_args = &commit::LocalMetaArgs {
            mutation_id,
            mutator_name: builder.create_string(mutator_name).into(),
            mutator_args_json: builder.create_vector(mutator_args_json).into(),
            original_hash: original_hash.map(|h| builder.create_string(h)),
        };
        let local_meta = commit::LocalMeta::create(&mut builder, local_meta_args);
        Commit::new_impl(
            builder,
            local_create_date,
            basis_hash,
            checksum,
            commit::MetaTyped::SnapshotMeta,
            local_meta.as_union_value(),
            value_hash,
        )
    }

    pub fn new_snapshot(
        local_create_date: &str,
        basis_hash: &str,
        checksum: &str,
        last_mutation_id: u64,
        server_state_id: &str,
        value_hash: &str,
    ) -> Commit {
        let mut builder = FlatBufferBuilder::default();
        let snapshot_meta_args = &commit::SnapshotMetaArgs {
            last_mutation_id,
            server_state_id: builder.create_string(server_state_id).into(),
        };
        let snapshot_meta = commit::SnapshotMeta::create(&mut builder, snapshot_meta_args);
        Commit::new_impl(
            builder,
            local_create_date,
            basis_hash,
            checksum,
            commit::MetaTyped::SnapshotMeta,
            snapshot_meta.as_union_value(),
            value_hash,
        )
    }

    pub fn load(buffer: Vec<u8>) -> Result<Commit, LoadError> {
        Commit::validate(&buffer)?;
        Ok(Commit {
            buffer: (buffer, 0),
        })
    }

    pub fn meta(&self) -> Meta {
        Meta {
            fb: self.commit().meta().unwrap(),
        }
    }

    pub fn value_hash(&self) -> &str {
        self.commit().value_hash().unwrap()
    }

    fn validate(buffer: &[u8]) -> Result<(), LoadError> {
        let root = commit::get_root_as_commit(buffer);
        root.value_hash().ok_or(LoadError::MissingValueHash)?;

        let meta = root.meta().ok_or(LoadError::MissingMeta)?;
        meta.local_create_date()
            .ok_or(LoadError::MissingLocalCreateDate)?;
        meta.basis_hash().ok_or(LoadError::MissingBasisHash)?;
        meta.checksum().ok_or(LoadError::MissingChecksum)?;

        match meta.typed_type() {
            commit::MetaTyped::LocalMeta => Commit::validate_local_meta(
                meta.typed_as_local_meta().ok_or(LoadError::MissingTyped)?,
            ),
            commit::MetaTyped::SnapshotMeta => Commit::validate_snapshot_meta(
                meta.typed_as_snapshot_meta()
                    .ok_or(LoadError::MissingTyped)?,
            ),
            _ => Err(LoadError::UnknownMetaType),
        }
    }

    fn validate_local_meta(local_meta: commit::LocalMeta) -> Result<(), LoadError> {
        let mid = local_meta.mutation_id();
        if mid == 0 {
            return Err(LoadError::MissingMutationID);
        }
        local_meta
            .mutator_name()
            .ok_or(LoadError::MissingMutatorName)?;
        local_meta
            .mutator_args_json()
            .ok_or(LoadError::MissingMutatorArgsJSON)?;
        local_meta
            .original_hash()
            .ok_or(LoadError::MissingOriginalHash)?;
        Ok(())
    }

    fn validate_snapshot_meta(snapshot_meta: commit::SnapshotMeta) -> Result<(), LoadError> {
        let mid = snapshot_meta.last_mutation_id();
        if mid == 0 {
            return Err(LoadError::MissingLastMutationID);
        }
        snapshot_meta
            .server_state_id()
            .ok_or(LoadError::MissingServerStateID)?;
        Ok(())
    }

    fn commit(&self) -> commit::Commit {
        commit::get_root_as_commit(&self.buffer.0[self.buffer.1..])
    }

    fn new_impl(
        mut builder: FlatBufferBuilder,
        local_create_date: &str,
        basis_hash: &str,
        checksum: &str,
        union_type: commit::MetaTyped,
        union_value: flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>,
        value_hash: &str,
    ) -> Commit {
        let meta_args = &commit::MetaArgs {
            local_create_date: builder.create_string(local_create_date).into(),
            basis_hash: builder.create_string(basis_hash).into(),
            checksum: builder.create_string(checksum).into(),
            typed_type: union_type,
            typed: union_value.into(),
        };
        let meta = commit::Meta::create(&mut builder, meta_args);
        let commit_args = &commit::CommitArgs {
            meta: meta.into(),
            value_hash: builder.create_string(value_hash).into(),
        };
        let commit = commit::Commit::create(&mut builder, commit_args);
        builder.finish(commit, None);
        Commit {
            buffer: builder.collapse(),
        }
    }
}

pub struct Meta<'a> {
    fb: commit::Meta<'a>,
}

#[allow(dead_code)]
impl<'a> Meta<'a> {
    pub fn local_create_date(&self) -> &str {
        self.fb.local_create_date().unwrap()
    }

    pub fn basis_hash(&self) -> &str {
        self.fb.basis_hash().unwrap()
    }
    pub fn checksum(&self) -> &str {
        self.fb.checksum().unwrap()
    }

    pub fn typed(&self) -> MetaTyped {
        match self.fb.typed_type() {
            commit::MetaTyped::LocalMeta => MetaTyped::Local(LocalMeta {
                fb: self.fb.typed_as_local_meta().unwrap(),
            }),
            commit::MetaTyped::SnapshotMeta => MetaTyped::Snapshot(SnapshotMeta {
                fb: self.fb.typed_as_snapshot_meta().unwrap(),
            }),
            commit::MetaTyped::NONE => panic!("notreached"),
        }
    }
}

pub enum MetaTyped<'a> {
    Local(LocalMeta<'a>),
    Snapshot(SnapshotMeta<'a>),
}

pub struct LocalMeta<'a> {
    fb: commit::LocalMeta<'a>,
}

#[allow(dead_code)]
impl<'a> LocalMeta<'a> {
    pub fn mutation_id(&self) -> u64 {
        self.fb.mutation_id()
    }

    pub fn mutator_name(&self) -> &str {
        self.fb.mutator_name().unwrap()
    }

    pub fn mutator_args_json(&self) -> &[u8] {
        self.fb.mutator_args_json().unwrap()
    }

    pub fn original_hash(&self) -> Option<&str> {
        // original_hash is legitimately optional, it's only present if the
        // local commit was rebased.
        self.fb.original_hash()
    }
}

pub struct SnapshotMeta<'a> {
    fb: commit::SnapshotMeta<'a>,
}

#[allow(dead_code)]
impl<'a> SnapshotMeta<'a> {
    pub fn last_mutation_id(&self) -> u64 {
        self.fb.last_mutation_id()
    }

    pub fn server_state_id(&self) -> &str {
        self.fb.server_state_id().unwrap()
    }
}

pub enum LoadError {
    MissingMutatorName,
    MissingMutatorArgsJSON,
    MissingMutationID,
    MissingOriginalHash,
    MissingLastMutationID,
    MissingServerStateID,
    MissingLocalCreateDate,
    MissingBasisHash,
    MissingChecksum,
    UnknownMetaType,
    MissingTyped,
    MissingMeta,
    MissingValueHash,
}
