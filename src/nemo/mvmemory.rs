use crate::types::{ObjectId, TxnIndex, Version};
use arc_swap::ArcSwapOption;
use dashmap::DashMap;
use std::collections::{BTreeMap, HashSet};
use std::ops::Deref;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

/// Reading from MVMemory results in either `OK`, `NOT_FOUND` or `READ_ERROR`
#[derive(PartialEq)]
pub(crate) enum ReadStatus {
    /// Successful read gives the `Version` read
    OK(Version),
    /// Happens when can't find any entries for the object id that was read. In the full
    /// Block-STM implementation this would indicate that we need to read from storage.
    NotFound,
    /// Happens when the version read was marked as `ESTIMATE` which indicates that there is a
    /// dependency. This is useful to identify dependencies before triggering execution.
    ReadError(TxnIndex),
}

/// Entry in MVMemory.\
/// **NOTE:** Because we're only simulating execution we don't need to store the actual value that
/// would be used during execution.
struct WriteCell {
    /// Flag to mark entry as `ESTIMATE` to denote dependencies
    estimate: AtomicBool,
    /// Version associated with the entry
    version: Version,
}

/// Shared memory data structure from/to which transactions read/write.
pub(crate) struct MVMemory {
    /// For each object store all transactions that touch it, along with their entry.
    data: DashMap<ObjectId, BTreeMap<TxnIndex, WriteCell>>,
    /// Store the last objects a transaction wrote to. Used to identify if an incarnation wrote to
    /// new objects, which would mean higher transactions need to be re-validated.
    last_write_set: Vec<ArcSwapOption<HashSet<ObjectId>>>,
    /// Store teh last objects a transaction read from. Used to validate that the values read are
    /// still up to date.
    last_read_set: Vec<ArcSwapOption<HashSet<(ObjectId, Option<Version>)>>>,
}

impl MVMemory {
    pub fn new(num_txs: usize) -> MVMemory {
        MVMemory {
            data: DashMap::new(),
            last_write_set: (0..=num_txs).map(|_| ArcSwapOption::empty()).collect(),
            last_read_set: (0..=num_txs).map(|_| ArcSwapOption::empty()).collect(),
        }
    }

    /// Method to store the write-set of incarnation in MVMemory.
    fn apply_write_set(&self, version: Version, write_set: &HashSet<ObjectId>) {
        // Paper has write_set = set of (location, value)
        for object_id in write_set {
            let mut map = self.data.entry(*object_id).or_default();
            map.insert(
                version.0,
                WriteCell {
                    estimate: AtomicBool::new(false),
                    version,
                },
            );
        }
    }

    /// Method to update the `last_write_set` associated with a transaction.
    fn rcu_update_written_locations(
        &self,
        txn_index: TxnIndex,
        new_objects: HashSet<ObjectId>,
    ) -> bool {
        let prev_objects = match self.last_write_set[txn_index].load_full() {
            Some(prev_objects_ptr) => prev_objects_ptr.deref().clone(),
            None => HashSet::new(),
        };
        for object_id in prev_objects.difference(&new_objects) {
            let mut map = self.data.get_mut(object_id).expect("Path must exist");
            map.remove(&txn_index);
        }

        let diff: HashSet<ObjectId> = new_objects.difference(&prev_objects).cloned().collect();
        self.last_write_set[txn_index].store(Some(Arc::new(new_objects)));
        !diff.is_empty()
    }

    /// Method to record the `read_set` and `write_set` of a transaction, updating the
    /// `last_read_set` and `last_write_set`.
    ///
    /// Returns a bool indicating whether the transaction wrote to new locations as compared to
    /// previous incarnations, if true it means that higher transactions need to be validated
    /// again.
    pub fn record(
        &self,
        version: Version,
        read_set: HashSet<(ObjectId, Option<Version>)>,
        write_set: HashSet<ObjectId>,
    ) -> bool {
        let (txn_index, _) = version;
        self.apply_write_set(version, &write_set);
        // NOTE: Don't need to extract locations from write_set as pseudocode does since we don't store the values written
        let wrote_new_locations = self.rcu_update_written_locations(txn_index, write_set);
        self.last_read_set[txn_index].store(Some(Arc::new(read_set)));

        wrote_new_locations
    }

    /// Method to mark all writes as `ESTIMATE`. Called when a transaction fails validation, and is
    /// used to represent that these values are uncertain. A transaction that reads an entry marked
    /// as `ESTIMATE` is then not executed in order to avoid likely re-execution.
    pub fn convert_writes_to_estimates(&self, txn_index: TxnIndex) {
        let prev_objects = self.last_write_set[txn_index].load_full().unwrap();
        for object_id in &*prev_objects {
            self.data
                .get_mut(object_id)
                .expect("Path must exist!")
                .get_mut(&txn_index)
                .expect("Path must exist!")
                .estimate
                .store(true, SeqCst);
        }
    }

    /// Method to read the latest available version of an object.
    ///
    /// Returns a `ReadStatus` where `NOT_FOUND` indicates that there is no entry for that object,
    /// `READ_ERROR` indicates that the latest entry is marked as `ESTIMATE` and `OK` means that the
    /// read succeeded also containing the version read.
    pub fn read(&self, object_id: &ObjectId, txn_index: TxnIndex) -> ReadStatus {
        if !self.data.contains_key(object_id) {
            return ReadStatus::NotFound;
        }
        let map = self.data.get(object_id).expect("Object id should exist!");
        let s: Vec<&WriteCell> = map
            .iter()
            .filter(|e| e.0 < &txn_index)
            .map(|e| e.1)
            .collect();

        if s.is_empty() {
            return ReadStatus::NotFound;
        }
        let write_cell = s.last().expect("Shouldn't be empty!");
        if write_cell.estimate.load(SeqCst) {
            return ReadStatus::ReadError(write_cell.version.0); // Return READ_ERROR(blocking_txn_idx)
        }
        // If this were real execution we would also have the read value here
        ReadStatus::OK(write_cell.version)
    }

    /// Method to validate that the read set of a transaction is still up to date. Does so by
    /// comparing the latest version of each object read and checking that it is still the same as
    /// when the transaction was executed.
    ///
    /// Returns true if read set is still up to date, else false.
    pub fn validate_read_set(&self, txn_index: TxnIndex) -> bool {
        let prior_reads = match self.last_read_set[txn_index].load_full() {
            Some(ptr) => ptr.deref().clone(),
            None => HashSet::new(),
        };
        for (object_id, old_version) in prior_reads.iter() {
            // Check that we're still reading the same version
            match self.read(object_id, txn_index) {
                ReadStatus::ReadError(_) => {
                    return false;
                }
                ReadStatus::NotFound if old_version.is_some() => {
                    return false;
                }
                ReadStatus::OK(new_version) => {
                    if old_version.is_none() {
                        return false;
                    }
                    if new_version != old_version.unwrap() {
                        return false;
                    }
                }
                _ => {}
            }
        }
        true
    }
}
