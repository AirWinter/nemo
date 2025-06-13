use crate::types::{Incarnation, TxnIndex, Version};
use std::cmp::min;
use std::hint;
use std::mem::take;
use std::ops::Deref;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

#[derive(PartialEq, Debug)]
enum TransactionStatus {
    /// Indicates that the transaction is ready to execute and which incarnation is next.
    ReadyToExecute(Incarnation),
    /// Indicates that the transaction is currently executing this incarnation.
    Executing(Incarnation),
    /// Indicates that execution of this incarnation has completed.
    Executed(Incarnation),
    /// Indicates that the transaction was aborted and which incarnation it was.
    Aborting(Incarnation),
}

/// The different tasks that the scheduler can give for a worker to do.
pub enum SchedulerTask {
    /// Task for executing a specific `Version` of a transaction.
    ExecutionTask(Version),
    /// Task for validating a specific `Version` of a transaction.
    ValidationTask(Version),
    /// Empty task to represent that there is currently no task available.
    NoTask,
    /// Done means that all tasks have been completed and there will be no more tasks for this
    /// epoch.
    Done,
}

/// There is one scheduler shared across all worker threads responsible for giving tasks to the
/// different workers.
pub struct Scheduler {
    /// Shared index that tracks the minimum transaction index that still needs to be executed.
    /// Threads increment this index and attempt to create execution tasks based on it.
    execution_idx: AtomicUsize,
    /// Shared index that tracks the minimum transaction index that still needs to be validated.
    /// Threads increment this index and attempt to create validation tasks based on it.
    validation_idx: AtomicUsize,
    /// The number of times either `execution_idx` or `validation_idx` was decreased.
    decrease_cnt: AtomicUsize,
    /// The number of tasks that are currently in `Executed` state
    executed_cnt: AtomicUsize,
    /// Counts the number of tasks currently being done.
    num_active_tasks: AtomicUsize,
    /// Boolean marker indicating whether all tasks have been completed.
    done_marker: AtomicBool,
    /// Index of last transaction to execute in block, initially block size
    stop_idx: AtomicUsize,
    /// For each transaction store the indexes of transactions that depend on it.
    txn_dependency: Vec<Mutex<Vec<TxnIndex>>>,
    /// For each transaction store the state in which it currently is.
    txn_status: Vec<Mutex<TransactionStatus>>,
    /// Keep track of how many times transaction execution is triggered as this is the most
    /// expensive operation
    num_executions: AtomicUsize,
    /// Keep track of how many times validation is done
    num_validations: AtomicUsize,
}

impl Scheduler {
    pub fn new(num_txns: usize) -> Self {
        Self {
            execution_idx: AtomicUsize::new(0),
            validation_idx: AtomicUsize::new(0),
            decrease_cnt: AtomicUsize::new(0),
            executed_cnt: AtomicUsize::new(0),
            num_active_tasks: AtomicUsize::new(0),
            done_marker: AtomicBool::new(false),
            stop_idx: AtomicUsize::new(num_txns),
            txn_dependency: (0..num_txns).map(|_| Mutex::new(Vec::new())).collect(),
            txn_status: (0..num_txns)
                .map(|_| Mutex::new(TransactionStatus::ReadyToExecute(0)))
                .collect(),
            num_executions: AtomicUsize::new(0),
            num_validations: AtomicUsize::new(0),
        }
    }

    pub fn increment_num_executions(&self) {
        self.num_executions.fetch_add(1, SeqCst);
    }

    pub fn load_num_executions(&self) -> usize {
        self.num_executions.load(SeqCst)
    }

    pub fn load_num_validations(&self) -> usize {
        self.num_validations.load(SeqCst)
    }

    pub fn recover_transaction_status(&self) -> Vec<String> {
        self.txn_status
            .iter()
            .map(|padded_mutex| {
                let lock = padded_mutex.lock().unwrap();
                match lock.deref() {
                    TransactionStatus::ReadyToExecute(_) => String::from("ReadyToExecute"),
                    TransactionStatus::Aborting(_) => String::from("Aborting"),
                    TransactionStatus::Executing(_) => String::from("Executing"),
                    TransactionStatus::Executed(_) => String::from("Executed"),
                }
            })
            .collect()
    }

    /// Decreases the execution index, increases decrease_cnt if it actually decreased.
    fn decrease_execution_idx(&self, target_idx: &TxnIndex) {
        if self.execution_idx.fetch_min(*target_idx, SeqCst) > *target_idx {
            self.decrease_cnt.fetch_add(1, SeqCst);
        }
    }

    /// Checks whether the done marker is set. The marker can only be set by 'check_done'.
    pub fn done(&self) -> bool {
        self.done_marker.load(Ordering::Acquire)
    }

    /// Decreases the validation index, increases decrease_cnt if it actually decreased.
    fn decrease_validation_idx(&self, target_idx: usize) {
        if self.validation_idx.fetch_min(target_idx, SeqCst) > target_idx {
            self.decrease_cnt.fetch_add(1, SeqCst);
        }
    }

    /// Method to check whether all tasks have been completed.
    fn check_done(&self) -> bool {
        let block_size = self.stop_idx.load(SeqCst);
        let observed_cnt = self.decrease_cnt.load(SeqCst);
        let executed_cnt = self.executed_cnt.load(SeqCst);
        let execution_idx = self.execution_idx.load(SeqCst);
        let validation_idx = self.validation_idx.load(SeqCst);
        let num_active_tasks = self.num_active_tasks.load(SeqCst);
        if min(execution_idx, validation_idx) < block_size
            || num_active_tasks > 0
            || executed_cnt != block_size
        {
            // There is work remaining.
            return false;
        }

        if min(
            self.execution_idx.load(SeqCst),
            self.validation_idx.load(SeqCst),
        ) >= block_size
            && self.num_active_tasks.load(SeqCst) == 0
            && self.decrease_cnt.load(SeqCst) == observed_cnt
            && self.executed_cnt.load(SeqCst) == block_size
        {
            self.done_marker.store(true, Ordering::Release);
            true
        } else {
            false
        }
    }

    /// Method to change a transaction's status from `ReadyToExecute` to `Executing`.
    ///
    /// Returns `None` if the transaction's status wasn't `ReadyToExecute` else returns the
    /// `Version` that was marked as `Executing`.
    fn try_incarnate(&self, txn_idx: usize) -> Option<Version> {
        if txn_idx < self.txn_status.len() {
            let mut status = self.txn_status[txn_idx]
                .lock()
                .expect("Can't handle poisoned locks");
            if let TransactionStatus::ReadyToExecute(incarnation) = *status {
                *status = TransactionStatus::Executing(incarnation);
                return Some((txn_idx, incarnation));
            }
        }
        self.num_active_tasks.fetch_sub(1, SeqCst);
        None
    }

    /// Method to get the next version that should be executed.
    ///
    /// Returns `None` if there are no more transactions to execute, else return the next `Version`.
    fn next_version_to_execute(&self) -> Option<Version> {
        if self.execution_idx.load(SeqCst) >= self.stop_idx.load(SeqCst) {
            // Avoid pointlessly spinning, and give priority to other threads that may
            // be working to finish the remaining tasks.
            if !self.check_done() {
                hint::spin_loop();
            }
            return None;
        }
        self.num_active_tasks.fetch_add(1, SeqCst);
        let idx_to_execute = self.execution_idx.fetch_add(1, SeqCst);
        self.try_incarnate(idx_to_execute)
    }

    /// Method to get the next version that should be validated.
    ///
    /// Returns `None` if there are no more transactions to validate, else return the next `Version`.
    fn next_version_to_validate(&self) -> Option<Version> {
        let block_size = self.stop_idx.load(SeqCst);
        if self.validation_idx.load(SeqCst) >= block_size {
            // Avoid pointlessly spinning, and give priority to other threads that may
            // be working to finish the remaining tasks.
            if !self.check_done() {
                hint::spin_loop();
            }
            return None;
        }
        self.num_active_tasks.fetch_add(1, SeqCst);
        let idx_to_validate = self.validation_idx.fetch_add(1, SeqCst); // Fetch and increment
        if idx_to_validate < block_size {
            let status = self.txn_status[idx_to_validate]
                .lock()
                .expect("Can't handle poisoned locks");
            if let TransactionStatus::Executed(incarnation) = *status {
                return Some((idx_to_validate, incarnation));
            }
        }
        self.num_active_tasks.fetch_sub(1, SeqCst);
        None
    }

    /// Method to get the next task a worker should do.
    pub fn next_task(&self) -> SchedulerTask {
        if self.done() {
            // No more tasks.
            return SchedulerTask::Done;
        }

        let validation_idx = self.validation_idx.load(SeqCst);
        let execution_idx = self.execution_idx.load(SeqCst);
        if validation_idx < execution_idx {
            if let Some(version_to_validate) = self.next_version_to_validate() {
                return SchedulerTask::ValidationTask(version_to_validate);
            }
        } else if let Some(version_to_execute) = self.next_version_to_execute() {
            return SchedulerTask::ExecutionTask(version_to_execute);
        }
        SchedulerTask::NoTask
    }

    /// Method to add a dependency if the blocking transaction hasn't been executed yet.
    ///
    /// Returns true if dependency was added and false if it wasn't because the dependency is
    /// already resolved.
    pub fn add_dependency(&self, txn_index: TxnIndex, blocking_txn_idx: TxnIndex) -> bool {
        // If transaction is already executed then dependency has already been resolved so return false
        if let TransactionStatus::Executed(_) = *self.txn_status[blocking_txn_idx]
            .lock()
            .expect("Can't deal with poisoned locks!")
        {
            return false;
        }

        let mut guard_tx = self.txn_status[txn_index]
            .lock()
            .expect("Can't deal with poisoned locks!");
        if let TransactionStatus::Executing(incarnation) = *guard_tx {
            *guard_tx = TransactionStatus::Aborting(incarnation);
        }
        self.txn_dependency[blocking_txn_idx]
            .lock()
            .expect("Can't deal with poisoned locks!")
            .push(txn_index);

        self.num_active_tasks.fetch_sub(1, SeqCst);
        true
    }

    /// Method to change the status of a given transaction from `Aborting` to `ReadyToExecute`.
    fn set_ready_status(&self, txn_index: &TxnIndex) {
        let mut status = self.txn_status[*txn_index]
            .lock()
            .expect("Can't handle poisoned locks");
        if let TransactionStatus::Aborting(incarnation) = *status {
            *status = TransactionStatus::ReadyToExecute(incarnation + 1);
        }
    }

    /// Once a transaction that had dependencies finishes execution, the dependencies are resolved.
    /// This means that dependent transactions can be given the READY status and that the execution
    /// index needs to be decreased.
    fn resume_dependencies(&self, dependent_txn_indices: &Vec<TxnIndex>) {
        for dep_txn_idx in dependent_txn_indices {
            self.set_ready_status(dep_txn_idx)
        }
        if let Some(min_dependency_idx) = dependent_txn_indices.iter().min() {
            self.decrease_execution_idx(min_dependency_idx)
        }
    }

    /// After a transaction is executed then change its status to `EXECUTED`, prepare a validation
    /// task for it and re-validate any dependencies if the write set changed.
    pub fn finish_execution(&self, txn_index: TxnIndex, wrote_new_path: bool) -> SchedulerTask {
        let mut status = self.txn_status[txn_index]
            .lock()
            .expect("Can't handle poisoned locks");
        // Status should have been EXECUTING
        if let TransactionStatus::Executing(incarnation) = *status {
            *status = TransactionStatus::Executed(incarnation);
            self.executed_cnt.fetch_add(1, SeqCst);
            drop(status);
            // Get dependent transactions and reset it to be empty to resolve dependencies
            let deps = {
                take(
                    &mut *self.txn_dependency[txn_index]
                        .lock()
                        .expect("Can't handle poisoned locks"),
                )
            };
            self.resume_dependencies(&deps);
            if self.validation_idx.load(SeqCst) > txn_index {
                // If transaction changed its write set then higher up transactions need to be revalidated
                if wrote_new_path {
                    self.decrease_validation_idx(txn_index);
                } else {
                    return SchedulerTask::ValidationTask((txn_index, incarnation));
                }
            }
            self.num_active_tasks.fetch_sub(1, SeqCst);
        }
        SchedulerTask::NoTask
    }

    /// Abort a transaction because it failed validation, changing its status from `Executed` to
    /// `Aborting`.
    ///
    /// Return true if abort worked and false otherwise.
    pub fn try_validation_abort(&self, txn_index: TxnIndex, incarnation: Incarnation) -> bool {
        let mut status = self.txn_status[txn_index]
            .lock()
            .expect("Can't handle poisoned locks");

        if let TransactionStatus::Executed(existing_incarnation) = *status {
            if existing_incarnation == incarnation {
                *status = TransactionStatus::Aborting(incarnation);
                self.executed_cnt.fetch_sub(1, SeqCst);
                return true;
            }
        }
        false
    }

    /// After validation check if transaction was aborted and thus needs to be re-executed.
    /// Otherwise, just decrease the `num_active_tasks`.
    ///
    /// Returns a `ExecutionTask` if the transaction was aborted and `NoTask` otherwise.
    pub fn finish_validation(&self, txn_index: TxnIndex, aborted: bool) -> SchedulerTask {
        self.num_validations.fetch_add(1, SeqCst);

        if aborted {
            self.set_ready_status(&txn_index); // Increments incarnation
            self.decrease_validation_idx(txn_index + 1);
            if self.execution_idx.load(SeqCst) > txn_index {
                if let Some(new_version) = self.try_incarnate(txn_index) {
                    return SchedulerTask::ExecutionTask(new_version);
                }
            }
        }
        self.num_active_tasks.fetch_sub(1, SeqCst);
        SchedulerTask::NoTask
    }
}
