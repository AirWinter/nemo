use crate::types::{Incarnation, TxnIndex, Version};
use std::cmp::min;
use std::collections::HashSet;
use std::mem::take;
use std::ops::Deref;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release, SeqCst};
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Mutex;

#[derive(PartialEq, Debug)]
/// All possible statuses for each transaction. Each status contains the latest incarnation number.
enum TransactionStatus {
    /// Indicates that the transaction is ready to execute and which incarnation is next.
    ReadyToExecute(Incarnation),
    /// Indicates that the transaction is currently executing this incarnation.
    Executing(Incarnation),
    /// Indicates that execution of this incarnation has completed.
    Executed(Incarnation),
    /// Indicates that the transaction has been validated and won't ever need to be re-validated.
    /// Useful for transactions that don't touch the shared state and can skip validation.
    Validated(Incarnation),
    ReadyToCommit(Incarnation),
    /// Indicates that the transaction was aborted and which incarnation it was.
    Aborting(Incarnation),
    /// Indicates that the transaction depends on another transaction and must wait for that
    /// transaction to pass validation before it can be executed.
    Waiting(Incarnation),
}

/// The different tasks that the scheduler can give for a worker to do.
#[derive(PartialEq, Eq, Debug)]
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
    /// Counts the number of tasks currently being done.
    pub num_active_tasks: AtomicUsize,
    /// Boolean marker indicating whether all tasks have been completed.
    done_marker: AtomicBool,
    /// Number of transactions in the block
    block_size: usize,
    /// For each transaction store the indexes of transactions that depend on it.
    txn_dependency: Vec<Mutex<HashSet<TxnIndex>>>,
    /// For each transaction store the indexes of transactions it depends on. Used for knowing if a
    /// transaction should go into `WAITING`.
    depends_on: Vec<Mutex<HashSet<TxnIndex>>>,
    /// For each transaction store the state in which it currently is.
    txn_status: Vec<Mutex<TransactionStatus>>,
    /// Count number of times execution is simulated. In practice this would be the number of times
    /// the VM is used to execute a transaction.
    num_executions: AtomicUsize,
    /// Count number of validations done.
    num_validations: AtomicUsize,
    /// Count number of times the greedy commit rule is applied
    num_greedy: AtomicUsize,
}

impl Scheduler {
    pub fn new(num_txns: usize) -> Self {
        Self {
            execution_idx: AtomicUsize::new(0),
            validation_idx: AtomicUsize::new(0),
            decrease_cnt: AtomicUsize::new(0),
            num_active_tasks: AtomicUsize::new(0),
            done_marker: AtomicBool::new(false),
            block_size: num_txns,
            txn_dependency: (0..num_txns).map(|_| Mutex::new(HashSet::new())).collect(),
            depends_on: (0..num_txns).map(|_| Mutex::new(HashSet::new())).collect(),
            txn_status: (0..num_txns)
                .map(|_| Mutex::new(TransactionStatus::ReadyToExecute(0)))
                .collect(),
            num_executions: AtomicUsize::new(0),
            num_validations: AtomicUsize::new(0),
            num_greedy: AtomicUsize::new(0),
        }
    }

    pub fn increment_num_executions(&self) {
        self.num_executions.fetch_add(1, Relaxed);
    }

    pub fn load_num_executions(&self) -> usize {
        self.num_executions.load(Relaxed)
    }

    pub fn load_num_validations(&self) -> usize {
        self.num_validations.load(Relaxed)
    }

    pub fn load_num_greedy(&self) -> usize {
        self.num_greedy.load(Relaxed)
    }

    pub fn recover_transaction_status(&self) -> Vec<String> {
        self.txn_status
            .iter()
            .map(|padded_mutex| {
                let lock = padded_mutex.lock().unwrap();
                match lock.deref() {
                    TransactionStatus::ReadyToExecute(_) => String::from("ReadyToExecute"),
                    TransactionStatus::Waiting(_) => String::from("Waiting"),
                    TransactionStatus::Validated(_) => String::from("Validated"),
                    TransactionStatus::ReadyToCommit(_) => String::from("ReadyToCommit"),
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
        self.done_marker.load(Acquire)
    }

    /// Decreases the validation index, increases decrease_cnt if it actually decreased.
    fn decrease_validation_idx(&self, target_idx: usize) {
        if self.validation_idx.fetch_min(target_idx, SeqCst) > target_idx {
            self.decrease_cnt.fetch_add(1, SeqCst);
        }
    }

    /// Method to check whether all tasks have been completed.
    fn check_done(&self) {
        let observed_cnt = self.decrease_cnt.load(SeqCst);
        let execution_idx = self.execution_idx.load(SeqCst);
        let validation_idx = self.validation_idx.load(SeqCst);
        let num_active_tasks = self.num_active_tasks.load(SeqCst);

        if min(execution_idx, validation_idx) >= self.block_size
            && num_active_tasks == 0
            && self.decrease_cnt.load(SeqCst) == observed_cnt
        {
            self.done_marker.store(true, Release);
        }
    }

    /// Method to change a transaction's status from `ReadyToExecute` to `Executing`.
    ///
    /// Returns `None` if the transaction's status wasn't `ReadyToExecute` else returns the
    /// `Version` that was marked as `Executing`.
    fn try_incarnate(&self, txn_idx: usize) -> Option<Version> {
        if txn_idx < self.block_size {
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
        if self.execution_idx.load(SeqCst) >= self.block_size {
            self.check_done();
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
        if self.validation_idx.load(SeqCst) >= self.block_size {
            self.check_done();
            return None;
        }
        self.num_active_tasks.fetch_add(1, SeqCst);
        let idx_to_validate = self.validation_idx.fetch_add(1, SeqCst); // Fetch and increment
        if idx_to_validate < self.block_size {
            let status = self.txn_status[idx_to_validate]
                .lock()
                .expect("Can't handle poisoned locks");
            if let TransactionStatus::Executed(incarnation) = *status {
                return Some((idx_to_validate, incarnation));
            }
            if let TransactionStatus::Validated(incarnation) = *status {
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
    pub fn add_dependencies(&self, txn_index: TxnIndex, blocking_txs: HashSet<TxnIndex>) -> bool {
        let mut dependencies_added = false;

        let mut guard = self.depends_on[txn_index]
            .lock()
            .expect("Can't deal with poisoned locks!");

        for blocking_txn_idx in blocking_txs {
            // If transaction is already validated then dependency has already been resolved
            if let TransactionStatus::Validated(_) = *self.txn_status[blocking_txn_idx]
                .lock()
                .expect("Can't deal with poisoned locks!")
            {
                continue;
            }

            (*self.txn_dependency[blocking_txn_idx]
                .lock()
                .expect("Can't deal with poisoned locks!"))
            .insert(txn_index);
            (*guard).insert(blocking_txn_idx);
            dependencies_added = true;
        }
        drop(guard);

        // If all dependencies are already resolved then return false
        if !dependencies_added {
            return false;
        }

        self.set_waiting_status(txn_index);

        self.num_active_tasks.fetch_sub(1, SeqCst);
        true
    }

    /// Method used for remembering dependencies even when execution succeeded. Different from
    /// `add_dependencies` in that it doesn't set the status and also doesn't affect `num_active_tasks`.
    fn mark_dependencies(&self, txn_index: TxnIndex, depends_on: HashSet<TxnIndex>) {
        for dep_idx in depends_on {
            // If transaction is already validated then dependency has already been resolved
            if let TransactionStatus::Validated(_) = *self.txn_status[dep_idx]
                .lock()
                .expect("Can't deal with poisoned locks")
            {
                continue;
            }
            (*self.txn_dependency[dep_idx]
                .lock()
                .expect("Can't deal with poisoned locks!"))
            .insert(txn_index);
            (*self.depends_on[txn_index]
                .lock()
                .expect("Can't deal with poisoned locks!"))
            .insert(dep_idx);
        }
    }

    /// Method to change the status of a given transaction from either `Waiting(i)` or `Aborting(i)` to `ReadyToExecute(i+1)`.
    fn set_ready_status(&self, txn_index: TxnIndex) {
        let mut status = self.txn_status[txn_index]
            .lock()
            .expect("Can't handle poisoned locks");
        if let TransactionStatus::Aborting(i) = *status {
            *status = TransactionStatus::ReadyToExecute(i + 1);
        }
        if let TransactionStatus::Waiting(i) = *status {
            *status = TransactionStatus::ReadyToExecute(i + 1);
        }
    }

    /// Method to change the status of a given transaction from either `ReadyToExecute` or `Executing` to `ReadyToExecute`
    fn set_waiting_status(&self, txn_index: TxnIndex) {
        let mut status = self.txn_status[txn_index]
            .lock()
            .expect("Can't handle poisoned locks");
        if let TransactionStatus::ReadyToExecute(i) = *status {
            *status = TransactionStatus::Waiting(i);
        }
        if let TransactionStatus::Executing(i) = *status {
            *status = TransactionStatus::Waiting(i);
        }
        if let TransactionStatus::Aborting(i) = *status {
            *status = TransactionStatus::Waiting(i);
        }
    }

    /// Method to change the status of a given transaction from `Executed` to `Validated`
    fn set_validated_status(&self, txn_index: TxnIndex) {
        let mut status = self.txn_status[txn_index]
            .lock()
            .expect("Can't handle poisoned locks");
        if let TransactionStatus::Executed(i) = *status {
            *status = TransactionStatus::Validated(i);
        }
    }

    /// Once a transaction that had dependencies finishes execution, the dependencies are resolved.
    /// This means that dependent transactions can be given the READY status and that the execution
    /// index needs to be decreased.
    fn resume_dependencies(&self, txn_index: TxnIndex) {
        let mut min_dependency_idx: Option<TxnIndex> = None;
        let dependent_txn_indices = {
            take(
                &mut *self.txn_dependency[txn_index]
                    .lock()
                    .expect("Can't handle poisoned locks"),
            )
        };

        // extra param: dependency: &TxnIndex,
        for dep_txn_idx in dependent_txn_indices {
            let mut guard_depends_on = self.depends_on[dep_txn_idx]
                .lock()
                .expect("Can't deal with poisoned locks!");

            (*guard_depends_on).remove(&txn_index);
            let is_empty = (*guard_depends_on).is_empty();
            drop(guard_depends_on);
            // Transaction is only ready to execute if all dependencies are resolved
            if is_empty {
                // Only set ready status if WAITING or ABORTING
                self.set_ready_status(dep_txn_idx);

                // Update min dependency index
                match min_dependency_idx {
                    None => min_dependency_idx = Some(dep_txn_idx),
                    Some(min_idx) if min_idx > dep_txn_idx => {
                        min_dependency_idx = Some(dep_txn_idx)
                    }
                    _ => {}
                }
            }
        }

        if let Some(min_idx) = min_dependency_idx {
            self.decrease_execution_idx(&min_idx);
        }
    }

    /// After a transaction is executed then change its status to `EXECUTED`, prepare a validation
    /// task for it and re-validate any dependencies if the write set changed.
    pub fn finish_execution(
        &self,
        txn_index: TxnIndex,
        wrote_new_path: bool,
        touches_shared_state: bool,
        depends_on: HashSet<TxnIndex>,
    ) -> SchedulerTask {
        let mut status = self.txn_status[txn_index]
            .lock()
            .expect("Can't handle poisoned locks");
        // Status should have been EXECUTING
        if let TransactionStatus::Executing(incarnation) = *status {
            // If transaction doesn't touch shared state then doesn't need to be validated. Change
            // the status so that next_version_to_validate doesn't create a validation task.
            if !touches_shared_state {
                *status = TransactionStatus::ReadyToCommit(incarnation);
                self.num_active_tasks.fetch_sub(1, SeqCst);
                self.num_greedy.fetch_add(1, Relaxed);
                return SchedulerTask::NoTask;
            }
            *status = TransactionStatus::Executed(incarnation);
            drop(status);

            // Remember the transactions we depend on
            self.mark_dependencies(txn_index, depends_on);

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
                return true;
            }
        }

        if let TransactionStatus::Validated(existing_incarnation) = *status {
            if existing_incarnation == incarnation {
                *status = TransactionStatus::Aborting(incarnation);
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
        self.num_validations.fetch_add(1, Relaxed);

        if aborted {
            // Mark dependencies improvement toggleable
            let is_empty = self.depends_on[txn_index]
                .lock()
                .expect("Can't deal with poisoned locks!")
                .is_empty();
            if is_empty {
                self.set_ready_status(txn_index); // Increments incarnation
            } else {
                self.set_waiting_status(txn_index);
            }

            // Revalidate all higher transactions
            self.decrease_validation_idx(txn_index + 1);
            if self.execution_idx.load(SeqCst) > txn_index {
                if let Some(new_version) = self.try_incarnate(txn_index) {
                    return SchedulerTask::ExecutionTask(new_version);
                }
                // If try incarnate fails it will do -1 to num_active tasks so return here in order
                // to avoid doing it a second time below.
                return SchedulerTask::NoTask;
            }
        } else {
            // Change status to Validated
            self.set_validated_status(txn_index);
            // Get dependent transactions and reset it to be empty to resolve dependencies
            self.resume_dependencies(txn_index);
        }

        self.num_active_tasks.fetch_sub(1, SeqCst);
        SchedulerTask::NoTask
    }
}
