use crate::nemo_no_pq::mvmemory::{MVMemory, ReadStatus};
use crate::nemo_no_pq::scheduler::{Scheduler, SchedulerTask};
use crate::transaction::Transaction;
use crate::types::{ObjectId, TxnIndex, Version};
use std::collections::HashSet;
use std::thread::sleep;
use std::time::Duration;

/// Each thread continuously loop for as long as all tasks are not completed.
fn run(block: &Vec<Transaction>, scheduler: &Scheduler, shared_memory: &MVMemory) {
    let mut scheduler_task = SchedulerTask::NoTask;
    loop {
        match scheduler_task {
            SchedulerTask::ExecutionTask(version) => {
                scheduler_task = try_execute(version, block, scheduler, shared_memory)
            }
            SchedulerTask::ValidationTask(version) => {
                scheduler_task = needs_reexecution(version, scheduler, shared_memory);
            }
            SchedulerTask::NoTask => scheduler_task = scheduler.next_task(),
            SchedulerTask::Done => {
                break;
            }
        }
    }
}

/// Try to execute specific incarnation of a transaction. If there is a dependency then the
/// transaction is not executed
fn try_execute(
    version: Version,
    block: &Vec<Transaction>,
    scheduler: &Scheduler,
    shared_memory: &MVMemory,
) -> SchedulerTask {
    let (txn_index, _) = version;
    let txn = &block[txn_index];
    let vm_result = execute(txn_index, block, shared_memory, scheduler);

    match vm_result {
        (Some(blocking_txn_idxs), _, _) => {
            // If the dependencies have already been resolved
            if !scheduler.add_dependencies(txn_index, blocking_txn_idxs) {
                return try_execute(version, block, scheduler, shared_memory);
            }
            SchedulerTask::NoTask
        }
        (None, read_set, write_set) => {
            let depends_on: HashSet<TxnIndex> = read_set
                .clone()
                .into_iter()
                .filter_map(|(_, version)| version.map(|v| v.0))
                .collect();

            let wrote_new_location = shared_memory.record(version, read_set, write_set);
            scheduler.finish_execution(
                txn_index,
                wrote_new_location,
                txn.touches_shared_state,
                depends_on,
            )
        }
    }
}

/// Determine whether a transaction incarnation needs to be re-executed. Validation consists of
/// verifying that the read set is still up to date. If it isn't then the transaction is aborted
/// creating a new incarnation that needs to be executed.
fn needs_reexecution(
    version: Version,
    scheduler: &Scheduler,
    shared_memory: &MVMemory,
) -> SchedulerTask {
    let (txn_index, incarnation) = version;
    let read_set_valid = shared_memory.validate_read_set(txn_index);
    let aborted = !read_set_valid && scheduler.try_validation_abort(txn_index, incarnation);
    if aborted {
        shared_memory.convert_writes_to_estimates(txn_index);
    }
    scheduler.finish_validation(txn_index, aborted)
}

/// Function to simulate execution, which replaces the `execute` function from the VM module. If
/// this were to do real execution then it would use a VM.
fn execute(
    txn_index: TxnIndex,
    block: &[Transaction],
    shared_memory: &MVMemory,
    scheduler: &Scheduler,
) -> (
    Option<HashSet<TxnIndex>>,
    HashSet<(ObjectId, Option<Version>)>,
    HashSet<ObjectId>,
) {
    let txn = &block[txn_index];

    // Simulate execution
    let execution_duration = Duration::from_millis(txn.duration_ms.unwrap_or(20) as u64);
    sleep(execution_duration);
    scheduler.increment_num_executions();

    let mut read_set: HashSet<(ObjectId, Option<Version>)> = HashSet::new();
    let mut write_set: HashSet<ObjectId> = HashSet::new();
    let mut blocking_txs: Option<HashSet<TxnIndex>> = None;

    for a in &txn.accessed_shared_objects {
        if a.is_read() {
            let result = shared_memory.read(&a.object_id, txn_index);

            match result {
                ReadStatus::ReadError(blocking_txn_index) => {
                    if let Some(set) = &mut blocking_txs {
                        set.insert(blocking_txn_index);
                    } else {
                        blocking_txs = Some(HashSet::from_iter([blocking_txn_index]))
                    }
                }
                ReadStatus::OK(version) => {
                    read_set.insert((a.object_id, Some(version)));
                }
                ReadStatus::NotFound => {
                    read_set.insert((a.object_id, None));
                }
            }
        }
        if a.is_write() {
            write_set.insert(a.object_id);
        }
    }

    (blocking_txs, read_set, write_set)
}

/// Task that every thread runs.
pub(crate) fn worker_job(
    scheduler: &Scheduler,
    shared_memory: &MVMemory,
    block: &Vec<Transaction>,
) {
    run(block, scheduler, shared_memory);
}
