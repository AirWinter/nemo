use crate::nemo_no_pq::executor::worker_job;
use crate::nemo_no_pq::mvmemory::{MVMemory, ReadStatus};
use crate::nemo_no_pq::scheduler::Scheduler;
use crate::transaction::Transaction;
use crate::types::{ExecutionResult, ObjectId, TxnIndex, Version};
use crossbeam::thread;
use std::collections::HashSet;
use std::sync::atomic::Ordering::SeqCst;
use std::time::Instant;

/// Implementation for our paper without the priority scheduling.
pub struct NemoNoPq {
    number_of_workers: usize,
}

impl NemoNoPq {
    pub fn new(number_of_workers: usize) -> Self {
        NemoNoPq { number_of_workers }
    }

    pub fn execute_transactions(&self, block: Vec<Transaction>) -> ExecutionResult {
        let num_txns = block.len();
        let shared_memory = MVMemory::new(num_txns);
        let scheduler = Scheduler::new(num_txns);

        // ====================== Start Duration ======================
        let start = Instant::now();

        // Preprocess transactions to identify if they have any dependencies
        for (txn_index, txn) in block.iter().enumerate() {
            let mut read_set: HashSet<(ObjectId, Option<Version>)> = HashSet::new();
            let mut write_set: HashSet<ObjectId> = HashSet::new();
            let mut ready: bool = true;
            let mut blocking_txs: HashSet<TxnIndex> = HashSet::new();

            for object_access in &txn.prior_knowledge {
                if object_access.is_write() {
                    write_set.insert(object_access.object_id);
                }

                if object_access.is_read() {
                    read_set.insert((object_access.object_id, None));
                    if let ReadStatus::ReadError(blocking_txn_id) =
                        shared_memory.read(&object_access.object_id, txn_index)
                    {
                        ready = false;
                        blocking_txs.insert(blocking_txn_id);
                    }
                }
            }
            // Make sure to mark entries as ESTIMATE so that higher transactions can identify dependencies
            shared_memory.record((txn_index, 0), read_set, write_set);
            shared_memory.convert_writes_to_estimates(txn_index);

            // If transaction isn't ready to execute then put it in Waiting + remember dependencies
            if !ready {
                scheduler.add_dependencies(txn_index, blocking_txs);
            }
        }
        scheduler.num_active_tasks.store(0, SeqCst);

        // For each thread run worker job
        thread::scope(|s| {
            for _ in 1..=self.number_of_workers {
                let scheduler_ref = &scheduler;
                let shared_memory_ref = &shared_memory;
                let block_ref = &block;

                s.spawn(move |_| {
                    worker_job(scheduler_ref, shared_memory_ref, block_ref);
                });
            }
        })
        .unwrap();

        // ====================== End Duration ======================
        let duration = start.elapsed();

        let num_executions = scheduler.load_num_executions();
        let num_validations = scheduler.load_num_validations();
        let num_greedy = scheduler.load_num_greedy();

        // ====================== Verify Results ======================
        // Verify that all statuses are either `Validated` or `ReadyToCommit`
        let transaction_status = scheduler.recover_transaction_status();
        let incorrect_statuses: Vec<(TxnIndex, String)> = transaction_status
            .clone()
            .iter()
            .enumerate()
            .filter(|(_, s)| !s.as_str().eq("Validated") && !s.as_str().eq("ReadyToCommit"))
            .map(|(i, s)| (i, s.to_string()))
            .collect();
        if !incorrect_statuses.is_empty() {
            panic!(
                "NemoNoPq: {:?} transactions don't have either status Validated or ReadyToCommit!",
                incorrect_statuses
            );
        }

        let mut failed_validation: Vec<TxnIndex> = Vec::new();
        // Revalidate all transactions
        for idx in 0..num_txns {
            if !shared_memory.validate_read_set(idx) {
                failed_validation.push(idx);
            }
        }

        if !failed_validation.is_empty() {
            panic!(
                "NemoNoPq: {:?} failed validation! Incorrect final state!",
                failed_validation
            );
        }

        ExecutionResult::new(
            duration.as_millis() as f64 / 1000.0,
            num_executions,
            num_validations,
            num_greedy,
        )
    }
}
