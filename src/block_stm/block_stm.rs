use crate::block_stm::executor::worker_job;
use crate::block_stm::mvmemory::MVMemory;
use crate::block_stm::scheduler::Scheduler;
use crate::transaction::Transaction;
use crate::types::{ExecutionResult, TxnIndex};
use crossbeam::thread;
use std::time::Instant;

/// Implementation for [Block-STM](https://arxiv.org/pdf/2203.06871) that looks to follow
/// the pseudocode as closely as possible, whilst integrating it into the contention simulator.
pub struct BlockStm {
    number_of_workers: usize,
}

impl BlockStm {
    pub fn new(number_of_workers: usize) -> Self {
        BlockStm { number_of_workers }
    }

    pub fn execute_transactions(&self, block: Vec<Transaction>) -> ExecutionResult {
        let num_txns = block.len();
        let shared_memory = MVMemory::new(num_txns);
        let scheduler = Scheduler::new(num_txns);

        // ====================== Start Duration ======================
        let start = Instant::now();

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
        let transaction_status = scheduler.recover_transaction_status();

        // ====================== Verify Final State ======================
        // Verify that all statuses are `Executed`
        let incorrect_statuses: Vec<(TxnIndex, String)> = transaction_status
            .clone()
            .iter()
            .enumerate()
            .filter(|(_, s)| !s.as_str().eq("Executed"))
            .map(|(i, s)| (i, s.to_string()))
            .collect();
        if !incorrect_statuses.is_empty() {
            panic!(
                "Block-STM: {:?} transactions don't have either status Executed!",
                incorrect_statuses
            );
        }

        // Revalidate all transactions
        for idx in 0..num_txns {
            if !shared_memory.validate_read_set(idx) {
                panic!(
                    "Block-STM: {} failed validation! Incorrect final state!",
                    idx
                );
            }
        }

        ExecutionResult::new(
            duration.as_millis() as f64 / 1000.0,
            num_executions,
            num_validations,
            0,
        )
    }
}
