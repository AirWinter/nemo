use crate::iota::dependency_graph::DependencyGraph;
use crate::transaction::Transaction;
use crate::types::{ExecutionResult, ObjectId};
use std::cmp::{max, Reverse};
use std::collections::{BinaryHeap, HashMap};

/// Code to simulate the pessimistic execution done by IOTA/Sui.
pub struct Iota {
    number_of_workers: usize,
}

impl Iota {
    pub fn new(number_of_workers: usize) -> Iota {
        Iota { number_of_workers }
    }

    pub fn execute_transactions(&self, block: Vec<Transaction>) -> ExecutionResult {
        // Build Dependency Graph
        let mut dep_graph = DependencyGraph::new();

        // For each object keep track of last transaction that wrote
        let mut objects: HashMap<ObjectId, usize> = HashMap::new();

        // Keep track of transactions that no longer have any dependencies, ordered so that
        // transactions that are free earlier are popped first. Tiebreaker is txn_index.
        let mut ready_transactions = BinaryHeap::new();

        // Go through each transaction and build dependency graph
        for (txn_index, txn) in block.iter().enumerate() {
            // Go through all exhaustive read/write sets as IOTA does
            for x in &txn.exhaustive_accessed_shared_objects {
                // If reading -> Look for blocking txn
                if x.is_read() {
                    // Check if what they're reading is in the HashMap
                    if let Some(from) = objects.get(&x.object_id) {
                        dep_graph.add_dependency(*from, txn_index);
                    }
                }

                // Add all writes to the hashmap, do so after reading because we don't depend on ourselves
                if x.is_write() {
                    objects.insert(x.object_id, txn_index);
                }
            }

            if !dep_graph.has_dependencies(txn_index) {
                // Find all transactions that are ready for execution and add them to PQ with time 0
                ready_transactions.push(Reverse((0, txn_index)))
            }
        }

        // Keep track of until when the worker is busy
        let mut workers = BinaryHeap::new();
        for _ in 0..self.number_of_workers {
            workers.push(Reverse(0));
        }

        // Get next transaction that is ready to be executed until empty
        while let Some(Reverse((time, txn_index))) = ready_transactions.pop() {
            let txn_duration = block.get(txn_index).unwrap().duration_ms.unwrap() as i32;
            // Should always have the same number of workers
            assert_eq!(workers.len(), self.number_of_workers);
            // Assign it to the next worker that is free
            if let Some(Reverse(next_free_worker)) = workers.pop() {
                // Next time this worker is free
                let next_time_free = max(time, next_free_worker) + txn_duration;
                workers.push(Reverse(next_time_free));

                // Remove dependencies -> Add to ready dependencies with time when they are free
                let deps = dep_graph.remove_dependency(&txn_index);
                for dep_index in deps {
                    ready_transactions.push(Reverse((next_time_free, dep_index)))
                }
            } else {
                panic!("Should have found next available worker!")
            }
            // Should always have the same number of workers
            assert_eq!(workers.len(), self.number_of_workers);
        }

        // Get the largest number in the workers => Duration in ms
        let duration = workers.iter().map(|r| r.0).max().unwrap();

        ExecutionResult::new(duration as f64 / 1000.0, block.len(), 0, 0)
    }
}
