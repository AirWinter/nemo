use crate::transaction::Transaction;
use crate::types::ExecutionResult;

pub struct Sequential {
    number_of_workers: usize,
}

impl Sequential {
    pub fn new() -> Self {
        Sequential {
            number_of_workers: 1,
        }
    }

    pub fn execute_transactions(&self, block: Vec<Transaction>) -> ExecutionResult {
        let mut time_counter = 0;
        let mut num_executions = 0;

        for tx in block {
            time_counter += tx.duration_ms.unwrap_or(20);
            num_executions += 1;
        }

        ExecutionResult::new(time_counter as f64 / 1000.0, num_executions, 0, 0)
    }
}
