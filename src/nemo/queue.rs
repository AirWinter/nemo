use crate::types::TxnIndex;
use std::cmp::Ordering::{Equal, Greater, Less};
use std::collections::{BinaryHeap, HashSet};
use std::sync::Mutex;

pub static EXECUTION_TASK: bool = true;
pub static VALIDATION_TASK: bool = false;

#[derive(PartialEq, Eq)]
pub struct ScoredTask {
    pub txn_index: TxnIndex,
    pub is_execution_task: bool,
    score: usize,
}

impl ScoredTask {
    pub fn new(txn_index: TxnIndex, is_execution_task: bool, score: usize) -> Self {
        Self {
            txn_index,
            is_execution_task,
            score,
        }
    }
}

impl PartialOrd for ScoredTask {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredTask {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.score == other.score
            && self.txn_index == other.txn_index
            && self.is_execution_task == other.is_execution_task
        {
            return Equal;
        }
        if self.score > other.score {
            return Greater;
        }
        if self.score < other.score {
            return Less;
        }
        // If we make it here then we know the scores are equal and the indices are not
        if self.txn_index < other.txn_index {
            return Greater;
        }

        if self.txn_index > other.txn_index {
            return Less;
        }

        // If we make it here then we know the scores and the indices are equal but not the task type
        if self.is_execution_task > other.is_execution_task {
            Greater
        } else {
            Less
        }
    }
}

pub struct TaskQueue {
    data: Mutex<(BinaryHeap<ScoredTask>, HashSet<(TxnIndex, bool)>)>,
}

impl TaskQueue {
    pub fn new() -> Self {
        Self {
            data: Mutex::new((BinaryHeap::new(), HashSet::new())),
        }
    }

    pub fn insert(&self, task: ScoredTask) -> bool {
        let mut guard = self.data.lock().expect("Can't handle poisoned locks!");
        if guard.1.contains(&(task.txn_index, task.is_execution_task)) {
            return false;
        }

        if guard.0.len() > usize::MAX / 2 {
            panic!("TOO MANY ITEMS IN QUEUE");
        }
        guard.1.insert((task.txn_index, task.is_execution_task));
        guard.0.push(task);
        true
    }

    pub fn pop(&self) -> Option<ScoredTask> {
        let mut guard = self.data.lock().expect("Can't handle poisoned locks");
        if let Some(task) = guard.0.pop() {
            guard.1.remove(&(task.txn_index, task.is_execution_task));
            Some(task)
        } else {
            None
        }
    }

    pub fn is_empty(&self) -> bool {
        self.data
            .lock()
            .expect("Can't deal with poisoned locks!")
            .0
            .is_empty()
    }
}
