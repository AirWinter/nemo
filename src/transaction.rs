use crate::types::{ObjectAccess, TransactionId};

/// Represents a transaction in the system.
#[derive(Debug, Clone, PartialEq)]
pub struct Transaction {
    /// Unique identifier for the transaction.
    pub id: TransactionId,

    /// TODO: Duration in ms -> simulated execution
    pub duration_ms: Option<usize>,

    /// True if transaction either read/writes to shared objects, false otherwise. If it is false
    /// then it means that the transaction is independent of all other transactions in the block.
    pub touches_shared_state: bool,

    /// The exhaustive set of objects that the transaction can use. So regardless of the transaction
    /// logic or the application state it can't use any other objects. This is a superset of
    /// `access_shared_objects`.
    pub exhaustive_accessed_shared_objects: Vec<ObjectAccess>,

    /// A list of shared objects (by their unique identifiers) that the transaction depends on.
    pub accessed_shared_objects: Vec<ObjectAccess>,

    /// A list of objects representing the prior knowledge that is available about the read/write
    /// set of a transaction. Could be empty if there is no prior knowledge, but any object in this
    /// list is guaranteed to be in accessed_shared_objects.
    pub prior_knowledge: Vec<ObjectAccess>,
}
