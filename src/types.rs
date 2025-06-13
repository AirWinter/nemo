pub type TxnIndex = usize;
pub type Version = (TxnIndex, Incarnation);
pub type Incarnation = usize;
pub type ObjectId = usize;
pub type TransactionId = usize;

#[derive(Debug, Clone, PartialEq)]
pub enum AccessType {
    Read,
    Write,
    ReadWrite,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObjectAccess {
    pub object_id: ObjectId,
    pub access_type: AccessType,
}

impl ObjectAccess {
    pub fn new(object_id: ObjectId, access_type: AccessType) -> Self {
        Self {
            object_id,
            access_type,
        }
    }

    pub fn is_write(&self) -> bool {
        match self.access_type {
            AccessType::Read => false,
            AccessType::Write | AccessType::ReadWrite => true,
        }
    }

    pub fn is_read(&self) -> bool {
        match self.access_type {
            AccessType::Read | AccessType::ReadWrite => true,
            AccessType::Write => false,
        }
    }
}

pub struct ExecutionResult {
    pub duration: f64,
    pub number_of_executions: usize,
    pub number_of_validations: usize,
    pub number_of_greedy: usize,
}

impl ExecutionResult {
    pub fn new(
        duration: f64,
        number_of_executions: usize,
        number_of_validations: usize,
        number_of_greedy: usize,
    ) -> Self {
        ExecutionResult {
            duration,
            number_of_executions,
            number_of_validations,
            number_of_greedy,
        }
    }
}
