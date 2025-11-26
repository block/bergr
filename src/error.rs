use std::fmt;

/// Expected errors that should exit with code 1
#[derive(Debug)]
pub enum ExpectedError {
    /// Invalid user input (non-existent table, invalid parameters, etc.)
    UserInput(String),
    /// Operation failed validation (corrupt table, missing files, etc.)
    Failed(String),
}

impl fmt::Display for ExpectedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExpectedError::UserInput(msg) => write!(f, "{}", msg),
            ExpectedError::Failed(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for ExpectedError {}
