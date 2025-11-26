use std::fmt;

/// Expected user-facing errors (missing files, validation failures, etc.)
/// Exit code: 1
#[derive(Debug)]
pub struct ObviousError(pub String);

impl fmt::Display for ObviousError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ObviousError {}
