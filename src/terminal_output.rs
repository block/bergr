use anyhow::Result;
use futures::{Stream, StreamExt};
use serde::Serialize;
use std::io::{Write, Stdout};

/// Terminal output handler for displaying JSON objects and streams
pub struct TerminalOutput<W: Write> {
    writer: W,
}

impl Default for TerminalOutput<Stdout> {
    fn default() -> Self {
        Self { writer: std::io::stdout() }
    }
}

impl TerminalOutput<Stdout> {
    /// Create a new TerminalOutput writing to stdout
    pub fn new() -> Self {
        Self::default()
    }
}

impl<W: Write> TerminalOutput<W> {
    /// Create a TerminalOutput with a custom writer
    pub fn with_writer(writer: W) -> Self {
        Self { writer }
    }

    /// Display a single object as pretty-printed JSON
    pub fn display_object<T: Serialize>(&mut self, item: &T) -> Result<()> {
        let json = serde_json::to_string_pretty(item)?;
        writeln!(self.writer, "{}", json)?;
        Ok(())
    }

    /// Display items from a stream as JSON Lines (JSONL) format
    pub async fn display_stream<T: Serialize>(&mut self, stream: impl Stream<Item = Result<T>>) -> Result<()> {
        tokio::pin!(stream);
        while let Some(result) = stream.next().await {
            let item = result?;
            let json = serde_json::to_string(&item)?;
            writeln!(self.writer, "{}", json)?;
        }
        Ok(())
    }
}
