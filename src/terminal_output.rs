use anyhow::Result;
use futures::{Stream, StreamExt};
use serde::Serialize;
use std::io::{Stdout, Write};

/// Terminal output handler for displaying JSON objects and streams
pub struct TerminalOutput<W: Write> {
    writer: W,
}

impl Default for TerminalOutput<Stdout> {
    fn default() -> Self {
        Self {
            writer: std::io::stdout(),
        }
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
    pub async fn display_stream<T: Serialize>(
        &mut self,
        stream: impl Stream<Item = Result<T>>,
    ) -> Result<()> {
        tokio::pin!(stream);
        while let Some(result) = stream.next().await {
            let item = result?;
            let json = serde_json::to_string(&item)?;
            writeln!(self.writer, "{}", json)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use serde::Deserialize;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestData {
        name: String,
        value: i32,
    }

    #[test]
    fn test_display_object() -> Result<()> {
        let mut buffer = Vec::new();
        let mut output = TerminalOutput::with_writer(&mut buffer);

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        output.display_object(&data)?;

        let output_str = String::from_utf8(buffer)?;
        let parsed: TestData = serde_json::from_str(output_str.trim())?;

        assert_eq!(parsed, data);
        // Verify it's pretty-printed (contains newlines)
        assert!(output_str.contains('\n'));

        Ok(())
    }

    #[tokio::test]
    async fn test_display_stream() -> Result<()> {
        let mut buffer = Vec::new();
        let mut output = TerminalOutput::with_writer(&mut buffer);

        let items = vec![
            TestData {
                name: "first".to_string(),
                value: 1,
            },
            TestData {
                name: "second".to_string(),
                value: 2,
            },
            TestData {
                name: "third".to_string(),
                value: 3,
            },
        ];

        let test_stream = stream::iter(items.clone().into_iter().map(Ok));
        output.display_stream(test_stream).await?;

        let output_str = String::from_utf8(buffer)?;
        let lines: Vec<&str> = output_str.lines().collect();

        // Should have 3 lines of JSONL output
        assert_eq!(lines.len(), 3);

        // Parse and verify each line
        for (idx, line) in lines.iter().enumerate() {
            let parsed: TestData = serde_json::from_str(line)?;
            assert_eq!(parsed, items[idx]);
            // JSONL should be compact (no extra newlines within each line)
            assert!(!line.contains('\n'));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_display_stream_empty() -> Result<()> {
        let mut buffer = Vec::new();
        let mut output = TerminalOutput::with_writer(&mut buffer);

        let empty_stream = stream::iter(Vec::<Result<TestData>>::new());
        output.display_stream(empty_stream).await?;

        let output_str = String::from_utf8(buffer)?;
        assert_eq!(output_str, "");

        Ok(())
    }

    #[tokio::test]
    async fn test_display_stream_with_error() {
        let mut buffer = Vec::new();
        let mut output = TerminalOutput::with_writer(&mut buffer);

        let items: Vec<Result<TestData>> = vec![
            Ok(TestData {
                name: "first".to_string(),
                value: 1,
            }),
            Err(anyhow::anyhow!("test error")),
            Ok(TestData {
                name: "third".to_string(),
                value: 3,
            }),
        ];

        let test_stream = stream::iter(items);
        let result = output.display_stream(test_stream).await;

        // Should fail on the error
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("test error"));
    }

    #[test]
    fn test_new_creates_stdout_output() {
        let _output = TerminalOutput::new();
        // If this compiles and runs, it works
    }

    #[test]
    fn test_default_creates_stdout_output() {
        let _output = TerminalOutput::<Stdout>::default();
        // If this compiles and runs, it works
    }
}
