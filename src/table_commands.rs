use crate::cli::{SnapshotCmd, TableCommands};
use crate::error::ExpectedError;
use crate::s3_lister::{S3FileCache, parse_s3_url, s3_client_from_file_io};
use crate::terminal_output::TerminalOutput;
use anyhow::{Context, Result};
use async_stream::try_stream;
use futures::{Stream, StreamExt, stream};
use iceberg::TableIdent;
use iceberg::io::FileIO;
use iceberg::spec::{Manifest, ManifestList, TableMetadata};
use iceberg::table::{StaticTable, Table};
use serde::Serialize;
use std::io::Write;
use tracing::{debug, instrument};

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum FileType {
    Metadata,
    ManifestList,
    Manifest,
    Data,
}

#[derive(Debug, Serialize)]
struct FileRecord {
    r#type: FileType,
    path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    exists: Option<bool>,
}

/// Load a Table from a metadata file location
#[instrument(skip(file_io))]
pub async fn load_table(file_io: &FileIO, location: &str) -> Result<Table> {
    let table_ident = TableIdent::from_strs(["bergr", "table"])?;
    let static_table =
        StaticTable::from_metadata_file(location, table_ident, file_io.clone()).await?;
    Ok(static_table.into_table())
}

#[instrument(skip(table, output))]
pub async fn handle_table_command<W: Write>(
    table: &Table,
    command: TableCommands,
    output: &mut TerminalOutput<W>,
) -> Result<()> {
    match command {
        TableCommands::Metadata => handle_metadata(table.metadata(), output),
        TableCommands::Schemas => handle_schemas(table.metadata(), output).await,
        TableCommands::Schema { schema_id } => handle_schema(table.metadata(), &schema_id, output),
        TableCommands::Snapshots => handle_snapshots(table.metadata(), output).await,
        TableCommands::Snapshot {
            snapshot_id,
            command,
        } => handle_snapshot(&table, &snapshot_id, command, output).await,
    }
}

fn handle_metadata<W: Write>(
    metadata: &TableMetadata,
    output: &mut TerminalOutput<W>,
) -> Result<()> {
    output.display_object(metadata)
}

async fn handle_schemas<W: Write>(
    metadata: &TableMetadata,
    output: &mut TerminalOutput<W>,
) -> Result<()> {
    let schemas_stream = stream::iter(metadata.schemas_iter().map(Ok));
    output.display_stream(schemas_stream).await
}

fn handle_schema<W: Write>(
    metadata: &TableMetadata,
    schema_id: &str,
    output: &mut TerminalOutput<W>,
) -> Result<()> {
    let id = if schema_id == "current" {
        metadata.current_schema_id()
    } else {
        schema_id
            .parse::<i32>()
            .context("Schema ID must be an integer")?
    };

    let schema = metadata
        .schema_by_id(id)
        .ok_or_else(|| anyhow::anyhow!("Schema {} not found", id))?;

    output.display_object(schema)
}

async fn handle_snapshots<W: Write>(
    metadata: &TableMetadata,
    output: &mut TerminalOutput<W>,
) -> Result<()> {
    let snapshots_stream = stream::iter(metadata.snapshots().map(Ok));
    output.display_stream(snapshots_stream).await
}

async fn handle_snapshot<W: Write>(
    table: &Table,
    snapshot_id: &str,
    command: Option<SnapshotCmd>,
    output: &mut TerminalOutput<W>,
) -> Result<()> {
    let metadata = table.metadata();

    let id = if snapshot_id == "current" {
        metadata
            .current_snapshot_id()
            .ok_or_else(|| anyhow::anyhow!("Table has no current snapshot"))?
    } else {
        snapshot_id
            .parse::<i64>()
            .context("Snapshot ID must be an integer")?
    };

    let snapshot = metadata
        .snapshots()
        .find(|s| s.snapshot_id() == id)
        .ok_or_else(|| anyhow::anyhow!("Snapshot {} not found", id))?;

    match command {
        None => output.display_object(snapshot),
        Some(SnapshotCmd::Files { verify }) => {
            handle_snapshot_files(table, snapshot, verify, output).await
        }
    }
}

async fn handle_snapshot_files<W: Write>(
    table: &Table,
    snapshot: &iceberg::spec::Snapshot,
    verify: bool,
    output: &mut TerminalOutput<W>,
) -> Result<()> {
    // Determine if we can use S3 optimization: verify mode and files are on S3
    let use_s3_optimization = verify && parse_s3_url(snapshot.manifest_list()).is_some();

    if use_s3_optimization {
        // Try to build an S3 client from the table's FileIO credentials
        debug!("Attempting to build S3 client from FileIO for optimized verification");
        if let Some(s3_client) = s3_client_from_file_io(table.file_io().clone()) {
            debug!("Using S3 prefix listing for optimized file verification");
            return handle_snapshot_files_with_s3_cache(table, snapshot, output, &s3_client).await;
        }
        debug!("Could not build S3 client from FileIO, falling back to streaming verification");
    }

    handle_snapshot_files_streaming(table, snapshot, verify, output).await
}

/// Verify snapshot files using S3 prefix listing (optimized path).
///
/// This approach:
/// 1. Reads all manifests to collect data file paths
/// 2. Builds an S3FileCache by listing the common prefix
/// 3. Checks existence against the cache (no HeadObject calls)
async fn handle_snapshot_files_with_s3_cache<W: Write>(
    table: &Table,
    snapshot: &iceberg::spec::Snapshot,
    output: &mut TerminalOutput<W>,
    s3_client: &aws_sdk_s3::Client,
) -> Result<()> {
    let file_io = table.file_io();
    let format_version = table.metadata().format_version();

    // Phase 1: Output manifest list and collect manifests
    let manifest_list_location = snapshot.manifest_list();
    output.display_object(&FileRecord {
        r#type: FileType::ManifestList,
        path: manifest_list_location.to_string(),
        exists: Some(true),
    })?;

    let manifest_list_bytes = fetch_bytes(file_io, manifest_list_location).await?;
    let manifest_list = ManifestList::parse_with_version(&manifest_list_bytes, format_version)
        .context("Failed to parse manifest list")?;

    // Phase 2: Read all manifests and collect data file paths
    let mut all_data_files: Vec<String> = Vec::new();
    let mut manifest_records: Vec<FileRecord> = Vec::new();

    let tasks = manifest_list.entries().iter().map(|manifest_file| {
        let manifest_location = manifest_file.manifest_path.clone();
        let file_io = file_io.clone();
        async move {
            let bytes_result = fetch_bytes(&file_io, &manifest_location).await;
            (manifest_location, bytes_result)
        }
    });

    let mut stream = stream::iter(tasks).buffered(7);

    while let Some((manifest_location, bytes_result)) = stream.next().await {
        manifest_records.push(FileRecord {
            r#type: FileType::Manifest,
            path: manifest_location.clone(),
            exists: Some(true),
        });

        let manifest_bytes = bytes_result?;
        let manifest =
            Manifest::parse_avro(manifest_bytes.as_slice()).context("Failed to parse manifest")?;

        let data_files: Vec<String> = manifest
            .entries()
            .iter()
            .filter(|entry| {
                entry.status() == iceberg::spec::ManifestStatus::Added
                    || entry.status() == iceberg::spec::ManifestStatus::Existing
            })
            .map(|entry| entry.data_file().file_path().to_string())
            .collect();

        all_data_files.extend(data_files);
    }

    // Phase 3: Build S3 cache from collected paths
    debug!(
        "Building S3 file cache for {} data files",
        all_data_files.len()
    );
    let s3_cache = S3FileCache::new(s3_client, &all_data_files).await?;
    debug!("S3 cache contains {} files", s3_cache.len());

    // Phase 4: Output manifest records
    for record in manifest_records {
        output.display_object(&record)?;
    }

    // Phase 5: Output data file records with existence from cache
    let mut missing_count = 0;
    for path in all_data_files {
        let exists = s3_cache.exists(&path);
        if !exists {
            missing_count += 1;
        }
        output.display_object(&FileRecord {
            r#type: FileType::Data,
            path,
            exists: Some(exists),
        })?;
    }

    if missing_count > 0 {
        return Err(anyhow::Error::new(ExpectedError::Failed(format!(
            "table is corrupt - {} file(s) missing",
            missing_count
        ))));
    }

    Ok(())
}

/// Stream snapshot files with per-file existence checks (fallback path).
async fn handle_snapshot_files_streaming<W: Write>(
    table: &Table,
    snapshot: &iceberg::spec::Snapshot,
    verify: bool,
    output: &mut TerminalOutput<W>,
) -> Result<()> {
    let stream = iterate_files(
        table.file_io(),
        snapshot,
        table.metadata().format_version(),
        verify,
    );

    // Count missing files while displaying the stream
    let mut missing_count = 0;
    let mut stream = Box::pin(stream);

    while let Some(result) = stream.next().await {
        let record = result?;

        // Check if this is a missing file
        if record.exists == Some(false) {
            missing_count += 1;
        }

        // Display the record
        output.display_object(&record)?;
    }

    // If any files are missing, return a Failed error wrapped in anyhow::Error
    if verify && missing_count > 0 {
        return Err(anyhow::Error::new(ExpectedError::Failed(format!(
            "table is corrupt - {} file(s) missing",
            missing_count
        ))));
    }

    Ok(())
}

#[instrument(skip(file_io))]
fn iterate_files<'a>(
    file_io: &'a FileIO,
    snapshot: &'a iceberg::spec::Snapshot,
    format_version: iceberg::spec::FormatVersion,
    verify: bool,
) -> impl Stream<Item = Result<FileRecord>> + 'a {
    try_stream! {
        let implicitly_exists = if verify { Some(true) } else { None };
        let manifest_list_location = snapshot.manifest_list();
        yield FileRecord {
            r#type: FileType::ManifestList,
            path: manifest_list_location.to_string(),
            exists: implicitly_exists,
        };

        let manifest_list_bytes = fetch_bytes(file_io, manifest_list_location).await?;
        let manifest_list = ManifestList::parse_with_version(&manifest_list_bytes, format_version)
            .context("Failed to parse manifest list")?;

        let tasks = manifest_list.entries().iter().map(|manifest_file| {
            let manifest_location = manifest_file.manifest_path.clone();
            let file_io = file_io.clone();
            async move {
                let bytes_result = fetch_bytes(&file_io, &manifest_location).await;
                (manifest_location, bytes_result)
            }
        });

        let mut stream = stream::iter(tasks).buffered(7);

        while let Some((manifest_location, bytes_result)) = stream.next().await {
            yield FileRecord {
                r#type: FileType::Manifest,
                path: manifest_location.clone(),
                exists: implicitly_exists,
            };

            let manifest_bytes = bytes_result?;
            let manifest = Manifest::parse_avro(manifest_bytes.as_slice())
                .context("Failed to parse manifest")?;

            // Collect data file paths and check existence in parallel
            let data_files: Vec<String> = manifest
                .entries()
                .iter()
                .filter(|entry| {
                    entry.status() == iceberg::spec::ManifestStatus::Added
                        || entry.status() == iceberg::spec::ManifestStatus::Existing
                })
                .map(|entry| entry.data_file().file_path().to_string())
                .collect();

            let tasks = data_files.into_iter().map(|path| {
                let file_io = file_io.clone();
                async move {
                    let exists = if verify {
                        Some(file_io.exists(&path).await.unwrap_or(false))
                    } else {
                        None
                    };
                    (path, exists)
                }
            });

            let mut data_stream = stream::iter(tasks).buffered(13);

            while let Some((path, exists)) = data_stream.next().await {
                yield FileRecord {
                    r#type: FileType::Data,
                    path,
                    exists,
                };
            }
        }
    }
}

async fn fetch_bytes(file_io: &FileIO, location: &str) -> Result<Vec<u8>> {
    let input_file = file_io.new_input(location)?;
    let bytes = input_file.read().await?;
    Ok(bytes.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::io::{FileIOBuilder, FileWrite};

    async fn create_memory_file_io(files: Vec<(&str, &str)>) -> FileIO {
        let file_io = FileIOBuilder::new("memory").build().unwrap();

        for (path, content) in files {
            let output_file = file_io.new_output(path).unwrap();
            let mut writer = output_file.writer().await.unwrap();
            writer
                .write(bytes::Bytes::from(content.to_string()))
                .await
                .unwrap();
            writer.close().await.unwrap();
        }

        file_io
    }

    /// Returns metadata for an empty Iceberg table (no snapshots)
    fn empty_metadata() -> serde_json::Value {
        serde_json::json!({
            "format-version": 2,
            "table-uuid": "9c2c0c2c-9c2c-9c2c-9c2c-9c2c0c2c0c2c",
            "location": "s3://bucket/table",
            "last-sequence-number": 1,
            "last-updated-ms": 1600000000000_i64,
            "last-column-id": 1,
            "current-schema-id": 0,
            "schemas": [
                {
                    "type": "struct",
                    "schema-id": 0,
                    "fields": [
                        {
                            "id": 1,
                            "name": "id",
                            "required": true,
                            "type": "int"
                        }
                    ]
                }
            ],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 999,
            "default-sort-order-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "properties": {},
            "refs": {},
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": []
        })
    }

    /// Returns metadata with a single snapshot added
    fn metadata_with_snapshot(snapshot_id: i64, manifest_list: &str) -> serde_json::Value {
        let mut metadata = empty_metadata();

        metadata["current-snapshot-id"] = serde_json::json!(snapshot_id);
        metadata["snapshots"] = serde_json::json!([
            {
                "snapshot-id": snapshot_id,
                "sequence-number": 1,
                "timestamp-ms": 1600000000000_i64,
                "manifest-list": manifest_list,
                "summary": { "operation": "append" },
                "schema-id": 0
            }
        ]);

        metadata
    }

    /// Returns minimal metadata with one snapshot (for backwards compatibility)
    fn minimal_metadata() -> String {
        serde_json::to_string(&metadata_with_snapshot(
            123,
            "s3://bucket/table/snap-123.avro",
        ))
        .unwrap()
    }

    #[tokio::test]
    async fn test_handle_schema_current() -> Result<()> {
        let metadata_json = minimal_metadata();
        let location = "s3://bucket/table/metadata.json";
        let file_io = create_memory_file_io(vec![(location, &metadata_json)]).await;
        let table = load_table(&file_io, location).await?;

        let mut buffer = Vec::new();
        let mut output = TerminalOutput::with_writer(&mut buffer);
        handle_table_command(
            &table,
            TableCommands::Schema {
                schema_id: "current".to_string(),
            },
            &mut output,
        )
        .await?;

        // Verify JSON output
        let output_str = String::from_utf8(buffer)?;
        let schema: serde_json::Value = serde_json::from_str(output_str.trim())?;

        assert_eq!(schema["type"], "struct");
        assert_eq!(schema["schema-id"], 0);
        assert_eq!(schema["fields"][0]["name"], "id");
        assert_eq!(schema["fields"][0]["type"], "int");

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_snapshot_current() -> Result<()> {
        let metadata_json = minimal_metadata();
        let location = "s3://bucket/table/metadata.json";
        let file_io = create_memory_file_io(vec![(location, &metadata_json)]).await;
        let table = load_table(&file_io, location).await?;

        let mut buffer = Vec::new();
        let mut output = TerminalOutput::with_writer(&mut buffer);
        handle_table_command(
            &table,
            TableCommands::Snapshot {
                snapshot_id: "current".to_string(),
                command: None,
            },
            &mut output,
        )
        .await?;

        // Verify JSON output
        let output_str = String::from_utf8(buffer)?;
        let snapshot: serde_json::Value = serde_json::from_str(output_str.trim())?;

        assert_eq!(snapshot["snapshot-id"], 123);
        assert_eq!(snapshot["manifest-list"], "s3://bucket/table/snap-123.avro");
        assert_eq!(snapshot["summary"]["operation"], "append");

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_metadata() -> Result<()> {
        let metadata_json = minimal_metadata();
        let location = "s3://bucket/table/metadata.json";
        let file_io = create_memory_file_io(vec![(location, &metadata_json)]).await;
        let table = load_table(&file_io, location).await?;

        let mut buffer = Vec::new();
        let mut output = TerminalOutput::with_writer(&mut buffer);
        handle_table_command(&table, TableCommands::Metadata, &mut output).await?;

        // Verify JSON output contains metadata fields
        let output_str = String::from_utf8(buffer)?;
        let metadata: serde_json::Value = serde_json::from_str(output_str.trim())?;

        assert_eq!(metadata["format-version"], 2);
        assert_eq!(metadata["location"], "s3://bucket/table");
        assert_eq!(metadata["current-schema-id"], 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_schemas() -> Result<()> {
        let metadata_json = minimal_metadata();
        let location = "s3://bucket/table/metadata.json";
        let file_io = create_memory_file_io(vec![(location, &metadata_json)]).await;
        let table = load_table(&file_io, location).await?;

        let mut buffer = Vec::new();
        let mut output = TerminalOutput::with_writer(&mut buffer);
        handle_table_command(&table, TableCommands::Schemas, &mut output).await?;

        // Verify JSONL output (one schema per line)
        let output_str = String::from_utf8(buffer)?;
        let lines: Vec<&str> = output_str.lines().collect();

        assert_eq!(lines.len(), 1); // minimal_metadata has 1 schema

        let schema: serde_json::Value = serde_json::from_str(lines[0])?;
        assert_eq!(schema["schema-id"], 0);
        assert_eq!(schema["type"], "struct");

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_schema_by_id() -> Result<()> {
        let metadata_json = minimal_metadata();
        let location = "s3://bucket/table/metadata.json";
        let file_io = create_memory_file_io(vec![(location, &metadata_json)]).await;
        let table = load_table(&file_io, location).await?;

        let mut buffer = Vec::new();
        let mut output = TerminalOutput::with_writer(&mut buffer);
        handle_table_command(
            &table,
            TableCommands::Schema {
                schema_id: "0".to_string(),
            },
            &mut output,
        )
        .await?;

        let output_str = String::from_utf8(buffer)?;
        let schema: serde_json::Value = serde_json::from_str(output_str.trim())?;

        assert_eq!(schema["schema-id"], 0);
        assert_eq!(schema["fields"][0]["name"], "id");

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_schema_invalid_id() -> Result<()> {
        let metadata_json = minimal_metadata();
        let location = "s3://bucket/table/metadata.json";
        let file_io = create_memory_file_io(vec![(location, &metadata_json)]).await;
        let table = load_table(&file_io, location).await?;

        let mut buffer = Vec::new();
        let mut output = TerminalOutput::with_writer(&mut buffer);
        let result = handle_table_command(
            &table,
            TableCommands::Schema {
                schema_id: "invalid".to_string(),
            },
            &mut output,
        )
        .await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Schema ID must be an integer")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_schema_not_found() -> Result<()> {
        let metadata_json = minimal_metadata();
        let location = "s3://bucket/table/metadata.json";
        let file_io = create_memory_file_io(vec![(location, &metadata_json)]).await;
        let table = load_table(&file_io, location).await?;

        let mut buffer = Vec::new();
        let mut output = TerminalOutput::with_writer(&mut buffer);
        let result = handle_table_command(
            &table,
            TableCommands::Schema {
                schema_id: "999".to_string(),
            },
            &mut output,
        )
        .await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Schema 999 not found")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_snapshots() -> Result<()> {
        let metadata_json = minimal_metadata();
        let location = "s3://bucket/table/metadata.json";
        let file_io = create_memory_file_io(vec![(location, &metadata_json)]).await;
        let table = load_table(&file_io, location).await?;

        let mut buffer = Vec::new();
        let mut output = TerminalOutput::with_writer(&mut buffer);
        handle_table_command(&table, TableCommands::Snapshots, &mut output).await?;

        // Verify JSONL output
        let output_str = String::from_utf8(buffer)?;
        let lines: Vec<&str> = output_str.lines().collect();

        assert_eq!(lines.len(), 1); // minimal_metadata has 1 snapshot

        let snapshot: serde_json::Value = serde_json::from_str(lines[0])?;
        assert_eq!(snapshot["snapshot-id"], 123);

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_snapshot_by_id() -> Result<()> {
        let metadata_json = minimal_metadata();
        let location = "s3://bucket/table/metadata.json";
        let file_io = create_memory_file_io(vec![(location, &metadata_json)]).await;
        let table = load_table(&file_io, location).await?;

        let mut buffer = Vec::new();
        let mut output = TerminalOutput::with_writer(&mut buffer);
        handle_table_command(
            &table,
            TableCommands::Snapshot {
                snapshot_id: "123".to_string(),
                command: None,
            },
            &mut output,
        )
        .await?;

        let output_str = String::from_utf8(buffer)?;
        let snapshot: serde_json::Value = serde_json::from_str(output_str.trim())?;

        assert_eq!(snapshot["snapshot-id"], 123);
        assert_eq!(snapshot["manifest-list"], "s3://bucket/table/snap-123.avro");

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_snapshot_invalid_id() -> Result<()> {
        let metadata_json = minimal_metadata();
        let location = "s3://bucket/table/metadata.json";
        let file_io = create_memory_file_io(vec![(location, &metadata_json)]).await;
        let table = load_table(&file_io, location).await?;

        let mut buffer = Vec::new();
        let mut output = TerminalOutput::with_writer(&mut buffer);
        let result = handle_table_command(
            &table,
            TableCommands::Snapshot {
                snapshot_id: "invalid".to_string(),
                command: None,
            },
            &mut output,
        )
        .await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Snapshot ID must be an integer")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_snapshot_not_found() -> Result<()> {
        let metadata_json = minimal_metadata();
        let location = "s3://bucket/table/metadata.json";
        let file_io = create_memory_file_io(vec![(location, &metadata_json)]).await;
        let table = load_table(&file_io, location).await?;

        let mut buffer = Vec::new();
        let mut output = TerminalOutput::with_writer(&mut buffer);
        let result = handle_table_command(
            &table,
            TableCommands::Snapshot {
                snapshot_id: "999".to_string(),
                command: None,
            },
            &mut output,
        )
        .await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Snapshot 999 not found")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_snapshot_no_current() -> Result<()> {
        let metadata_json = serde_json::to_string(&empty_metadata()).unwrap();
        let location = "s3://bucket/table/metadata.json";
        let file_io = create_memory_file_io(vec![(location, &metadata_json)]).await;
        let table = load_table(&file_io, location).await?;

        let mut buffer = Vec::new();
        let mut output = TerminalOutput::with_writer(&mut buffer);
        let result = handle_table_command(
            &table,
            TableCommands::Snapshot {
                snapshot_id: "current".to_string(),
                command: None,
            },
            &mut output,
        )
        .await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Table has no current snapshot")
        );

        Ok(())
    }
}
