use anyhow::{Context, Result};
use iceberg::spec::{TableMetadata, ManifestList, Manifest};
use iceberg::io::FileIO;
use tracing::instrument;
use crate::cli::{TableCommands, SnapshotCmd};
use futures::{stream, Stream, StreamExt};
use async_stream::try_stream;

#[derive(Debug)]
pub enum FileType {
    Metadata,
    ManifestList,
    Manifest,
    Data,
}

#[instrument(skip(file_io))]
pub async fn handle_at_command(file_io: &FileIO, location: &str, command: TableCommands) -> Result<String> {
    let metadata = fetch_metadata(file_io, location).await?;
    handle_table_command(file_io, &metadata, command).await
}

pub async fn handle_table_command(file_io: &FileIO, metadata: &TableMetadata, command: TableCommands) -> Result<String> {
    match command {
        TableCommands::Metadata => handle_metadata(metadata),
        TableCommands::Schemas => handle_schemas(metadata),
        TableCommands::Schema { schema_id } => handle_schema(metadata, &schema_id),
        TableCommands::Snapshots => handle_snapshots(metadata),
        TableCommands::Snapshot { snapshot_id, command } => handle_snapshot(file_io, metadata, &snapshot_id, command).await,
    }
}

fn handle_metadata(metadata: &TableMetadata) -> Result<String> {
    Ok(serde_json::to_string_pretty(metadata)?)
}

fn handle_schemas(metadata: &TableMetadata) -> Result<String> {
    let schemas: Vec<_> = metadata.schemas_iter().collect();
    Ok(serde_json::to_string_pretty(&schemas)?)
}

fn handle_schema(metadata: &TableMetadata, schema_id: &str) -> Result<String> {
    let id = if schema_id == "current" {
        metadata.current_schema_id()
    } else {
        schema_id.parse::<i32>()
            .context("Schema ID must be an integer")?
    };

    let schema = metadata.schema_by_id(id)
        .ok_or_else(|| anyhow::anyhow!("Schema {} not found", id))?;

    Ok(serde_json::to_string_pretty(schema)?)
}

fn handle_snapshots(metadata: &TableMetadata) -> Result<String> {
    let snapshots: Vec<_> = metadata.snapshots().collect();
    Ok(serde_json::to_string_pretty(&snapshots)?)
}

async fn handle_snapshot(file_io: &FileIO, metadata: &TableMetadata, snapshot_id: &str, command: Option<SnapshotCmd>) -> Result<String> {
    let id = if snapshot_id == "current" {
        metadata.current_snapshot_id()
            .ok_or_else(|| anyhow::anyhow!("Table has no current snapshot"))?
    } else {
        snapshot_id.parse::<i64>()
            .context("Snapshot ID must be an integer")?
    };

    let snapshot = metadata.snapshots()
        .find(|s| s.snapshot_id() == id)
        .ok_or_else(|| anyhow::anyhow!("Snapshot {} not found", id))?;

    match command {
        None => Ok(serde_json::to_string_pretty(snapshot)?),
        Some(SnapshotCmd::Files) => {
            let stream = iterate_files(file_io, snapshot, metadata.format_version());
            tokio::pin!(stream);

            while let Some(result) = stream.next().await {
                let (file_type, path) = result?;
                let type_str = match file_type {
                    FileType::Metadata => "metadata",
                    FileType::ManifestList => "manifest-list",
                    FileType::Manifest => "manifest",
                    FileType::Data => "data",
                };
                let record = serde_json::json!({
                    "type": type_str,
                    "path": path
                });
                println!("{}", record);
            }
            // We return an empty string because we've already printed to stdout
            Ok(String::new())
        }
    }
}

#[instrument(skip(file_io))]
fn iterate_files<'a>(
    file_io: &'a FileIO,
    snapshot: &'a iceberg::spec::Snapshot,
    format_version: iceberg::spec::FormatVersion,
) -> impl Stream<Item = Result<(FileType, String)>> + 'a {
    try_stream! {
        let manifest_list_location = snapshot.manifest_list();
        yield (FileType::ManifestList, manifest_list_location.to_string());

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
            yield (FileType::Manifest, manifest_location.clone());

            let manifest_bytes = bytes_result?;
            let manifest = Manifest::parse_avro(manifest_bytes.as_slice())
                .context("Failed to parse manifest")?;

            for entry in manifest.entries() {
                // Only list added or existing files
                if entry.status() == iceberg::spec::ManifestStatus::Added || entry.status() == iceberg::spec::ManifestStatus::Existing {
                    yield (FileType::Data, entry.data_file().file_path().to_string());
                }
            }
        }
    }
}

async fn fetch_bytes(file_io: &FileIO, location: &str) -> Result<Vec<u8>> {
    let input_file = file_io.new_input(location)?;
    let bytes = input_file.read().await?;
    Ok(bytes.to_vec())
}

#[instrument(skip(file_io))]
pub async fn fetch_metadata(file_io: &FileIO, location: &str) -> Result<TableMetadata> {
    let bytes = fetch_bytes(file_io, location).await?;

    let metadata: TableMetadata = serde_json::from_slice(&bytes)
        .context("Failed to parse Iceberg table metadata")?;

    Ok(metadata)
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
            writer.write(bytes::Bytes::from(content.to_string())).await.unwrap();
            writer.close().await.unwrap();
        }

        file_io
    }

    fn minimal_metadata() -> String {
        r#"{
            "format-version": 2,
            "table-uuid": "9c2c0c2c-9c2c-9c2c-9c2c-9c2c0c2c0c2c",
            "location": "s3://bucket/table",
            "last-sequence-number": 1,
            "last-updated-ms": 1600000000000,
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
            "current-snapshot-id": 123,
            "refs": {},
            "snapshots": [
                {
                    "snapshot-id": 123,
                    "sequence-number": 1,
                    "timestamp-ms": 1600000000000,
                    "manifest-list": "s3://bucket/table/snap-123.avro",
                    "summary": { "operation": "append" },
                    "schema-id": 0
                }
            ],
            "snapshot-log": [],
            "metadata-log": []
        }"#.to_string()
    }

    #[tokio::test]
    async fn test_fetch_metadata() -> Result<()> {
        let metadata_json = minimal_metadata();
        let location = "s3://bucket/table/metadata.json";

        // Note: memory backend ignores the scheme (s3://) and treats path as key
        let file_io = create_memory_file_io(vec![(location, &metadata_json)]).await;

        let metadata = fetch_metadata(&file_io, location).await?;

        assert_eq!(metadata.format_version(), iceberg::spec::FormatVersion::V2);
        assert_eq!(metadata.location(), "s3://bucket/table");

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_schema_current() -> Result<()> {
        let metadata_json = minimal_metadata();
        let location = "s3://bucket/table/metadata.json";
        let file_io = create_memory_file_io(vec![(location, &metadata_json)]).await;

        let output = handle_at_command(&file_io, location, TableCommands::Schema { schema_id: "current".to_string() }).await?;

        // Verify output contains schema fields
        assert!(output.contains("\"name\": \"id\""));
        assert!(output.contains("\"type\": \"int\""));

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_snapshot_current() -> Result<()> {
        let metadata_json = minimal_metadata();
        let location = "s3://bucket/table/metadata.json";
        let file_io = create_memory_file_io(vec![(location, &metadata_json)]).await;

        let output = handle_at_command(&file_io, location, TableCommands::Snapshot { snapshot_id: "current".to_string(), command: None }).await?;

        // Verify output contains snapshot details
        assert!(output.contains("\"snapshot-id\": 123"));
        assert!(output.contains("\"operation\": \"append\""));

        Ok(())
    }
}
