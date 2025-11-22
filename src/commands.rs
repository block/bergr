use anyhow::{Context, Result};
use iceberg::spec::{TableMetadata, ManifestList, Manifest};
use url::Url;
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

#[instrument(skip(client))]
pub async fn handle_at_command(client: &aws_sdk_s3::Client, location: &str, command: TableCommands) -> Result<String> {
    let metadata = fetch_metadata(client, location).await?;

    let output = match command {
        TableCommands::Metadata => serde_json::to_string_pretty(&metadata)?,
        TableCommands::Schemas => {
            let schemas: Vec<_> = metadata.schemas_iter().collect();
            serde_json::to_string_pretty(&schemas)?
        },
        TableCommands::Schema { schema_id } => {
            let id = if schema_id == "current" {
                metadata.current_schema_id()
            } else {
                schema_id.parse::<i32>()
                    .context("Schema ID must be an integer")?
            };

            let schema = metadata.schema_by_id(id)
                .ok_or_else(|| anyhow::anyhow!("Schema {} not found", id))?;

            serde_json::to_string_pretty(schema)?
        },
        TableCommands::Snapshots => {
            let snapshots: Vec<_> = metadata.snapshots().collect();
            serde_json::to_string_pretty(&snapshots)?
        },
        TableCommands::Snapshot { snapshot_id, command } => {
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
                None => serde_json::to_string_pretty(snapshot)?,
                Some(SnapshotCmd::Files) => {
                    println!("metadata {}", location);
                    let stream = iterate_files(client, snapshot, metadata.format_version());
                    tokio::pin!(stream);

                    while let Some(result) = stream.next().await {
                        let (file_type, path) = result?;
                        let type_str = match file_type {
                            FileType::Metadata => "metadata",
                            FileType::ManifestList => "manifest-list",
                            FileType::Manifest => "manifest",
                            FileType::Data => "data",
                        };
                        println!("{} {}", type_str, path);
                    }
                    // We return an empty string because we've already printed to stdout
                    String::new()
                }
            }
        }
    };

    Ok(output)
}

#[instrument(skip(client))]
fn iterate_files<'a>(
    client: &'a aws_sdk_s3::Client,
    snapshot: &'a iceberg::spec::Snapshot,
    format_version: iceberg::spec::FormatVersion,
) -> impl Stream<Item = Result<(FileType, String)>> + 'a {
    try_stream! {
        let manifest_list_location = snapshot.manifest_list();
        yield (FileType::ManifestList, manifest_list_location.to_string());

        let manifest_list_bytes = fetch_bytes(client, manifest_list_location).await?;
        let manifest_list = ManifestList::parse_with_version(&manifest_list_bytes, format_version)
            .context("Failed to parse manifest list")?;

        let tasks = manifest_list.entries().iter().map(|manifest_file| {
            let manifest_location = manifest_file.manifest_path.clone();
            let client = client.clone();
            async move {
                let bytes_result = fetch_bytes(&client, &manifest_location).await;
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

async fn fetch_bytes(client: &aws_sdk_s3::Client, location: &str) -> Result<Vec<u8>> {
    // We use the `url` crate to parse the S3 URI.
    let url = Url::parse(location).context("Failed to parse URL")?;

    if url.scheme() != "s3" {
        anyhow::bail!("Only s3:// locations are currently supported");
    }

    let bucket = url.host_str().context("Missing bucket in S3 URL")?;
    let key = url.path().trim_start_matches('/');

    let response = client.get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .with_context(|| format!("Failed to fetch S3 object at s3://{}/{}", bucket, key))?;

    Ok(response.body.collect().await?.into_bytes().to_vec())
}

#[instrument(skip(client))]
pub async fn fetch_metadata(client: &aws_sdk_s3::Client, location: &str) -> Result<TableMetadata> {
    let bytes = fetch_bytes(client, location).await?;

    let metadata: TableMetadata = serde_json::from_slice(&bytes)
        .context("Failed to parse Iceberg table metadata")?;

    Ok(metadata)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_smithy_runtime::client::http::test_util::StaticReplayClient;
    use aws_sdk_s3::config::Region;
    use aws_credential_types::Credentials;
    use aws_config::BehaviorVersion;

    fn create_mock_client(metadata_json: &str) -> aws_sdk_s3::Client {
        let http_client = StaticReplayClient::new(vec![
            aws_smithy_runtime::client::http::test_util::ReplayEvent::new(
                http::Request::builder()
                    .uri("https://s3.us-east-1.amazonaws.com/bucket/table/metadata.json")
                    .body(aws_sdk_s3::primitives::SdkBody::empty())
                    .unwrap(),
                http::Response::builder()
                    .status(200)
                    .body(aws_sdk_s3::primitives::SdkBody::from(metadata_json.to_string()))
                    .unwrap(),
            )
        ]);

        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .http_client(http_client)
            .credentials_provider(Credentials::for_tests())
            .build();

        aws_sdk_s3::Client::from_conf(config)
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
        let client = create_mock_client(&metadata_json);
        let location = "s3://bucket/table/metadata.json";

        let metadata = fetch_metadata(&client, location).await?;

        assert_eq!(metadata.format_version(), iceberg::spec::FormatVersion::V2);
        assert_eq!(metadata.location(), "s3://bucket/table");

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_schema_current() -> Result<()> {
        let metadata_json = minimal_metadata();
        let client = create_mock_client(&metadata_json);
        let location = "s3://bucket/table/metadata.json";

        let output = handle_at_command(&client, location, TableCommands::Schema { schema_id: "current".to_string() }).await?;

        // Verify output contains schema fields
        assert!(output.contains("\"name\": \"id\""));
        assert!(output.contains("\"type\": \"int\""));

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_snapshot_current() -> Result<()> {
        let metadata_json = minimal_metadata();
        let client = create_mock_client(&metadata_json);
        let location = "s3://bucket/table/metadata.json";

        let output = handle_at_command(&client, location, TableCommands::Snapshot { snapshot_id: "current".to_string(), command: None }).await?;

        // Verify output contains snapshot details
        assert!(output.contains("\"snapshot-id\": 123"));
        assert!(output.contains("\"operation\": \"append\""));

        Ok(())
    }
}
