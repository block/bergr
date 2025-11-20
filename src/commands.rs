use anyhow::{Context, Result};
use iceberg::spec::TableMetadata;
use url::Url;
use tracing::{debug, instrument};
use crate::cli::AtCommands;

#[instrument(skip(client))]
pub async fn handle_at_command(client: &aws_sdk_s3::Client, location: &str, command: AtCommands) -> Result<String> {
    let metadata = fetch_metadata(client, location).await?;

    let output = match command {
        AtCommands::Metadata => serde_json::to_string_pretty(&metadata)?,
        AtCommands::Schemas => {
            let schemas: Vec<_> = metadata.schemas_iter().collect();
            serde_json::to_string_pretty(&schemas)?
        },
        AtCommands::Schema { schema_id } => {
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
        AtCommands::Snapshots => {
            let snapshots: Vec<_> = metadata.snapshots().collect();
            serde_json::to_string_pretty(&snapshots)?
        },
        AtCommands::Snapshot { snapshot_id } => {
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

            serde_json::to_string_pretty(snapshot)?
        }
    };

    Ok(output)
}

#[instrument(skip(client))]
pub async fn fetch_metadata(client: &aws_sdk_s3::Client, location: &str) -> Result<TableMetadata> {
    // We use the `url` crate to parse the S3 URI.
    let url = Url::parse(location).context("Failed to parse URL")?;

    if url.scheme() != "s3" {
        anyhow::bail!("Only s3:// locations are currently supported");
    }

    let bucket = url.host_str().context("Missing bucket in S3 URL")?;
    let key = url.path().trim_start_matches('/');

    debug!(bucket, key, "fetching metadata from s3");

    let response = client.get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .with_context(|| format!("Failed to fetch S3 object at s3://{}/{}", bucket, key))?;

    debug!("successfully fetched metadata, reading body");

    let bytes = response.body.collect().await?.into_bytes();

    let metadata: TableMetadata = serde_json::from_slice(&bytes)
        .context("Failed to parse Iceberg table metadata")?;

    debug!("successfully parsed table metadata");

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

        let output = handle_at_command(&client, location, AtCommands::Schema { schema_id: "current".to_string() }).await?;
        
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

        let output = handle_at_command(&client, location, AtCommands::Snapshot { snapshot_id: "current".to_string() }).await?;

        // Verify output contains snapshot details
        assert!(output.contains("\"snapshot-id\": 123"));
        assert!(output.contains("\"operation\": \"append\""));

        Ok(())
    }
}
