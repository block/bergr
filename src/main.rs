use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use clap::{Parser, Subcommand};
use iceberg::spec::TableMetadata;
use url::Url;

/// bergr: A tool for inspecting Apache Iceberg tables
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Operate on a table at a specific location
    At {
        /// The location of the table or metadata file (e.g., s3://bucket/path/to/metadata.json)
        location: String,

        #[command(subcommand)]
        command: AtCommands,
    },
}

#[derive(Subcommand)]
enum AtCommands {
    /// Print the table metadata
    Metadata,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command line arguments
    let cli = Cli::parse();

    match cli.command {
        Commands::At { location, command } => {
            match command {
                AtCommands::Metadata => {
                    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
                    let s3_client = aws_sdk_s3::Client::new(&config);
                    let metadata = fetch_metadata(&s3_client, &location).await?;
                    println!("{}", serde_json::to_string_pretty(&metadata)?);
                }
            }
        }
    }

    Ok(())
}

async fn fetch_metadata(client: &aws_sdk_s3::Client, location: &str) -> Result<TableMetadata> {
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

    let bytes = response.body.collect().await?.into_bytes();

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

    #[tokio::test]
    async fn test_fetch_metadata() -> Result<()> {
        // minimal valid iceberg metadata
        let metadata_json = r#"{
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
            "current-snapshot-id": -1,
            "refs": {},
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": []
        }"#;

        let http_client = StaticReplayClient::new(vec![
            aws_smithy_runtime::client::http::test_util::ReplayEvent::new(
                http::Request::builder()
                    .uri("https://s3.us-east-1.amazonaws.com/bucket/table/metadata.json")
                    .body(aws_sdk_s3::primitives::SdkBody::empty())
                    .unwrap(),
                http::Response::builder()
                    .status(200)
                    .body(aws_sdk_s3::primitives::SdkBody::from(metadata_json))
                    .unwrap(),
            )
        ]);

        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .http_client(http_client)
            .credentials_provider(Credentials::for_tests())
            .build();
            
        let client = aws_sdk_s3::Client::from_conf(config);

        let location = "s3://bucket/table/metadata.json";
        let metadata = fetch_metadata(&client, location).await?;

        assert_eq!(metadata.format_version(), iceberg::spec::FormatVersion::V2);
        assert_eq!(metadata.location(), "s3://bucket/table");

        Ok(())
    }
}
