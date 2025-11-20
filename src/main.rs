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
                    handle_metadata(&location).await?;
                }
            }
        }
    }

    Ok(())
}

async fn handle_metadata(location: &str) -> Result<()> {
    // We use the `url` crate to parse the S3 URI.
    // This helps us easily extract the bucket and key (path).
    let url = Url::parse(location).context("Failed to parse URL")?;

    if url.scheme() != "s3" {
        anyhow::bail!("Only s3:// locations are currently supported");
    }

    let bucket = url.host_str().context("Missing bucket in S3 URL")?;
    // `url.path()` returns the path with a leading slash, which S3 doesn't want for the key.
    let key = url.path().trim_start_matches('/');

    // Load AWS credentials and configuration from the environment (e.g., ~/.aws/credentials, env vars).
    // `BehaviorVersion::latest()` ensures we use the latest recommended defaults.
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    
    // Create an S3 client using the loaded configuration.
    let s3_client = aws_sdk_s3::Client::new(&config);

    // Fetch the object from S3.
    // We use `context` to add more meaning to the error if it fails.
    let response = s3_client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .with_context(|| format!("Failed to fetch S3 object at s3://{}/{}", bucket, key))?;

    // Read the body of the response into a byte vector.
    let bytes = response.body.collect().await?.into_bytes();

    // Parse the bytes into an Iceberg TableMetadata struct.
    // This validates that the file is indeed a valid Iceberg metadata file.
    let metadata: TableMetadata = serde_json::from_slice(&bytes)
        .context("Failed to parse Iceberg table metadata")?;

    // Print the metadata as formatted JSON.
    println!("{}", serde_json::to_string_pretty(&metadata)?);

    Ok(())
}
