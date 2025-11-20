use anyhow::Result;
use aws_config::BehaviorVersion;
use clap::Parser;
use bergr::cli::{Cli, Commands, AtCommands};
use bergr::commands::fetch_metadata;

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
