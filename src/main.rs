use anyhow::Result;
use aws_config::BehaviorVersion;
use clap::Parser;
use bergr::cli::{Cli, Commands};
use bergr::commands::handle_at_command;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command line arguments
    let cli = Cli::parse();

    if cli.debug {
        tracing_subscriber::registry()
            .with(fmt::layer().with_writer(std::io::stderr))
            .with(
                EnvFilter::builder()
                    .with_default_directive(tracing::Level::INFO.into())
                    .from_env_lossy()
                    .add_directive("bergr=debug".parse().unwrap()),
            )
            .init();
    }

    match cli.command {
        Commands::At { location, command } => {
            let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
            let s3_client = aws_sdk_s3::Client::new(&config);
            
            let output = handle_at_command(&s3_client, &location, command).await?;
            println!("{}", output);
        }
    }

    Ok(())
}
