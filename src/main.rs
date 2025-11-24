use anyhow::Result;
use clap::Parser;
use bergr::aws::SdkConfigCredentialLoader;
use bergr::cli::{Cli, Commands};
use bergr::table_commands::handle_table_command;
use bergr::terminal_output::TerminalOutput;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};
use iceberg::io::{CustomAwsCredentialLoader, FileIO};
use aws_config::BehaviorVersion;
use std::sync::Arc;

async fn build_file_io(location: &str) -> Result<FileIO> {
    let mut builder = FileIO::from_path(location)?;

    if location.starts_with("s3://") {
        let aws_config = aws_config::load_defaults(BehaviorVersion::latest()).await;

        if let Some(region) = aws_config.region() {
            builder = builder.with_prop("s3.region", region.to_string());
        }

        let sdk_loader = SdkConfigCredentialLoader::from(aws_config);
        let credential_loader = CustomAwsCredentialLoader::new(Arc::new(sdk_loader));
        builder = builder.with_extension(credential_loader);
    }

    Ok(builder.build()?)
}

#[tokio::main]
async fn main() -> Result<()> {
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
            let file_io = build_file_io(&location).await?;
            let mut output = TerminalOutput::new();
            handle_table_command(&file_io, &location, command, &mut output).await?;
        }
    }

    Ok(())
}
