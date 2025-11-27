use anyhow::Result;
use bergr::aws::{get_aws_config, glue_catalog, s3_file_io};
use bergr::catalog_commands::handle_catalog_command;
use bergr::cli::{Cli, Commands};
use bergr::error::ExpectedError;
use bergr::rest::rest_catalog;
use bergr::table_commands::{handle_table_command, load_table};
use bergr::terminal_output::TerminalOutput;
use clap::Parser;
use iceberg::io::FileIO;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

async fn build_file_io(location: &str) -> Result<FileIO> {
    if location.starts_with("s3://") || location.starts_with("s3a://") {
        let aws_config = get_aws_config().await;
        return s3_file_io(&aws_config).await;
    }

    Ok(FileIO::from_path(location)?.build()?)
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    if cli.debug {
        tracing_subscriber::registry()
            .with(fmt::layer().with_writer(std::io::stderr))
            .with(
                EnvFilter::builder()
                    .with_default_directive(tracing::Level::INFO.into())
                    .from_env_lossy()
                    .add_directive("bergr=debug".parse().unwrap())
                    // AWS SDK tracing for Glue, S3, and HTTP operations
                    .add_directive("aws_sdk_glue=debug".parse().unwrap())
                    .add_directive("aws_sdk_s3=debug".parse().unwrap())
                    .add_directive("aws_smithy_runtime=debug".parse().unwrap()),
            )
            .init();
    }

    if let Err(err) = run(cli.command).await {
        // Check if this is a wrapped ExpectedError (expected user-facing error)
        if let Some(expected_error) = err.downcast_ref::<ExpectedError>() {
            eprintln!("ERROR: {expected_error}");
            std::process::exit(1);
        } else if cli.debug {
            // Debug mode: show full error chain
            eprintln!("ERROR: {err:?}");
            std::process::exit(2);
        } else {
            // Normal mode: show top-level message with hint
            eprintln!("ERROR: {err}");
            eprintln!("       (use --debug for more details)");
            std::process::exit(2);
        }
    }
}

async fn run(command: Commands) -> Result<()> {
    match command {
        Commands::From { location, command } => {
            let file_io = build_file_io(&location).await?;
            let table = load_table(&file_io, &location).await?;
            let mut output = TerminalOutput::new();
            handle_table_command(&table, command, &mut output).await?;
        }
        Commands::Glue { command } => {
            let aws_config = get_aws_config().await;
            let catalog = glue_catalog(&aws_config).await?;
            let mut output = TerminalOutput::new();
            handle_catalog_command(&catalog, command, &mut output).await?;
        }
        Commands::Rest {
            uri,
            warehouse,
            command,
        } => {
            let catalog = rest_catalog(&uri, warehouse.as_deref()).await?;
            let mut output = TerminalOutput::new();
            handle_catalog_command(&catalog, command, &mut output).await?;
        }
    }

    Ok(())
}
