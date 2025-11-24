use anyhow::Result;
use clap::Parser;
use bergr::aws::s3_file_io;
use bergr::cli::{Cli, Commands};
use bergr::table_commands::handle_table_command;
use bergr::terminal_output::TerminalOutput;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};
use iceberg::io::FileIO;

async fn build_file_io(location: &str) -> Result<FileIO> {
    if location.starts_with("s3://") {
        return s3_file_io().await;
    }

    Ok(FileIO::from_path(location)?.build()?)
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
