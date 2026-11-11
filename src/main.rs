use anyhow::Result;
use aws_config::Region;
use bergr::aws::{aws_rest_catalog, get_aws_config, glue_catalog, s3_file_io};
use bergr::catalog_commands::handle_catalog_command;
use bergr::cli::{Cli, Commands};
use bergr::error::ExpectedError;
use bergr::rest::rest_catalog;
use bergr::rest_auth::RestAuth;
use bergr::table_commands::{handle_table_command, load_table};
use bergr::terminal_output::TerminalOutput;
use clap::Parser;
use iceberg::io::FileIO;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

/// Logged at debug level with `--debug`: bergr itself, plus the AWS SDK's Glue, S3 and HTTP operations.
const DEBUG_TARGETS: [&str; 4] = ["bergr", "aws_sdk_glue", "aws_sdk_s3", "aws_smithy_runtime"];
#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    if cli.debug {
        let filter = DEBUG_TARGETS.iter().fold(
            EnvFilter::builder()
                .with_default_directive(tracing::Level::INFO.into())
                .from_env_lossy(),
            |filter, target| {
                filter.add_directive(
                    format!("{target}=debug")
                        .parse()
                        .expect("debug targets are valid directives"),
                )
            },
        );
        tracing_subscriber::registry()
            .with(fmt::layer().with_writer(std::io::stderr))
            .with(filter)
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
            let is_s3 = location.starts_with("s3://") || location.starts_with("s3a://");
            let (file_io, s3) = if is_s3 {
                let aws_config = get_aws_config().await?;
                (
                    s3_file_io(&aws_config),
                    Some(aws_sdk_s3::Client::new(&aws_config)),
                )
            } else {
                (FileIO::new_with_fs(), None)
            };
            let table = load_table(&file_io, &location).await?;
            let mut output = TerminalOutput::new();
            handle_table_command(&table, command, &mut output, s3.as_ref()).await?;
        }
        Commands::Glue { command } => {
            let aws_config = get_aws_config().await?;
            let catalog = glue_catalog(&aws_config).await?;
            let s3 = aws_sdk_s3::Client::new(&aws_config);
            let mut output = TerminalOutput::new();
            handle_catalog_command(&catalog, command, &mut output, Some(&s3)).await?;
        }
        Commands::Rest {
            uri,
            warehouse,
            auth,
            access_delegation,
            command,
        } => {
            let mut output = TerminalOutput::new();
            match RestAuth::resolve(auth, &uri, |name| std::env::var(name).ok())? {
                RestAuth::SigV4(scope) => {
                    let aws_config = get_aws_config()
                        .await?
                        .to_builder()
                        .region(Region::new(scope.region.clone()))
                        .build();
                    let catalog =
                        aws_rest_catalog(&aws_config, scope, &uri, warehouse.as_deref()).await?;
                    let s3 = aws_sdk_s3::Client::new(&aws_config);
                    handle_catalog_command(&catalog, command, &mut output, Some(&s3)).await?;
                }
                RestAuth::Http(auth) => {
                    let catalog =
                        rest_catalog(&uri, warehouse.as_deref(), &auth, access_delegation).await?;
                    handle_catalog_command(&catalog, command, &mut output, None).await?;
                }
            }
        }
    }

    Ok(())
}
