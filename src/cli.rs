use clap::{Parser, Subcommand};

/// bergr: A tool for inspecting Apache Iceberg tables
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Operate on a table at a specific location
    At {
        /// The location of the table or metadata file (e.g., s3://bucket/path/to/metadata.json)
        location: String,

        #[command(subcommand)]
        command: AtCommands,
    },
}

#[derive(Subcommand)]
pub enum AtCommands {
    /// Print the table metadata
    Metadata,
}
