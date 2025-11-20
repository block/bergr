use clap::{Parser, Subcommand};

/// bergr: A tool for inspecting Apache Iceberg tables
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    /// Enable debug logging
    #[arg(long, global = true)]
    pub debug: bool,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Operate on a table at a specific location
    At {
        /// The location of the table or metadata file (e.g., s3://bucket/path/to/metadata.json)
        location: String,

        #[command(subcommand)]
        command: AtCommands,
    },
}

#[derive(Subcommand, Debug)]
pub enum AtCommands {
    /// Print the table metadata
    Metadata,
    /// Print the current table schema
    Schema,
    /// List all snapshots
    Snapshots,
    /// Inspect a specific snapshot
    Snapshot {
        /// The snapshot ID, or "current"
        snapshot_id: String,
    },
}
