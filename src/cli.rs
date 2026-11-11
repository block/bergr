use crate::rest::AccessDelegation;
use crate::rest_auth::AuthType;
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
    /// Operate on a table from a specific location
    From {
        /// The location of the table or metadata file (e.g., s3://bucket/path/to/metadata.json)
        location: String,

        #[command(subcommand)]
        command: TableCommands,
    },
    /// Interact with AWS Glue Data Catalog
    Glue {
        #[command(subcommand)]
        command: CatalogCommands,
    },
    /// Interact with a REST catalog
    Rest {
        /// The REST catalog URI (e.g., http://localhost:8181)
        uri: String,

        /// Optional warehouse location (e.g., s3://my-bucket/warehouse)
        #[arg(long)]
        warehouse: Option<String>,

        /// How to authenticate to the catalog. Settings, including secrets, are read from
        /// environment variables, never from arguments.
        #[arg(long, value_enum, env = "BERGR_REST_AUTH", default_value_t = AuthType::Auto)]
        auth: AuthType,

        /// Whether to ask the catalog for credentials to read table files with. Ignored for
        /// AWS Glue, whose table files are read with your AWS credentials.
        #[arg(
            long,
            value_enum,
            env = "BERGR_REST_ACCESS_DELEGATION",
            default_value_t = AccessDelegation::VendedCredentials
        )]
        access_delegation: AccessDelegation,

        #[command(subcommand)]
        command: CatalogCommands,
    },
}

#[derive(Subcommand, Debug)]
pub enum TableCommands {
    /// Print the table metadata
    Metadata,
    /// List all schemas
    Schemas,
    /// Inspect a specific schema
    Schema {
        /// The schema ID, or "current"
        schema_id: String,
    },
    /// List all snapshots
    Snapshots,
    /// Inspect a specific snapshot
    Snapshot {
        /// The snapshot ID, or "current"
        snapshot_id: String,
        #[command(subcommand)]
        command: SnapshotCmd,
    },
}

#[derive(Subcommand, Debug)]
pub enum SnapshotCmd {
    /// Show snapshot details
    Info,
    /// List files in the snapshot
    Files {
        /// Verify that data files exist
        #[arg(long)]
        verify: bool,
    },
}

#[derive(Subcommand, Debug)]
pub enum CatalogCommands {
    /// List namespaces in the catalog
    Namespaces,
    /// Inspect a specific namespace
    Namespace {
        /// The namespace name (e.g., "default" or "db.schema")
        name: String,
        #[command(subcommand)]
        command: NamespaceCmd,
    },
    /// Inspect a specific table
    Table {
        /// The table identifier (e.g., "namespace.table" or "db.schema.table")
        name: String,
        #[command(subcommand)]
        command: TableCommands,
    },
}

#[derive(Subcommand, Debug)]
pub enum NamespaceCmd {
    /// Show namespace details
    Info,
    /// List tables in the namespace
    Tables,
}
