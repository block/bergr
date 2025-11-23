# Apache Iceberg Rust

The `iceberg-rust` project is the official Rust implementation of the Apache Iceberg open table format. It provides a native Rust API for interacting with Iceberg tables, catalogs, and data.

## Project Structure

The project is organized as a workspace with multiple crates:

- **`iceberg`**: The core crate containing the Iceberg specification implementation, key abstractions (Table, Catalog, FileIO), and common utilities.
- **`iceberg-catalog-rest`**: Implementation of the REST Catalog protocol.
- **`iceberg-catalog-glue`**: Implementation for AWS Glue Data Catalog.
- **`iceberg-catalog-hms`**: Implementation for Hive Metastore.
- **`iceberg-datafusion`**: Integration with the Apache Arrow DataFusion query engine.

## Key Abstractions

### 1. Catalog (`iceberg::Catalog`)
The `Catalog` trait is the entry point for accessing Iceberg tables. It defines operations to:
- List and manage namespaces.
- Load, create, and drop tables.

Common implementations include `RestCatalog`, `GlueCatalog`, and `MemoryCatalog`.

### 2. Table (`iceberg::table::Table`)
Represents an Iceberg table. It provides high-level methods to:
- Inspect metadata (`metadata()`).
- Create scans (`scan()`).
- Perform transactions (`new_transaction()`).

### 3. FileIO (`iceberg::io::FileIO`)
`FileIO` is the abstraction for reading and writing files in the underlying storage (S3, GCS, Azure, Local). It handles:
- **Abstraction**: Code doesn't need to know if it's reading from S3 or a local disk.
- **Configuration**: Handles credentials and region settings (e.g., for S3).
- **Input/Output**: Returns `InputFile` and `OutputFile` structs for reading/writing.

**Relevance to `bergr`**: `bergr` currently uses `aws-sdk-s3` directly for some operations. adopting `FileIO` would allow `bergr` to support any storage backend supported by `iceberg-rust` (GCS, Azure, etc.) without changing code.

### 4. Metadata Specifications (`iceberg::spec`)
This module contains the Rust structs that map directly to the Iceberg JSON/Avro specifications.

- **`TableMetadata`**: The parsed content of `metadata.json`. Contains schemas, partition specs, and the snapshot log.
- **`Snapshot`**: Represents a specific state of the table. Contains a reference to the manifest list.
- **`ManifestList`**: Parsed content of the manifest list file (Avro). Lists manifest files.
- **`Manifest`**: Parsed content of a manifest file (Avro). Lists data files.
- **`Schema`**: The schema of the table (fields, types, IDs).

### 5. Table Scan (`iceberg::scan::TableScan`)
A builder API for configuring a read operation on the table. It allows:
- Filtering data (push-down predicates).
- Selecting columns (projection).
- Selecting a specific snapshot.

## Relevance to `bergr`

`bergr` is a tool for *inspecting* Iceberg tables, often at a level lower than what a standard query engine does.

- **Current State**: `bergr` seems to be manually parsing some metadata and using `aws-sdk-s3` directly.
- **Opportunity**: `bergr` can leverage `iceberg-rust`'s `spec` module to handle the complex parsing of Avro manifests and JSON metadata, ensuring correctness and compliance with the spec.
- **Hybrid Approach**: `bergr` might use `iceberg::table::Table` for high-level inspection but drop down to `iceberg::spec` types when displaying raw details of manifests or metadata files.
