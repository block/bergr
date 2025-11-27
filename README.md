# bergr

`bergr` is a lightweight CLI for inspecting [Apache Iceberg](https://iceberg.apache.org/) tables.

## Features

- **Multiple catalog types**: AWS Glue, REST catalogs, or direct file access
- **Inspect tables**: View metadata, schemas, snapshots, and data files
- **Explore catalogs**: List namespaces and tables

## Installation

```bash
cargo install --path .
```

## Usage

### AWS Glue Data Catalog

```bash
# Inspect a table
bergr glue table my_database.my_table metadata
bergr glue table my_database.my_table snapshots
bergr glue table my_database.my_table snapshot current info
bergr glue table my_database.my_table schemas

# List the files in a table (optionally checking they actually exist)
bergr glue table my_database.my_table snapshot current files
bergr glue table my_database.my_table snapshot current files --verify

# List databases and tables
bergr glue namespaces
bergr glue namespace my_database info
bergr glue namespace my_database tables
```

### REST catalog

```bash
# Inspect a table
bergr rest http://localhost:8181 table my_namespace.my_table ...

# List namespaces and tables
bergr rest http://localhost:8181 namespaces
bergr rest http://localhost:8181 namespace my_namespace info
bergr rest http://localhost:8181 namespace my_namespace tables
```

### Direct access via metadata file location

```bash
bergr from s3://bucket/path/to/metadata.json metadata
bergr from s3://bucket/path/to/metadata.json snapshots
bergr from s3://bucket/path/to/metadata.json snapshot current info
```

## License

[Apache License, Version 2.0](LICENSE)
