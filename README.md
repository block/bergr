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

AWS Glue's Iceberg REST endpoint is detected from the URI, and requests to it are signed with SigV4 using your AWS credentials. Pass the catalog ID as the warehouse: the AWS account ID, for the default catalog.

```bash
bergr rest https://glue.us-west-2.amazonaws.com/iceberg --warehouse 123456789012 namespaces
```

Glue's REST endpoint needs `glue:GetCatalog` and `lakeformation:GetDataAccess` permissions, in addition to those the `glue` command needs.

#### Authentication

Choose how to authenticate with `--auth`, or set `BERGR_REST_AUTH`. Each type reads its settings, including secrets, from environment variables, so secrets never appear in shell history or `ps`.

| `--auth` | Reads |
|---|---|
| `auto` (default) | Behaves as `sigv4` for AWS Glue endpoints, otherwise `none` |
| `none` | nothing |
| `sigv4` | the standard AWS credential variables and profile |
| `bearer` | `BERGR_REST_BEARER_TOKEN` |
| `basic` | `BERGR_REST_BASIC_USER`, `BERGR_REST_BASIC_PASSWORD` |
| `oauth2` | `BERGR_REST_OAUTH2_CREDENTIAL` (`client_id:secret`); optionally `BERGR_REST_OAUTH2_SCOPE` and `BERGR_REST_OAUTH2_SERVER_URI` |

```bash
BERGR_REST_BEARER_TOKEN="$(cat ~/.config/my-catalog-token)" \
  bergr rest --auth bearer https://catalog.example.com namespaces
```

`bearer` and `oauth2` correspond to the Iceberg REST catalog properties `token`, `credential`, `scope` and `oauth2-server-uri`. `sigv4` currently only supports AWS Glue endpoints (`glue.<region>.amazonaws.com`).

### Direct access via metadata file location

```bash
bergr from s3://bucket/path/to/metadata.json metadata
bergr from s3://bucket/path/to/metadata.json snapshots
bergr from s3://bucket/path/to/metadata.json snapshot current info
```

## License

[Apache License, Version 2.0](LICENSE)
