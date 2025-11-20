# bergr

`bergr` is a lightweight utility for inspecting [Apache Iceberg](https://iceberg.apache.org/) tables.

It provides both a Command Line Interface (CLI) and a RESTful API to explore your Iceberg catalogs, namespaces, and tables.

## Features

- **Explore Catalogs**: List namespaces and tables.
- **Inspect Tables**: View table metadata, schemas, and file lists.
- **REST API**: Serve table information over HTTP for integration with other tools or for easy browsing.
- **CLI**: Quick access to table details directly from your terminal.

## Installation

(Coming soon)

## Usage

### CLI

```bash
# List namespaces
bergr namespaces

# List tables in a namespace
bergr namespace <namespace> tables

# Inspect a table
bergr namespace <namespace> table <table> metadata
bergr namespace <namespace> table <table> schema
bergr namespace <namespace> table <table> files
```

### Server

Start the REST API server:

```bash
bergr serve --port 8080
```

Endpoints:
- `GET /namespaces`
- `GET /namespace/{namespace}/tables`
- `GET /namespace/{namespace}/table/{table}`
- `GET /namespace/{namespace}/table/{table}/metadata`
- `GET /namespace/{namespace}/table/{table}/schema`
- `GET /namespace/{namespace}/table/{table}/files`

## License

[Apache License, Version 2.0](LICENSE)
