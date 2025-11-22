# Iceberg REST Catalog API

The Iceberg REST Catalog API defines a standard protocol for interacting with Iceberg catalogs over HTTP. It decouples the catalog implementation from the client, allowing any Iceberg client to interact with any catalog that implements this protocol.

`bergr` models its operations and terminology on this API to ensure consistency and familiarity.

## Core Concepts

*   **Catalog**: The central service that manages metadata for Iceberg tables.
*   **Namespace**: A logical grouping of tables, similar to a "database" or "schema" in traditional RDBMS. Namespaces can be hierarchical (e.g., `accounting.tax.paid`).
*   **Table**: The fundamental unit of data storage in Iceberg, consisting of metadata and data files.
*   **Prefix**: An optional configuration usually used to specify a specific catalog or scope within a REST endpoint (e.g., `/v1/{prefix}/...`).

## Read Operations

As `bergr` is primarily a read-only inspection tool, we focus on the operations for discovering and reading metadata.

### Namespaces

Namespaces organize tables. Operations allow navigating the hierarchy.

#### List Namespaces
*   **Endpoint**: `GET /v1/{prefix}/namespaces`
*   **Parameters**:
    *   `parent` (optional): Filter to show only namespaces under a specific parent. If omitted, lists top-level namespaces.
    *   `pageToken` (optional): For pagination.
    *   `pageSize` (optional): Number of results per page.
*   **Response**: A list of namespace identifiers.

#### Load Namespace Metadata
*   **Endpoint**: `GET /v1/{prefix}/namespaces/{namespace}`
*   **Description**: Retrieves metadata about a specific namespace (e.g., properties like owner, location).
*   **Response**: Namespace details and properties.

#### Check Namespace Exists
*   **Endpoint**: `HEAD /v1/{prefix}/namespaces/{namespace}`
*   **Description**: Efficiently check if a namespace exists without fetching full metadata.

### Tables

Tables are the leaf nodes containing actual data.

#### List Tables
*   **Endpoint**: `GET /v1/{prefix}/namespaces/{namespace}/tables`
*   **Description**: Lists all table identifiers within a specific namespace.
*   **Parameters**:
    *   `pageToken` (optional): For pagination.
    *   `pageSize` (optional): Number of results per page.
*   **Response**: A list of table identifiers.

#### Load Table
*   **Endpoint**: `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}`
*   **Description**: Retrieves the full metadata for a table. This includes the schema, partition spec, snapshot history, and current snapshot details.
*   **Response**: `LoadTableResponse` containing the `metadata.json` content and config.

#### Check Table Exists
*   **Endpoint**: `HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}`
*   **Description**: Efficiently check if a table exists.

## Terminology Mapping

`bergr` uses the following terms to align with the REST API:

| Term | Description |
| :--- | :--- |
| **Namespace** | Used instead of "Database" or "Schema". Represents a grouping of tables. |
| **Table** | An Iceberg table. |
| **Identifier** | A unique name for a table, usually composed of a namespace and a table name (e.g., `marketing.campaigns`). |
