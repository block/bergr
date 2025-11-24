use crate::cli::CatalogCommands;
use crate::terminal_output::TerminalOutput;
use anyhow::Result;
use futures::stream;
use iceberg::{Catalog, NamespaceIdent};
use serde::Serialize;
use std::collections::HashMap;
use std::io::Write;

pub async fn handle_catalog_command<W: Write>(
    catalog: &dyn Catalog,
    command: CatalogCommands,
    output: &mut TerminalOutput<W>,
) -> Result<()> {
    use crate::cli::NamespaceCmd;

    match command {
        CatalogCommands::Namespaces => list_namespaces(catalog, output).await,
        CatalogCommands::Namespace { name, command } => match command {
            None => get_namespace(catalog, &name, output).await,
            Some(NamespaceCmd::Tables) => list_tables_in_namespace(catalog, &name, output).await,
        },
    }
}

async fn list_namespaces<W: Write>(
    catalog: &dyn Catalog,
    output: &mut TerminalOutput<W>,
) -> Result<()> {
    let namespaces = catalog.list_namespaces(None).await?;

    let namespace_stream = stream::iter(namespaces.into_iter().map(|ns| {
        let namespace = ns.as_ref().join(".");
        Ok(namespace)
    }));

    output.display_stream(namespace_stream).await
}

#[derive(Debug, Serialize)]
struct NamespaceInfo {
    name: String,
    properties: HashMap<String, String>,
}

async fn get_namespace<W: Write>(
    catalog: &dyn Catalog,
    name: &str,
    output: &mut TerminalOutput<W>,
) -> Result<()> {
    // Parse namespace name (e.g., "db.schema" -> ["db", "schema"])
    let parts: Vec<String> = name.split('.').map(String::from).collect();
    let namespace_ident = NamespaceIdent::from_vec(parts)?;

    let namespace = catalog.get_namespace(&namespace_ident).await?;

    let info = NamespaceInfo {
        name: namespace.name().as_ref().join("."),
        properties: namespace.properties().clone(),
    };

    output.display_object(&info)
}

async fn list_tables_in_namespace<W: Write>(
    catalog: &dyn Catalog,
    name: &str,
    output: &mut TerminalOutput<W>,
) -> Result<()> {
    // Parse namespace name (e.g., "db.schema" -> ["db", "schema"])
    let parts: Vec<String> = name.split('.').map(String::from).collect();
    let namespace_ident = NamespaceIdent::from_vec(parts)?;

    let tables = catalog.list_tables(&namespace_ident).await?;

    let table_stream = stream::iter(tables.into_iter().map(|table_ident| {
        // Format as "namespace.table"
        let full_name = format!(
            "{}.{}",
            table_ident.namespace().as_ref().join("."),
            table_ident.name()
        );
        Ok(full_name)
    }));

    output.display_stream(table_stream).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::memory::MEMORY_CATALOG_WAREHOUSE;
    use iceberg::{CatalogBuilder, MemoryCatalog, NamespaceIdent};
    use std::collections::HashMap;

    async fn create_memory_catalog() -> Result<MemoryCatalog> {
        let mut props = HashMap::new();
        props.insert(
            MEMORY_CATALOG_WAREHOUSE.to_string(),
            "memory://".to_string(),
        );

        let catalog = iceberg::memory::MemoryCatalogBuilder::default()
            .load("test", props)
            .await?;

        Ok(catalog)
    }

    #[tokio::test]
    async fn test_list_namespaces_empty() -> Result<()> {
        let catalog = create_memory_catalog().await?;

        let mut buffer = Vec::new();
        let mut output = TerminalOutput::with_writer(&mut buffer);

        handle_catalog_command(&catalog, CatalogCommands::Namespaces, &mut output).await?;

        let output_str = String::from_utf8(buffer)?;
        assert_eq!(output_str, "");

        Ok(())
    }

    #[tokio::test]
    async fn test_list_namespaces_with_data() -> Result<()> {
        let catalog = create_memory_catalog().await?;

        // Create namespaces
        catalog
            .create_namespace(&NamespaceIdent::new("default".to_string()), HashMap::new())
            .await?;
        catalog
            .create_namespace(
                &NamespaceIdent::new("analytics".to_string()),
                HashMap::new(),
            )
            .await?;

        let mut buffer = Vec::new();
        let mut output = TerminalOutput::with_writer(&mut buffer);

        handle_catalog_command(&catalog, CatalogCommands::Namespaces, &mut output).await?;

        let output_str = String::from_utf8(buffer)?;
        let lines: Vec<&str> = output_str.lines().collect();

        assert_eq!(lines.len(), 2);

        let mut namespaces: Vec<String> = lines
            .iter()
            .map(|line| serde_json::from_str(line))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        // Sort namespaces for order-independent comparison
        namespaces.sort();
        assert_eq!(namespaces, vec!["analytics", "default"]);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_namespace() -> Result<()> {
        let catalog = create_memory_catalog().await?;

        // Create namespace with properties
        let mut props = HashMap::new();
        props.insert("owner".to_string(), "data-team".to_string());
        props.insert("description".to_string(), "Production data".to_string());

        catalog
            .create_namespace(&NamespaceIdent::new("production".to_string()), props)
            .await?;

        let mut buffer = Vec::new();
        let mut output = TerminalOutput::with_writer(&mut buffer);

        handle_catalog_command(
            &catalog,
            CatalogCommands::Namespace {
                name: "production".to_string(),
                command: None,
            },
            &mut output,
        )
        .await?;

        let output_str = String::from_utf8(buffer)?;

        // Should output namespace properties as JSON
        let namespace: serde_json::Value = serde_json::from_str(&output_str)?;
        assert_eq!(namespace["name"], "production");
        assert_eq!(namespace["properties"]["owner"], "data-team");
        assert_eq!(namespace["properties"]["description"], "Production data");

        Ok(())
    }

    #[tokio::test]
    async fn test_list_tables_in_namespace() -> Result<()> {
        let catalog = create_memory_catalog().await?;

        // Create namespace
        let namespace_ident = NamespaceIdent::new("analytics".to_string());
        catalog
            .create_namespace(&namespace_ident, HashMap::new())
            .await?;

        // Create tables using TableCreation
        use iceberg::TableCreation;
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()?;

        catalog
            .create_table(
                &namespace_ident,
                TableCreation::builder()
                    .name("events".to_string())
                    .schema(schema.clone())
                    .build(),
            )
            .await?;

        catalog
            .create_table(
                &namespace_ident,
                TableCreation::builder()
                    .name("users".to_string())
                    .schema(schema)
                    .build(),
            )
            .await?;

        let mut buffer = Vec::new();
        let mut output = TerminalOutput::with_writer(&mut buffer);

        handle_catalog_command(
            &catalog,
            CatalogCommands::Namespace {
                name: "analytics".to_string(),
                command: Some(crate::cli::NamespaceCmd::Tables),
            },
            &mut output,
        )
        .await?;

        let output_str = String::from_utf8(buffer)?;
        let lines: Vec<&str> = output_str.lines().collect();

        assert_eq!(lines.len(), 2);

        // Parse and verify table names (order may vary)
        let mut tables: Vec<String> = lines
            .iter()
            .map(|line| serde_json::from_str(line))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        tables.sort();

        assert_eq!(tables, vec!["analytics.events", "analytics.users"]);

        Ok(())
    }
}
