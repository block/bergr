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
    match command {
        CatalogCommands::Namespaces => list_namespaces(catalog, output).await,
        CatalogCommands::Namespace { name } => get_namespace(catalog, &name, output).await,
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
}
