use crate::cli::CatalogCommands;
use crate::terminal_output::TerminalOutput;
use anyhow::Result;
use futures::stream;
use iceberg::Catalog;
use std::io::Write;

pub async fn handle_catalog_command<W: Write>(
    catalog: &dyn Catalog,
    command: CatalogCommands,
    output: &mut TerminalOutput<W>,
) -> Result<()> {
    match command {
        CatalogCommands::Namespaces => list_namespaces(catalog, output).await,
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
}
