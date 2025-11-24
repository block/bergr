use crate::cli::CatalogCommands;
use crate::terminal_output::TerminalOutput;
use anyhow::Result;
use futures::stream;
use iceberg::Catalog;
use serde::Serialize;
use std::io::Write;

#[derive(Debug, Serialize, serde::Deserialize)]
struct NamespaceInfo {
    namespace: Vec<String>,
}

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
        Ok(NamespaceInfo {
            namespace: ns.as_ref().iter().map(|s| s.to_string()).collect(),
        })
    }));

    output.display_stream(namespace_stream).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::NamespaceIdent;
    use iceberg::table::Table;
    use std::collections::HashMap;

    // Mock catalog for testing
    #[derive(Debug)]
    struct MockCatalog {
        namespaces: Vec<NamespaceIdent>,
    }

    #[async_trait::async_trait]
    impl Catalog for MockCatalog {
        async fn list_namespaces(
            &self,
            _parent: Option<&NamespaceIdent>,
        ) -> iceberg::Result<Vec<NamespaceIdent>> {
            Ok(self.namespaces.clone())
        }

        async fn create_namespace(
            &self,
            _namespace: &NamespaceIdent,
            _properties: HashMap<String, String>,
        ) -> iceberg::Result<iceberg::Namespace> {
            unimplemented!()
        }

        async fn get_namespace(
            &self,
            _namespace: &NamespaceIdent,
        ) -> iceberg::Result<iceberg::Namespace> {
            unimplemented!()
        }

        async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> iceberg::Result<bool> {
            unimplemented!()
        }

        async fn update_namespace(
            &self,
            _namespace: &NamespaceIdent,
            _properties: HashMap<String, String>,
        ) -> iceberg::Result<()> {
            unimplemented!()
        }

        async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<()> {
            unimplemented!()
        }

        async fn list_tables(
            &self,
            _namespace: &NamespaceIdent,
        ) -> iceberg::Result<Vec<iceberg::TableIdent>> {
            unimplemented!()
        }

        async fn create_table(
            &self,
            _namespace: &NamespaceIdent,
            _creation: iceberg::TableCreation,
        ) -> iceberg::Result<Table> {
            unimplemented!()
        }

        async fn load_table(&self, _table: &iceberg::TableIdent) -> iceberg::Result<Table> {
            unimplemented!()
        }

        async fn register_table(
            &self,
            _table: &iceberg::TableIdent,
            _metadata_file_location: String,
        ) -> iceberg::Result<Table> {
            unimplemented!()
        }

        async fn drop_table(&self, _table: &iceberg::TableIdent) -> iceberg::Result<()> {
            unimplemented!()
        }

        async fn table_exists(&self, _table: &iceberg::TableIdent) -> iceberg::Result<bool> {
            unimplemented!()
        }

        async fn rename_table(
            &self,
            _src: &iceberg::TableIdent,
            _dest: &iceberg::TableIdent,
        ) -> iceberg::Result<()> {
            unimplemented!()
        }

        async fn update_table(&self, _commit: iceberg::TableCommit) -> iceberg::Result<Table> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn test_list_namespaces_empty() -> Result<()> {
        let catalog = MockCatalog { namespaces: vec![] };

        let mut buffer = Vec::new();
        let mut output = TerminalOutput::with_writer(&mut buffer);

        handle_catalog_command(&catalog, CatalogCommands::Namespaces, &mut output).await?;

        let output_str = String::from_utf8(buffer)?;
        assert_eq!(output_str, "");

        Ok(())
    }

    #[tokio::test]
    async fn test_list_namespaces_with_data() -> Result<()> {
        let catalog = MockCatalog {
            namespaces: vec![
                NamespaceIdent::new("default".to_string()),
                NamespaceIdent::new("analytics".to_string()),
            ],
        };

        let mut buffer = Vec::new();
        let mut output = TerminalOutput::with_writer(&mut buffer);

        handle_catalog_command(&catalog, CatalogCommands::Namespaces, &mut output).await?;

        let output_str = String::from_utf8(buffer)?;
        let lines: Vec<&str> = output_str.lines().collect();

        assert_eq!(lines.len(), 2);

        // Parse and verify each line
        let ns1: NamespaceInfo = serde_json::from_str(lines[0])?;
        assert_eq!(ns1.namespace, vec!["default"]);

        let ns2: NamespaceInfo = serde_json::from_str(lines[1])?;
        assert_eq!(ns2.namespace, vec!["analytics"]);

        Ok(())
    }
}
