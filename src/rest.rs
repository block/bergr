//! REST catalog integration utilities

use anyhow::Result;
use iceberg::CatalogBuilder;
use iceberg_catalog_rest::{
    RestCatalog, RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use std::collections::HashMap;

/// Create a REST catalog with the given URI and optional warehouse location
///
/// # Arguments
///
/// * `uri` - The REST catalog endpoint URL (e.g., "http://localhost:8181")
/// * `warehouse` - Optional warehouse location (e.g., "s3://my-bucket/warehouse")
///
/// # Returns
///
/// A configured `RestCatalog` instance
pub async fn rest_catalog(uri: &str, warehouse: Option<&str>) -> Result<RestCatalog> {
    let mut props = HashMap::new();

    // Required: REST catalog URI
    props.insert(REST_CATALOG_PROP_URI.to_string(), uri.to_string());

    // Optional: warehouse location
    // If not provided, use a default value (some REST catalogs may not require this)
    let warehouse_value = warehouse.unwrap_or("s3://iceberg-warehouse");
    props.insert(
        REST_CATALOG_PROP_WAREHOUSE.to_string(),
        warehouse_value.to_string(),
    );

    let catalog = RestCatalogBuilder::default().load("rest", props).await?;

    Ok(catalog)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rest_catalog_builder() -> Result<()> {
        // This test verifies that we can construct the catalog builder correctly
        // The REST catalog builder doesn't actually connect during initialization,
        // so we can verify the builder works even without a real server

        let uri = "http://localhost:8181";
        let warehouse = Some("s3://test-warehouse");

        // This should succeed - the catalog is created lazily
        let result = rest_catalog(uri, warehouse).await;

        // The catalog should be created successfully
        assert!(
            result.is_ok(),
            "Catalog creation should succeed: {:?}",
            result.err()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_rest_catalog_without_warehouse() -> Result<()> {
        let uri = "http://localhost:8181";

        // Test that it works without an explicit warehouse (uses default)
        let result = rest_catalog(uri, None).await;

        // Should succeed with default warehouse
        assert!(
            result.is_ok(),
            "Catalog creation with default warehouse should succeed: {:?}",
            result.err()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_rest_catalog_with_different_uris() -> Result<()> {
        // Test various URI formats
        let test_cases = vec![
            "http://localhost:8181",
            "https://catalog.example.com",
            "http://192.168.1.1:8080",
        ];

        for uri in test_cases {
            let result = rest_catalog(uri, None).await;
            assert!(
                result.is_ok(),
                "Catalog creation should succeed for URI {}: {:?}",
                uri,
                result.err()
            );
        }

        Ok(())
    }
}
