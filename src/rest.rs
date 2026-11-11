//! REST catalog integration utilities

use crate::basic_auth::BasicAuthManager;
use crate::rest_auth::HttpAuth;
use anyhow::Result;
use clap::ValueEnum;
use iceberg::CatalogBuilder;
use iceberg_catalog_rest::{
    AUTH_TYPE_NONE, AUTH_TYPE_OAUTH2, REST_CATALOG_PROP_AUTH_TYPE, REST_CATALOG_PROP_URI,
    REST_CATALOG_PROP_WAREHOUSE, RestCatalog, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use std::collections::HashMap;
use std::sync::Arc;

/// iceberg-catalog-rest sends `header.*` properties as request headers
const ACCESS_DELEGATION_HEADER_PROP: &str = "header.X-Iceberg-Access-Delegation";

/// Whether to ask the catalog for credentials to read table files with (the Iceberg REST
/// `X-Iceberg-Access-Delegation` header)
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AccessDelegation {
    /// Ask the catalog to vend short-lived storage credentials with each table
    #[default]
    VendedCredentials,
    /// Tell the catalog that bergr doesn't need delegated access, and read table files with your
    /// own storage credentials (e.g. AWS environment variables or profile)
    None,
}

impl AccessDelegation {
    fn header_value(self) -> &'static str {
        match self {
            AccessDelegation::VendedCredentials => "vended-credentials",
            AccessDelegation::None => "none",
        }
    }
}

/// Catalog properties for a REST catalog with the given URI and optional warehouse
pub fn rest_catalog_props(uri: &str, warehouse: Option<&str>) -> HashMap<String, String> {
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

    props
}

/// iceberg-catalog-rest properties that configure its built-in authentication
fn auth_props(auth: &HttpAuth) -> HashMap<String, String> {
    let entries: Vec<(&str, Option<&str>)> = match auth {
        HttpAuth::None => vec![(REST_CATALOG_PROP_AUTH_TYPE, Some(AUTH_TYPE_NONE))],
        HttpAuth::Bearer { token } => vec![
            (REST_CATALOG_PROP_AUTH_TYPE, Some(AUTH_TYPE_OAUTH2)),
            ("token", Some(token.expose())),
        ],
        HttpAuth::OAuth2 {
            credential,
            scope,
            server_uri,
        } => vec![
            (REST_CATALOG_PROP_AUTH_TYPE, Some(AUTH_TYPE_OAUTH2)),
            ("credential", Some(credential.expose())),
            ("scope", scope.as_deref()),
            ("oauth2-server-uri", server_uri.as_deref()),
        ],
        HttpAuth::Basic { .. } => vec![],
    };
    entries
        .into_iter()
        .filter_map(|(key, value)| value.map(|v| (key.to_string(), v.to_string())))
        .collect()
}

/// Create a REST catalog with the given URI, optional warehouse location, and authentication
///
/// Table files are read with the storage settings, and any credentials, that the catalog returns
/// for each table.
///
/// # Arguments
///
/// * `uri` - The REST catalog endpoint URL (e.g., "http://localhost:8181")
/// * `warehouse` - Optional warehouse location (e.g., "s3://my-bucket/warehouse")
/// * `auth` - How to authenticate requests to the catalog
/// * `access_delegation` - Whether to ask the catalog for vended storage credentials
///
/// # Returns
///
/// A configured `RestCatalog` instance
pub async fn rest_catalog(
    uri: &str,
    warehouse: Option<&str>,
    auth: &HttpAuth,
    access_delegation: AccessDelegation,
) -> Result<RestCatalog> {
    let mut props = rest_catalog_props(uri, warehouse);
    props.extend(auth_props(auth));
    props.insert(
        ACCESS_DELEGATION_HEADER_PROP.to_string(),
        access_delegation.header_value().to_string(),
    );

    let builder = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()));
    let builder = match auth {
        HttpAuth::Basic { user, password } => {
            builder.with_auth_manager(BasicAuthManager::new(user, password)?)
        }
        _ => builder,
    };

    Ok(builder.load("rest", props).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::Catalog;
    use mockito::{Matcher, Server, ServerGuard};

    #[tokio::test]
    async fn test_rest_catalog_builder() -> Result<()> {
        // This test verifies that we can construct the catalog builder correctly
        // The REST catalog builder doesn't actually connect during initialization,
        // so we can verify the builder works even without a real server

        let uri = "http://localhost:8181";
        let warehouse = Some("s3://test-warehouse");

        // This should succeed - the catalog is created lazily
        let result =
            rest_catalog(uri, warehouse, &HttpAuth::None, AccessDelegation::default()).await;

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
        let result = rest_catalog(uri, None, &HttpAuth::None, AccessDelegation::default()).await;

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
            let result =
                rest_catalog(uri, None, &HttpAuth::None, AccessDelegation::default()).await;
            assert!(
                result.is_ok(),
                "Catalog creation should succeed for URI {}: {:?}",
                uri,
                result.err()
            );
        }

        Ok(())
    }

    /// Expect `GET /v1/config` and `GET /v1/namespaces`, each with the given `Authorization`
    /// header, then list namespaces with `auth`.
    async fn list_namespaces_expecting(
        server: &mut ServerGuard,
        auth: &HttpAuth,
        authorization: Matcher,
    ) -> Result<()> {
        let config = server
            .mock("GET", "/v1/config")
            .match_query(Matcher::Any)
            .match_header("authorization", authorization.clone())
            .with_body(r#"{"defaults": {}, "overrides": {}}"#)
            .create_async()
            .await;
        let namespaces = server
            .mock("GET", "/v1/namespaces")
            .match_header("authorization", authorization)
            .with_body(r#"{"namespaces": []}"#)
            .create_async()
            .await;

        let catalog = rest_catalog(&server.url(), None, auth, AccessDelegation::default()).await?;
        catalog.list_namespaces(None).await?;

        config.assert_async().await;
        namespaces.assert_async().await;
        Ok(())
    }

    #[tokio::test]
    async fn none_sends_no_authorization() -> Result<()> {
        let mut server = Server::new_async().await;
        list_namespaces_expecting(&mut server, &HttpAuth::None, Matcher::Missing).await
    }

    #[tokio::test]
    async fn bearer_sends_token() -> Result<()> {
        let mut server = Server::new_async().await;
        let auth = HttpAuth::Bearer {
            token: "tok".to_string().into(),
        };
        list_namespaces_expecting(&mut server, &auth, "Bearer tok".into()).await
    }

    #[tokio::test]
    async fn basic_sends_credentials() -> Result<()> {
        let mut server = Server::new_async().await;
        let auth = HttpAuth::Basic {
            user: "alice".into(),
            password: "s3cret".to_string().into(),
        };
        list_namespaces_expecting(&mut server, &auth, "Basic YWxpY2U6czNjcmV0".into()).await
    }

    #[tokio::test]
    async fn oauth2_exchanges_credential_for_token() -> Result<()> {
        let mut server = Server::new_async().await;
        let tokens = server
            .mock("POST", "/v1/oauth/tokens")
            .match_body(Matcher::AllOf(vec![
                Matcher::UrlEncoded("grant_type".into(), "client_credentials".into()),
                Matcher::UrlEncoded("client_id".into(), "id".into()),
                Matcher::UrlEncoded("client_secret".into(), "secret".into()),
                Matcher::UrlEncoded("scope".into(), "PRINCIPAL_ROLE:ALL".into()),
            ]))
            .with_body(
                r#"{"access_token": "exchanged", "token_type": "bearer",
                    "issued_token_type": "urn:ietf:params:oauth:token-type:access_token"}"#,
            )
            .create_async()
            .await;
        let auth = HttpAuth::OAuth2 {
            credential: "id:secret".to_string().into(),
            scope: Some("PRINCIPAL_ROLE:ALL".into()),
            server_uri: None,
        };

        list_namespaces_expecting(&mut server, &auth, "Bearer exchanged".into()).await?;
        tokens.assert_async().await;
        Ok(())
    }

    const TABLE_METADATA: &str = r#"{
        "format-version": 2,
        "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
        "location": "s3://bucket/db/events",
        "last-sequence-number": 0,
        "last-updated-ms": 1700000000000,
        "last-column-id": 1,
        "current-schema-id": 0,
        "schemas": [{"type": "struct", "schema-id": 0,
                     "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]}],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}]
    }"#;

    #[tokio::test]
    async fn load_table_requests_and_uses_vended_credentials() -> Result<()> {
        let mut server = Server::new_async().await;
        server
            .mock("GET", "/v1/config")
            .match_query(Matcher::Any)
            .with_body(r#"{"defaults": {}, "overrides": {}}"#)
            .create_async()
            .await;
        let load_table = server
            .mock("GET", "/v1/namespaces/db/tables/events")
            .match_header("x-iceberg-access-delegation", "vended-credentials")
            .with_body(format!(
                r#"{{"metadata-location": "s3://bucket/db/events/metadata/00000.metadata.json",
                    "metadata": {TABLE_METADATA},
                    "config": {{"s3.access-key-id": "vended-key", "s3.secret-access-key": "vended-secret"}}}}"#
            ))
            .create_async()
            .await;

        let catalog = rest_catalog(
            &server.url(),
            None,
            &HttpAuth::None,
            AccessDelegation::default(),
        )
        .await?;
        let table = catalog
            .load_table(&iceberg::TableIdent::from_strs(["db", "events"])?)
            .await?;

        load_table.assert_async().await;
        assert_eq!(
            table.file_io().config().props().get("s3.access-key-id"),
            Some(&"vended-key".to_string())
        );
        Ok(())
    }

    #[tokio::test]
    async fn access_delegation_none_is_sent_as_none() -> Result<()> {
        let mut server = Server::new_async().await;
        let config = server
            .mock("GET", "/v1/config")
            .match_query(Matcher::Any)
            .match_header("x-iceberg-access-delegation", "none")
            .with_body(r#"{"defaults": {}, "overrides": {}}"#)
            .create_async()
            .await;
        let load_table = server
            .mock("GET", "/v1/namespaces/db/tables/events")
            .match_header("x-iceberg-access-delegation", "none")
            .with_body(format!(
                r#"{{"metadata-location": "s3://bucket/db/events/metadata/00000.metadata.json",
                    "metadata": {TABLE_METADATA}}}"#
            ))
            .create_async()
            .await;

        let catalog =
            rest_catalog(&server.url(), None, &HttpAuth::None, AccessDelegation::None).await?;
        catalog
            .load_table(&iceberg::TableIdent::from_strs(["db", "events"])?)
            .await?;

        config.assert_async().await;
        load_table.assert_async().await;
        Ok(())
    }
}
