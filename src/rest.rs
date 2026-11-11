//! REST catalog integration utilities

use crate::basic_auth::BasicAuthManager;
use crate::rest_auth::HttpAuth;
use anyhow::Result;
use iceberg::CatalogBuilder;
use iceberg_catalog_rest::{
    AUTH_TYPE_NONE, AUTH_TYPE_OAUTH2, REST_CATALOG_PROP_AUTH_TYPE, REST_CATALOG_PROP_URI,
    REST_CATALOG_PROP_WAREHOUSE, RestCatalog, RestCatalogBuilder,
};
use std::collections::HashMap;

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
/// # Arguments
///
/// * `uri` - The REST catalog endpoint URL (e.g., "http://localhost:8181")
/// * `warehouse` - Optional warehouse location (e.g., "s3://my-bucket/warehouse")
/// * `auth` - How to authenticate requests to the catalog
///
/// # Returns
///
/// A configured `RestCatalog` instance
pub async fn rest_catalog(
    uri: &str,
    warehouse: Option<&str>,
    auth: &HttpAuth,
) -> Result<RestCatalog> {
    let mut props = rest_catalog_props(uri, warehouse);
    props.extend(auth_props(auth));

    let builder = match auth {
        HttpAuth::Basic { user, password } => {
            RestCatalogBuilder::default().with_auth_manager(BasicAuthManager::new(user, password)?)
        }
        _ => RestCatalogBuilder::default(),
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
        let result = rest_catalog(uri, warehouse, &HttpAuth::None).await;

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
        let result = rest_catalog(uri, None, &HttpAuth::None).await;

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
            let result = rest_catalog(uri, None, &HttpAuth::None).await;
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

        let catalog = rest_catalog(&server.url(), None, auth).await?;
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
}
