//! HTTP basic authentication for REST catalogs, which iceberg-catalog-rest doesn't provide

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use http::HeaderValue;
use http::header::AUTHORIZATION;
use iceberg::sensitive::SensitiveString;
use iceberg::{Error, ErrorKind, Result};
use iceberg_catalog_rest::{AuthManager, AuthSession, HttpClient, HttpRequest};
use std::collections::HashMap;
use std::sync::Arc;

/// An [`AuthManager`] that adds an `Authorization: Basic …` header to every request.
#[derive(Debug, Clone)]
pub struct BasicAuthManager {
    session: Arc<BasicAuthSession>,
}

impl BasicAuthManager {
    pub fn new(user: &str, password: &SensitiveString) -> Result<Self> {
        let credentials = STANDARD.encode(format!("{user}:{}", password.expose()));
        let mut header = HeaderValue::try_from(format!("Basic {credentials}")).map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "invalid basic auth header").with_source(e)
        })?;
        header.set_sensitive(true);
        Ok(Self {
            session: Arc::new(BasicAuthSession { header }),
        })
    }
}

#[async_trait]
impl AuthManager for BasicAuthManager {
    async fn init_session(
        &self,
        _client: &HttpClient,
        _props: &HashMap<String, String>,
    ) -> Result<Box<dyn AuthSession>> {
        Ok(Box::new(self.session.as_ref().clone()))
    }

    async fn catalog_session(
        &self,
        _client: &HttpClient,
        _props: &HashMap<String, String>,
    ) -> Result<Arc<dyn AuthSession>> {
        Ok(self.session.clone())
    }
}

/// Holds the header value marked sensitive, so `Debug` output doesn't show it.
#[derive(Debug, Clone)]
struct BasicAuthSession {
    header: HeaderValue,
}

#[async_trait]
impl AuthSession for BasicAuthSession {
    async fn authenticate(&self, request: &mut HttpRequest) -> Result<()> {
        request
            .headers_mut()
            .insert(AUTHORIZATION, self.header.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manager() -> BasicAuthManager {
        BasicAuthManager::new("alice", &"s3cret".to_string().into()).unwrap()
    }

    #[tokio::test]
    async fn adds_basic_auth_header() {
        let mut request = HttpRequest::new(
            reqwest::Client::new()
                .get("https://catalog.example.com/v1/config")
                .build()
                .unwrap(),
        );
        manager().session.authenticate(&mut request).await.unwrap();

        // "alice:s3cret", base64-encoded
        assert_eq!(request.headers()[AUTHORIZATION], "Basic YWxpY2U6czNjcmV0");
    }

    #[test]
    fn debug_output_redacts_credentials() {
        let debug = format!("{:?}", manager());
        assert!(!debug.contains("YWxpY2U6czNjcmV0"), "{debug}");
    }
}
