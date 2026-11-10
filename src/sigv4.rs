//! AWS SigV4 request signing for REST catalogs hosted by AWS Glue
//!
//! A stopgap until iceberg-rust ships its own SigV4 support for REST catalogs. This is a
//! minimal signer: unlike upstream's, it doesn't reproduce Iceberg Java's signing quirks,
//! and has only been tested against Glue.
//!
//! TODO: replace with iceberg-catalog-rest's `SigV4AuthManager` once it's released
//! (https://github.com/apache/iceberg-rust/pull/3092).

use async_trait::async_trait;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use aws_sigv4::http_request::{SignableBody, SignableRequest, SigningSettings, sign};
use aws_sigv4::sign::v4;
use iceberg::{Error, ErrorKind, Result};
use iceberg_catalog_rest::{AuthManager, AuthSession, HttpClient, HttpRequest};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use url::Url;

/// Where (and as which service) requests to an AWS-hosted REST catalog are signed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SigningScope {
    pub service: String,
    pub region: String,
}

impl SigningScope {
    /// Derive the scope from an AWS Glue endpoint URI, e.g.
    /// `https://glue.us-west-2.amazonaws.com/iceberg` -> (`glue`, `us-west-2`).
    ///
    /// Returns `None` for URIs that aren't AWS Glue Iceberg REST endpoints.
    pub fn from_uri(uri: &str) -> Option<Self> {
        let url = Url::parse(uri).ok()?;
        let host = url.host_str()?;
        let labels: Vec<&str> = host.split('.').collect();
        match labels.as_slice() {
            ["glue", region, "amazonaws", "com"] => Some(Self {
                service: "glue".to_string(),
                region: region.to_string(),
            }),
            _ => None,
        }
    }
}

/// An [`AuthManager`] that signs every request with AWS SigV4.
#[derive(Debug, Clone)]
pub struct SigV4AuthManager {
    session: Arc<SigV4Session>,
}

impl SigV4AuthManager {
    pub fn new(scope: SigningScope, credentials: SharedCredentialsProvider) -> Self {
        Self {
            session: Arc::new(SigV4Session { scope, credentials }),
        }
    }
}

#[async_trait]
impl AuthManager for SigV4AuthManager {
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

#[derive(Debug, Clone)]
struct SigV4Session {
    scope: SigningScope,
    credentials: SharedCredentialsProvider,
}

fn unexpected(message: &str, source: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::new(ErrorKind::Unexpected, message.to_string()).with_source(source)
}

impl SigV4Session {
    fn sign_with(
        &self,
        request: &mut HttpRequest,
        identity: &aws_smithy_runtime_api::client::identity::Identity,
        time: SystemTime,
    ) -> Result<()> {
        let body = request.body().as_bytes().ok_or_else(|| {
            Error::new(
                ErrorKind::FeatureUnsupported,
                "cannot SigV4-sign a streaming request body",
            )
        })?;

        let headers = request
            .headers()
            .iter()
            .map(|(name, value)| {
                value
                    .to_str()
                    .map(|v| (name.as_str(), v))
                    .map_err(|e| unexpected("non-UTF-8 header value", e))
            })
            .collect::<Result<Vec<_>>>()?;

        let params = v4::SigningParams::builder()
            .identity(identity)
            .region(&self.scope.region)
            .name(&self.scope.service)
            .time(time)
            .settings(SigningSettings::default())
            .build()
            .map_err(|e| unexpected("invalid SigV4 signing params", e))?
            .into();

        let signable = SignableRequest::new(
            request.method().as_str(),
            request.url_str(),
            headers.into_iter(),
            SignableBody::Bytes(body),
        )
        .map_err(|e| unexpected("request is not signable", e))?;

        let (instructions, _signature) = sign(signable, &params)
            .map_err(|e| unexpected("SigV4 signing failed", e))?
            .into_parts();
        let (signed_headers, _query_params) = instructions.into_parts();

        signed_headers.into_iter().try_for_each(|header| {
            let name: http::HeaderName = header
                .name()
                .parse()
                .map_err(|e| unexpected("invalid signed header name", e))?;
            let value: http::HeaderValue = header
                .value()
                .parse()
                .map_err(|e| unexpected("invalid signed header value", e))?;
            request.headers_mut().insert(name, value);
            Ok(())
        })
    }
}

#[async_trait]
impl AuthSession for SigV4Session {
    async fn authenticate(&self, request: &mut HttpRequest) -> Result<()> {
        let credentials = self
            .credentials
            .provide_credentials()
            .await
            .map_err(|e| unexpected("failed to load AWS credentials", e))?;
        self.sign_with(request, &credentials.into(), SystemTime::now())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_credential_types::Credentials;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn scope_from_glue_uri() {
        assert_eq!(
            SigningScope::from_uri("https://glue.us-west-2.amazonaws.com/iceberg"),
            Some(SigningScope {
                service: "glue".into(),
                region: "us-west-2".into()
            })
        );
    }

    #[test]
    fn scope_from_non_aws_uri() {
        assert_eq!(SigningScope::from_uri("http://localhost:8181"), None);
        assert_eq!(
            SigningScope::from_uri("https://s3.us-west-2.amazonaws.com"),
            None
        );
    }

    fn session() -> SigV4Session {
        SigV4Session {
            scope: SigningScope {
                service: "glue".into(),
                region: "us-west-2".into(),
            },
            credentials: SharedCredentialsProvider::new(Credentials::new(
                "AKIDEXAMPLE",
                "secret",
                Some("token".into()),
                None,
                "test",
            )),
        }
    }

    fn request(method: reqwest::Method, url: &str, body: Option<&'static str>) -> HttpRequest {
        let builder = reqwest::Client::new().request(method, url);
        let builder = match body {
            Some(b) => builder.body(b),
            None => builder,
        };
        HttpRequest::new(builder.build().unwrap())
    }

    #[tokio::test]
    async fn adds_sigv4_headers() {
        let mut req = request(
            reqwest::Method::GET,
            "https://glue.us-west-2.amazonaws.com/iceberg/v1/config?warehouse=123456789012",
            None,
        );
        session().authenticate(&mut req).await.unwrap();

        let headers = req.headers();
        let auth = headers["authorization"].to_str().unwrap();
        assert!(auth.starts_with("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/"));
        assert!(auth.contains("/us-west-2/glue/aws4_request"));
        assert!(headers.contains_key("x-amz-date"));
        assert_eq!(headers["x-amz-security-token"], "token");
    }

    #[test]
    fn signature_covers_body() {
        let identity = Credentials::new("AKIDEXAMPLE", "secret", None, None, "test").into();
        let time = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let url = "https://glue.us-west-2.amazonaws.com/iceberg/v1/catalogs/1/namespaces";
        let sign = |body| {
            let mut req = request(reqwest::Method::POST, url, Some(body));
            session().sign_with(&mut req, &identity, time).unwrap();
            req.headers()["authorization"].clone()
        };

        assert_eq!(sign("{}"), sign("{}"));
        assert_ne!(sign("{}"), sign("{\"x\":1}"));
    }
}
