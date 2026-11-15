//! AWS integration utilities for credential loading

use crate::error::ExpectedError;
use crate::rest::rest_catalog_props;
use crate::sigv4::{SigV4AuthManager, SigningScope};
use anyhow::{Context as _, Result};
use aws_config::meta::credentials::CredentialsProviderChain;
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use iceberg::CatalogBuilder;
use iceberg::io::{
    CLIENT_REGION, FileIO, FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS,
    S3_REGION, S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN, Storage, StorageConfig, StorageFactory,
};
use iceberg_catalog_glue::{
    AWS_ACCESS_KEY_ID, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN,
    GLUE_CATALOG_PROP_WAREHOUSE, GlueCatalog, GlueCatalogBuilder,
};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder};
use iceberg_storage_opendal::{
    AwsCredential, CustomAwsCredentialLoader, OpenDalStorageFactory, ProvideCredential,
};
use reqsign_core::{Context, Error as ReqsignError, ErrorKind, Result as ReqsignResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Build a custom credentials provider chain that only uses Environment and Profile providers.
/// This explicitly excludes IMDS, ECS, and Web Identity Token providers.
fn build_credentials_provider() -> SharedCredentialsProvider {
    let chain = CredentialsProviderChain::first_try(
        "Environment",
        aws_config::environment::credentials::EnvironmentVariableCredentialsProvider::new(),
    )
    .or_else(
        "Profile",
        aws_config::profile::credentials::ProfileFileCredentialsProvider::builder().build(),
    );

    SharedCredentialsProvider::new(chain)
}

/// Adapts an AWS SDK credential provider to the `ProvideCredential` trait
/// used by iceberg-storage-opendal (via reqsign). This allows the iceberg
/// storage layer to dynamically refresh credentials rather than using static
/// strings.
#[derive(Debug)]
struct SdkCredentialLoader {
    provider: SharedCredentialsProvider,
}

impl ProvideCredential for SdkCredentialLoader {
    type Credential = AwsCredential;

    async fn provide_credential(&self, _: &Context) -> ReqsignResult<Option<AwsCredential>> {
        let creds = self.provider.provide_credentials().await.map_err(|e| {
            ReqsignError::new(
                ErrorKind::CredentialInvalid,
                "failed to load AWS credentials",
            )
            .with_source(e)
        })?;
        Ok(Some(AwsCredential {
            access_key_id: creds.access_key_id().to_string(),
            secret_access_key: creds.secret_access_key().to_string(),
            session_token: creds.session_token().map(|s| s.to_string()),
            expires_in: None,
        }))
    }
}

/// Build a `CustomAwsCredentialLoader` from the SDK config's credential provider.
fn credential_loader(aws_config: &aws_config::SdkConfig) -> Option<CustomAwsCredentialLoader> {
    aws_config
        .credentials_provider()
        .map(|provider| CustomAwsCredentialLoader::new(SdkCredentialLoader { provider }))
}

/// Build an `OpenDalStorageFactory` for S3 with optional dynamic credentials.
fn s3_storage_factory(aws_config: &aws_config::SdkConfig) -> Arc<OpenDalStorageFactory> {
    Arc::new(OpenDalStorageFactory::S3 {
        customized_credential_load: credential_loader(aws_config),
    })
}

/// S3 storage that reads a table's files with credentials vended by the catalog, when the table's
/// config has them, and with bergr's own AWS credentials otherwise.
#[derive(Debug, Serialize, Deserialize)]
struct VendedFirstS3StorageFactory {
    /// Process-local, so not serialised; a deserialised factory has no fallback.
    #[serde(skip)]
    fallback: Option<CustomAwsCredentialLoader>,
}

#[typetag::serde(name = "BergrVendedFirstS3StorageFactory")]
impl StorageFactory for VendedFirstS3StorageFactory {
    fn build(&self, config: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
        OpenDalStorageFactory::S3 {
            customized_credential_load: storage_credential_loader(
                config.props(),
                self.fallback.as_ref(),
            ),
        }
        .build(config)
    }
}

/// A custom loader replaces opendal's whole credential chain, including any keys in the table's
/// config, so leave it out when the catalog has vended keys.
fn storage_credential_loader(
    props: &HashMap<String, String>,
    fallback: Option<&CustomAwsCredentialLoader>,
) -> Option<CustomAwsCredentialLoader> {
    fallback
        .cloned()
        .filter(|_| !props.contains_key(S3_ACCESS_KEY_ID))
}

/// The S3 credentials a catalog vended for a table, if any, from the table's storage properties.
fn vended_credentials(props: &HashMap<String, String>) -> Option<Credentials> {
    Some(Credentials::new(
        props.get(S3_ACCESS_KEY_ID)?,
        props.get(S3_SECRET_ACCESS_KEY)?,
        props.get(S3_SESSION_TOKEN).cloned(),
        None,
        "catalog",
    ))
}

/// An S3 client that uses the keys a catalog vended for a table, if it vended any, configured from
/// the table's storage properties the way opendal reads them.
pub fn vended_s3_client(props: &HashMap<String, String>) -> Option<aws_sdk_s3::Client> {
    let credentials = vended_credentials(props)?;
    let region = props
        .get(CLIENT_REGION)
        .or_else(|| props.get(S3_REGION))
        .map(|region| Region::new(region.clone()));
    let path_style = props
        .get(S3_PATH_STYLE_ACCESS)
        .is_some_and(|value| ["true", "t", "1", "on"].contains(&value.to_lowercase().as_str()));

    let mut config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .credentials_provider(credentials)
        .region(region)
        .force_path_style(path_style);
    config.set_endpoint_url(props.get(S3_ENDPOINT).cloned());
    Some(aws_sdk_s3::Client::from_conf(config.build()))
}

pub async fn get_aws_config() -> Result<aws_config::SdkConfig> {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .credentials_provider(build_credentials_provider())
        .load()
        .await;

    check_credentials(&config).await?;
    Ok(config)
}

/// Resolve credentials now, so a missing or broken configuration fails early with a clear error.
pub async fn check_credentials(aws_config: &aws_config::SdkConfig) -> Result<()> {
    let Some(provider) = aws_config.credentials_provider() else {
        return Ok(());
    };
    provider
        .provide_credentials()
        .await
        .map(|_| ())
        .map_err(|err| {
            ExpectedError::UserInput(format!(
                "AWS credentials not configured: {:#}",
                anyhow::Error::new(err)
            ))
            .into()
        })
}

pub fn s3_file_io(aws_config: &aws_config::SdkConfig) -> FileIO {
    let mut builder = FileIOBuilder::new(s3_storage_factory(aws_config));

    if let Some(region) = aws_config.region() {
        builder = builder.with_prop(S3_REGION, region.to_string());
    }

    builder.build()
}

pub async fn glue_catalog(aws_config: &aws_config::SdkConfig) -> Result<GlueCatalog> {
    let mut props = HashMap::new();

    // Required warehouse (not actually used for read-only ops)
    props.insert(
        GLUE_CATALOG_PROP_WAREHOUSE.to_string(),
        "s3://iceberg-warehouse".to_string(),
    );

    if let Some(region) = aws_config.region() {
        props.insert(AWS_REGION_NAME.to_string(), region.to_string());
    }

    if let Some(creds_provider) = aws_config.credentials_provider()
        && let Ok(creds) = creds_provider.provide_credentials().await
    {
        props.insert(
            AWS_ACCESS_KEY_ID.to_string(),
            creds.access_key_id().to_string(),
        );
        props.insert(
            AWS_SECRET_ACCESS_KEY.to_string(),
            creds.secret_access_key().to_string(),
        );

        if let Some(session_token) = creds.session_token() {
            props.insert(AWS_SESSION_TOKEN.to_string(), session_token.to_string());
        }
    }

    let catalog = GlueCatalogBuilder::default()
        .with_storage_factory(s3_storage_factory(aws_config))
        .load("glue", props)
        .await?;

    Ok(catalog)
}

/// Create a REST catalog hosted by AWS Glue, with SigV4-signed requests
pub async fn aws_rest_catalog(
    aws_config: &aws_config::SdkConfig,
    scope: SigningScope,
    uri: &str,
    warehouse: Option<&str>,
) -> Result<RestCatalog> {
    let credentials = aws_config
        .credentials_provider()
        .context("AWS credentials not configured")?;

    let mut props = rest_catalog_props(uri, warehouse);
    props.insert(S3_REGION.to_string(), scope.region.clone());

    let catalog = RestCatalogBuilder::default()
        .with_auth_manager(SigV4AuthManager::new(scope, credentials))
        .with_storage_factory(Arc::new(VendedFirstS3StorageFactory {
            fallback: credential_loader(aws_config),
        }))
        .load("rest", props)
        .await?;

    Ok(catalog)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_config::{BehaviorVersion, Region};
    use aws_credential_types::Credentials;

    async fn test_aws_config() -> aws_config::SdkConfig {
        let creds = Credentials::new(
            "test_access_key",
            "test_secret_key",
            Some("test_session_token".to_string()),
            None,
            "test",
        );

        aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new("us-west-2"))
            .credentials_provider(creds)
            .load()
            .await
    }

    #[derive(Debug)]
    struct NoCredentials;

    impl ProvideCredentials for NoCredentials {
        fn provide_credentials<'a>(
            &'a self,
        ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
        where
            Self: 'a,
        {
            aws_credential_types::provider::future::ProvideCredentials::ready(Err(
                aws_credential_types::provider::error::CredentialsError::not_loaded(
                    "no credentials here",
                ),
            ))
        }
    }

    #[tokio::test]
    async fn check_credentials_reports_missing_credentials_as_expected_error() {
        let aws_config = aws_config::defaults(BehaviorVersion::latest())
            .credentials_provider(NoCredentials)
            .load()
            .await;

        let err = check_credentials(&aws_config).await.unwrap_err();

        let expected = err
            .downcast_ref::<ExpectedError>()
            .expect("should be an ExpectedError");
        assert!(
            expected.to_string().contains("AWS credentials"),
            "unexpected message: {expected}"
        );
    }

    #[tokio::test]
    async fn check_credentials_accepts_working_credentials() {
        let aws_config = test_aws_config().await;

        assert!(check_credentials(&aws_config).await.is_ok());
    }

    fn vended_props() -> HashMap<String, String> {
        HashMap::from([
            (S3_ACCESS_KEY_ID.to_string(), "vended-key".to_string()),
            (
                S3_SECRET_ACCESS_KEY.to_string(),
                "vended-secret".to_string(),
            ),
            (S3_SESSION_TOKEN.to_string(), "vended-token".to_string()),
        ])
    }

    #[tokio::test]
    async fn storage_uses_no_custom_loader_when_the_catalog_vends_keys() {
        let fallback = credential_loader(&test_aws_config().await);

        assert!(storage_credential_loader(&vended_props(), fallback.as_ref()).is_none());
    }

    #[tokio::test]
    async fn storage_falls_back_to_bergr_credentials_without_vended_keys() {
        let fallback = credential_loader(&test_aws_config().await);

        assert!(storage_credential_loader(&HashMap::new(), fallback.as_ref()).is_some());
    }

    #[test]
    fn no_vended_s3_client_without_vended_keys() {
        assert!(vended_s3_client(&HashMap::new()).is_none());
    }

    #[test]
    fn vended_credentials_come_from_the_table_props() {
        let credentials = vended_credentials(&vended_props()).unwrap();

        assert_eq!(credentials.access_key_id(), "vended-key");
        assert_eq!(credentials.secret_access_key(), "vended-secret");
        assert_eq!(credentials.session_token(), Some("vended-token"));
    }

    #[test]
    fn vended_s3_client_prefers_client_region_as_opendal_does() {
        let mut props = vended_props();
        props.insert(S3_REGION.to_string(), "us-east-1".to_string());
        props.insert(CLIENT_REGION.to_string(), "us-west-2".to_string());

        let client = vended_s3_client(&props).unwrap();

        assert_eq!(
            client.config().region().map(|r| r.as_ref()),
            Some("us-west-2")
        );
    }

    #[tokio::test]
    async fn test_s3_file_io_sets_region() {
        let aws_config = test_aws_config().await;
        let file_io = s3_file_io(&aws_config);
        let props = file_io.config().props();

        assert_eq!(props.get(S3_REGION), Some(&"us-west-2".to_string()));
    }

    #[tokio::test]
    async fn test_sdk_credential_loader_returns_credentials() {
        let aws_config = test_aws_config().await;
        let provider = aws_config
            .credentials_provider()
            .expect("should have a credentials provider");
        let loader = SdkCredentialLoader { provider };

        let cred = loader
            .provide_credential(&Context::new())
            .await
            .expect("should load credential")
            .expect("should have credential");

        assert_eq!(cred.access_key_id, "test_access_key");
        assert_eq!(cred.secret_access_key, "test_secret_key");
        assert_eq!(cred.session_token, Some("test_session_token".to_string()));
    }

    #[tokio::test]
    async fn test_glue_catalog_with_aws_config() -> Result<()> {
        let aws_config = test_aws_config().await;
        let catalog = glue_catalog(&aws_config).await?;
        let file_io = catalog.file_io();
        let props = file_io.config().props();

        assert_eq!(props.get(S3_REGION), Some(&"us-west-2".to_string()));

        Ok(())
    }
}
