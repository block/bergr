//! AWS integration utilities for credential loading

use anyhow::Result;
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_config::meta::credentials::CredentialsProviderChain;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use iceberg::CatalogBuilder;
use iceberg::io::{FileIO, FileIOBuilder, S3_REGION};
use iceberg_catalog_glue::{
    AWS_ACCESS_KEY_ID, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN,
    GLUE_CATALOG_PROP_WAREHOUSE, GlueCatalog, GlueCatalogBuilder,
};
use iceberg_storage_opendal::{
    AwsCredential, AwsCredentialLoad, CustomAwsCredentialLoader, OpenDalStorageFactory,
};
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

/// Adapts an AWS SDK credential provider to the `AwsCredentialLoad` trait
/// used by iceberg-storage-opendal. This allows the iceberg storage layer
/// to dynamically refresh credentials rather than using static strings.
struct SdkCredentialLoader {
    provider: SharedCredentialsProvider,
}

#[async_trait]
impl AwsCredentialLoad for SdkCredentialLoader {
    async fn load_credential(&self, _: reqwest::Client) -> anyhow::Result<Option<AwsCredential>> {
        let creds = self.provider.provide_credentials().await?;
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
        .map(|provider| CustomAwsCredentialLoader::new(Arc::new(SdkCredentialLoader { provider })))
}

/// Build an `OpenDalStorageFactory` for S3 with optional dynamic credentials.
fn s3_storage_factory(aws_config: &aws_config::SdkConfig) -> Arc<OpenDalStorageFactory> {
    Arc::new(OpenDalStorageFactory::S3 {
        configured_scheme: "s3".to_string(),
        customized_credential_load: credential_loader(aws_config),
    })
}

pub async fn get_aws_config() -> aws_config::SdkConfig {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .credentials_provider(build_credentials_provider())
        .load()
        .await;

    // Try to resolve credentials to detect configuration errors early
    if let Some(creds_provider) = config.credentials_provider() {
        creds_provider
            .provide_credentials()
            .await
            .expect("AWS credentials not configured");
    }

    config
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

    #[tokio::test]
    async fn test_s3_file_io_sets_region() {
        let aws_config = test_aws_config().await;
        let file_io = s3_file_io(&aws_config);
        let props = file_io.config().props();

        assert_eq!(props.get(S3_REGION), Some(&"us-west-2".to_string()));
    }

    #[tokio::test]
    async fn test_credential_loader_returns_credentials() {
        let aws_config = test_aws_config().await;
        let loader = credential_loader(&aws_config).expect("should have a credential loader");

        let cred = loader
            .load_credential(reqwest::Client::new())
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
