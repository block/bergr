//! AWS integration utilities for credential loading

use anyhow::Result;
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_credential_types::provider::ProvideCredentials;
use iceberg::CatalogBuilder;
use iceberg::io::{CustomAwsCredentialLoader, FileIO, FileIOBuilder};
use iceberg_catalog_glue::{GLUE_CATALOG_PROP_WAREHOUSE, GlueCatalog, GlueCatalogBuilder};
use reqsign::{AwsCredential, AwsCredentialLoad};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::OnceCell;

static AWS_CONFIG: OnceCell<aws_config::SdkConfig> = OnceCell::const_new();

pub async fn get_aws_config() -> &'static aws_config::SdkConfig {
    AWS_CONFIG
        .get_or_init(|| async { aws_config::load_defaults(BehaviorVersion::latest()).await })
        .await
}

/// Implement AwsCredentialLoad via aws_config::SdkConfig.
pub struct SdkConfigCredentialLoader(pub aws_config::SdkConfig);

impl From<aws_config::SdkConfig> for SdkConfigCredentialLoader {
    fn from(config: aws_config::SdkConfig) -> Self {
        Self(config)
    }
}

#[async_trait]
impl AwsCredentialLoad for SdkConfigCredentialLoader {
    async fn load_credential(&self, _client: reqwest::Client) -> Result<Option<AwsCredential>> {
        if let Some(creds_provider) = self.0.credentials_provider() {
            if let Ok(creds) = creds_provider.provide_credentials().await {
                return Ok(Some(AwsCredential {
                    access_key_id: creds.access_key_id().to_string(),
                    secret_access_key: creds.secret_access_key().to_string(),
                    session_token: creds.session_token().map(|s| s.to_string()),
                    expires_in: None,
                }));
            }
        }
        Ok(None)
    }
}

pub async fn s3_file_io() -> Result<FileIO> {
    let aws_config = get_aws_config().await;
    let mut builder = FileIOBuilder::new("s3");

    let sdk_loader = SdkConfigCredentialLoader::from(aws_config.clone());
    let credential_loader = CustomAwsCredentialLoader::new(Arc::new(sdk_loader));
    builder = builder.with_extension(credential_loader);

    if let Some(region) = aws_config.region() {
        builder = builder.with_prop("s3.region", region.to_string());
    }

    return Ok(builder.build()?);
}

pub async fn glue_catalog() -> Result<GlueCatalog> {
    let mut props = HashMap::new();
    props.insert(
        // The warehouse location is required by the builder but not used
        // for read-only operations like listing namespaces.
        GLUE_CATALOG_PROP_WAREHOUSE.to_string(),
        "s3://iceberg-warehouse".to_string(),
    );

    let catalog = GlueCatalogBuilder::default().load("glue", props).await?;

    Ok(catalog)
}
