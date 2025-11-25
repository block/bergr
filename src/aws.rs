//! AWS integration utilities for credential loading

use anyhow::Result;
use aws_config::BehaviorVersion;
use aws_credential_types::provider::ProvideCredentials;
use iceberg::CatalogBuilder;
use iceberg::io::{FileIO, FileIOBuilder, S3_ACCESS_KEY_ID, S3_REGION, S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN};
use iceberg_catalog_glue::{
    AWS_ACCESS_KEY_ID, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN,
    GLUE_CATALOG_PROP_WAREHOUSE, GlueCatalog, GlueCatalogBuilder,
};
use std::collections::HashMap;
use tokio::sync::OnceCell;

static AWS_CONFIG: OnceCell<aws_config::SdkConfig> = OnceCell::const_new();

pub async fn get_aws_config() -> &'static aws_config::SdkConfig {
    AWS_CONFIG
        .get_or_init(|| async { aws_config::load_defaults(BehaviorVersion::latest()).await })
        .await
}

pub async fn s3_file_io(aws_config: &aws_config::SdkConfig) -> Result<FileIO> {
    let mut builder = FileIOBuilder::new("s3");

    // Add region from AWS config
    if let Some(region) = aws_config.region() {
        builder = builder.with_prop(S3_REGION, region.to_string());
    }

    // Extract and add credentials from AWS SDK
    if let Some(creds_provider) = aws_config.credentials_provider() {
        if let Ok(creds) = creds_provider.provide_credentials().await {
            builder = builder.with_prop(S3_ACCESS_KEY_ID, creds.access_key_id());
            builder = builder.with_prop(S3_SECRET_ACCESS_KEY, creds.secret_access_key());

            if let Some(session_token) = creds.session_token() {
                builder = builder.with_prop(S3_SESSION_TOKEN, session_token);
            }
        }
    }

    Ok(builder.build()?)
}

pub async fn glue_catalog(aws_config: &aws_config::SdkConfig) -> Result<GlueCatalog> {
    let mut props = HashMap::new();

    // Required warehouse (not actually used for read-only ops)
    props.insert(
        GLUE_CATALOG_PROP_WAREHOUSE.to_string(),
        "s3://iceberg-warehouse".to_string(),
    );

    // Add region from AWS config
    if let Some(region) = aws_config.region() {
        props.insert(AWS_REGION_NAME.to_string(), region.to_string());
    }

    // Extract and add credentials from AWS SDK
    if let Some(creds_provider) = aws_config.credentials_provider() {
        if let Ok(creds) = creds_provider.provide_credentials().await {
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
    }

    let catalog = GlueCatalogBuilder::default().load("glue", props).await?;

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
    async fn test_s3_file_io_with_aws_config() -> Result<()> {
        // Create a test AWS config with mock credentials
        let aws_config = test_aws_config().await;

        // Build FileIO with credentials from aws_config
        let file_io = s3_file_io(&aws_config).await?;

        // Inspect the properties that were set
        let (_scheme, props, _extensions) = file_io.into_builder().into_parts();

        // Verify region was extracted and set
        assert_eq!(props.get(S3_REGION), Some(&"us-west-2".to_string()));

        // Verify credentials were extracted and set
        assert_eq!(
            props.get(S3_ACCESS_KEY_ID),
            Some(&"test_access_key".to_string())
        );
        assert_eq!(
            props.get(S3_SECRET_ACCESS_KEY),
            Some(&"test_secret_key".to_string())
        );
        assert_eq!(
            props.get(S3_SESSION_TOKEN),
            Some(&"test_session_token".to_string())
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_glue_catalog_with_aws_config() -> Result<()> {
        // Create a test AWS config with mock credentials
        let aws_config = test_aws_config().await;

        // Build GlueCatalog with credentials from aws_config
        let catalog = glue_catalog(&aws_config).await?;

        // Get the FileIO that GlueCatalog created internally
        let file_io = catalog.file_io();

        // Inspect the properties that were passed through
        let (_scheme, props, _extensions) = file_io.into_builder().into_parts();

        // Verify region was extracted and set
        assert_eq!(props.get(S3_REGION), Some(&"us-west-2".to_string()));

        // Verify credentials were extracted and set
        assert_eq!(
            props.get(S3_ACCESS_KEY_ID),
            Some(&"test_access_key".to_string())
        );
        assert_eq!(
            props.get(S3_SECRET_ACCESS_KEY),
            Some(&"test_secret_key".to_string())
        );
        assert_eq!(
            props.get(S3_SESSION_TOKEN),
            Some(&"test_session_token".to_string())
        );

        Ok(())
    }
}
