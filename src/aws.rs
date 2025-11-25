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

pub async fn s3_file_io() -> Result<FileIO> {
    let aws_config = get_aws_config().await;
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

pub async fn glue_catalog() -> Result<GlueCatalog> {
    let aws_config = get_aws_config().await;
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
