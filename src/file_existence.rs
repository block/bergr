//! File existence checking with optimized implementations.
//!
//! Provides a trait for checking file existence, with implementations that
//! either delegate to FileIO or use a pre-loaded set of known locations.

use anyhow::{Context, Result};
use async_trait::async_trait;
use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Builder as S3ConfigBuilder;
use iceberg::io::{FileIO, S3_ACCESS_KEY_ID, S3_REGION, S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN};
use std::collections::HashMap;
use std::collections::HashSet;
use tracing::debug;
use tracing::info;

/// Checks whether files exist at given locations.
#[async_trait]
pub trait FileExistenceChecker: Send + Sync {
    /// Checks if a file exists at the given path.
    async fn exists(&self, path: &str) -> Result<bool>;
}

/// Checks file existence by delegating to FileIO.
pub struct FileIOExistenceChecker {
    file_io: FileIO,
}

impl FileIOExistenceChecker {
    pub fn new(file_io: FileIO) -> Self {
        Self { file_io }
    }
}

#[async_trait]
impl FileExistenceChecker for FileIOExistenceChecker {
    async fn exists(&self, path: &str) -> Result<bool> {
        Ok(self.file_io.exists(path).await?)
    }
}

/// Checks file existence against a pre-loaded set of known locations.
pub struct PreloadedExistenceChecker {
    locations: HashSet<String>,
}

impl PreloadedExistenceChecker {
    fn new(locations: HashSet<String>) -> Self {
        Self { locations }
    }
}

#[async_trait]
impl FileExistenceChecker for PreloadedExistenceChecker {
    async fn exists(&self, path: &str) -> Result<bool> {
        Ok(self.locations.contains(path))
    }
}

/// Creates a file existence checker, using S3 prefix listing if possible.
///
/// If the data prefix is on S3 and we can extract credentials from the FileIO,
/// returns a `PreloadedExistenceChecker` that has listed all objects under the prefix.
///
/// Otherwise, returns a `FileIOExistenceChecker` that delegates to per-file checks.
pub async fn create_existence_checker(
    file_io: FileIO,
    data_prefix: &str,
) -> Result<Box<dyn FileExistenceChecker>> {
    debug!(data_prefix = %data_prefix, "Creating file existence checker");

    // Try S3 optimization: parse URL and build client
    if let Some((bucket, prefix)) = parse_s3_url(data_prefix)
        && let Some(client) = s3_client_from_file_io(file_io.clone())
    {
        let locations = list_objects_with_prefix(&client, bucket, prefix).await?;
        debug!(
            file_count = locations.len(),
            "Using preloaded S3 existence checker"
        );
        return Ok(Box::new(PreloadedExistenceChecker::new(locations)));
    }

    debug!("Using FileIO existence checker");
    Ok(Box::new(FileIOExistenceChecker::new(file_io)))
}

/// Parses an S3 URL into bucket and key components.
fn parse_s3_url(url: &str) -> Option<(&str, &str)> {
    let rest = url
        .strip_prefix("s3://")
        .or_else(|| url.strip_prefix("s3a://"))?;
    let slash_pos = rest.find('/')?;
    let bucket = &rest[..slash_pos];
    let key = &rest[slash_pos + 1..];
    Some((bucket, key))
}

/// Lists all objects in an S3 bucket with the given prefix.
async fn list_objects_with_prefix(
    client: &Client,
    bucket: &str,
    prefix: &str,
) -> Result<HashSet<String>> {
    info!(bucket = %bucket, prefix = %prefix, "Listing S3 objects");
    let mut existing_files = HashSet::new();

    let mut paginator = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .into_paginator()
        .send();

    while let Some(result) = paginator.next().await {
        let page = result.context("Failed to list S3 objects")?;
        for object in page.contents() {
            if let Some(key) = object.key() {
                existing_files.insert(format!("s3://{}/{}", bucket, key));
            }
        }
    }

    Ok(existing_files)
}

/// Attempts to build an S3 client from the credentials stored in a FileIO.
fn s3_client_from_file_io(file_io: FileIO) -> Option<Client> {
    let (scheme, props, _extensions) = file_io.into_builder().into_parts();
    debug!(scheme = %scheme, "Extracting S3 credentials from FileIO");

    if scheme != "s3" && scheme != "s3a" {
        debug!("FileIO scheme is not S3, cannot build S3 client");
        return None;
    }

    s3_client_from_props(&props)
}

/// Builds an S3 client from a properties map containing S3 credentials.
fn s3_client_from_props(props: &HashMap<String, String>) -> Option<Client> {
    let access_key_id = props.get(S3_ACCESS_KEY_ID);
    let secret_access_key = props.get(S3_SECRET_ACCESS_KEY);
    let region = props.get(S3_REGION);

    debug!(
        has_access_key = access_key_id.is_some(),
        has_secret_key = secret_access_key.is_some(),
        has_region = region.is_some(),
        "Checking FileIO properties for S3 credentials"
    );

    let access_key_id = access_key_id?;
    let secret_access_key = secret_access_key?;
    let region = region?;

    let credentials = Credentials::new(
        access_key_id,
        secret_access_key,
        props.get(S3_SESSION_TOKEN).cloned(),
        None,
        "iceberg-file-io",
    );

    let config = S3ConfigBuilder::new()
        .behavior_version_latest()
        .region(Region::new(region.clone()))
        .credentials_provider(credentials)
        .build();

    Some(Client::from_conf(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_url() {
        assert_eq!(
            parse_s3_url("s3://bucket/path/to/file.parquet"),
            Some(("bucket", "path/to/file.parquet"))
        );
        assert_eq!(
            parse_s3_url("s3a://bucket/path/to/file.parquet"),
            Some(("bucket", "path/to/file.parquet"))
        );
        assert_eq!(parse_s3_url("gs://bucket/path"), None);
        assert_eq!(parse_s3_url("not-a-url"), None);
    }

    #[tokio::test]
    async fn test_preloaded_checker() {
        let mut locations = HashSet::new();
        locations.insert("s3://bucket/data/file1.parquet".to_string());
        locations.insert("s3://bucket/data/file2.parquet".to_string());

        let checker = PreloadedExistenceChecker::new(locations);

        assert!(
            checker
                .exists("s3://bucket/data/file1.parquet")
                .await
                .unwrap()
        );
        assert!(
            checker
                .exists("s3://bucket/data/file2.parquet")
                .await
                .unwrap()
        );
        assert!(
            !checker
                .exists("s3://bucket/data/file3.parquet")
                .await
                .unwrap()
        );
    }
}
