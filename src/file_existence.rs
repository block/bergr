//! File existence checking with optimized implementations.
//!
//! Provides a trait for checking file existence, with implementations that
//! either delegate to FileIO or use a pre-loaded set of known locations.

use anyhow::{Context, Result};
use async_trait::async_trait;
use aws_sdk_s3::Client;
use iceberg::io::FileIO;
use std::collections::HashSet;
use tracing::debug;
use tracing::{info, warn};

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
///
/// Stores only the suffix of each path (after stripping the common base URL prefix)
/// to reduce memory usage when there are many files.
pub struct PreloadedExistenceChecker {
    base_url: String,
    suffixes: HashSet<Box<str>>,
}

impl PreloadedExistenceChecker {
    fn new(base_url: String, suffixes: HashSet<Box<str>>) -> Self {
        Self { base_url, suffixes }
    }
}

#[async_trait]
impl FileExistenceChecker for PreloadedExistenceChecker {
    async fn exists(&self, path: &str) -> Result<bool> {
        Ok(path
            .strip_prefix(&self.base_url)
            .is_some_and(|suffix| self.suffixes.contains(suffix)))
    }
}

/// Creates a file existence checker, using S3 prefix listing if possible.
///
/// If the data prefix is on S3 and an S3 client is provided, uses a bulk
/// `ListObjectsV2` call and returns a `PreloadedExistenceChecker`.
///
/// Otherwise, or if the listing fails, returns a `FileIOExistenceChecker` that delegates to
/// per-file checks. Listing is only an optimisation, and credentials that can read a table's files
/// can't always list them; Polaris's vended keys, for example, can't.
pub async fn create_existence_checker(
    file_io: FileIO,
    data_prefix: &str,
    s3_client: Option<&Client>,
) -> Result<Box<dyn FileExistenceChecker>> {
    debug!(data_prefix = %data_prefix, "Creating file existence checker");

    // Try S3 optimization: parse URL and use provided client
    if let Some((bucket, prefix)) = parse_s3_url(data_prefix)
        && let Some(client) = s3_client
    {
        match list_object_suffixes(client, bucket, prefix).await {
            Ok(suffixes) => {
                debug!(
                    file_count = suffixes.len(),
                    "Using preloaded S3 existence checker"
                );
                let base_url = format!("s3://{}/{}", bucket, prefix);
                return Ok(Box::new(PreloadedExistenceChecker::new(base_url, suffixes)));
            }
            Err(err) => warn!(error = format!("{err:#}"), "Couldn't list S3 objects"),
        }
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

/// Lists all objects in an S3 bucket with the given prefix, returning suffixes only.
///
/// Returns the portion of each object key after the prefix, to save memory.
async fn list_object_suffixes(
    client: &Client,
    bucket: &str,
    prefix: &str,
) -> Result<HashSet<Box<str>>> {
    info!(bucket = %bucket, prefix = %prefix, "Listing S3 objects");
    let mut suffixes = HashSet::new();

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
                // Strip the prefix to get just the suffix
                let suffix = key.strip_prefix(prefix).unwrap_or(key);
                suffixes.insert(suffix.into());
            }
        }
    }

    Ok(suffixes)
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

    /// An S3 client whose every request is denied, as with vended keys that can read objects but
    /// not list them.
    fn access_denied_s3_client(server: &mockito::Server) -> Client {
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "key", "secret", None, None, "test",
            ))
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .endpoint_url(server.url())
            .force_path_style(true)
            .build();
        Client::from_conf(config)
    }

    #[tokio::test]
    async fn falls_back_to_file_io_when_listing_is_denied() {
        let mut server = mockito::Server::new_async().await;
        let _denied = server
            .mock("GET", mockito::Matcher::Any)
            .with_status(403)
            .with_body("<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>")
            .create_async()
            .await;
        let file_io = FileIO::new_with_memory();
        file_io
            .new_output("s3://bucket/data/file1.parquet")
            .unwrap()
            .write(bytes::Bytes::from("data"))
            .await
            .unwrap();

        let checker = create_existence_checker(
            file_io,
            "s3://bucket/data/",
            Some(&access_denied_s3_client(&server)),
        )
        .await
        .unwrap();

        assert!(
            checker
                .exists("s3://bucket/data/file1.parquet")
                .await
                .unwrap()
        );
        assert!(
            !checker
                .exists("s3://bucket/data/file2.parquet")
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_preloaded_checker() {
        let mut suffixes = HashSet::new();
        suffixes.insert("file1.parquet".into());
        suffixes.insert("file2.parquet".into());

        let checker = PreloadedExistenceChecker::new("s3://bucket/data/".to_string(), suffixes);

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

    #[tokio::test]
    async fn test_preloaded_checker_with_non_matching_prefix() {
        let mut suffixes = HashSet::new();
        suffixes.insert("file1.parquet".into());

        let checker = PreloadedExistenceChecker::new("s3://bucket/data/".to_string(), suffixes);

        // Path with different prefix should return false, not panic
        assert!(
            !checker
                .exists("s3://other-bucket/data/file1.parquet")
                .await
                .unwrap()
        );
    }
}
