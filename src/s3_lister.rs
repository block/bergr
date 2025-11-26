//! S3 file existence checking using prefix listing
//!
//! This module provides efficient batch existence checking for S3 files by doing
//! a prefix listing instead of individual HeadObject calls.

use anyhow::{Context, Result};
use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Builder as S3ConfigBuilder;
use iceberg::io::FileIO;
use std::collections::HashMap;
use std::collections::HashSet;
use tracing::debug;

/// Parses an S3 URL into bucket and key components.
///
/// Supports both `s3://bucket/key` and `s3a://bucket/key` formats.
pub fn parse_s3_url(url: &str) -> Option<(&str, &str)> {
    let rest = url
        .strip_prefix("s3://")
        .or_else(|| url.strip_prefix("s3a://"))?;
    let slash_pos = rest.find('/')?;
    let bucket = &rest[..slash_pos];
    let key = &rest[slash_pos + 1..];
    Some((bucket, key))
}

/// Lists all objects in an S3 bucket with the given prefix.
///
/// Returns a HashSet of full S3 URLs (s3://bucket/key format).
pub async fn list_objects_with_prefix(
    client: &Client,
    bucket: &str,
    prefix: &str,
) -> Result<HashSet<String>> {
    debug!(bucket = %bucket, prefix = %prefix, "Listing S3 objects");
    let mut existing_files = HashSet::new();
    let mut continuation_token: Option<String> = None;

    loop {
        let mut request = client.list_objects_v2().bucket(bucket).prefix(prefix);

        if let Some(token) = continuation_token {
            request = request.continuation_token(token);
        }

        let response = request.send().await.context("Failed to list S3 objects")?;

        for object in response.contents() {
            if let Some(key) = object.key() {
                existing_files.insert(format!("s3://{}/{}", bucket, key));
            }
        }

        if response.is_truncated() == Some(true) {
            continuation_token = response.next_continuation_token().map(|s| s.to_string());
        } else {
            break;
        }
    }

    Ok(existing_files)
}

/// A cache of existing S3 files for efficient existence checking.
pub struct S3FileCache {
    existing_files: HashSet<String>,
}

impl S3FileCache {
    /// Creates a cache from a pre-loaded set of file paths.
    pub fn new(existing_files: HashSet<String>) -> Self {
        Self { existing_files }
    }

    /// Checks if a file exists in the cache.
    ///
    /// Note: This normalizes s3a:// URLs to s3:// for comparison.
    pub fn exists(&self, path: &str) -> bool {
        // Normalize s3a:// to s3:// for lookup
        let normalized = if let Some(rest) = path.strip_prefix("s3a://") {
            format!("s3://{}", rest)
        } else {
            path.to_string()
        };
        self.existing_files.contains(&normalized)
    }
}

/// Attempts to load a cache of existing S3 files under the given prefix.
///
/// Returns `None` if:
/// - The prefix is not an S3 URL
/// - An S3 client cannot be built from the FileIO credentials
/// - The S3 listing fails
///
/// This is designed to be called early, potentially in parallel with other operations,
/// to pre-populate the cache before verifying individual files.
pub async fn try_load_s3_file_cache(file_io: FileIO, data_prefix: &str) -> Option<S3FileCache> {
    debug!(data_prefix = %data_prefix, "Attempting to pre-load S3 file cache");

    // Parse the S3 URL to get bucket and prefix
    let (bucket, prefix) = parse_s3_url(data_prefix)?;

    // Try to build an S3 client from the FileIO
    let client = s3_client_from_file_io(file_io)?;

    // List objects - if this fails, return None rather than propagating the error
    match list_objects_with_prefix(&client, bucket, prefix).await {
        Ok(existing_files) => {
            debug!(
                file_count = existing_files.len(),
                "Pre-loaded S3 file cache"
            );
            Some(S3FileCache::new(existing_files))
        }
        Err(e) => {
            debug!(error = %e, "Failed to pre-load S3 file cache, will fall back to per-file checks");
            None
        }
    }
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

    #[test]
    fn test_s3_file_cache_exists() {
        let mut existing = HashSet::new();
        existing.insert("s3://bucket/data/file1.parquet".to_string());
        existing.insert("s3://bucket/data/file2.parquet".to_string());

        let cache = S3FileCache::new(existing);

        assert!(cache.exists("s3://bucket/data/file1.parquet"));
        assert!(cache.exists("s3a://bucket/data/file1.parquet")); // s3a normalized to s3
        assert!(!cache.exists("s3://bucket/data/file3.parquet"));
    }
}

// Property keys used by iceberg's S3 storage
const S3_ACCESS_KEY_ID: &str = "s3.access-key-id";
const S3_SECRET_ACCESS_KEY: &str = "s3.secret-access-key";
const S3_SESSION_TOKEN: &str = "s3.session-token";
const S3_REGION: &str = "s3.region";

/// Attempts to build an S3 client from the credentials stored in a FileIO.
///
/// Returns `None` if the FileIO is not configured for S3 or lacks credentials.
/// This consumes the FileIO since `into_builder()` takes ownership.
pub fn s3_client_from_file_io(file_io: FileIO) -> Option<Client> {
    let (scheme, props, _extensions) = file_io.into_builder().into_parts();
    debug!(scheme = %scheme, "Extracting S3 credentials from FileIO");

    // Only works for S3-scheme FileIO
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
