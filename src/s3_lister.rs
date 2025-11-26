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
use std::collections::{HashMap, HashSet};
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

/// Finds the longest common prefix among a set of S3 paths.
///
/// Returns None if paths are empty or have different buckets.
pub fn find_common_prefix<'a>(
    paths: impl IntoIterator<Item = &'a str>,
) -> Option<(String, String)> {
    let mut iter = paths.into_iter();
    let first = iter.next()?;
    let (bucket, first_key) = parse_s3_url(first)?;

    let mut common_prefix = first_key.to_string();

    for path in iter {
        let (other_bucket, other_key) = parse_s3_url(path)?;
        if other_bucket != bucket {
            return None; // Different buckets, can't use common prefix
        }

        // Find common prefix between current common_prefix and other_key
        let common_len = common_prefix
            .chars()
            .zip(other_key.chars())
            .take_while(|(a, b)| a == b)
            .count();
        common_prefix.truncate(common_len);
    }

    // Truncate to last '/' to get a directory prefix
    if let Some(last_slash) = common_prefix.rfind('/') {
        common_prefix.truncate(last_slash + 1);
    } else {
        common_prefix.clear();
    }

    Some((bucket.to_string(), common_prefix))
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
    /// Creates a new cache by listing objects with the common prefix of the given paths.
    pub async fn new(client: &Client, paths: &[String]) -> Result<Self> {
        let (bucket, prefix) = find_common_prefix(paths.iter().map(|s| s.as_str()))
            .context("Could not determine common S3 prefix for file listing")?;

        let existing_files = list_objects_with_prefix(client, &bucket, &prefix).await?;

        Ok(Self { existing_files })
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

    /// Returns the number of files in the cache.
    pub fn len(&self) -> usize {
        self.existing_files.len()
    }

    /// Returns true if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.existing_files.is_empty()
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
    fn test_find_common_prefix_single_path() {
        let paths = vec!["s3://bucket/data/table/part-00000.parquet"];
        let result = find_common_prefix(paths.iter().copied());
        assert_eq!(
            result,
            Some(("bucket".to_string(), "data/table/".to_string()))
        );
    }

    #[test]
    fn test_find_common_prefix_multiple_paths() {
        let paths = vec![
            "s3://bucket/data/table/part-00000.parquet",
            "s3://bucket/data/table/part-00001.parquet",
            "s3://bucket/data/table/part-00002.parquet",
        ];
        let result = find_common_prefix(paths.iter().copied());
        assert_eq!(
            result,
            Some(("bucket".to_string(), "data/table/".to_string()))
        );
    }

    #[test]
    fn test_find_common_prefix_different_subdirs() {
        let paths = vec![
            "s3://bucket/data/table/2024/01/part-00000.parquet",
            "s3://bucket/data/table/2024/02/part-00001.parquet",
        ];
        let result = find_common_prefix(paths.iter().copied());
        assert_eq!(
            result,
            Some(("bucket".to_string(), "data/table/2024/".to_string()))
        );
    }

    #[test]
    fn test_find_common_prefix_different_buckets() {
        let paths = vec![
            "s3://bucket1/data/file.parquet",
            "s3://bucket2/data/file.parquet",
        ];
        let result = find_common_prefix(paths.iter().copied());
        assert_eq!(result, None);
    }

    #[test]
    fn test_find_common_prefix_empty() {
        let paths: Vec<&str> = vec![];
        let result = find_common_prefix(paths.iter().copied());
        assert_eq!(result, None);
    }

    #[test]
    fn test_find_common_prefix_with_s3a() {
        let paths = vec![
            "s3a://bucket/data/table/part-00000.parquet",
            "s3a://bucket/data/table/part-00001.parquet",
        ];
        let result = find_common_prefix(paths.iter().copied());
        assert_eq!(
            result,
            Some(("bucket".to_string(), "data/table/".to_string()))
        );
    }

    #[test]
    fn test_s3_file_cache_exists() {
        let mut existing = HashSet::new();
        existing.insert("s3://bucket/data/file1.parquet".to_string());
        existing.insert("s3://bucket/data/file2.parquet".to_string());

        let cache = S3FileCache {
            existing_files: existing,
        };

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
