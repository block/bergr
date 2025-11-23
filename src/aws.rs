//! AWS integration utilities for credential loading

use anyhow::Result;
use async_trait::async_trait;
use aws_credential_types::provider::ProvideCredentials;
use reqsign::{AwsCredential, AwsCredentialLoad};

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
