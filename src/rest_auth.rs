//! Authentication settings for REST catalogs
//!
//! Settings, including secrets, are only ever read from environment variables, never from
//! command-line arguments, which are visible in `ps` and end up in shell history.

use crate::error::ExpectedError;
use crate::sigv4::SigningScope;
use anyhow::Result;
use clap::ValueEnum;
use iceberg::sensitive::SensitiveString;

pub const BEARER_TOKEN: &str = "BERGR_REST_BEARER_TOKEN";
pub const BASIC_USER: &str = "BERGR_REST_BASIC_USER";
pub const BASIC_PASSWORD: &str = "BERGR_REST_BASIC_PASSWORD";
pub const OAUTH2_CREDENTIAL: &str = "BERGR_REST_OAUTH2_CREDENTIAL";
pub const OAUTH2_SCOPE: &str = "BERGR_REST_OAUTH2_SCOPE";
pub const OAUTH2_SERVER_URI: &str = "BERGR_REST_OAUTH2_SERVER_URI";

/// How `bergr rest` authenticates to the catalog
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AuthType {
    /// `sigv4` for AWS Glue endpoints (glue.<region>.amazonaws.com), otherwise `none`
    #[default]
    Auto,
    /// No authentication
    None,
    /// AWS SigV4 signing, with the standard AWS credential variables and profile; region and
    /// signing name come from the AWS Glue hostname
    Sigv4,
    /// Bearer token from BERGR_REST_BEARER_TOKEN (iceberg-catalog-rest's `token`)
    Bearer,
    /// HTTP basic authentication, from BERGR_REST_BASIC_USER and BERGR_REST_BASIC_PASSWORD
    Basic,
    /// OAuth2 client credentials from BERGR_REST_OAUTH2_CREDENTIAL (`client_id:secret`), and
    /// optionally BERGR_REST_OAUTH2_SCOPE and BERGR_REST_OAUTH2_SERVER_URI (iceberg-catalog-rest's
    /// `credential`, `scope` and `oauth2-server-uri`)
    Oauth2,
}

impl AuthType {
    fn name(self) -> String {
        self.to_possible_value()
            .map(|value| value.get_name().to_string())
            .unwrap_or_default()
    }
}

/// Authentication for a REST catalog, with its settings resolved
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestAuth {
    /// Sign requests with AWS SigV4, which needs AWS configuration to build
    SigV4(SigningScope),
    /// Authenticate with HTTP headers alone
    Http(HttpAuth),
}

/// Authentication schemes that need nothing beyond their settings
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpAuth {
    None,
    Bearer {
        token: SensitiveString,
    },
    Basic {
        user: String,
        password: SensitiveString,
    },
    OAuth2 {
        credential: SensitiveString,
        scope: Option<String>,
        server_uri: Option<String>,
    },
}

fn user_input(message: String) -> anyhow::Error {
    ExpectedError::UserInput(message).into()
}

impl RestAuth {
    /// Resolve `auth_type` for the catalog at `uri`, reading settings with `lookup`
    /// (normally `std::env::var`). An empty setting counts as unset.
    pub fn resolve(
        auth_type: AuthType,
        uri: &str,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<Self> {
        let optional = |name: &str| lookup(name).filter(|value| !value.is_empty());
        let required = |name: &str| {
            optional(name)
                .ok_or_else(|| user_input(format!("--auth {} requires {name}", auth_type.name())))
        };

        match auth_type {
            AuthType::Auto => Ok(SigningScope::from_uri(uri)
                .map(RestAuth::SigV4)
                .unwrap_or(RestAuth::Http(HttpAuth::None))),
            AuthType::None => Ok(RestAuth::Http(HttpAuth::None)),
            AuthType::Sigv4 => SigningScope::from_uri(uri).map(RestAuth::SigV4).ok_or_else(|| {
                user_input(format!(
                    "--auth sigv4 only supports AWS Glue endpoints (glue.<region>.amazonaws.com), not {uri}"
                ))
            }),
            AuthType::Bearer => Ok(RestAuth::Http(HttpAuth::Bearer {
                token: required(BEARER_TOKEN)?.into(),
            })),
            AuthType::Basic => {
                let user = required(BASIC_USER)?;
                if user.contains(':') {
                    return Err(user_input(format!("{BASIC_USER} must not contain ':'")));
                }
                Ok(RestAuth::Http(HttpAuth::Basic {
                    user,
                    password: required(BASIC_PASSWORD)?.into(),
                }))
            }
            AuthType::Oauth2 => Ok(RestAuth::Http(HttpAuth::OAuth2 {
                credential: required(OAUTH2_CREDENTIAL)?.into(),
                scope: optional(OAUTH2_SCOPE),
                server_uri: optional(OAUTH2_SERVER_URI),
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    const GLUE_URI: &str = "https://glue.us-west-2.amazonaws.com/iceberg";
    const OTHER_URI: &str = "https://catalog.example.com";

    fn resolve(auth_type: AuthType, uri: &str, vars: &[(&str, &str)]) -> Result<RestAuth> {
        let env: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        RestAuth::resolve(auth_type, uri, |name| env.get(name).cloned())
    }

    fn error_message(result: Result<RestAuth>) -> String {
        let err = result.expect_err("should fail");
        assert!(
            err.downcast_ref::<ExpectedError>().is_some(),
            "should be a user-input error: {err:?}"
        );
        err.to_string()
    }

    fn glue_scope() -> SigningScope {
        SigningScope {
            service: "glue".into(),
            region: "us-west-2".into(),
        }
    }

    #[test]
    fn auto_signs_glue_requests() {
        assert_eq!(
            resolve(AuthType::Auto, GLUE_URI, &[]).unwrap(),
            RestAuth::SigV4(glue_scope())
        );
    }

    #[test]
    fn auto_is_none_for_other_catalogs() {
        assert_eq!(
            resolve(AuthType::Auto, OTHER_URI, &[(BEARER_TOKEN, "ignored")]).unwrap(),
            RestAuth::Http(HttpAuth::None)
        );
    }

    #[test]
    fn none_disables_signing_for_glue() {
        assert_eq!(
            resolve(AuthType::None, GLUE_URI, &[]).unwrap(),
            RestAuth::Http(HttpAuth::None)
        );
    }

    #[test]
    fn sigv4_for_glue() {
        assert_eq!(
            resolve(AuthType::Sigv4, GLUE_URI, &[]).unwrap(),
            RestAuth::SigV4(glue_scope())
        );
    }

    #[test]
    fn sigv4_rejects_unrecognised_endpoints() {
        assert!(error_message(resolve(AuthType::Sigv4, OTHER_URI, &[])).contains("AWS Glue"));
    }

    #[test]
    fn bearer_reads_token() {
        assert_eq!(
            resolve(AuthType::Bearer, OTHER_URI, &[(BEARER_TOKEN, "tok")]).unwrap(),
            RestAuth::Http(HttpAuth::Bearer {
                token: "tok".to_string().into()
            })
        );
    }

    #[test]
    fn bearer_requires_token() {
        assert_eq!(
            error_message(resolve(AuthType::Bearer, OTHER_URI, &[])),
            "--auth bearer requires BERGR_REST_BEARER_TOKEN"
        );
    }

    #[test]
    fn empty_setting_counts_as_unset() {
        assert_eq!(
            error_message(resolve(AuthType::Bearer, OTHER_URI, &[(BEARER_TOKEN, "")])),
            "--auth bearer requires BERGR_REST_BEARER_TOKEN"
        );
    }

    #[test]
    fn basic_reads_user_and_password() {
        assert_eq!(
            resolve(
                AuthType::Basic,
                OTHER_URI,
                &[(BASIC_USER, "alice"), (BASIC_PASSWORD, "s3cret")]
            )
            .unwrap(),
            RestAuth::Http(HttpAuth::Basic {
                user: "alice".into(),
                password: "s3cret".to_string().into()
            })
        );
    }

    #[test]
    fn basic_requires_password() {
        assert_eq!(
            error_message(resolve(
                AuthType::Basic,
                OTHER_URI,
                &[(BASIC_USER, "alice")]
            )),
            "--auth basic requires BERGR_REST_BASIC_PASSWORD"
        );
    }

    #[test]
    fn basic_rejects_colon_in_user() {
        assert_eq!(
            error_message(resolve(
                AuthType::Basic,
                OTHER_URI,
                &[(BASIC_USER, "al:ice"), (BASIC_PASSWORD, "s3cret")]
            )),
            "BERGR_REST_BASIC_USER must not contain ':'"
        );
    }

    #[test]
    fn oauth2_reads_credential_and_options() {
        assert_eq!(
            resolve(
                AuthType::Oauth2,
                OTHER_URI,
                &[
                    (OAUTH2_CREDENTIAL, "id:secret"),
                    (OAUTH2_SCOPE, "PRINCIPAL_ROLE:ALL"),
                    (OAUTH2_SERVER_URI, "https://auth.example.com/token"),
                ]
            )
            .unwrap(),
            RestAuth::Http(HttpAuth::OAuth2 {
                credential: "id:secret".to_string().into(),
                scope: Some("PRINCIPAL_ROLE:ALL".into()),
                server_uri: Some("https://auth.example.com/token".into()),
            })
        );
    }

    #[test]
    fn oauth2_options_are_optional() {
        assert_eq!(
            resolve(
                AuthType::Oauth2,
                OTHER_URI,
                &[(OAUTH2_CREDENTIAL, "secret")]
            )
            .unwrap(),
            RestAuth::Http(HttpAuth::OAuth2 {
                credential: "secret".to_string().into(),
                scope: None,
                server_uri: None,
            })
        );
    }

    #[test]
    fn oauth2_requires_credential() {
        assert_eq!(
            error_message(resolve(AuthType::Oauth2, OTHER_URI, &[])),
            "--auth oauth2 requires BERGR_REST_OAUTH2_CREDENTIAL"
        );
    }

    #[test]
    fn debug_output_redacts_secrets() {
        let auth = resolve(
            AuthType::Basic,
            OTHER_URI,
            &[(BASIC_USER, "alice"), (BASIC_PASSWORD, "s3cret")],
        )
        .unwrap();
        assert!(!format!("{auth:?}").contains("s3cret"));
    }
}
