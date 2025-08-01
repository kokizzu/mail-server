/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use super::{EmailType, MemoryDirectory};
use crate::{Principal, QueryBy, QueryParams, backend::RcptType};

use mail_send::Credentials;

impl MemoryDirectory {
    pub async fn query(&self, by: QueryParams<'_>) -> trc::Result<Option<Principal>> {
        match by.by {
            QueryBy::Name(name) => {
                for principal in &self.principals {
                    if principal.name() == name {
                        return Ok(Some(principal.clone()));
                    }
                }
            }
            QueryBy::Id(uid) => {
                for principal in &self.principals {
                    if principal.id == uid {
                        return Ok(Some(principal.clone()));
                    }
                }
            }
            QueryBy::Credentials(credentials) => {
                let (username, secret) = match credentials {
                    Credentials::Plain { username, secret } => (username, secret),
                    Credentials::OAuthBearer { token } => (token, token),
                    Credentials::XOauth2 { username, secret } => (username, secret),
                };

                for principal in &self.principals {
                    if principal.name() == username {
                        return if principal.verify_secret(secret, false).await? {
                            Ok(Some(principal.clone()))
                        } else {
                            Ok(None)
                        };
                    }
                }
            }
        }
        Ok(None)
    }

    pub async fn email_to_id(&self, address: &str) -> trc::Result<Option<u32>> {
        Ok(self.emails_to_ids.get(address).and_then(|names| {
            names
                .iter()
                .map(|t| match t {
                    EmailType::Primary(uid) | EmailType::Alias(uid) | EmailType::List(uid) => *uid,
                })
                .next()
        }))
    }

    pub async fn rcpt(&self, address: &str) -> trc::Result<RcptType> {
        Ok(self.emails_to_ids.contains_key(address).into())
    }

    pub async fn vrfy(&self, address: &str) -> trc::Result<Vec<String>> {
        let mut result = Vec::new();
        for (key, value) in &self.emails_to_ids {
            if key.contains(address) && value.iter().any(|t| matches!(t, EmailType::Primary(_))) {
                result.push(key.into())
            }
        }
        Ok(result)
    }

    pub async fn expn(&self, address: &str) -> trc::Result<Vec<String>> {
        let mut result = Vec::new();
        for (key, value) in &self.emails_to_ids {
            if key == address {
                for item in value {
                    if let EmailType::List(uid) = item {
                        for principal in &self.principals {
                            if principal.id == *uid {
                                if let Some(addr) = principal.emails.first() {
                                    result.push(addr.clone())
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }
        Ok(result)
    }

    pub async fn is_local_domain(&self, domain: &str) -> trc::Result<bool> {
        Ok(self.domains.contains(domain))
    }
}
