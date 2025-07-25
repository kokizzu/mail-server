/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use trc::AddContext;

use crate::{
    Directory, DirectoryInner, Principal, QueryParams,
    backend::{RcptType, internal::lookup::DirectoryStore},
};

impl Directory {
    pub async fn query(&self, by: QueryParams<'_>) -> trc::Result<Option<Principal>> {
        match &self.store {
            DirectoryInner::Internal(store) => store.query(by).await,
            DirectoryInner::Ldap(store) => store.query(by).await,
            DirectoryInner::Sql(store) => store.query(by).await,
            DirectoryInner::Imap(store) => store.query(by.by).await,
            DirectoryInner::Smtp(store) => store.query(by.by).await,
            DirectoryInner::Memory(store) => store.query(by).await,
            DirectoryInner::OpenId(store) => store.query(by).await,
        }
        .caused_by(trc::location!())
    }

    pub async fn email_to_id(&self, address: &str) -> trc::Result<Option<u32>> {
        match &self.store {
            DirectoryInner::Internal(store) => store.email_to_id(address).await,
            DirectoryInner::Ldap(store) => store.email_to_id(address).await,
            DirectoryInner::Sql(store) => store.email_to_id(address).await,
            DirectoryInner::Imap(store) => store.email_to_id(address).await,
            DirectoryInner::Smtp(store) => store.email_to_id(address).await,
            DirectoryInner::Memory(store) => store.email_to_id(address).await,
            DirectoryInner::OpenId(store) => store.email_to_id(address).await,
        }
        .caused_by(trc::location!())
    }

    pub async fn is_local_domain(&self, domain: &str) -> trc::Result<bool> {
        // Check cache
        if let Some(cache) = &self.cache {
            if let Some(result) = cache.get_domain(domain) {
                return Ok(result);
            }
        }

        let result = match &self.store {
            DirectoryInner::Internal(store) => store.is_local_domain(domain).await,
            DirectoryInner::Ldap(store) => store.is_local_domain(domain).await,
            DirectoryInner::Sql(store) => store.is_local_domain(domain).await,
            DirectoryInner::Imap(store) => store.is_local_domain(domain).await,
            DirectoryInner::Smtp(store) => store.is_local_domain(domain).await,
            DirectoryInner::Memory(store) => store.is_local_domain(domain).await,
            DirectoryInner::OpenId(store) => store.is_local_domain(domain).await,
        }
        .caused_by(trc::location!())?;

        // Update cache
        if let Some(cache) = &self.cache {
            cache.set_domain(domain, result);
        }

        Ok(result)
    }

    pub async fn rcpt(&self, email: &str) -> trc::Result<RcptType> {
        // Check cache
        if let Some(cache) = &self.cache {
            if let Some(result) = cache.get_rcpt(email) {
                return Ok(result);
            }
        }

        let result = match &self.store {
            DirectoryInner::Internal(store) => store.rcpt(email).await,
            DirectoryInner::Ldap(store) => store.rcpt(email).await,
            DirectoryInner::Sql(store) => store.rcpt(email).await,
            DirectoryInner::Imap(store) => store.rcpt(email).await,
            DirectoryInner::Smtp(store) => store.rcpt(email).await,
            DirectoryInner::Memory(store) => store.rcpt(email).await,
            DirectoryInner::OpenId(store) => store.rcpt(email).await,
        }
        .caused_by(trc::location!())?;

        // Update cache
        if let Some(cache) = &self.cache {
            cache.set_rcpt(email, &result);
        }

        Ok(result)
    }

    pub async fn vrfy(&self, address: &str) -> trc::Result<Vec<String>> {
        match &self.store {
            DirectoryInner::Internal(store) => store.vrfy(address).await,
            DirectoryInner::Ldap(store) => store.vrfy(address).await,
            DirectoryInner::Sql(store) => store.vrfy(address).await,
            DirectoryInner::Imap(store) => store.vrfy(address).await,
            DirectoryInner::Smtp(store) => store.vrfy(address).await,
            DirectoryInner::Memory(store) => store.vrfy(address).await,
            DirectoryInner::OpenId(store) => store.vrfy(address).await,
        }
        .caused_by(trc::location!())
    }

    pub async fn expn(&self, address: &str) -> trc::Result<Vec<String>> {
        match &self.store {
            DirectoryInner::Internal(store) => store.expn(address).await,
            DirectoryInner::Ldap(store) => store.expn(address).await,
            DirectoryInner::Sql(store) => store.expn(address).await,
            DirectoryInner::Imap(store) => store.expn(address).await,
            DirectoryInner::Smtp(store) => store.expn(address).await,
            DirectoryInner::Memory(store) => store.expn(address).await,
            DirectoryInner::OpenId(store) => store.expn(address).await,
        }
        .caused_by(trc::location!())
    }

    pub fn has_bearer_token_support(&self) -> bool {
        match &self.store {
            DirectoryInner::Internal(_)
            | DirectoryInner::Ldap(_)
            | DirectoryInner::Sql(_)
            | DirectoryInner::Imap(_)
            | DirectoryInner::Smtp(_)
            | DirectoryInner::Memory(_) => false,
            DirectoryInner::OpenId(_) => true,
        }
    }
}

impl DirectoryInner {
    pub fn is_enterprise_directory(&self) -> bool {
        false
    }
}
