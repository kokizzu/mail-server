/*
 * Copyright (c) 2023 Stalwart Labs Ltd.
 *
 * This file is part of Stalwart Mail Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

use std::borrow::Cow;

use mail_auth::AuthenticatedMessage;
use smtp_proto::request::parser::Rfc5321Parser;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    config::Milter,
    core::{Session, SessionAddress, SessionData},
    inbound::{milter::MilterClient, IsTls},
    queue::DomainPart,
    DAEMON_NAME,
};

use super::{Action, Error, Macros, Modification};

enum Rejection {
    Action(Action),
    Error(Error),
}

impl<T: AsyncWrite + AsyncRead + IsTls + Unpin> Session<T> {
    pub async fn run_milters(
        &self,
        message: &AuthenticatedMessage<'_>,
    ) -> Result<Vec<Modification>, Cow<'static, [u8]>> {
        let milters = &self.core.session.config.data.milters;
        if milters.is_empty() {
            return Ok(Vec::new());
        }

        let mut modifications = Vec::new();
        for milter in milters {
            if !*milter.enable.eval(self).await {
                continue;
            }

            match self.connect_and_run(milter, message).await {
                Ok(new_modifications) => {
                    if !modifications.is_empty() {
                        // The message body can only be replaced once, so we need to remove
                        // any previous replacements.
                        modifications.retain(|m| !matches!(m, Modification::ReplaceBody { .. }));
                        modifications.extend(new_modifications);
                    } else {
                        modifications = new_modifications;
                    }
                }
                Err(Rejection::Action(action)) => {
                    tracing::debug!(
                        parent: &self.span,
                        milter.host = &milter.hostname,
                        milter.port = &milter.port,
                        context = "milter",
                        event = "reject",
                        action = ?action,
                        "Milter rejected message.");

                    return Err(match action {
                        Action::Discard => {
                            (b"250 2.0.0 Message queued for delivery.\r\n"[..]).into()
                        }
                        Action::Reject => (b"503 5.5.3 Message rejected.\r\n"[..]).into(),
                        Action::TempFail => {
                            (b"451 4.3.5 Unable to accept message at this time.\r\n"[..]).into()
                        }
                        Action::ReplyCode { code, text } => {
                            let mut response = Vec::with_capacity(text.len() + 6);
                            response.extend_from_slice(code.as_slice());
                            response.push(b' ');
                            response.extend_from_slice(text.as_bytes());
                            if !text.ends_with('\n') {
                                response.extend_from_slice(b"\r\n");
                            }
                            response.into()
                        }
                        Action::Shutdown => (b"421 4.3.0 Server shutting down.\r\n"[..]).into(),
                        Action::ConnectionFailure => (b""[..]).into(), // TODO: Not very elegant design, fix.
                        Action::Accept | Action::Continue => unreachable!(),
                    });
                }
                Err(Rejection::Error(err)) => {
                    tracing::warn!(
                        parent: &self.span,
                        milter.host = &milter.hostname,
                        milter.port = &milter.port,
                        context = "milter",
                        event = "error",
                        reason = ?err,
                        "Milter filter failed");
                    if milter.tempfail_on_error {
                        return Err(
                            (b"451 4.3.5 Unable to accept message at this time.\r\n"[..]).into(),
                        );
                    }
                }
            }
        }

        Ok(modifications)
    }

    async fn connect_and_run(
        &self,
        milter: &Milter,
        message: &AuthenticatedMessage<'_>,
    ) -> Result<Vec<Modification>, Rejection> {
        // Build client
        let client = MilterClient::connect(milter, self.span.clone()).await?;
        if !milter.tls {
            self.run(client, message).await
        } else {
            self.run(
                client
                    .into_tls(
                        if !milter.tls_allow_invalid_certs {
                            &self.core.queue.connectors.pki_verify
                        } else {
                            &self.core.queue.connectors.dummy_verify
                        },
                        &milter.hostname,
                    )
                    .await?,
                message,
            )
            .await
        }
    }

    async fn run<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        mut client: MilterClient<S>,
        message: &AuthenticatedMessage<'_>,
    ) -> Result<Vec<Modification>, Rejection> {
        // Option negotiation
        client.init().await?;

        // Connect stage
        let client_ptr = self
            .data
            .iprev
            .as_ref()
            .and_then(|ip_rev| ip_rev.ptr.as_ref())
            .and_then(|ptrs| ptrs.first());
        client
            .connection(
                client_ptr.unwrap_or(&self.data.helo_domain),
                self.data.remote_ip,
                self.data.remote_port,
                Macros::new()
                    .with_daemon_name(DAEMON_NAME)
                    .with_local_hostname(&self.instance.hostname)
                    .with_client_address(self.data.remote_ip)
                    .with_client_port(self.data.remote_port)
                    .with_client_ptr(client_ptr.map(|p| p.as_str()).unwrap_or("unknown")),
            )
            .await?
            .assert_continue()?;

        // EHLO/HELO
        let (tls_version, tls_ciper) = self.stream.tls_version_and_cipher();
        client
            .helo(
                &self.data.helo_domain,
                Macros::new()
                    .with_cipher(tls_ciper)
                    .with_tls_version(tls_version),
            )
            .await?
            .assert_continue()?;

        // Mail from
        let addr = &self.data.mail_from.as_ref().unwrap().address_lcase;
        client
            .mail_from(
                &format!("<{addr}>"),
                None::<&[&str]>,
                Macros::new()
                    .with_mail_address(addr)
                    .with_sasl_login_name(&self.data.authenticated_as),
            )
            .await?
            .assert_continue()?;

        // Rcpt to
        for rcpt in &self.data.rcpt_to {
            client
                .rcpt_to(
                    &format!("<{}>", rcpt.address_lcase),
                    None::<&[&str]>,
                    Macros::new().with_rcpt_address(&rcpt.address_lcase),
                )
                .await?
                .assert_continue()?;
        }

        // Headers
        client
            .headers(message.raw_parsed_headers().iter().cloned())
            .await?
            .assert_continue()?;

        // Data
        client.data().await?.assert_continue()?;

        // Message body
        let (action, modifications) = client.body(message.raw_message()).await?;
        action.assert_continue()?;

        // Quit
        let _ = client.quit().await;

        // Return modifications
        Ok(modifications)
    }
}

impl SessionData {
    pub fn apply_modifications(
        &mut self,
        modifications: Vec<Modification>,
        message: &AuthenticatedMessage<'_>,
    ) -> Option<Vec<u8>> {
        let mut body = Vec::new();
        let mut header_changes = Vec::new();
        let mut needs_rewrite = false;

        for modification in modifications {
            match modification {
                Modification::ChangeFrom { sender, mut args } => {
                    // Change sender
                    let sender = strip_brackets(&sender);
                    let address_lcase = sender.to_lowercase();
                    let mut mail_from = SessionAddress {
                        domain: address_lcase.domain_part().to_string(),
                        address_lcase,
                        address: sender,
                        flags: 0,
                        dsn_info: None,
                    };
                    if !args.is_empty() {
                        args.push('\n');
                        match Rfc5321Parser::new(&mut args.as_bytes().iter())
                            .mail_from_parameters(String::new())
                        {
                            Ok(addr) => {
                                mail_from.flags = addr.flags;
                                mail_from.dsn_info = addr.env_id;
                            }
                            Err(err) => {
                                tracing::debug!(
                                    context = "milter",
                                    event = "error",
                                    reason = ?err,
                                    "Failed to parse milter mailFrom parameters.");
                            }
                        }
                    }
                    self.mail_from = Some(mail_from);
                }
                Modification::AddRcpt {
                    recipient,
                    mut args,
                } => {
                    // Add recipient
                    let recipient = strip_brackets(&recipient);
                    if recipient.contains('@') {
                        let address_lcase = recipient.to_lowercase();
                        let mut rcpt = SessionAddress {
                            domain: address_lcase.domain_part().to_string(),
                            address_lcase,
                            address: recipient,
                            flags: 0,
                            dsn_info: None,
                        };
                        if !args.is_empty() {
                            args.push('\n');
                            match Rfc5321Parser::new(&mut args.as_bytes().iter())
                                .rcpt_to_parameters(String::new())
                            {
                                Ok(addr) => {
                                    rcpt.flags = addr.flags;
                                    rcpt.dsn_info = addr.orcpt;
                                }
                                Err(err) => {
                                    tracing::debug!(
                                    context = "milter",
                                    event = "error",
                                    reason = ?err,
                                    "Failed to parse milter rcptTo parameters.");
                                }
                            }
                        }

                        if !self.rcpt_to.contains(&rcpt) {
                            self.rcpt_to.push(rcpt);
                        }
                    }
                }
                Modification::DeleteRcpt { recipient } => {
                    let recipient = strip_brackets(&recipient);
                    self.rcpt_to.retain(|r| r.address_lcase != recipient);
                }
                Modification::ReplaceBody { value } => {
                    body.extend(value);
                }
                Modification::AddHeader { name, value } => {
                    header_changes.push((0, name, value, false));
                }
                Modification::InsertHeader { index, name, value } => {
                    header_changes.push((index, name, value, false));
                    needs_rewrite = true;
                }
                Modification::ChangeHeader { index, name, value } => {
                    if message
                        .raw_parsed_headers()
                        .iter()
                        .any(|(n, _)| n.eq_ignore_ascii_case(name.as_bytes()))
                    {
                        header_changes.push((index, name, value, true));
                        needs_rewrite = true;
                    } else {
                        header_changes.push((0, name, value, false));
                    }
                }
                Modification::Quarantine { reason } => {
                    header_changes.push((0, "X-Quarantine".to_string(), reason, false));
                }
            }
        }

        // If there are no header changes return
        if header_changes.is_empty() {
            return if !body.is_empty() {
                let mut new_message = Vec::with_capacity(body.len() + message.raw_headers().len());
                new_message.extend_from_slice(message.raw_headers());
                new_message.extend(body);
                Some(new_message)
            } else {
                None
            };
        }

        let new_body = if !body.is_empty() {
            &body[..]
        } else {
            message.raw_body()
        };

        if needs_rewrite {
            let mut headers = message
                .raw_parsed_headers()
                .iter()
                .map(|(h, v)| (Cow::from(*h), Cow::from(*v)))
                .collect::<Vec<_>>();

            // Perform changes
            for (index, header_name, header_value, is_change) in header_changes {
                if is_change {
                    let mut header_count = 0;
                    for (pos, (name, value)) in headers.iter_mut().enumerate() {
                        if name.eq_ignore_ascii_case(header_name.as_bytes()) {
                            header_count += 1;
                            if header_count == index {
                                if !header_value.is_empty() {
                                    *value = Cow::from(header_value.into_bytes());
                                } else {
                                    headers.remove(pos);
                                }
                                break;
                            }
                        }
                    }
                } else {
                    let mut header_pos = 0;
                    if index > 0 {
                        let mut header_count = 0;
                        for (pos, (name, _)) in headers.iter().enumerate() {
                            if name.eq_ignore_ascii_case(header_name.as_bytes()) {
                                header_pos = pos;
                                header_count += 1;
                                if header_count == index {
                                    break;
                                }
                            }
                        }
                    }

                    headers.insert(
                        header_pos,
                        (
                            Cow::from(header_name.into_bytes()),
                            Cow::from(header_value.into_bytes()),
                        ),
                    );
                }
            }

            // Write new headers
            let mut new_message = Vec::with_capacity(
                new_body.len()
                    + message.raw_headers().len()
                    + headers
                        .iter()
                        .map(|(h, v)| h.len() + v.len() + 4)
                        .sum::<usize>(),
            );
            for (header, value) in headers {
                new_message.extend_from_slice(header.as_ref());
                if value.first().map_or(false, |c| c.is_ascii_whitespace()) {
                    new_message.extend_from_slice(b":");
                } else {
                    new_message.extend_from_slice(b": ");
                }
                new_message.extend_from_slice(value.as_ref());
                if !value.last().map_or(false, |c| *c == b'\n') {
                    new_message.extend_from_slice(b"\r\n");
                }
            }
            new_message.extend_from_slice(b"\r\n");
            new_message.extend(new_body);
            Some(new_message)
        } else {
            let mut new_message = Vec::with_capacity(
                new_body.len()
                    + message.raw_headers().len()
                    + header_changes
                        .iter()
                        .map(|(_, h, v, _)| h.len() + v.len() + 4)
                        .sum::<usize>(),
            );
            for (_, header, value, _) in header_changes {
                new_message.extend_from_slice(header.as_bytes());
                new_message.extend_from_slice(b": ");
                new_message.extend_from_slice(value.as_bytes());
                if !value.ends_with('\n') {
                    new_message.extend_from_slice(b"\r\n");
                }
            }
            new_message.extend_from_slice(message.raw_headers());
            new_message.extend(new_body);
            Some(new_message)
        }
    }
}

impl Action {
    fn assert_continue(self) -> Result<(), Rejection> {
        match self {
            Action::Continue | Action::Accept => Ok(()),
            action => Err(Rejection::Action(action)),
        }
    }
}

impl From<Error> for Rejection {
    fn from(err: Error) -> Self {
        Rejection::Error(err)
    }
}

fn strip_brackets(addr: &str) -> String {
    let addr = addr.trim();
    if let Some(addr) = addr.strip_prefix('<') {
        if let Some((addr, _)) = addr.rsplit_once('>') {
            addr.trim().to_string()
        } else {
            addr.trim().to_string()
        }
    } else {
        addr.to_string()
    }
}