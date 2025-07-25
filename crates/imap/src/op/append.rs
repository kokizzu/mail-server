/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::{sync::Arc, time::Instant};

use directory::Permission;
use email::message::ingest::{EmailIngest, IngestEmail, IngestSource};
use imap_proto::{
    Command, ResponseCode, StatusResponse,
    protocol::{append::Arguments, select::HighestModSeq},
    receiver::Request,
};

use crate::{
    core::{ImapUidToId, MailboxId, SelectedMailbox, Session, SessionData},
    spawn_op,
};
use common::listener::SessionStream;
use jmap_proto::types::{acl::Acl, keyword::Keyword, state::StateChange, type_state::DataType};
use mail_parser::MessageParser;

use super::{ImapContext, ToModSeq};

impl<T: SessionStream> Session<T> {
    pub async fn handle_append(&mut self, request: Request<Command>) -> trc::Result<()> {
        // Validate access
        self.assert_has_permission(Permission::ImapAppend)?;

        let op_start = Instant::now();
        let arguments = request.parse_append(self.version)?;
        let (data, selected_mailbox) = self.state.session_mailbox_state();

        // Refresh mailboxes
        data.synchronize_mailboxes(false)
            .await
            .imap_ctx(&arguments.tag, trc::location!())?;

        // Obtain mailbox
        let mailbox = if let Some(mailbox) = data.get_mailbox_by_name(&arguments.mailbox_name) {
            mailbox
        } else {
            return Err(trc::ImapEvent::Error
                .into_err()
                .details("Mailbox does not exist.")
                .code(ResponseCode::TryCreate)
                .id(arguments.tag));
        };
        let is_qresync = self.is_qresync;

        spawn_op!(data, {
            let response = data
                .append_messages(arguments, selected_mailbox, mailbox, is_qresync, op_start)
                .await?
                .into_bytes();

            data.write_bytes(response).await
        })
    }
}

impl<T: SessionStream> SessionData<T> {
    async fn append_messages(
        &self,
        arguments: Arguments,
        selected_mailbox: Option<Arc<SelectedMailbox>>,
        mailbox: MailboxId,
        is_qresync: bool,
        op_start: Instant,
    ) -> trc::Result<StatusResponse> {
        // Verify ACLs
        let account_id = mailbox.account_id;
        let mailbox_id = mailbox.mailbox_id;
        if !self
            .check_mailbox_acl(account_id, mailbox_id, Acl::AddItems)
            .await
            .imap_ctx(&arguments.tag, trc::location!())?
        {
            return Err(trc::ImapEvent::Error
                .into_err()
                .details(
                    "You do not have the required permissions to append messages to this mailbox.",
                )
                .code(ResponseCode::NoPerm)
                .id(arguments.tag));
        }

        // Obtain access token
        let access_token = self
            .server
            .get_access_token(mailbox.account_id)
            .await
            .imap_ctx(&arguments.tag, trc::location!())?;
        let spam_train = self.server.email_bayes_can_train(&access_token);

        // Append messages
        let mut response = StatusResponse::completed(Command::Append);
        let mut created_ids = Vec::with_capacity(arguments.messages.len());
        let mut last_change_id = None;
        for message in arguments.messages {
            match self
                .server
                .email_ingest(IngestEmail {
                    raw_message: &message.message,
                    message: MessageParser::new().parse(&message.message),
                    access_token: &access_token,
                    mailbox_ids: vec![mailbox_id],
                    keywords: message.flags.into_iter().map(Keyword::from).collect(),
                    received_at: message.received_at.map(|d| d as u64),
                    source: IngestSource::Imap,
                    spam_classify: false,
                    spam_train,
                    session_id: self.session_id,
                })
                .await
            {
                Ok(email) => {
                    created_ids.push(ImapUidToId {
                        uid: email.imap_uids[0],
                        id: email.id.document_id(),
                    });
                    last_change_id = Some(email.change_id);
                }
                Err(err) => {
                    return Err(
                        if err.matches(trc::EventType::Limit(trc::LimitEvent::Quota)) {
                            err.details("Disk quota exceeded.")
                                .code(ResponseCode::OverQuota)
                        } else if err.matches(trc::EventType::Limit(trc::LimitEvent::TenantQuota)) {
                            err.details("Organization disk quota exceeded.")
                                .code(ResponseCode::OverQuota)
                        } else {
                            err
                        }
                        .id(arguments.tag),
                    );
                }
            }
        }

        // Broadcast changes
        if let Some(change_id) = last_change_id {
            self.server
                .broadcast_state_change(
                    StateChange::new(account_id, change_id)
                        .with_change(DataType::Email)
                        .with_change(DataType::Mailbox)
                        .with_change(DataType::Thread),
                )
                .await;
        }

        trc::event!(
            Imap(trc::ImapEvent::Append),
            SpanId = self.session_id,
            MailboxName = arguments.mailbox_name.clone(),
            AccountId = account_id,
            MailboxId = mailbox_id,
            DocumentId = created_ids
                .iter()
                .map(|r| trc::Value::from(r.id))
                .collect::<Vec<_>>(),
            Elapsed = op_start.elapsed()
        );

        if !created_ids.is_empty() {
            let uids = created_ids.iter().map(|id| id.uid).collect();
            match selected_mailbox {
                Some(selected_mailbox) if selected_mailbox.id == mailbox => {
                    // Write updated modseq
                    if is_qresync {
                        self.write_bytes(
                            HighestModSeq::new(last_change_id.unwrap_or_default().to_modseq())
                                .into_bytes(),
                        )
                        .await?;
                    }

                    selected_mailbox.append_messages(created_ids, last_change_id);
                }
                _ => {}
            };
            let uid_validity = self
                .mailbox_state(&mailbox)
                .map(|m| m.uid_validity as u32)
                .unwrap_or_default();

            response = response.with_code(ResponseCode::AppendUid { uid_validity, uids });
        }

        Ok(response.with_tag(arguments.tag))
    }
}
