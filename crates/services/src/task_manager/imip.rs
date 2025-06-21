/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::task_manager::Task;
use calcard::icalendar::{ICalendarMethod, ICalendarParticipationStatus};
use common::{
    DEFAULT_LOGO, Server,
    listener::{ServerInstance, stream::NullIo},
};
use groupware::{calendar::itip::ItipIngest, scheduling::ItipMessages};
use mail_builder::{
    MessageBuilder,
    headers::{HeaderType, content_type::ContentType},
    mime::{BodyPart, MimePart},
};
use smtp::core::{Session, SessionData};
use smtp_proto::{MailFrom, RcptTo};
use std::{sync::Arc, time::Duration};
use store::{
    ValueKey,
    write::{AlignedBytes, Archive, TaskQueueClass, ValueClass, now},
};
use trc::AddContext;

pub trait SendImipTask: Sync + Send {
    fn send_imip(
        &self,
        task: &Task,
        server_instance: Arc<ServerInstance>,
    ) -> impl Future<Output = bool> + Send;
}

impl SendImipTask for Server {
    async fn send_imip(&self, task: &Task, server_instance: Arc<ServerInstance>) -> bool {
        match send_imip(self, task, server_instance).await {
            Ok(result) => result,
            Err(err) => {
                trc::error!(
                    err.account_id(task.account_id)
                        .document_id(task.document_id)
                        .caused_by(trc::location!())
                        .details("Failed to process alarm")
                );
                false
            }
        }
    }
}

async fn send_imip(
    server: &Server,
    task: &Task,
    server_instance: Arc<ServerInstance>,
) -> trc::Result<bool> {
    // Obtain access token
    let access_token = server
        .get_access_token(task.account_id)
        .await
        .caused_by(trc::location!())?;

    // Obtain iMIP payload
    let Some(archive) = server
        .store()
        .get_value::<Archive<AlignedBytes>>(ValueKey {
            account_id: task.account_id,
            collection: 0,
            document_id: task.document_id,
            class: ValueClass::TaskQueue(TaskQueueClass::SendImip {
                due: task.due,
                is_payload: true,
            }),
        })
        .await
        .caused_by(trc::location!())?
    else {
        trc::event!(
            Calendar(trc::CalendarEvent::ItipMessageError),
            AccountId = task.account_id,
            DocumentId = task.document_id,
            Reason = "Missing iMIP payload",
        );
        return Ok(true);
    };

    let imip = archive
        .unarchive::<ItipMessages>()
        .caused_by(trc::location!())?;

    let sender_domain = imip
        .messages
        .first()
        .and_then(|msg| msg.from.rsplit('@').next())
        .unwrap_or("localhost");

    // Obtain logo image
    let logo = match server.logo_resource(sender_domain).await {
        Ok(logo) => logo,
        Err(err) => {
            trc::error!(
                err.caused_by(trc::location!())
                    .details("Failed to fetch logo image")
            );
            None
        }
    };
    let (logo_content_type, logo_contents) = if let Some(logo) = &logo {
        (logo.content_type.as_ref(), logo.contents.as_slice())
    } else {
        ("image/svg+xml", DEFAULT_LOGO.as_bytes())
    };
    let logo_cid = format!("logo.{}@{sender_domain}", now());

    for itip_message in imip.messages.iter() {
        for recipient in itip_message.to.iter() {
            let mut rsvp_urls = Vec::new();
            if itip_message.method == ICalendarMethod::Request {
                if let Some(rsvp_url) = server
                    .http_rsvp_url(task.account_id, task.document_id, recipient.as_str())
                    .await
                {
                    rsvp_urls = [
                        ICalendarParticipationStatus::Accepted,
                        ICalendarParticipationStatus::Declined,
                        ICalendarParticipationStatus::Tentative,
                    ]
                    .into_iter()
                    .map(|status| (rsvp_url.url(&status, "en"), status))
                    .collect();
                }
            }

            let todo = "use templates";
            let subject = "subject";
            let txt_body = "text body";
            let mut html_body = "<html><body>HTML body</body></html>".to_string();

            for (url, method) in rsvp_urls {
                html_body.push_str(&format!("<a href=\"{url}\">{}</a>", method.as_str()));
            }

            let message = MessageBuilder::new()
                .from((access_token.name.as_str(), itip_message.from.as_str()))
                .to(recipient.as_str())
                .header("Auto-Submitted", HeaderType::Text("auto-generated".into()))
                .header(
                    "Reply-To",
                    HeaderType::Text(itip_message.from.as_str().into()),
                )
                .subject(subject)
                .body(MimePart::new(
                    ContentType::new("multipart/mixed"),
                    BodyPart::Multipart(vec![
                        MimePart::new(
                            ContentType::new("multipart/alternative"),
                            BodyPart::Multipart(vec![
                                MimePart::new(
                                    ContentType::new("text/plain"),
                                    BodyPart::Text(txt_body.into()),
                                ),
                                MimePart::new(
                                    ContentType::new("text/html"),
                                    BodyPart::Text(html_body.into()),
                                ),
                            ]),
                        ),
                        MimePart::new(
                            ContentType::new("text/calendar")
                                .attribute("method", itip_message.method.as_str())
                                .attribute("charset", "utf-8"),
                            BodyPart::Text(itip_message.message.as_str().into()),
                        )
                        .attachment("event.ics"),
                        MimePart::new(
                            ContentType::new(logo_content_type),
                            BodyPart::Binary(logo_contents.into()),
                        )
                        .inline()
                        .cid(logo_cid.as_str()),
                    ]),
                ))
                .write_to_vec()
                .unwrap_or_default();

            // Send message
            let server_ = server.clone();
            let server_instance = server_instance.clone();
            let access_token = access_token.clone();
            let from = itip_message.from.to_string();
            let to = recipient.to_string();
            let account_id = task.account_id;
            let document_id = task.document_id;
            tokio::spawn(async move {
                let mut session = Session::<NullIo>::local(
                    server_,
                    server_instance,
                    SessionData::local(access_token, None, vec![], vec![], 0),
                );

                // MAIL FROM
                let _ = session
                    .handle_mail_from(MailFrom {
                        address: from.clone(),
                        ..Default::default()
                    })
                    .await;
                if let Some(error) = session.has_failed() {
                    trc::event!(
                        Calendar(trc::CalendarEvent::ItipMessageError),
                        AccountId = account_id,
                        DocumentId = document_id,
                        From = from,
                        To = to,
                        Reason = format!("Server rejected MAIL-FROM: {}", error.trim()),
                    );
                    return;
                }

                // RCPT TO
                session.params.rcpt_errors_wait = Duration::from_secs(0);
                let _ = session
                    .handle_rcpt_to(RcptTo {
                        address: to.clone(),
                        ..Default::default()
                    })
                    .await;
                if let Some(error) = session.has_failed() {
                    trc::event!(
                        Calendar(trc::CalendarEvent::ItipMessageError),
                        AccountId = account_id,
                        DocumentId = document_id,
                        From = from,
                        To = to,
                        Reason = format!("Server rejected RCPT-TO: {}", error.trim()),
                    );
                    return;
                }

                // DATA
                session.data.message = message;
                let response = session.queue_message().await;
                if let smtp::core::State::Accepted(queue_id) = session.state {
                    trc::event!(
                        Calendar(trc::CalendarEvent::ItipMessageSent),
                        From = from,
                        To = to,
                        AccountId = account_id,
                        DocumentId = document_id,
                        QueueId = queue_id,
                    );
                } else {
                    trc::event!(
                        Calendar(trc::CalendarEvent::ItipMessageError),
                        From = from,
                        To = to,
                        AccountId = account_id,
                        DocumentId = document_id,
                        Reason = format!(
                            "Server rejected DATA: {}",
                            std::str::from_utf8(&response).unwrap().trim()
                        ),
                    );
                }
            })
            .await
            .map_err(|_| {
                trc::Error::new(trc::EventType::Server(trc::ServerEvent::ThreadError))
                    .caused_by(trc::location!())
            })?;
        }
    }

    Ok(true)
}
