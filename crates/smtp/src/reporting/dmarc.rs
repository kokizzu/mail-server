/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use super::{AggregateTimestamp, SerializedSize};
use crate::{
    core::Session,
    queue::{DomainPart, RecipientDomain},
    reporting::SmtpReporting,
};
use ahash::AHashMap;
use common::{
    Server,
    config::smtp::report::AggregateFrequency,
    ipc::{DmarcEvent, ToHash},
    listener::SessionStream,
};
use compact_str::ToCompactString;
use mail_auth::{
    ArcOutput, AuthenticatedMessage, AuthenticationResults, DkimOutput, DkimResult, DmarcOutput,
    SpfResult,
    common::verify::VerifySignature,
    dmarc::{self, URI},
    report::{AuthFailureType, IdentityAlignment, PolicyPublished, Record, Report, SPFDomainScope},
};
use std::{collections::hash_map::Entry, future::Future};
use store::{
    Deserialize, IterateParams, Serialize, ValueKey,
    write::{AlignedBytes, Archive, Archiver, BatchBuilder, QueueClass, ReportEvent, ValueClass},
};
use trc::{AddContext, OutgoingReportEvent};
use utils::config::Rate;

#[derive(
    Debug,
    PartialEq,
    Eq,
    rkyv::Serialize,
    rkyv::Deserialize,
    rkyv::Archive,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct DmarcFormat {
    pub rua: Vec<URI>,
    pub policy: PolicyPublished,
    pub records: Vec<Record>,
}

impl<T: SessionStream> Session<T> {
    #[allow(clippy::too_many_arguments)]
    pub async fn send_dmarc_report(
        &self,
        message: &AuthenticatedMessage<'_>,
        auth_results: &AuthenticationResults<'_>,
        rejected: bool,
        dmarc_output: DmarcOutput,
        dkim_output: &[DkimOutput<'_>],
        arc_output: &Option<ArcOutput<'_>>,
    ) {
        let dmarc_record = dmarc_output.dmarc_record_cloned().unwrap();
        let config = &self.server.core.smtp.report.dmarc;

        // Send failure report
        if let (Some(failure_rate), Some(report_options)) = (
            self.server
                .eval_if::<Rate, _>(&config.send, self, self.data.session_id)
                .await,
            dmarc_output.failure_report(),
        ) {
            // Verify that any external reporting addresses are authorized
            let rcpts = match self
                .server
                .core
                .smtp
                .resolvers
                .dns
                .verify_dmarc_report_address(
                    dmarc_output.domain(),
                    dmarc_record.ruf(),
                    Some(&self.server.inner.cache.dns_txt),
                )
                .await
            {
                Some(rcpts) => {
                    if !rcpts.is_empty() {
                        let mut new_rcpts = Vec::with_capacity(rcpts.len());

                        for rcpt in rcpts {
                            if self.throttle_rcpt(rcpt.uri(), &failure_rate, "dmarc").await {
                                new_rcpts.push(rcpt.uri());
                            }
                        }

                        new_rcpts
                    } else {
                        if !dmarc_record.ruf().is_empty() {
                            trc::event!(
                                OutgoingReport(OutgoingReportEvent::UnauthorizedReportingAddress),
                                SpanId = self.data.session_id,
                                Url = dmarc_record
                                    .ruf()
                                    .iter()
                                    .map(|u| trc::Value::String(u.uri().to_compact_string()))
                                    .collect::<Vec<_>>(),
                            );
                        }
                        vec![]
                    }
                }
                None => {
                    trc::event!(
                        OutgoingReport(OutgoingReportEvent::ReportingAddressValidationError),
                        SpanId = self.data.session_id,
                        Url = dmarc_record
                            .ruf()
                            .iter()
                            .map(|u| trc::Value::String(u.uri().to_compact_string()))
                            .collect::<Vec<_>>(),
                    );

                    vec![]
                }
            };

            // Throttle recipient
            if !rcpts.is_empty() {
                let mut report = Vec::with_capacity(128);
                let from_addr = self
                    .server
                    .eval_if(&config.address, self, self.data.session_id)
                    .await
                    .unwrap_or_else(|| "MAILER-DAEMON@localhost".to_compact_string());
                let mut auth_failure = self
                    .new_auth_failure(AuthFailureType::Dmarc, rejected)
                    .with_authentication_results(auth_results.to_string())
                    .with_headers(std::str::from_utf8(message.raw_headers()).unwrap_or_default());

                // Report the first failed signature
                let dkim_failed = if let (
                    dmarc::Report::Dkim
                    | dmarc::Report::DkimSpf
                    | dmarc::Report::All
                    | dmarc::Report::Any,
                    Some(signature),
                ) = (
                    &report_options,
                    dkim_output.iter().find_map(|o| {
                        let s = o.signature()?;
                        if !matches!(o.result(), DkimResult::Pass) {
                            Some(s)
                        } else {
                            None
                        }
                    }),
                ) {
                    auth_failure = auth_failure
                        .with_dkim_domain(signature.domain())
                        .with_dkim_selector(signature.selector())
                        .with_dkim_identity(signature.identity());
                    true
                } else {
                    false
                };

                // Report SPF failure
                let spf_failed = if let (
                    dmarc::Report::Spf
                    | dmarc::Report::DkimSpf
                    | dmarc::Report::All
                    | dmarc::Report::Any,
                    Some(output),
                ) = (
                    &report_options,
                    self.data
                        .spf_ehlo
                        .as_ref()
                        .and_then(|s| {
                            if s.result() != SpfResult::Pass {
                                s.into()
                            } else {
                                None
                            }
                        })
                        .or_else(|| {
                            self.data.spf_mail_from.as_ref().and_then(|s| {
                                if s.result() != SpfResult::Pass {
                                    s.into()
                                } else {
                                    None
                                }
                            })
                        }),
                ) {
                    auth_failure =
                        auth_failure.with_spf_dns(format!("txt : {} : v=SPF1", output.domain()));
                    // TODO use DNS record
                    true
                } else {
                    false
                };

                auth_failure
                    .with_identity_alignment(if dkim_failed && spf_failed {
                        IdentityAlignment::DkimSpf
                    } else if dkim_failed {
                        IdentityAlignment::Dkim
                    } else {
                        IdentityAlignment::Spf
                    })
                    .write_rfc5322(
                        (
                            self.server
                                .eval_if(&config.name, self, self.data.session_id)
                                .await
                                .unwrap_or_else(|| "Mail Delivery Subsystem".to_compact_string())
                                .as_str(),
                            from_addr.as_str(),
                        ),
                        &rcpts.join(", "),
                        &self
                            .server
                            .eval_if(&config.subject, self, self.data.session_id)
                            .await
                            .unwrap_or_else(|| "DMARC Report".to_compact_string()),
                        &mut report,
                    )
                    .ok();

                trc::event!(
                    OutgoingReport(OutgoingReportEvent::DmarcReport),
                    SpanId = self.data.session_id,
                    From = from_addr.to_string(),
                    To = rcpts
                        .iter()
                        .map(|a| trc::Value::String(a.to_compact_string()))
                        .collect::<Vec<_>>(),
                );

                // Send report
                self.server
                    .send_report(
                        &from_addr,
                        rcpts.into_iter(),
                        report,
                        &config.sign,
                        true,
                        self.data.session_id,
                    )
                    .await;
            } else {
                trc::event!(
                    OutgoingReport(OutgoingReportEvent::DmarcRateLimited),
                    SpanId = self.data.session_id,
                    Limit = vec![
                        trc::Value::from(failure_rate.requests),
                        trc::Value::from(failure_rate.period)
                    ],
                );
            }
        }

        // Send aggregate reports
        let interval = self
            .server
            .eval_if(
                &self.server.core.smtp.report.dmarc_aggregate.send,
                self,
                self.data.session_id,
            )
            .await
            .unwrap_or(AggregateFrequency::Never);

        if matches!(interval, AggregateFrequency::Never) || dmarc_record.rua().is_empty() {
            return;
        }

        // Create DMARC report record
        let mut report_record = Record::new()
            .with_dmarc_output(&dmarc_output)
            .with_dkim_output(dkim_output)
            .with_source_ip(self.data.remote_ip)
            .with_header_from(message.from().domain_part())
            .with_envelope_from(
                self.data
                    .mail_from
                    .as_ref()
                    .map(|mf| mf.domain.as_str())
                    .unwrap_or_else(|| self.data.helo_domain.as_str()),
            );
        if let Some(spf_ehlo) = &self.data.spf_ehlo {
            report_record = report_record.with_spf_output(spf_ehlo, SPFDomainScope::Helo);
        }
        if let Some(spf_mail_from) = &self.data.spf_mail_from {
            report_record = report_record.with_spf_output(spf_mail_from, SPFDomainScope::MailFrom);
        }
        if let Some(arc_output) = arc_output {
            report_record = report_record.with_arc_output(arc_output);
        }

        // Submit DMARC report event
        self.server
            .schedule_report(DmarcEvent {
                domain: dmarc_output.into_domain(),
                report_record,
                dmarc_record,
                interval,
            })
            .await;
    }
}

pub trait DmarcReporting: Sync + Send {
    fn send_dmarc_aggregate_report(&self, event: ReportEvent) -> impl Future<Output = ()> + Send;
    fn generate_dmarc_aggregate_report(
        &self,
        event: &ReportEvent,
        rua: &mut Vec<URI>,
        serialized_size: Option<&mut serde_json::Serializer<SerializedSize>>,
        span_id: u64,
    ) -> impl Future<Output = trc::Result<Option<Report>>> + Send;
    fn delete_dmarc_report(&self, event: ReportEvent) -> impl Future<Output = ()> + Send;
    fn schedule_dmarc(&self, event: Box<DmarcEvent>) -> impl Future<Output = ()> + Send;
}

impl DmarcReporting for Server {
    async fn send_dmarc_aggregate_report(&self, event: ReportEvent) {
        let span_id = self.inner.data.span_id_gen.generate();

        trc::event!(
            OutgoingReport(OutgoingReportEvent::DmarcAggregateReport),
            SpanId = span_id,
            ReportId = event.seq_id,
            Domain = event.domain.clone(),
            RangeFrom = trc::Value::Timestamp(event.seq_id),
            RangeTo = trc::Value::Timestamp(event.due),
        );

        // Generate report
        let mut serialized_size = serde_json::Serializer::new(SerializedSize::new(
            self.eval_if(
                &self.core.smtp.report.dmarc_aggregate.max_size,
                &RecipientDomain::new(event.domain.as_str()),
                span_id,
            )
            .await
            .unwrap_or(25 * 1024 * 1024),
        ));
        let mut rua = Vec::new();
        let report = match self
            .generate_dmarc_aggregate_report(&event, &mut rua, Some(&mut serialized_size), span_id)
            .await
        {
            Ok(Some(report)) => report,
            Ok(None) => {
                trc::event!(
                    OutgoingReport(OutgoingReportEvent::NotFound),
                    SpanId = span_id,
                    CausedBy = trc::location!()
                );

                return;
            }
            Err(err) => {
                trc::error!(err.span_id(span_id).details("Failed to read DMARC report"));
                return;
            }
        };

        // Verify external reporting addresses
        let rua = match self
            .core
            .smtp
            .resolvers
            .dns
            .verify_dmarc_report_address(&event.domain, &rua, Some(&self.inner.cache.dns_txt))
            .await
        {
            Some(rcpts) => {
                if !rcpts.is_empty() {
                    rcpts
                        .into_iter()
                        .map(|u| u.uri().to_string())
                        .collect::<Vec<_>>()
                } else {
                    trc::event!(
                        OutgoingReport(OutgoingReportEvent::UnauthorizedReportingAddress),
                        SpanId = span_id,
                        Url = rua
                            .iter()
                            .map(|u| trc::Value::String(u.uri().to_compact_string()))
                            .collect::<Vec<_>>(),
                    );

                    self.delete_dmarc_report(event).await;
                    return;
                }
            }
            None => {
                trc::event!(
                    OutgoingReport(OutgoingReportEvent::ReportingAddressValidationError),
                    SpanId = span_id,
                    Url = rua
                        .iter()
                        .map(|u| trc::Value::String(u.uri().to_compact_string()))
                        .collect::<Vec<_>>(),
                );

                self.delete_dmarc_report(event).await;
                return;
            }
        };

        // Serialize report
        let config = &self.core.smtp.report.dmarc_aggregate;
        let from_addr = self
            .eval_if(
                &config.address,
                &RecipientDomain::new(event.domain.as_str()),
                span_id,
            )
            .await
            .unwrap_or_else(|| "MAILER-DAEMON@localhost".to_compact_string());
        let mut message = Vec::with_capacity(2048);
        let _ = report.write_rfc5322(
            &self
                .eval_if(
                    &self.core.smtp.report.submitter,
                    &RecipientDomain::new(event.domain.as_str()),
                    span_id,
                )
                .await
                .unwrap_or_else(|| "localhost".to_compact_string()),
            (
                self.eval_if(
                    &config.name,
                    &RecipientDomain::new(event.domain.as_str()),
                    span_id,
                )
                .await
                .unwrap_or_else(|| "Mail Delivery Subsystem".to_compact_string())
                .as_str(),
                from_addr.as_str(),
            ),
            rua.iter().map(|a| a.as_str()),
            &mut message,
        );

        // Send report
        self.send_report(
            &from_addr,
            rua.iter(),
            message,
            &config.sign,
            false,
            event.seq_id,
        )
        .await;

        self.delete_dmarc_report(event).await;
    }

    async fn generate_dmarc_aggregate_report(
        &self,
        event: &ReportEvent,
        rua: &mut Vec<URI>,
        mut serialized_size: Option<&mut serde_json::Serializer<SerializedSize>>,
        span_id: u64,
    ) -> trc::Result<Option<Report>> {
        // Deserialize report
        let dmarc = match self
            .store()
            .get_value::<Archive<AlignedBytes>>(ValueKey::from(ValueClass::Queue(
                QueueClass::DmarcReportHeader(event.clone()),
            )))
            .await?
        {
            Some(dmarc) => dmarc.deserialize::<DmarcFormat>()?,
            None => {
                return Ok(None);
            }
        };
        let _ = std::mem::replace(rua, dmarc.rua);

        // Create report
        let config = &self.core.smtp.report.dmarc_aggregate;
        let mut report = Report::new()
            .with_policy_published(dmarc.policy)
            .with_date_range_begin(event.seq_id)
            .with_date_range_end(event.due)
            .with_report_id(format!("{}_{}", event.policy_hash, event.seq_id))
            .with_email(
                self.eval_if(
                    &config.address,
                    &RecipientDomain::new(event.domain.as_str()),
                    span_id,
                )
                .await
                .unwrap_or_else(|| "MAILER-DAEMON@localhost".to_compact_string()),
            );
        if let Some(org_name) = self
            .eval_if::<String, _>(
                &config.org_name,
                &RecipientDomain::new(event.domain.as_str()),
                span_id,
            )
            .await
        {
            report = report.with_org_name(org_name);
        }
        if let Some(contact_info) = self
            .eval_if::<String, _>(
                &config.contact_info,
                &RecipientDomain::new(event.domain.as_str()),
                span_id,
            )
            .await
        {
            report = report.with_extra_contact_info(contact_info);
        }

        if let Some(serialized_size) = serialized_size.as_deref_mut() {
            let _ = serde::Serialize::serialize(&report, serialized_size);
        }

        // Group duplicates
        let from_key = ValueKey::from(ValueClass::Queue(QueueClass::DmarcReportEvent(
            ReportEvent {
                due: event.due,
                policy_hash: event.policy_hash,
                seq_id: 0,
                domain: event.domain.clone(),
            },
        )));
        let to_key = ValueKey::from(ValueClass::Queue(QueueClass::DmarcReportEvent(
            ReportEvent {
                due: event.due,
                policy_hash: event.policy_hash,
                seq_id: u64::MAX,
                domain: event.domain.clone(),
            },
        )));
        let mut record_map = AHashMap::with_capacity(dmarc.records.len());
        self.core
            .storage
            .data
            .iterate(IterateParams::new(from_key, to_key).ascending(), |_, v| {
                let archive = <Archive<AlignedBytes> as Deserialize>::deserialize(v)?;

                match record_map.entry(archive.deserialize::<Record>()?) {
                    Entry::Occupied(mut e) => {
                        *e.get_mut() += 1;
                        Ok(true)
                    }
                    Entry::Vacant(e) => {
                        if serialized_size
                            .as_deref_mut()
                            .is_none_or(|serialized_size| {
                                serde::Serialize::serialize(e.key(), serialized_size).is_ok()
                            })
                        {
                            e.insert(1u32);
                            Ok(true)
                        } else {
                            Ok(false)
                        }
                    }
                }
            })
            .await
            .caused_by(trc::location!())?;

        for (record, count) in record_map {
            report = report.with_record(record.with_count(count));
        }

        Ok(Some(report))
    }

    async fn delete_dmarc_report(&self, event: ReportEvent) {
        let from_key = ReportEvent {
            due: event.due,
            policy_hash: event.policy_hash,
            seq_id: 0,
            domain: event.domain.clone(),
        };
        let to_key = ReportEvent {
            due: event.due,
            policy_hash: event.policy_hash,
            seq_id: u64::MAX,
            domain: event.domain.clone(),
        };

        if let Err(err) = self
            .core
            .storage
            .data
            .delete_range(
                ValueKey::from(ValueClass::Queue(QueueClass::DmarcReportEvent(from_key))),
                ValueKey::from(ValueClass::Queue(QueueClass::DmarcReportEvent(to_key))),
            )
            .await
        {
            trc::error!(
                err.caused_by(trc::location!())
                    .details("Failed to delete DMARC report")
            );
            return;
        }

        let mut batch = BatchBuilder::new();
        batch.clear(ValueClass::Queue(QueueClass::DmarcReportHeader(event)));
        if let Err(err) = self.core.storage.data.write(batch.build_all()).await {
            trc::error!(
                err.caused_by(trc::location!())
                    .details("Failed to delete DMARC report")
            );
        }
    }

    async fn schedule_dmarc(&self, event: Box<DmarcEvent>) {
        let created = event.interval.to_timestamp();
        let deliver_at = created + event.interval.as_secs();
        let mut report_event = ReportEvent {
            due: deliver_at,
            policy_hash: event.dmarc_record.to_hash(),
            seq_id: created,
            domain: event.domain,
        };

        // Write policy if missing
        let mut builder = BatchBuilder::new();
        if self
            .core
            .storage
            .data
            .get_value::<()>(ValueKey::from(ValueClass::Queue(
                QueueClass::DmarcReportHeader(report_event.clone()),
            )))
            .await
            .unwrap_or_default()
            .is_none()
        {
            // Serialize report
            let entry = DmarcFormat {
                rua: event.dmarc_record.rua().to_vec(),
                policy: PolicyPublished::from_record(
                    report_event.domain.to_string(),
                    &event.dmarc_record,
                ),
                records: vec![],
            };

            // Write report
            builder.set(
                ValueClass::Queue(QueueClass::DmarcReportHeader(report_event.clone())),
                match Archiver::new(entry).serialize() {
                    Ok(data) => data.to_vec(),
                    Err(err) => {
                        trc::error!(
                            err.caused_by(trc::location!())
                                .details("Failed to serialize DMARC report")
                        );
                        return;
                    }
                },
            );
        }

        // Write entry
        report_event.seq_id = self.inner.data.queue_id_gen.generate();
        builder.set(
            ValueClass::Queue(QueueClass::DmarcReportEvent(report_event)),
            match Archiver::new(event.report_record).serialize() {
                Ok(data) => data.to_vec(),
                Err(err) => {
                    trc::error!(
                        err.caused_by(trc::location!())
                            .details("Failed to serialize DMARC report")
                    );
                    return;
                }
            },
        );

        if let Err(err) = self.core.storage.data.write(builder.build_all()).await {
            trc::error!(
                err.caused_by(trc::location!())
                    .details("Failed to write DMARC report")
            );
        }
    }
}
