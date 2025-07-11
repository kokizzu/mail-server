/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::time::Instant;

use common::listener::SessionStream;
use directory::Permission;
use imap_proto::receiver::Request;

use crate::core::{Command, Session, StatusResponse};

impl<T: SessionStream> Session<T> {
    pub async fn handle_checkscript(&mut self, request: Request<Command>) -> trc::Result<Vec<u8>> {
        // Validate access
        self.assert_has_permission(Permission::SieveCheckScript)?;

        let op_start = Instant::now();

        if request.tokens.is_empty() {
            return Err(trc::ManageSieveEvent::Error
                .into_err()
                .details("Expected script as a parameter."));
        }

        let script = request.tokens.into_iter().next().unwrap().unwrap_bytes();
        self.server
            .core
            .sieve
            .untrusted_compiler
            .compile(&script)
            .map(|_| {
                trc::event!(
                    ManageSieve(trc::ManageSieveEvent::CheckScript),
                    SpanId = self.session_id,
                    Size = script.len(),
                    Elapsed = op_start.elapsed()
                );

                StatusResponse::ok("Script is valid.").into_bytes()
            })
            .map_err(|err| {
                trc::ManageSieveEvent::Error
                    .into_err()
                    .details(err.to_string())
            })
    }
}
