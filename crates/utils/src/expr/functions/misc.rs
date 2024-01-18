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

use std::net::IpAddr;

use mail_auth::common::resolver::ToReverseName;

use crate::expr::Variable;

pub(crate) fn fn_is_empty(v: Vec<Variable>) -> Variable {
    match &v[0] {
        Variable::String(s) => s.is_empty(),
        Variable::Integer(_) | Variable::Float(_) => false,
        Variable::Array(a) => a.is_empty(),
    }
    .into()
}

pub(crate) fn fn_is_number(v: Vec<Variable>) -> Variable {
    matches!(&v[0], Variable::Integer(_) | Variable::Float(_)).into()
}

pub(crate) fn fn_is_ip_addr(v: Vec<Variable>) -> Variable {
    v[0].to_string().parse::<std::net::IpAddr>().is_ok().into()
}

pub(crate) fn fn_is_ipv4_addr(v: Vec<Variable>) -> Variable {
    v[0].to_string()
        .parse::<std::net::IpAddr>()
        .map_or(false, |ip| matches!(ip, IpAddr::V4(_)))
        .into()
}

pub(crate) fn fn_is_ipv6_addr(v: Vec<Variable>) -> Variable {
    v[0].to_string()
        .parse::<std::net::IpAddr>()
        .map_or(false, |ip| matches!(ip, IpAddr::V6(_)))
        .into()
}

pub(crate) fn fn_ip_reverse_name(v: Vec<Variable>) -> Variable {
    v[0].to_string()
        .parse::<std::net::IpAddr>()
        .map(|ip| ip.to_reverse_name())
        .unwrap_or_default()
        .into()
}
