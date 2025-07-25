/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use compact_str::{CompactString, ToCompactString};

use crate::{
    Command,
    protocol::{
        ProtocolVersion,
        list::{self, ReturnOption, SelectionOption},
        status::Status,
    },
    receiver::{Request, Token, bad},
    utf7::utf7_maybe_decode,
};

impl Request<Command> {
    #[allow(clippy::while_let_on_iterator)]
    pub fn parse_list(self, version: ProtocolVersion) -> trc::Result<list::Arguments> {
        match self.tokens.len() {
            0 | 1 => Err(self.into_error("Missing arguments.")),
            2 => {
                let mut tokens = self.tokens.into_iter();
                Ok(list::Arguments::Basic {
                    reference_name: tokens
                        .next()
                        .unwrap()
                        .unwrap_string()
                        .map_err(|v| bad(self.tag.to_compact_string(), v))?,
                    mailbox_name: utf7_maybe_decode(
                        tokens
                            .next()
                            .unwrap()
                            .unwrap_string()
                            .map_err(|v| bad(self.tag.to_compact_string(), v))?,
                        version,
                    ),
                    tag: self.tag,
                })
            }
            _ => {
                let mut tokens = self.tokens.into_iter();
                let mut selection_options = Vec::new();
                let mut return_options = Vec::new();
                let mut mailbox_name = Vec::new();

                let reference_name = match tokens.next().unwrap() {
                    Token::ParenthesisOpen => {
                        while let Some(token) = tokens.next() {
                            match token {
                                Token::ParenthesisClose => break,
                                Token::Argument(value) => {
                                    selection_options.push(
                                        SelectionOption::parse(&value)
                                            .map_err(|v| bad(self.tag.to_compact_string(), v))?,
                                    );
                                }
                                _ => {
                                    return Err(bad(
                                        self.tag.to_compact_string(),
                                        "Invalid selection option argument.",
                                    ));
                                }
                            }
                        }
                        tokens
                            .next()
                            .ok_or_else(|| {
                                bad(self.tag.to_compact_string(), "Missing reference name.")
                            })?
                            .unwrap_string()
                            .map_err(|v| bad(self.tag.to_compact_string(), v))?
                    }
                    token => token
                        .unwrap_string()
                        .map_err(|v| bad(self.tag.to_compact_string(), v))?,
                };

                match tokens
                    .next()
                    .ok_or_else(|| bad(self.tag.to_compact_string(), "Missing mailbox name."))?
                {
                    Token::ParenthesisOpen => {
                        while let Some(token) = tokens.next() {
                            match token {
                                Token::ParenthesisClose => break,
                                token => {
                                    mailbox_name.push(
                                        token
                                            .unwrap_string()
                                            .map_err(|v| bad(self.tag.to_compact_string(), v))?,
                                    );
                                }
                            }
                        }
                    }
                    token => {
                        mailbox_name.push(utf7_maybe_decode(
                            token
                                .unwrap_string()
                                .map_err(|v| bad(self.tag.to_compact_string(), v))?,
                            version,
                        ));
                    }
                }

                if tokens
                    .next()
                    .is_some_and(|token| token.eq_ignore_ascii_case(b"return"))
                {
                    if tokens
                        .next()
                        .is_none_or(|token| !token.is_parenthesis_open())
                    {
                        return Err(bad(
                            self.tag.to_compact_string(),
                            "Invalid return option, expected parenthesis.",
                        ));
                    }

                    while let Some(token) = tokens.next() {
                        match token {
                            Token::ParenthesisClose => break,
                            Token::Argument(value) => {
                                let mut return_option = ReturnOption::parse(&value)
                                    .map_err(|v| bad(self.tag.to_compact_string(), v))?;
                                if let ReturnOption::Status(status) = &mut return_option {
                                    if tokens
                                        .next()
                                        .is_none_or(|token| !token.is_parenthesis_open())
                                    {
                                        return Err(bad(
                                            CompactString::from_string_buffer(self.tag),
                                            "Invalid return option, expected parenthesis after STATUS.",
                                        ));
                                    }
                                    while let Some(token) = tokens.next() {
                                        match token {
                                            Token::ParenthesisClose => break,
                                            Token::Argument(value) => {
                                                status.push(Status::parse(&value).map_err(
                                                    |v| bad(self.tag.to_compact_string(), v),
                                                )?);
                                            }
                                            _ => {
                                                return Err(bad(
                                                    CompactString::from_string_buffer(self.tag),
                                                    "Invalid status return option argument.",
                                                ));
                                            }
                                        }
                                    }
                                }
                                return_options.push(return_option);
                            }
                            _ => {
                                return Err(bad(
                                    self.tag.to_compact_string(),
                                    "Invalid return option argument.",
                                ));
                            }
                        }
                    }
                }

                Ok(list::Arguments::Extended {
                    tag: self.tag,
                    reference_name,
                    mailbox_name,
                    selection_options,
                    return_options,
                })
            }
        }
    }
}

impl SelectionOption {
    pub fn parse(value: &[u8]) -> super::Result<Self> {
        hashify::tiny_map_ignore_case!(value,
            "SUBSCRIBED" => Self::Subscribed,
            "REMOTE" => Self::Remote,
            "RECURSIVEMATCH" => Self::RecursiveMatch,
            "SPECIAL-USE" => Self::SpecialUse,
        )
        .ok_or_else(|| {
            format!(
                "Unsupported selection option '{}'.",
                String::from_utf8_lossy(value)
            )
            .into()
        })
    }
}

impl ReturnOption {
    pub fn parse(value: &[u8]) -> super::Result<Self> {
        hashify::tiny_map_ignore_case!(value,
            "SUBSCRIBED" => Self::Subscribed,
            "CHILDREN" => Self::Children,
            "STATUS" => Self::Status(Vec::with_capacity(2)),
            "SPECIAL-USE" => Self::SpecialUse,
        )
        .ok_or_else(|| format!("Invalid return option {:?}", String::from_utf8_lossy(value)).into())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        protocol::{
            ProtocolVersion,
            list::{self, ReturnOption, SelectionOption},
            status::Status,
        },
        receiver::Receiver,
    };

    #[test]
    fn parse_list() {
        let mut receiver = Receiver::new();

        for (command, arguments) in [
            (
                "A682 LIST \"\" *\r\n",
                list::Arguments::Basic {
                    tag: "A682".into(),
                    reference_name: "".into(),
                    mailbox_name: "*".into(),
                },
            ),
            (
                "A02 LIST (SUBSCRIBED) \"\" \"*\"\r\n",
                list::Arguments::Extended {
                    tag: "A02".into(),
                    reference_name: "".into(),
                    mailbox_name: vec!["*".into()],
                    selection_options: vec![SelectionOption::Subscribed],
                    return_options: vec![],
                },
            ),
            (
                "A03 LIST () \"\" \"%\" RETURN (CHILDREN)\r\n",
                list::Arguments::Extended {
                    tag: "A03".into(),
                    reference_name: "".into(),
                    mailbox_name: vec!["%".into()],
                    selection_options: vec![],
                    return_options: vec![ReturnOption::Children],
                },
            ),
            (
                "A04 LIST (REMOTE) \"\" \"%\" RETURN (CHILDREN)\r\n",
                list::Arguments::Extended {
                    tag: "A04".into(),
                    reference_name: "".into(),
                    mailbox_name: vec!["%".into()],
                    selection_options: vec![SelectionOption::Remote],
                    return_options: vec![ReturnOption::Children],
                },
            ),
            (
                "A05 LIST (REMOTE SUBSCRIBED) \"\" \"*\"\r\n",
                list::Arguments::Extended {
                    tag: "A05".into(),
                    reference_name: "".into(),
                    mailbox_name: vec!["*".into()],
                    selection_options: vec![SelectionOption::Remote, SelectionOption::Subscribed],
                    return_options: vec![],
                },
            ),
            (
                "A06 LIST (REMOTE) \"\" \"*\" RETURN (SUBSCRIBED)\r\n",
                list::Arguments::Extended {
                    tag: "A06".into(),
                    reference_name: "".into(),
                    mailbox_name: vec!["*".into()],
                    selection_options: vec![SelectionOption::Remote],
                    return_options: vec![ReturnOption::Subscribed],
                },
            ),
            (
                "C04 LIST (SUBSCRIBED RECURSIVEMATCH) \"\" \"%\"\r\n",
                list::Arguments::Extended {
                    tag: "C04".into(),
                    reference_name: "".into(),
                    mailbox_name: vec!["%".into()],
                    selection_options: vec![
                        SelectionOption::Subscribed,
                        SelectionOption::RecursiveMatch,
                    ],
                    return_options: vec![],
                },
            ),
            (
                "C04 LIST (SUBSCRIBED RECURSIVEMATCH) \"\" \"%\" RETURN (CHILDREN)\r\n",
                list::Arguments::Extended {
                    tag: "C04".into(),
                    reference_name: "".into(),
                    mailbox_name: vec!["%".into()],
                    selection_options: vec![
                        SelectionOption::Subscribed,
                        SelectionOption::RecursiveMatch,
                    ],
                    return_options: vec![ReturnOption::Children],
                },
            ),
            (
                "a1 LIST \"\" (\"foo\")\r\n",
                list::Arguments::Extended {
                    tag: "a1".into(),
                    reference_name: "".into(),
                    mailbox_name: vec!["foo".into()],
                    selection_options: vec![],
                    return_options: vec![],
                },
            ),
            (
                "a3.1 LIST \"\" (% music/rock)\r\n",
                list::Arguments::Extended {
                    tag: "a3.1".into(),
                    reference_name: "".into(),
                    mailbox_name: vec!["%".into(), "music/rock".into()],
                    selection_options: vec![],
                    return_options: vec![],
                },
            ),
            (
                "BBB LIST \"\" (\"INBOX\" \"Drafts\" \"Sent/%\")\r\n",
                list::Arguments::Extended {
                    tag: "BBB".into(),
                    reference_name: "".into(),
                    mailbox_name: vec!["INBOX".into(), "Drafts".into(), "Sent/%".into()],
                    selection_options: vec![],
                    return_options: vec![],
                },
            ),
            (
                "A01 LIST \"\" % RETURN (STATUS (MESSAGES UNSEEN))\r\n",
                list::Arguments::Extended {
                    tag: "A01".into(),
                    reference_name: "".into(),
                    mailbox_name: vec!["%".into()],
                    selection_options: vec![],
                    return_options: vec![ReturnOption::Status(vec![
                        Status::Messages,
                        Status::Unseen,
                    ])],
                },
            ),
            (
                concat!(
                    "A02 LIST (SUBSCRIBED RECURSIVEMATCH) \"\" ",
                    "% RETURN (CHILDREN STATUS (MESSAGES))\r\n"
                ),
                list::Arguments::Extended {
                    tag: "A02".into(),
                    reference_name: "".into(),
                    mailbox_name: vec!["%".into()],
                    selection_options: vec![
                        SelectionOption::Subscribed,
                        SelectionOption::RecursiveMatch,
                    ],
                    return_options: vec![
                        ReturnOption::Children,
                        ReturnOption::Status(vec![Status::Messages]),
                    ],
                },
            ),
        ] {
            assert_eq!(
                receiver
                    .parse(&mut command.as_bytes().iter())
                    .unwrap()
                    .parse_list(ProtocolVersion::Rev2)
                    .unwrap(),
                arguments
            );
        }
    }
}
