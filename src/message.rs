use crate::{ProcessId, RequestNumber};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// Suzuki–Kasami token
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Token {
    pub ln: Vec<RequestNumber>,
    pub queue: VecDeque<ProcessId>,
}

/// Distributed messages: REQUEST, TOKEN, and new SIGNAL
#[derive(Clone, Debug)]
pub enum Message {
    Request { from: ProcessId, rn: RequestNumber },
    Token(Token),
    Signal,
}

impl Message {
    /// Serialize Message into JSON byte vector
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Message::Request { from, rn } => serde_json::to_vec(&("REQ", *from, *rn)).unwrap(),
            Message::Token(tok) => serde_json::to_vec(&("TOK", tok)).unwrap(),
            Message::Signal => serde_json::to_vec(&("SIG", None::<()>)).unwrap(),
        }
    }

    /// Deserialize JSON bytes back into a Message
    pub fn deserialize(bytes: &[u8]) -> Option<Self> {
        let v: serde_json::Value = serde_json::from_slice(bytes).ok()?;
        match v.get(0)?.as_str()? {
            "REQ" => Some(Message::Request {
                from: v[1].as_u64()? as ProcessId,
                rn: v[2].as_u64()?,
            }),
            "TOK" => {
                let tok: Token = serde_json::from_value(v[1].clone()).ok()?;
                Some(Message::Token(tok))
            }
            "SIG" => Some(Message::Signal),
            _ => None,
        }
    }
}
