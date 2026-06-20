use crate::error::{Error, Result};

pub enum Message {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(Vec<u8>),
    Request {
        index: u32,
        begin: u32,
        length: u32,
    },
    Piece {
        index: u32,
        begin: u32,
        data: Vec<u8>,
    },
    Cancel {
        index: u32,
        begin: u32,
        length: u32,
    },
}

impl Message {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Message::KeepAlive => 0u32.to_be_bytes().to_vec(),
            Message::Choke => vec![0, 0, 0, 1, 0],
            Message::Unchoke => vec![0, 0, 0, 1, 1],
            Message::Interested => vec![0, 0, 0, 1, 2],
            Message::NotInterested => vec![0, 0, 0, 1, 3],
            Message::Have(idx) => {
                let mut b = vec![0, 0, 0, 5, 4];
                b.extend_from_slice(&idx.to_be_bytes());
                b
            }
            Message::Bitfield(bits) => {
                let mut b = ((1 + bits.len()) as u32).to_be_bytes().to_vec();
                b.push(5);
                b.extend_from_slice(bits);
                b
            }
            Message::Request {
                index,
                begin,
                length,
            } => {
                let mut b = vec![0, 0, 0, 13, 6];
                b.extend_from_slice(&index.to_be_bytes());
                b.extend_from_slice(&begin.to_be_bytes());
                b.extend_from_slice(&length.to_be_bytes());
                b
            }
            Message::Piece { index, begin, data } => {
                let mut b = ((9 + data.len()) as u32).to_be_bytes().to_vec();
                b.push(7);
                b.extend_from_slice(&index.to_be_bytes());
                b.extend_from_slice(&begin.to_be_bytes());
                b.extend_from_slice(data);
                b
            }
            Message::Cancel {
                index,
                begin,
                length,
            } => {
                let mut b = vec![0, 0, 0, 13, 8];
                b.extend_from_slice(&index.to_be_bytes());
                b.extend_from_slice(&begin.to_be_bytes());
                b.extend_from_slice(&length.to_be_bytes());
                b
            }
        }
    }

    pub fn from_bytes(id: u8, payload: &[u8]) -> Result<Self> {
        let msg = match id {
            0 => Message::Choke,
            1 => Message::Unchoke,
            2 => Message::Interested,
            3 => Message::NotInterested,
            4 if payload.len() >= 4 => {
                Message::Have(u32::from_be_bytes(payload[..4].try_into().unwrap()))
            }
            5 => Message::Bitfield(payload.to_vec()),
            6 if payload.len() >= 12 => Message::Request {
                index: u32::from_be_bytes(payload[0..4].try_into().unwrap()),
                begin: u32::from_be_bytes(payload[4..8].try_into().unwrap()),
                length: u32::from_be_bytes(payload[8..12].try_into().unwrap()),
            },
            7 if payload.len() >= 8 => Message::Piece {
                index: u32::from_be_bytes(payload[0..4].try_into().unwrap()),
                begin: u32::from_be_bytes(payload[4..8].try_into().unwrap()),
                data: payload[8..].to_vec(),
            },
            8 if payload.len() >= 12 => Message::Cancel {
                index: u32::from_be_bytes(payload[0..4].try_into().unwrap()),
                begin: u32::from_be_bytes(payload[4..8].try_into().unwrap()),
                length: u32::from_be_bytes(payload[8..12].try_into().unwrap()),
            },
            _ => {
                return Err(Error::Error(format!(
                    "unknown message id {id} or bad payload len {}",
                    payload.len()
                )));
            }
        };
        Ok(msg)
    }
}
