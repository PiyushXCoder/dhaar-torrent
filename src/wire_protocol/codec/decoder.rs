use super::super::{Bitfield, Handshake, Message, WireItem};
use super::{CodecState, WireCodec};
use std::io;
use tokio_util::bytes::{self, Buf};
use tokio_util::codec::Decoder;
use tracing::debug;

impl Decoder for WireCodec {
    type Item = WireItem;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<WireItem>, Self::Error> {
        match &mut self.state {
            CodecState::HandshakePending {
                pstrlen,
                info_hash_sent,
            } => {
                let mut offset = 0;
                let handshake = {
                    let buf = src.as_ref();

                    // pstrlen
                    if src.len() < offset + 1 {
                        return Ok(None);
                    }

                    let len = if let Some(pstrlen) = pstrlen {
                        *pstrlen
                    } else {
                        *pstrlen = Some(buf[offset]);
                        buf[offset]
                    };

                    offset += 1;

                    // pstr
                    if src.len() < offset + len as usize {
                        return Ok(None);
                    }
                    let pstr = std::str::from_utf8(&buf[1..=len as usize]).unwrap();
                    if pstr != "BitTorrent protocol" {
                        return Err(std::io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid pstr",
                        ));
                    }
                    offset += len as usize;

                    // reserved
                    if src.len() < offset + 8 {
                        return Ok(None);
                    }
                    let reserved: [u8; 8] = buf[offset..offset + 8]
                        .try_into()
                        .expect("failed to copy reserved");
                    offset += 8;

                    // info_hash
                    if src.len() < offset + 20 {
                        return Ok(None);
                    }
                    if !*info_hash_sent {
                        let info_hash = buf[offset..offset + 20]
                            .try_into()
                            .expect("failed to copy info_hash");
                        *info_hash_sent = true;
                        return Ok(Some(WireItem::HandshakePartial { info_hash }));
                    }

                    let info_hash: [u8; 20] = buf[offset..offset + 20]
                        .try_into()
                        .expect("failed to copy info_hash");
                    offset += 20;

                    // peer_id
                    if src.len() < offset + 20 {
                        return Ok(None);
                    }
                    let peer_id: [u8; 20] = buf[offset..offset + 20]
                        .try_into()
                        .expect("failed to copy peer_id");
                    offset += 20;

                    Handshake {
                        pstrlen: len,
                        pstr: pstr.to_string(),
                        info_hash,
                        reserved,
                        peer_id,
                    }
                };
                src.advance(offset);
                self.state = CodecState::Normal;
                Ok(Some(WireItem::Handshake(handshake)))
            }
            CodecState::Normal => loop {
                // Parse length‑prefixed messages (4‑byte length + payload).
                // This is the same as in the earlier example.
                if src.len() < 4 {
                    return Ok(None);
                }
                let msg_len = u32::from_be_bytes(src[..4].try_into().unwrap()) as usize;
                if src.len() < 4 + msg_len {
                    return Ok(None);
                }

                // Skip keep‑alive (length 0) – we just ignore it.
                if msg_len == 0 {
                    src.advance(4);
                    continue; // keep scanning
                }

                let payload = &src[4..4 + msg_len];
                let id = payload[0];
                let rest = &payload[1..];

                let msg = match id {
                    0 => Message::Choke,
                    1 => Message::Unchoke,
                    2 => Message::Interested,
                    3 => Message::NotInterested,
                    4 => {
                        if rest.len() < 4 {
                            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid Have"));
                        }
                        Message::Have(u32::from_be_bytes(rest[..4].try_into().unwrap()))
                    }
                    5 => Message::Bitfield(Bitfield(rest.to_vec())),
                    6 => {
                        if rest.len() < 12 {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "Invalid Request",
                            ));
                        }
                        let index = u32::from_be_bytes(rest[0..4].try_into().unwrap());
                        let begin = u32::from_be_bytes(rest[4..8].try_into().unwrap());
                        let length = u32::from_be_bytes(rest[8..12].try_into().unwrap());
                        Message::Request {
                            index,
                            begin,
                            length,
                        }
                    }
                    7 => {
                        if rest.len() < 8 {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "Invalid Piece",
                            ));
                        }
                        let index = u32::from_be_bytes(rest[0..4].try_into().unwrap());
                        let begin = u32::from_be_bytes(rest[4..8].try_into().unwrap());
                        let block = rest[8..].to_vec();
                        Message::Piece {
                            index,
                            begin,
                            block,
                        }
                    }
                    8 => {
                        if rest.len() < 12 {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "Invalid Cancel",
                            ));
                        }
                        let index = u32::from_be_bytes(rest[0..4].try_into().unwrap());
                        let begin = u32::from_be_bytes(rest[4..8].try_into().unwrap());
                        let length = u32::from_be_bytes(rest[8..12].try_into().unwrap());
                        Message::Cancel {
                            index,
                            begin,
                            length,
                        }
                    }
                    9 => {
                        if rest.len() < 2 {
                            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid Port"));
                        }
                        Message::Port(u16::from_be_bytes(rest[..2].try_into().unwrap()))
                    }
                    _ => {
                        // Unknown id, most likely an extension we never
                        // advertised. Frames are length-prefixed, so skip this
                        // one and keep the connection alive.
                        debug!("skipping unknown message id {}", id);
                        src.advance(4 + msg_len);
                        continue;
                    }
                };

                src.advance(4 + msg_len);
                return Ok(Some(WireItem::Message(msg)));
            },
        }
    }
}
