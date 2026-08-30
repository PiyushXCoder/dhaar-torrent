use super::super::{Message, WireItem};
use super::WireCodec;
use std::io;
use tokio_util::bytes::BufMut;
use tokio_util::codec::Encoder;

impl Encoder<WireItem> for WireCodec {
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: WireItem,
        dst: &mut tokio_util::bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        match item {
            WireItem::Handshake(hs) => {
                // Write the handshake bytes
                dst.reserve(1 + hs.pstrlen as usize + 8 + 20 + 20);
                dst.put_u8(hs.pstrlen);
                dst.put_slice(hs.pstr.as_bytes());
                dst.put_slice(&hs.reserved);
                dst.put_slice(&hs.info_hash);
                dst.put_slice(&hs.peer_id);
            }
            WireItem::Message(msg) => {
                // Keep-alive is alone in having no id, so there is no header
                // to write: the length prefix is the entire frame.
                let Some((msg_id, payload_len)) = message_header(&msg) else {
                    dst.reserve(4);
                    dst.put_u32(0);
                    return Ok(());
                };
                dst.reserve(4 + 1 + payload_len);
                dst.put_u32(payload_len as u32 + 1);
                dst.put_u8(msg_id);

                match msg {
                    Message::KeepAlive
                    | Message::Choke
                    | Message::Unchoke
                    | Message::Interested
                    | Message::NotInterested => {}
                    Message::Have(index) => dst.put_u32(index),
                    Message::Bitfield(bitfield) => dst.put_slice(&bitfield.0),
                    Message::Request {
                        index,
                        begin,
                        length,
                    } => {
                        dst.put_u32(index);
                        dst.put_u32(begin);
                        dst.put_u32(length);
                    }
                    Message::Piece {
                        index,
                        begin,
                        block,
                    } => {
                        dst.put_u32(index);
                        dst.put_u32(begin);
                        dst.put_slice(&block);
                    }
                    Message::Cancel {
                        index,
                        begin,
                        length,
                    } => {
                        dst.put_u32(index);
                        dst.put_u32(begin);
                        dst.put_u32(length);
                    }
                    Message::Port(port) => dst.put_u16(port),
                }
            }
            WireItem::HandshakePartial { info_hash: _ } => {
                // This variant is only used for decoding; never encode it.
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Cannot encode HandshakePartial",
                ));
            }
        }
        Ok(())
    }
}

/// The message id and payload length written ahead of a message's fields.
/// `None` for keep-alive, which has neither.
fn message_header(message: &Message) -> Option<(u8, usize)> {
    Some(match message {
        Message::KeepAlive => return None,
        Message::Choke => (0, 0),
        Message::Unchoke => (1, 0),
        Message::Interested => (2, 0),
        Message::NotInterested => (3, 0),
        Message::Have(_) => (4, 4),
        Message::Bitfield(bitfield) => (5, bitfield.0.len()),
        Message::Request { .. } => (6, 12),
        Message::Piece { block, .. } => (7, 8 + block.len()),
        Message::Cancel { .. } => (8, 12),
        Message::Port(_) => (9, 2),
    })
}
