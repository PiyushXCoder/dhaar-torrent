use std::fmt;
use std::marker::PhantomData;

use serde::de::{DeserializeOwned, Visitor};
use serde::{Deserialize, Deserializer};

/// Magic newtype name that `BencodeDeserializer` recognizes to hand over the
/// raw byte span of the next value instead of decoding it (same trick as
/// `serde_json::value::RawValue`).
pub(crate) const RAW_TOKEN: &str = "$dhaar_torrent::bencode::Raw";

/// Deserializes to both the typed value and the exact bencoded bytes it came
/// from. Needed where the original encoding matters, e.g. hashing the `info`
/// dict: re-serializing the struct is not byte-identical (key order, unknown
/// keys), so the hash must be computed over the bytes captured here.
pub struct Raw<T> {
    _phantom: PhantomData<T>,
    pub bytes: Vec<u8>,
}

impl<T: fmt::Debug> fmt::Debug for Raw<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Raw")
            .field("bytes", &format_args!("<{} bytes>", self.bytes.len()))
            .finish()
    }
}

impl<'de, T> Deserialize<'de> for Raw<T>
where
    T: DeserializeOwned,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RawVisitor<T>(PhantomData<T>);

        impl<'de, T> Visitor<'de> for RawVisitor<T>
        where
            T: DeserializeOwned,
        {
            type Value = Raw<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a raw bencode value")
            }

            fn visit_bytes<E>(self, bytes: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Raw {
                    _phantom: PhantomData,
                    bytes: bytes.to_vec(),
                })
            }
        }

        deserializer.deserialize_newtype_struct(RAW_TOKEN, RawVisitor(PhantomData))
    }
}
