pub mod de;
pub mod error;

#[cfg(test)]
mod tests;

pub use de::{BencodeDeserializer, from_bytes};
