use num_traits::PrimInt;
use std::str::FromStr;

use serde::Deserialize;
#[allow(unused_imports)]
use serde::de::{
    self, DeserializeSeed, EnumAccess, IntoDeserializer, MapAccess, SeqAccess, VariantAccess,
    Visitor,
};
pub mod error;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct BencodeDeserializer<'de> {
    input: &'de [u8],
}

impl<'de> BencodeDeserializer<'de> {
    pub fn from_bytes(input: &'de [u8]) -> Self {
        BencodeDeserializer { input }
    }
}

pub fn from_bytes<'a, T>(s: &'a [u8]) -> error::Result<T>
where
    T: Deserialize<'a>,
{
    let mut deserializer = BencodeDeserializer::from_bytes(s);
    let t = T::deserialize(&mut deserializer)?;
    if deserializer.input.is_empty() {
        Ok(t)
    } else {
        Err(error::Error::Eof)
    }
}

impl<'de> BencodeDeserializer<'de> {
    fn peek_byte(&mut self) -> error::Result<u8> {
        self.input.first().copied().ok_or(error::Error::Eof)
    }

    fn next_byte(&mut self) -> error::Result<u8> {
        let byte = self.peek_byte()?;
        self.input = &self.input[1..];
        Ok(byte)
    }

    fn parse_string(&mut self) -> error::Result<&'de str> {
        let mut length = String::new();
        while let Ok(b) = self.next_byte() {
            let c = b as char;
            if c == ':' {
                break;
            }
            println!("{:?}", c);
            if !c.is_ascii_digit() {
                return Err(error::Error::Message("length is not a number".to_string()));
            }
            length += &c.to_string();
        }
        let length = length.parse::<usize>().map_err(|_| error::Error::Syntax)?;
        let string_bytes = &self.input[0..length];
        self.input = &self.input[length..];
        let string = std::str::from_utf8(string_bytes).map_err(|_| error::Error::Utf8)?;
        return Ok(string);
    }

    fn parse_bytes(&mut self) -> error::Result<&'de [u8]> {
        let mut length = String::new();
        while let Ok(b) = self.next_byte() {
            let c = b as char;
            if c == ':' {
                break;
            }
            if !c.is_ascii_digit() {
                return Err(error::Error::Message("length is not a number".to_string()));
            }
            length += &c.to_string();
        }
        let length = length.parse::<usize>().map_err(|_| error::Error::Syntax)?;
        let bytes = &self.input[0..length];
        self.input = &self.input[length..];
        return Ok(bytes);
    }

    fn parse_integer<T: PrimInt + FromStr>(&mut self) -> error::Result<T> {
        if self.next_byte()? as char != 'i' {
            return Err(error::Error::Syntax);
        }
        let mut result = String::new();
        let mut has_end = false;
        while let Ok(b) = self.next_byte() {
            let c = b as char;
            if c == 'e' {
                has_end = true;
                break;
            }
            if !c.is_ascii_digit() {
                return Err(error::Error::Message("Not a number".to_string()));
            }
            result += &c.to_string();
        }
        let result = result.parse::<T>().map_err(|_| error::Error::Syntax)?;
        if !has_end {
            return Err(error::Error::Syntax);
        }
        return Ok(result);
    }
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut BencodeDeserializer<'de> {
    type Error = error::Error;

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_bool<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        panic!("Bool is not supported")
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_i8(self.parse_integer()?)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_i16(self.parse_integer()?)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_i32(self.parse_integer()?)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_i64(self.parse_integer()?)
    }

    fn deserialize_i128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_i128(self.parse_integer()?)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_u8(self.parse_integer()?)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_u16(self.parse_integer()?)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_u32(self.parse_integer()?)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_u64(self.parse_integer()?)
    }

    fn deserialize_u128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_u128(self.parse_integer()?)
    }

    fn deserialize_f32<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        panic!("Float is not supported")
    }

    fn deserialize_f64<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        panic!("Float is not supported")
    }

    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        panic!("Char is not supported")
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_borrowed_str(self.parse_string()?)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_string(self.parse_string()?.to_owned())
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_borrowed_bytes(self.parse_bytes()?)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_byte_buf(self.parse_bytes()?.to_vec())
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_some(self)
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        if self.input.is_empty() {
            visitor.visit_unit()
        } else {
            Err(error::Error::Syntax)
        }
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        panic!("Enum is not supported")
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn is_human_readable(&self) -> bool {
        todo!()
    }
}
