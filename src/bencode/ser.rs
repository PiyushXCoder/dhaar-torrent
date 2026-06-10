use super::error;
use serde::{Serialize, ser};

pub struct BencodeSerializer {
    output: Vec<u8>,
}

impl BencodeSerializer {
    pub fn new() -> BencodeSerializer {
        BencodeSerializer { output: Vec::new() }
    }
}

pub fn to_bytes<T>(value: &T) -> error::Result<Vec<u8>>
where
    T: Serialize,
{
    let mut serializer = BencodeSerializer::new();
    value.serialize(&mut serializer)?;
    Ok(serializer.output)
}

impl<'a> ser::Serializer for &'a mut BencodeSerializer {
    type Ok = ();
    type Error = error::Error;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
        unimplemented!("Bool is not supported")
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.serialize_i128(i128::from(v))
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        self.serialize_i128(i128::from(v))
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        self.serialize_i128(i128::from(v))
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        self.serialize_i128(i128::from(v))
    }

    fn serialize_i128(self, v: i128) -> Result<Self::Ok, Self::Error> {
        self.output.extend(format!("i{}e", v).as_bytes().to_vec());
        Ok(())
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.serialize_i128(i128::from(v))
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.serialize_i128(i128::from(v))
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        self.serialize_i128(i128::from(v))
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        self.serialize_i128(i128::from(v))
    }

    fn serialize_u128(self, v: u128) -> Result<Self::Ok, Self::Error> {
        self.output.extend(format!("i{}e", v).as_bytes().to_vec());
        Ok(())
    }

    fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
        unimplemented!("Float is not supported")
    }

    fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
        unimplemented!("Float is not supported")
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        self.serialize_str(&v.to_string())
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.output
            .extend(format!("{}:", v.len()).as_bytes().to_vec());
        self.output.extend(v.as_bytes().to_vec());
        Ok(())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.output
            .extend(format!("{}:", v.len()).as_bytes().to_vec());
        self.output.extend(v);
        Ok(())
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!("None is not supported")
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        self.output.extend("de".as_bytes().to_vec());
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        unimplemented!("Unit variant is not supported")
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!("Newtype variant is not supported")
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        self.output.extend("l".as_bytes().to_vec());
        Ok(self)
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        unimplemented!("Tuple variant is not supported")
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        self.output.extend("d".as_bytes().to_vec());
        Ok(self)
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.serialize_map(Some(len))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        unimplemented!("Struct variant is not supported")
    }
}

impl<'a> ser::SerializeSeq for &'a mut BencodeSerializer {
    type Ok = ();
    type Error = error::Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), Self::Error> {
        self.output.extend("e".as_bytes().to_vec());
        Ok(())
    }
}

impl<'a> ser::SerializeTuple for &'a mut BencodeSerializer {
    type Ok = ();
    type Error = error::Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<(), Self::Error> {
        ser::SerializeSeq::end(self)
    }
}

impl<'a> ser::SerializeTupleStruct for &'a mut BencodeSerializer {
    type Ok = ();
    type Error = error::Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        ser::SerializeSeq::end(self)
    }
}

impl<'a> ser::SerializeTupleVariant for &'a mut BencodeSerializer {
    type Ok = ();
    type Error = error::Error;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!("Tuple variant is not supported")
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!("Tuple variant is not supported")
    }
}

impl<'a> ser::SerializeMap for &'a mut BencodeSerializer {
    type Ok = ();
    type Error = error::Error;

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.output.extend("e".as_bytes().to_vec());
        Ok(())
    }

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        key.serialize(&mut **self)
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }
}

impl<'a> ser::SerializeStruct for &'a mut BencodeSerializer {
    type Ok = ();
    type Error = error::Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        ser::SerializeMap::serialize_entry(self, key, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        ser::SerializeMap::end(self)
    }
}

impl<'a> ser::SerializeStructVariant for &'a mut BencodeSerializer {
    type Ok = ();
    type Error = error::Error;

    fn serialize_field<T>(&mut self, _key: &'static str, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!("Struct variant is not supported")
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!("Struct variant is not supported")
    }

    fn skip_field(&mut self, _key: &'static str) -> Result<(), Self::Error> {
        unimplemented!("Struct variant is not supported")
    }
}
