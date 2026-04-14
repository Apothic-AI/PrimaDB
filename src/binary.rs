use base64ct::{Base64UrlUnpadded, Encoding};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct BinaryBytes(pub Vec<u8>);

impl BinaryBytes {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }

    pub fn to_base64(&self) -> String {
        Base64UrlUnpadded::encode_string(self.as_slice())
    }

    pub fn from_base64(value: &str) -> Result<Self, String> {
        Base64UrlUnpadded::decode_vec(value)
            .map(Self)
            .map_err(|error| error.to_string())
    }
}

impl From<Vec<u8>> for BinaryBytes {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl From<&[u8]> for BinaryBytes {
    fn from(value: &[u8]) -> Self {
        Self(value.to_vec())
    }
}

impl Serialize for BinaryBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_base64())
    }
}

impl<'de> Deserialize<'de> for BinaryBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_base64(&value).map_err(D::Error::custom)
    }
}
