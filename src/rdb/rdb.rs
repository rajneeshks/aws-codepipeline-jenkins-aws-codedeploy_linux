use base64::prelude::BASE64_STANDARD;
use base64::Engine;

pub struct RDB {}

impl RDB {
    pub fn new() -> Self {
        Self {}
    }

    pub fn get(&self) -> Result<Vec<u8>, base64::DecodeError> {
        let mut buffer = vec![];
        let empty_rdb = BASE64_STANDARD.decode(b"UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")?;
        buffer.extend_from_slice(format!("${}\r\n", empty_rdb.len()).as_bytes());
        buffer.extend_from_slice(&empty_rdb);
        Ok(buffer)
    }
}
