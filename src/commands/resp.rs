use bytes::{BytesMut, BufMut};

const ARRAY_MARKER: char = '*';
const SIMPLE_STRING_MARKER: char = '+';
//const INTEGER_MARKER: char = ':';
//const SIMPLE_ERROR_MARKER: char = '-';
const BULK_STRING_MARKER: char = '$';

#[derive(Debug)]
pub enum DataType {
    Array(Vec<String>),
    SimpleString(String),
    SimpleError(String),
    Integers(i64),
    BulkString(String),
    Invalid(String),
}

impl DataType {
    pub fn new(buf: &BytesMut) -> Vec<Self> {
        match Self::parse(buf) {
            Ok(instance) => instance,
            Err(e) => vec![DataType::Invalid(e)],
        }
    }
    
    // returns length of current token
    // from start index to where it finds "\r\n"
    // includes \n in this count
    fn get_next_token(
        buf: &BytesMut,
        start: usize
    ) -> Result<usize, String> {
        let mut idx = start;
        while idx < buf.len() {
            if buf[idx-1] as char == '\r' && buf[idx] as char == '\n' {
                return Ok(idx);
            }
            idx += 1;
        }
        Err("Not Found".to_string())
    }

    // returns DataType and total bytes consumed for this token
    fn parse_simple_string(
        buf: &BytesMut,
        start: usize
    ) -> Result<(DataType, usize), String> {
        if buf[start] as char != SIMPLE_STRING_MARKER {
            return Err("its not a simple string - aborting parsing..".to_string());
        }
        // get to the next \r\n
        let end = Self::get_next_token(buf, start+1)?;

        if let Ok(cmd) = String::from_utf8(buf[start+1..=end-2].to_vec()) {
            return Ok((DataType::SimpleString(cmd.to_lowercase()), end-start));
        }
        Err("Error converting command appropriately!".to_string())
    }

    // parses bulk string that is subset of other command
    // returns only the string and length of buffer utilized
    fn _parse_bulk_string(
        buf: &BytesMut,
        start: usize,
    ) -> Result<(String, usize), String> {
        if buf[start] as char != BULK_STRING_MARKER {
            return Err("its not a bulk string - aborting parsing..".to_string());
        }
        // get to the next \r\n
        let mut end = Self::get_next_token(buf, start+1)?;
        let mut num_chars: usize = 0;
        
        // handle null bulk string ($-1\r\n)

        for i in start+1..=end-2 {
            num_chars = num_chars*10 + (buf[i] - '0' as u8) as usize;
        }
        // read numchars from the buffer and convert to a string
        if let Ok(cmd) = String::from_utf8(buf[end+1..end+1+num_chars].to_vec()) {
            // accomodate those 2 \r\n after the string
            return Ok((cmd.to_lowercase(), num_chars+2+1+end-start));
        }
        Err("Error converting bulk string appropriately!".to_string())
    }
    
    // parses incoming bulk string command
    fn parse_bulk_string(
        buf: &BytesMut,
        start: usize,
    ) -> Result<(Vec<DataType>, usize), String> {
        let (cmd, end) = Self::_parse_bulk_string(buf, start)?;
        Ok((vec![DataType::BulkString(cmd)], end-start))
    }

    // parses incoming command that is an array
    fn parse_array(
        buf: &BytesMut,
        start: usize
    ) -> Result<(DataType, usize), String> {
        if buf[start] as char != ARRAY_MARKER {
            return Err("its not a array - aborting parsing..".to_string());
        }
        let mut end = Self::get_next_token(buf, start+1)?;
        let mut num_args: usize = 0;
        for i in start+1..=end-2 {
            num_args = num_args*10 + (buf[i] - '0' as u8) as usize;
        }
        let mut result: Vec<String> = Vec::with_capacity(num_args);
        while num_args > 0 {
            let (cmd, length) = Self::_parse_bulk_string(buf, end+1)?;
            result.push(cmd);
            end += length;
            num_args -= 1;
        }
        Ok((DataType::Array(result), end-start))
    }

    fn parse(buf: &BytesMut) -> Result<Vec<DataType>, String> {
        let mut result: Vec<DataType> = vec![];
        println!("------------ parsing now: {:?}", buf);
        // read one character at a time
        let mut idx = 0;
        while idx < buf.len() {
            let mut len = 0;
            match buf[idx] as char {
                SIMPLE_STRING_MARKER => {
                    let (cmd, used_len) = Self::parse_simple_string(buf, idx)?;
                    idx += used_len+1;
                    result.push(cmd);
                },
                ARRAY_MARKER => {
                    let (cmd, used_len) = Self::parse_array(buf, idx)?;
                    idx += used_len+1;
                    result.push(cmd);
                },
                BULK_STRING_MARKER => {
                     let (cmd, used_len) = Self::parse_simple_string(buf, idx)?;
                     idx += used_len+1;
                     result.push(cmd);
                },
                _ => return Err(
                    format!("Invalid command or unimplemented type {}", buf[idx] as char)),
            };
        }
        println!("Parsing result: ");
        for i in 0..result.len() {
            println!("{:?}", result[i]);
        }
        Ok(result)
    }

    pub fn to_wire(&self, ss: &str) -> String {
        match self {
            DataType::SimpleString(_s) => format!("+{}\r\n", ss),
            DataType::SimpleError(_s) => format!("-{}\r\n", ss),
            DataType::Integers(val) => format!(":{val}\r\n"),
            DataType::BulkString(_s) => format!("${}\r\n{}\r\n", ss.chars().count(), ss),
            _ => format!("-Unsupported value or command: {}\r\n", ss),
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::SimpleString(s) => write!(f, "Simple String: {}", s),
            DataType::SimpleError(s) => write!(f, "Simple Error: {}", s),
            DataType::Integers(val) => write!(f, "Integer: {}", val),
            DataType::BulkString(s) => write!(f, "Bulk String: {:?}", s),
            DataType::Array(s) => write!(f, "Array: {:?}", s),
            DataType::Invalid(s) => write!(f, "invalid: {:?}", s),
            _ => write!(f, "{}", "-Unsupported value or command\r\n"),
        }
    }
}