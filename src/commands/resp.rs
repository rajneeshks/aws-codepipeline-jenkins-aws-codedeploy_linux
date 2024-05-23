use bytes::BytesMut;

const ARRAY_MARKER: char = '*';
const SIMPLE_STRING_MARKER: char = '+';
const INTEGER_MARKER: char = ':';
const SIMPLE_ERROR_MARKER: char = '-';
const BULK_STRING_MARKER: char = '$';

#[derive(Debug)]
pub enum DataType {
    Array(Vec<String>, usize, usize),
    SimpleString(String, usize, usize),
    SimpleError(String, usize, usize),
    Integers(i64, usize, usize),
    BulkString(Vec<u8>, usize, usize),
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
    fn get_next_token(buf: &BytesMut, start: usize) -> Result<usize, String> {
        let mut idx = start;
        while idx < buf.len() {
            if buf[idx - 1] as char == '\r' && buf[idx] as char == '\n' {
                return Ok(idx);
            }
            idx += 1;
        }
        Err("Not Found".to_string())
    }

    fn parse_simple_stuff(buf: &BytesMut, start: usize) -> Result<(String, usize), String> {
        // get to the next \r\n
        let end = Self::get_next_token(buf, start + 1)?;

        if let Ok(cmd) = String::from_utf8(buf[start + 1..=end - 2].to_vec()) {
            return Ok((cmd, end - start));
        }
        Err("Error parsing Simple type".to_string())
    }

    // returns DataType and total bytes consumed for this token
    fn parse_simple_error(buf: &BytesMut, start: usize) -> Result<(DataType, usize), String> {
        if buf[start] as char != SIMPLE_ERROR_MARKER {
            return Err("its not a simple error - aborting parsing..".to_string());
        }
        if let Ok((cmdstr, consumed_len)) = Self::parse_simple_stuff(buf, start) {
            return Ok((DataType::SimpleError(cmdstr, start, start+consumed_len), consumed_len));
        }
        Err("Error converting simple error appropriately!".to_string())
    }

    // returns DataType and total bytes consumed for this token
    fn parse_simple_string(buf: &BytesMut, start: usize) -> Result<(DataType, usize), String> {
        if buf[start] as char != SIMPLE_STRING_MARKER {
            return Err("its not a simple string - aborting parsing..".to_string());
        }
        if let Ok((cmdstr, consumed_len)) = Self::parse_simple_stuff(buf, start) {
            return Ok((DataType::SimpleString(cmdstr.to_lowercase(), start, start+consumed_len), consumed_len));
        }
        Err("Error converting Simple string appropriately!".to_string())
    }

    // parse integers
    // format: :[<+|->]<value>\r\n
    fn parse_integers(buf: &BytesMut, start: usize) -> Result<(DataType, usize), String> {
        if buf[start] as char != INTEGER_MARKER {
            return Err("its not a simple string - aborting parsing..".to_string());
        }
        let mut sign = None;
        let mut current = start;
        if buf[current + 1] as char == '-' || buf[current + 1] as char == '+' {
            sign = Some(buf[current + 1] as char);
            current += 1;
        }
        // get the number token
        let end = Self::get_next_token(buf, current + 1)?;
        let mut number: i64 = 0;
        for i in current + 1..=end - 2 {
            number = number * 10 + (buf[i] - '0' as u8) as i64;
        }
        // now process the sign
        if let Some(sign) = sign {
            if sign == '-' {
                number *= -1;
            }
        }
        Ok((DataType::Integers(number, start, end), end - start))
    }

    // parses bulk string that is subset of other command
    // returns only the string and length of buffer utilized
    fn _parse_bulk_string(buf: &BytesMut, start: usize) -> Result<(Vec<u8>, usize), String> {
        if buf[start] as char != BULK_STRING_MARKER {
            return Err("its not a bulk string - aborting parsing..".to_string());
        }
        // get to the next \r\n
        let end = Self::get_next_token(buf, start + 1)?;
        let mut num_chars: usize = 0;

        // handle null bulk string ($-1\r\n)

        for i in start + 1..=end - 2 {
            num_chars = num_chars * 10 + (buf[i] - '0' as u8) as usize;
        }
        // read numchars from the buffer and convert to a string
        let mut last_idx_consumed = end + 1 + num_chars;
        if last_idx_consumed <= buf.len() {
            let cmd = buf[end + 1..=last_idx_consumed - 1].to_vec();
            // check if there are any more \r\n after this - looks to be optional
            if last_idx_consumed <= buf.len() - 2 {
                if buf[last_idx_consumed] as char == '\r'
                    && buf[last_idx_consumed + 1] as char == '\n'
                {
                    last_idx_consumed += 1;
                } else {
                    last_idx_consumed -= 1;
                }
            } else {
                last_idx_consumed -= 1;
            }
            return Ok((cmd, last_idx_consumed));
        }

        Err("Error converting bulk string appropriately!".to_string())
    }

    // parses incoming bulk string command
    fn parse_bulk_string(buf: &BytesMut, start: usize) -> Result<(DataType, usize), String> {
        let (cmd, end) = Self::_parse_bulk_string(buf, start)?;
        Ok((DataType::BulkString(cmd, start, end), end - start))
    }

    // parses incoming command that is an array
    fn parse_array(buf: &BytesMut, start: usize) -> Result<(DataType, usize), String> {
        if buf[start] as char != ARRAY_MARKER {
            return Err("its not a array - aborting parsing..".to_string());
        }
        let mut end = Self::get_next_token(buf, start + 1)?;
        let mut num_args: usize = 0;
        for i in start + 1..=end - 2 {
            num_args = num_args * 10 + (buf[i] - '0' as u8) as usize;
        }
        let mut result: Vec<String> = Vec::with_capacity(num_args);
        while num_args > 0 {
            let (binary, new_end) = Self::_parse_bulk_string(buf, end + 1)?;
            if let Ok(cmd) = String::from_utf8(binary) {
                result.push(cmd.to_lowercase());
            } else {
                return Err("Unble to parse string from binary!".to_string());
            }
            end = new_end;
            num_args -= 1;
        }
        Ok((DataType::Array(result, start, end), end - start))
    }

    fn parse(buf: &BytesMut) -> Result<Vec<DataType>, String> {
        let mut result: Vec<DataType> = vec![];
        // read one character at a time
        let mut idx = 0;
        while idx < buf.len() {
            match buf[idx] as char {
                SIMPLE_STRING_MARKER => {
                    let (cmd, used_len) = Self::parse_simple_string(buf, idx)?;
                    idx += used_len + 1;
                    result.push(cmd);
                }
                ARRAY_MARKER => {
                    let (cmd, used_len) = Self::parse_array(buf, idx)?;
                    idx += used_len + 1;
                    result.push(cmd);
                }
                BULK_STRING_MARKER => {
                    let (cmd, used_len) = Self::parse_bulk_string(buf, idx)?;
                    idx += used_len + 1;
                    result.push(cmd);
                }
                SIMPLE_ERROR_MARKER => {
                    let (cmd, used_len) = Self::parse_simple_error(buf, idx)?;
                    idx += used_len + 1;
                    result.push(cmd);
                }
                INTEGER_MARKER => {
                    let (cmd, used_len) = Self::parse_integers(buf, idx)?;
                    idx += used_len + 1;
                    result.push(cmd);
                }
                _ => {
                    return Err(format!(
                        "Invalid command or unimplemented type {}",
                        buf[idx] as char
                    ))
                }
            };
        }
        Ok(result)
    }

    #[allow(dead_code)]
    pub fn to_wire(&self, ss: &str) -> String {
        match self {
            DataType::SimpleString(_s, _start, _end) => format!("+{}\r\n", ss),
            DataType::SimpleError(_s, _start, _end) => format!("-{}\r\n", ss),
            DataType::Integers(val, _start, _end) => format!(":{val}\r\n"),
            DataType::BulkString(_s, _start, _end) => format!("${}\r\n{}\r\n", ss.chars().count(), ss),
            _ => format!("-Unsupported value or command: {}\r\n", ss),
        }
    }
    
    pub fn len(&self) -> usize {
        match self {
            DataType::SimpleString(_s, start, end) => 1+end-start,
            DataType::SimpleError(_s, start, end) => 1+end-start,
            DataType::Integers(_val, start, end) => 1+end-start,
            DataType::BulkString(_s, start, end) => 1+end-start,
            DataType::Array(_s, start, end) => 1+end-start,
            DataType::Invalid(s) => 0,
        }
        
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::SimpleString(s, start, end) => write!(f, "Simple String: {} ({}:{})", s, start, end),
            DataType::SimpleError(s, start, end) => write!(f, "Simple Error: {} ({}:{})", s, start, end),
            DataType::Integers(val, start, end) => write!(f, "Integer: {} ({}:{})", val, start, end),
            DataType::BulkString(s, start, end) => write!(f, "Bulk String: {:?} ({}:{})", s, start, end),
            DataType::Array(s, start, end) => write!(f, "Array: {:?} ({}:{})", s, start, end),
            DataType::Invalid(s) => write!(f, "invalid: {:?}", s),
            _ => write!(f, "{}", "-Unsupported value or command\r\n"),
        }
    }
}