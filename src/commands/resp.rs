use bytes::BytesMut;

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
    BulkString(Vec<String>),
    Invalid(String),
}

impl DataType {
    pub fn new(buf: &BytesMut) -> Self {
        match Self::parse(buf) {
            Ok(instance) => instance,
            Err(e) => DataType::Invalid(e),
        }
    }

    fn parse_simple_string(mut token: String) -> Result<DataType, String> {
        token.remove(0); // remove first character
        Ok(DataType::SimpleString(token.to_lowercase()))
    }

    fn _parse_bulk_string(
        _tokens: &mut Vec<String>,
        _tokenid: usize,
    ) -> Result<Vec<DataType>, String> {
        Err("error".to_string())
    }

    fn parse_array(tokens: Vec<String>) -> Result<DataType, String> {
        let tokens = tokens.into_iter().enumerate().skip(1).fold(
            Vec::<String>::new(),
            |mut acc, (idx, s)| {
                if s.len() >= 2 {
                    if s.chars().nth(0).unwrap() != BULK_STRING_MARKER {
                        if idx == 2 {
                            acc.push(s.to_lowercase());
                        } else {
                            acc.push(s);
                        }
                    }
                }
                acc
            },
        );

        Ok(DataType::Array(tokens))
    }

    fn tokenize(buf: &BytesMut) -> Result<Vec<String>, String> {
        let istr = String::from_utf8(buf.to_vec()).unwrap();
        let tokens = istr.split("\r\n").fold(Vec::<String>::new(), |mut acc, s| {
            if s.len() > 1 {
                acc.push(s.to_string());
            }
            acc
        });
        Ok(tokens)
    }

    fn parse(buf: &BytesMut) -> Result<DataType, String> {
        let tokens = Self::tokenize(buf)?;
        if tokens.len() < 1 {
            return Err("invalid tokens".to_string());
        }
        if tokens[0].len() < 2 {
            return Err("invalid tokens".to_string());
        }
        println!("tokens - {:?}", tokens);
        match tokens[0].chars().nth(0).unwrap() {
            SIMPLE_STRING_MARKER => Self::parse_simple_string(tokens[0].clone()),
            ARRAY_MARKER => Self::parse_array(tokens),
            _ => Err("Invalid command".to_string()),
        }
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
            _ => write!(f, "{}", "-Unsupported value or command\r\n"),
        }
    }
}
