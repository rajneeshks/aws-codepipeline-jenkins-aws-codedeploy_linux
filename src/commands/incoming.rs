// incoming command formatting
use crate::commands::array;
use crate::commands::resp;
use crate::commands::ss;
use crate::store::db;
use bytes::BytesMut;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;

const COMMAND_DELIMITER: &str = "\r\n";

pub struct Incoming<'a> {
    buf: &'a BytesMut,
    length: usize,
    command: resp::DataType,
}

impl<'a, 'b> Incoming<'b> {
    pub fn new(buf: &'a BytesMut, length: usize) -> Incoming<'b>
    where
        'a: 'b,
    {
        let command = resp::DataType::new(buf);
        Self {
            buf,
            length,
            command,
        }
    }

    pub fn handle(&self, stream: &mut TcpStream, db: &Arc<db::DB>) -> std::io::Result<()> {
        match self.command {
            resp::DataType::SimpleString(ref cmd) => {
                let handler = ss::simple_string_command_handler(&cmd);
                handler(&self.command, stream, db)
            }
            resp::DataType::Array(ref cmd) => {
                let handler = array::array_type_handler(&cmd);
                handler(&self.command, stream, db)
            }
            _ => stream.write_all(format!("-{}\r\n", self.command).as_bytes()),
        }
    }

    // returns first token - could be command or a response
    pub fn get_command(&self) -> String {
        match self.command {
            resp::DataType::SimpleString(ref cmd) => cmd.clone(),
            _ => return "not implemented".to_string(),
        }
    }
}

impl std::fmt::Display for Incoming<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Incoming command: {}", self.command)
    }
}
