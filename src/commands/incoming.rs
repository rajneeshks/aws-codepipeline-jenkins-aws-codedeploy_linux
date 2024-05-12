// incoming command formatting
use crate::commands::array;
use crate::commands::resp;
use crate::commands::ss;
use bytes::BytesMut;
use std::io::Write;
use std::net::TcpStream;

const command_delimiter: &str = "\r\n";

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

    pub fn handle(&self, stream: &mut TcpStream) -> std::io::Result<()> {
        match self.command {
            resp::DataType::SimpleString(ref cmd) => {
                let handler = ss::SimpleStringCommandHandler(&cmd);
                handler(&self.command, stream)
            }
            resp::DataType::Array(ref cmd) => {
                let handler = array::ArrayTypeHandler(&cmd);
                handler(&self.command, stream)
            }
            _ => stream.write_all(format!("-{}\r\n", self.command).as_bytes()),
        }
    }
}

impl std::fmt::Display for Incoming<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Incoming command: {}", self.command)
    }
}
