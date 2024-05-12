use crate::commands::resp;
use std::io::Write;
use std::net::TcpStream;

pub fn command_name() -> String {
    "ping".to_string()
}

pub fn handler(_cmd: &resp::DataType, stream: &mut TcpStream) -> std::io::Result<()> {
    let response = "+PONG\r\n".to_string();
    stream.write_all(response.as_bytes())
}
