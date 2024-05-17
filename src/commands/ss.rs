use crate::commands::incoming;
use crate::commands::ping;
use crate::commands::resp;
use crate::repl::repl;
use crate::store::db;
use bytes::BytesMut;
use std::io::Write;
use std::net::TcpStream;
use std::sync::mpsc::Sender;
use std::sync::Arc;

pub fn invalid(stream: &mut TcpStream) -> std::io::Result<()> {
    let d = resp::DataType::Invalid("invalid command\r\n".to_string());
    stream.write_all(format!("{}", d).as_bytes())
}

pub struct InvalidCommand {}
impl InvalidCommand {
    pub fn new() -> Self {
        Self {}
    }
}

impl incoming::CommandHandler for InvalidCommand {
    fn handle(
        &self,
        stream: &mut TcpStream,
        _store: &Arc<db::DB>,
    ) -> std::io::Result<()> {
        invalid(stream)
    }
}

pub struct OkResponse {}
impl OkResponse {
    pub fn new() -> Self {
        Self {}
    }
}

impl incoming::CommandHandler for OkResponse {
    fn handle(
        &self,
        _stream: &mut TcpStream,
        _store: &Arc<db::DB>,
    ) -> std::io::Result<()> {
        println!("dropping it --- its slave connection likely");
        Ok(())
    }
}

pub fn simple_string_command_handler(
    cmd: &String,
) -> Box<dyn incoming::CommandHandler> 
{
    if cmd.contains("ping") {
        return Box::new(ping::Ping::new());
    } else if cmd.contains("ok") {
        return Box::new(OkResponse::new());
    }

    Box::new(InvalidCommand::new())
}
