use crate::commands::incoming;
use crate::commands::ping;
use crate::commands::resp;
use crate::commands::fullresync;
use crate::store::db;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;

pub fn invalid(stream: &mut TcpStream) -> std::io::Result<()> {
    println!("---------- ************* sending invalid command ***********-----------");
    let d = resp::DataType::Invalid("invalid command\r\n".to_string());
    stream.write_all(format!("{}", d).as_bytes())
}

pub struct InvalidCommand {
    replication_conn: bool,
}
impl InvalidCommand {
    pub fn new(replication_conn: bool) -> Self {
        Self {replication_conn}
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

pub struct OkResponse {
    replication_conn: bool,
}
impl OkResponse {
    pub fn new(replication_conn: bool) -> Self {
        Self {replication_conn}
    }
}

impl incoming::CommandHandler for OkResponse {
    fn handle(
        &self,
        _stream: &mut TcpStream,
        _store: &Arc<db::DB>,
    ) -> std::io::Result<()> {
        println!("dropping it --- is it on replication connection: {}", self.replication_conn);
        Ok(())
    }
}

pub fn simple_string_command_handler(
    cmd: &String,
    replication_conn: bool,
) -> Box<dyn incoming::CommandHandler> 
{
    if cmd.contains("ping") {
        return Box::new(ping::Ping::new(replication_conn));
    } else if cmd.contains("ok") {
        return Box::new(OkResponse::new(replication_conn));
    } else if cmd.contains("fullresync") {
        return Box::new(fullresync::FullResync::new(replication_conn));
    }

    Box::new(InvalidCommand::new(replication_conn))
}
