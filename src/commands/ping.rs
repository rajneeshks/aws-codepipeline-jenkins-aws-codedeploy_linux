use crate::commands::incoming;
use crate::store::db;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Ping {}

impl Ping {
    pub fn new() -> Self {
        Self {}
    }
}

impl incoming::CommandHandler for Ping {
    fn handle(
        &self,
        stream: &mut TcpStream,
        _db: &Arc<db::DB>,
    ) -> std::io::Result<()> {
        let response = "+PONG\r\n".to_string();
        stream.write_all(response.as_bytes())
    }
}
