use crate::commands::incoming;
use crate::store::db;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Ping {
    replication_conn: bool,
}

impl Ping {
    pub fn new(replication_conn: bool) -> Self {
        Self {replication_conn}
    }
}

impl incoming::CommandHandler for Ping {
    fn handle(
        &self,
        stream: &mut TcpStream,
        _db: &Arc<db::DB>,
    ) -> std::io::Result<()> {
        if self.replication_conn { return Ok(()); }
        let response = "+PONG\r\n".to_string();
        stream.write_all(response.as_bytes())
    }
}
