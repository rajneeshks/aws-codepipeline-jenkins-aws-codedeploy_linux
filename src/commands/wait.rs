use crate::commands::incoming;
use crate::store::db;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Wait<'a> {
    cmd: &'a Vec<String>,
    replication_conn: bool,
}

impl<'a> Wait<'a> {
    pub fn new(cmd: &'a Vec<String>, replication_conn: bool) -> Self {
        Self {cmd, replication_conn}
    }
}

impl<'a> incoming::CommandHandler for Wait<'a> {
    fn handle(&self, stream: &mut TcpStream, _db: &Arc<db::DB>) -> std::io::Result<()> {
        if self.replication_conn {
            return Ok(());
        }
        let response = format!(":{}\r\n", 0);
        stream.write_all(response.as_bytes())
    }
}
