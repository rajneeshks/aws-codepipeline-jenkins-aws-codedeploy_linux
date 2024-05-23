use crate::commands::incoming;
use crate::store::db;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Keys <'a>{
    cmd: &'a Vec<String>,
    replication_conn: bool,
}

impl<'a> Keys<'a> {
    pub fn new(cmd: &'a Vec<String>, replication_conn: bool) -> Self {
        Self {cmd, replication_conn}
    }
}

impl<'a> incoming::CommandHandler for Keys<'a> {
    fn handle(
        &self,
        stream: &mut TcpStream,
        db: &Arc<db::DB>,
    ) -> std::io::Result<()> {
        // read all the keys from DB and send them via an array.
        let (response, _count) = db.keys();
        stream.write_all(response.as_bytes())
    }
}
