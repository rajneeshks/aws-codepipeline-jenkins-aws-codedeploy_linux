use crate::commands::incoming;
use crate::store::db;
use std::net::TcpStream;
use std::sync::Arc;

#[allow(dead_code)]

#[derive(Debug, Clone)]
pub struct FullResync {
    replication_conn: bool,
}

impl FullResync {
    pub fn new(replication_conn: bool) -> Self {
        Self { replication_conn }
    }
}

impl incoming::CommandHandler for FullResync {
    fn handle(&self, _stream: &mut TcpStream, _db: &Arc<db::DB>) -> std::io::Result<()> {
        Ok(())
    }
}
