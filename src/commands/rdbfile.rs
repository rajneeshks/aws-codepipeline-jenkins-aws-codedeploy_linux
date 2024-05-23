use crate::commands::incoming;
use crate::slave::slave;
use crate::store::db;
use std::net::TcpStream;
use std::sync::Arc;

#[allow(dead_code)]

pub struct RDBFile<'a> {
    cmd: &'a Vec<u8>,
    replication_conn: bool,
}

impl<'a> RDBFile<'a> {
    pub fn new(cmd: &'a Vec<u8>, replication_conn: bool) -> Self {
        Self { cmd, replication_conn }
    }
}

impl<'a> incoming::CommandHandler for RDBFile<'a> {
    fn handle(&self, _stream: &mut TcpStream, _db: &Arc<db::DB>) -> std::io::Result<()> {
        Ok(())
    }

    fn track_offset(&self, slavecfg: &Option<slave::Config>, _stream: &mut TcpStream, _length: usize) -> std::io::Result<()>{
        if let Some(cfg) = slavecfg.as_ref() {
            println!("Started tracking offsets now!!");
            cfg.synced_in();
        } 
        Ok(())
    }
}
