use crate::commands::incoming;
use crate::store::db;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;
use crate::repl::repl;
use std::thread;
use std::time;

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
    fn handle(&self, _stream: &mut TcpStream, _db: &Arc<db::DB>) -> std::io::Result<()> {
        Ok(())
    }

    fn repl_config(
        &self,
        stream: &mut TcpStream,
        replcfg: &Arc<repl::ReplicationConfig>
    ) -> std::io::Result<()> {
        if self.replication_conn {
            return Ok(());
        }
        if replcfg.num_replicas() > 0 {
            let _ = replcfg.get_acks(0);
            println!("sent acks to the replicas - should be in a state machine");
            // TODO! make it better
            thread::sleep(time::Duration::from_millis(500));
        }
        let response = format!(":{}\r\n", replcfg.num_replicas_acked());
        stream.write_all(response.as_bytes())
    }
}
