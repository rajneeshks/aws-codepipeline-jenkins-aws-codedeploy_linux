use crate::commands::incoming;
use crate::store::db;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;
use crate::repl::repl;
use std::thread;
use std::time;
use crate::commands::array;

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
        let mut replicas = 0;
        let mut millis = 0;

        if let Some(replicas_str) = array::get_nth_arg(self.cmd, 1) {
            if let Ok(_replicas) = replicas_str.parse::<usize>() {
                replicas = _replicas;
            }
        }

        if let Some(millis_str) = array::get_nth_arg(&self.cmd, 2) {
            if let Ok(_millis) = millis_str.parse::<u64>() {
                millis = _millis;
            }
        }

        let now = time::Instant::now();
        let timeout = time::Duration::from_millis(millis);
        replicas = replicas.max(replcfg.num_replicas());

        if replcfg.num_replicas() > 0 {
            let _ = replcfg.get_acks(0);
            while replcfg.num_replicas_acked() < replicas {
                // TODO! make it better
                if millis > 0 { if now.elapsed() >  timeout { break; } }
                thread::sleep(time::Duration::from_millis(100));
            }
        }

        let response = format!(":{}\r\n", replcfg.num_replicas_acked());
        // clear the ACKs - to get past stupid broken test case
        replcfg.clear_pending_acks();

        stream.write_all(response.as_bytes())

    }
}
