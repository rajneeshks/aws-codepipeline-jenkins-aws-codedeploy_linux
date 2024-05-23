use crate::commands::array;
use crate::commands::incoming;
use crate::rdb::rdb;
use crate::repl::repl;
use crate::store::db;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;

#[allow(dead_code)]

#[derive(Debug, Clone)]
pub struct PSync<'a> {
    cmd: &'a Vec<String>,
    replication_conn: bool,
}

impl<'a> PSync<'a> {
    pub fn new(cmd: &'a Vec<String>, replication_conn: bool) -> Self {
        Self { cmd, replication_conn }
    }
}

impl<'a> incoming::CommandHandler for PSync<'a> {
    fn handle(
        &self,
        stream: &mut TcpStream,
        _db: &Arc<db::DB>,
    ) -> std::io::Result<()> {
        let mut response = vec![];
        response
            .extend_from_slice("+FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0\r\n".as_bytes());
        if let Ok(rdb_content) = rdb::RDB::empty() {
            response.extend_from_slice(&rdb_content);
        } else {
            println!("---------- Error decoding RDB content ----------");
        }

        // how do we know if this is master/slave connection
        stream.write_all(&response)
    }

    fn repl_config(
        &self,
        stream: &mut TcpStream,
        replcfg: &Arc<repl::ReplicationConfig>
    ) -> std::io::Result<()> {
        if let Err(e) = parse_psync_options(self.cmd, stream, replcfg) {
            println!("Error updating replication node sync!!: {}", e);
            return Err(std::io::Error::new(std::io::ErrorKind::Other, e));
        }
        Ok(())
    }
}

fn parse_psync_options(
    cmd: &Vec<String>,
    stream: &mut TcpStream,
    replcfg: &Arc<repl::ReplicationConfig>,
) -> Result<(), String> {
    let peer_addr_complete = format!("{}", stream.peer_addr().unwrap());
    let peer_addr = peer_addr_complete.split(":").collect::<Vec<&str>>();
    if peer_addr.len() != 2 {
        return Err(format!(
            "Invalid peer address format: {}",
            peer_addr_complete
        ));
    }
    if let Some(o) = array::get_nth_arg(cmd, 1) {
        println!("replconf master index: {}", o);
        // if this is ? , means it does not know master IDX
        if let Some(sync_id) = array::get_nth_arg(cmd, 2) {
            println!("next option: {}", sync_id);
            if let Ok(syncid) = sync_id.parse::<i64>() {
                // this will mark slave state ready for replication as well
                replcfg.update_psync_repl_id(&peer_addr_complete, syncid, stream);
                return Ok(());
            } else {
                println!("failed to parse {sync_id} into integer!");
            }
        }
    } else {
        println!("Is this a blunder!!! - unable to parse command: {:?}", cmd);
    }
    Err(format!("Error with PSYNC options - command: {:?}", cmd))
}
