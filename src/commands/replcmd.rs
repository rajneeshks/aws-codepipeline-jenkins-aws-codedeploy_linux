use crate::commands::array;
use crate::commands::incoming;
use crate::slave::slave;
use crate::repl::repl;
use crate::store::db;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ReplCommand<'a> {
    cmd: &'a Vec<String>,
    replication_conn: bool,
}

impl<'a> ReplCommand<'a> {
    pub fn new(cmd: &'a Vec<String>, replication_conn: bool) -> Self {
        Self { cmd, replication_conn }
    }
}

impl<'a> incoming::CommandHandler for ReplCommand<'a> {
    fn handle(
        &self,
        stream: &mut TcpStream,
        _db: &Arc<db::DB>,
    ) -> std::io::Result<()> {

        if self.replication_conn {
            return Ok(());
        }
        stream.write_all(b"+OK\r\n")
    }

    // should be done only if this is master node
    fn repl_config(
            &self,
            stream: &mut TcpStream,
            replcfg: &Arc<repl::ReplicationConfig>
        ) -> std::io::Result<()> {
            // we should receive these commands only over replication connection
            //if !self.replication_conn { return ss::invalid(stream); }

            if let Err(e) = parse_repl_options(self.cmd, stream, replcfg) {
                println!("Error creating replication node!!: {}", e);
                return Err(std::io::Error::new(std::io::ErrorKind::Other, e));
            }
            Ok(())
        }

    fn track_offset(&self, slavecfg: &Option<slave::Config>, stream: &mut TcpStream, length: usize) -> std::io::Result<()>{
        let mut offset = 0;
        if let Some(cfg) = slavecfg.as_ref() {
            offset = cfg.get_offset();
            cfg.track_offset(length as u64);
        }
        if self.cmd.len() >= 2 && self.cmd[1].to_lowercase().contains("getack") {
            // lets send it out!
            let offset_str = offset.to_string();
            let response = format!("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",
                offset_str.len(), offset);
            return stream.write_all(response.as_bytes());
        }
        Ok(())
    }
}

fn parse_repl_options(
    cmd: &Vec<String>,
    stream: &TcpStream,
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
    let mut optidx: usize = 1;
    println!("peer address: {:?}", peer_addr);
    if let Some(o) = array::get_nth_arg(cmd, optidx) {
        optidx += 1;
        if o.contains("listening-port") {
            if let Some(port) = array::get_nth_arg(cmd, 2) {
                optidx += 1;
                if let Ok(pp) = port.parse::<u16>() {
                    if let Ok(_) = replcfg.add_node(peer_addr[0], pp, &peer_addr_complete) {
                        return Ok(());
                    }
                }
            }
        } else if o.contains("capa") {
            println!("its second replconf - we should update slave capabilities");
            return Ok(());
        } else if o.contains("getack") || o.contains("GETACK") || o.contains("ACK") || o.contains("ack") {
            return Ok(());
        }
    }
    Err("Error with REPL options".to_string())
}
