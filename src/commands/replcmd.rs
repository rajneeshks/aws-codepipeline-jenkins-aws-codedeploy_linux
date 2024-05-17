use crate::commands::array;
use crate::commands::incoming;
use crate::commands::ss;
use crate::repl::repl;
use crate::store::db;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ReplCommand<'a> {
    cmd: &'a Vec<String>,
}

impl<'a> ReplCommand<'a> {
    pub fn new(cmd: &'a Vec<String>) -> Self {
        Self { cmd }
    }
}

impl<'a> incoming::CommandHandler for ReplCommand<'a> {
    fn handle(
        &self,
        stream: &mut TcpStream,
        db: &Arc<db::DB>,
    ) -> std::io::Result<()> {
        if db.role_master() {
            let response = "+OK\r\n".to_string();
            // parse repl options
            return stream.write_all(response.as_bytes());
        }
        ss::invalid(stream)
    }

    // should be done only if this is master node
    fn repl_config(
            &self,
            stream: &mut TcpStream,
            replcfg: &Arc<repl::ReplicationConfig>
        ) -> std::io::Result<()> {
            if let Err(e) = parse_repl_options(self.cmd, stream, replcfg) {
                println!("Error creating replication node!!: {}", e);
                return Err(std::io::Error::new(std::io::ErrorKind::Other, e));
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
        }
    }
    Err("Error with REPL options".to_string())
}
