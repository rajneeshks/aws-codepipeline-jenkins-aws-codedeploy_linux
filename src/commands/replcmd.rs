use crate::commands::array;
use crate::commands::incoming;
use crate::commands::resp;
use crate::commands::ss;
use crate::repl::repl;
use crate::store::db;
use bytes::BytesMut;
use std::io::Write;
use std::net::TcpStream;
use std::sync::mpsc::Sender;
use std::sync::Arc;

pub fn handler(
    datain: &incoming::Incoming,
    stream: &mut TcpStream,
    db: &Arc<db::DB>,
    replcfg: &Arc<repl::ReplicationConfig>,
    tx_ch: &Sender<BytesMut>,
) -> std::io::Result<()> {
    let cmd = &datain.command;
    if db.role_master() {
        let response = "+OK\r\n".to_string();
        // parse repl options
        if let Err(e) = parse_repl_options(cmd, stream, replcfg) {
            println!("Error creating replication node!!: {}", e);
        }
        return stream.write_all(response.as_bytes());
    }
    ss::invalid(datain, stream, db, replcfg, tx_ch)
}

fn parse_repl_options(
    cmd: &resp::DataType,
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
    println!("peer ddress: {:?}", peer_addr);
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
