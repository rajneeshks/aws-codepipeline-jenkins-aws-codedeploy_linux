use crate::commands::array;
use crate::commands::incoming;
use crate::commands::resp;
use crate::commands::ss;
use crate::rdb::rdb;
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
    if !db.role_master() {
        println!("I am no master - why should I respond to psync!!\n");
        return ss::invalid(datain, stream, db, replcfg, tx_ch);
    }

    let cmd = &datain.command;
    // find listening port
    // first command
    // parse capabilities
    let database = rdb::RDB::new();
    let mut response = vec![];
    response
        .extend_from_slice("+FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0\r\n".as_bytes());
    if let Ok(rdb_content) = database.get() {
        response.extend_from_slice(&rdb_content);
    } else {
        println!("---------- Error decoding RDB content ----------");
    }

    // now lets get this slave ready for syncing

    if let Err(e) = parse_psync_options(cmd, stream, replcfg) {
        println!("Error updating replication node sync!!: {}", e);
    }
    // mark this state as slave is insync
    stream.write_all(&response)
}

fn parse_psync_options(
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
    if let Some(o) = array::get_nth_arg(cmd, optidx) {
        optidx += 1;
        // if this is ? , means it does not know master IDX
        //if let Some(sync_id) = array::get_nth_arg(cmd, optidx) {
        //    println!("next option: {}", sync_id);
        //    optidx += 1;
        //if let Ok(syncid) = sync_id.parse::<i32>() {
        if let Ok(syncid) = o.parse::<i64>() {
            replcfg.update_psync_repl_id(&peer_addr_complete, syncid);
            return Ok(());
        } else {
            println!("failed to parse {o} into integer!");
        }
        //}
    } else {
        println!("Is this a blunder!!!");
    }
    Err("Error with PSYNC options".to_string())
}
