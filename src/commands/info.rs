use crate::commands::array;
use crate::commands::incoming;
use crate::repl::repl;
use crate::store::db;
use bytes::BytesMut;
use std::io::Write;
use std::net::TcpStream;
use std::sync::mpsc::Sender;
use std::sync::Arc;

pub const COMMAND_NAME: &str = "info";

pub fn handler(
    datain: &incoming::Incoming,
    stream: &mut TcpStream,
    db: &Arc<db::DB>,
    _replcfg: &Arc<repl::ReplicationConfig>,
    _tx_ch: &Sender<BytesMut>,
) -> std::io::Result<()> {
    // only be called when data type is appropriate
    let mut response = String::new();
    let cmd = &datain.command;
    let mut replication = false;
    if let Some(info_type) = array::get_nth_arg(cmd, 1) {
        if info_type.contains("replication") {
            replication = true;
        }
    }
    if replication {
        let mut role = "role:slave".to_string();
        if db.role_master() {
            role = "role:master".to_string();
            role.push_str("\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
            role.push_str("\r\nmaster_repl_offset:0");
        }
        let _ = std::fmt::write(
            &mut response,
            format_args!("${}\r\n{}\r\n", role.len(), role),
        );
    }
    stream.write_all(response.as_bytes())
}
