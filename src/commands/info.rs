use crate::commands::array;
use crate::commands::resp;
use crate::store::db;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;

pub const COMMAND_NAME: &str = "info";

pub fn handler(
    cmd: &resp::DataType,
    stream: &mut TcpStream,
    db: &Arc<db::DB>,
) -> std::io::Result<()> {
    // only be called when data type is appropriate
    let mut response = String::new(); //("*\r\n");
    let mut replication = false;
    if let Some(info_type) = array::get_nth_arg(cmd, 1) {
        if info_type.contains("replication") {
            replication = true;
        }
    }
    if replication {
        let mut master = false;
        let mut role = "role:slave";
        if db.role_master() {
            master = true;
            role = "role:master";
        }
        let _ = std::fmt::write(
            &mut response,
            format_args!("${}\r\n{}\r\n", role.len(), role),
        );
        if master {
            let repl_id = "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
            let _ = std::fmt::write(
                &mut response,
                format_args!("${}\r\n{}\r\n", repl_id.len(), repl_id),
            );
            let offset = "master_repl_offset:0";
            let _ = std::fmt::write(
                &mut response,
                format_args!("${}\r\n{}\r\n", offset.len(), offset),
            );
        }
    }
    stream.write_all(response.as_bytes())
}
