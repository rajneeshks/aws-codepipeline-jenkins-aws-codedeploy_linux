use crate::commands::incoming;
use crate::commands::resp;
use crate::repl::repl;
use crate::store::db;
use bytes::BytesMut;
use std::io::Write;
use std::net::TcpStream;
use std::sync::mpsc::Sender;
use std::sync::Arc;

pub const COMMAND_NAME: &str = "echo";

pub fn handler(
    datain: &incoming::Incoming,
    stream: &mut TcpStream,
    _db: &Arc<db::DB>,
    _replcfg: &Arc<repl::ReplicationConfig>,
    _tx_ch: &Sender<BytesMut>,
) -> std::io::Result<()> {
    // only be called when data type is appropriate
    let cmd = &datain.command;
    let mut response = String::new(); //("*\r\n");
    if let resp::DataType::Array(values) = cmd {
        if values.len() >= 2 {
            if values[0] == COMMAND_NAME {
                values.iter().skip(1).for_each(|val| {
                    let _ = std::fmt::write(
                        &mut response,
                        format_args!("${}\r\n{}\r\n", val.chars().count(), val),
                    );
                });
            }
        }
    }
    stream.write_all(response.as_bytes())
}
