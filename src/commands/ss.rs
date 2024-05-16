use crate::commands::incoming;
use crate::commands::ping;
use crate::commands::resp;
use crate::repl::repl;
use crate::store::db;
use bytes::BytesMut;
use std::io::Write;
use std::net::TcpStream;
use std::sync::mpsc::Sender;
use std::sync::Arc;

pub fn invalid(
    _: &incoming::Incoming,
    stream: &mut TcpStream,
    _store: &Arc<db::DB>,
    _replcfg: &Arc<repl::ReplicationConfig>,
    _tx_ch: &Sender<BytesMut>,
) -> std::io::Result<()> {
    let d = resp::DataType::Invalid("invalid command\r\n".to_string());
    stream.write_all(format!("{}", d).as_bytes())
}

pub fn dropit(
    _: &incoming::Incoming,
    stream: &mut TcpStream,
    _store: &Arc<db::DB>,
    _replcfg: &Arc<repl::ReplicationConfig>,
    _tx_ch: &Sender<BytesMut>,
) -> std::io::Result<()> {
    println!("dropping it --- its slave connection likely");
    Ok(())
}

pub fn simple_string_command_handler(
    cmd: &String,
) -> fn(
    &incoming::Incoming,
    &mut TcpStream,
    &Arc<db::DB>,
    &Arc<repl::ReplicationConfig>,
    &Sender<BytesMut>,
) -> std::io::Result<()> {
    if cmd.contains("ping") {
        return ping::handler;
    } else if cmd.contains("ok") {
        return dropit;
    }

    invalid
}
