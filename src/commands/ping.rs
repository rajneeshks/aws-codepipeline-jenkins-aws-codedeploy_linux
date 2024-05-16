use crate::commands::incoming;
use crate::repl::repl;
use crate::store::db;
use bytes::BytesMut;
use std::io::Write;
use std::net::TcpStream;
use std::sync::mpsc::Sender;
use std::sync::Arc;

pub fn handler(
    _datain: &incoming::Incoming,
    stream: &mut TcpStream,
    _db: &Arc<db::DB>,
    _replcfg: &Arc<repl::ReplicationConfig>,
    _tx_ch: &Sender<BytesMut>,
) -> std::io::Result<()> {
    let response = "+PONG\r\n".to_string();
    stream.write_all(response.as_bytes())
}
