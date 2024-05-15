use crate::commands::resp;
use crate::store::db;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;

pub fn handler(
    _cmd: &resp::DataType,
    stream: &mut TcpStream,
    _db: &Arc<db::DB>,
) -> std::io::Result<()> {
    let response = "+OK\r\n".to_string();
    stream.write_all(response.as_bytes())
}
