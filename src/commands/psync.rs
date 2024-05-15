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
    let response = "+FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0\r\n".to_string();
    stream.write_all(response.as_bytes())
}
