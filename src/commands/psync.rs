use crate::commands::resp;
use crate::rdb::rdb;
use crate::store::db;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;

pub fn handler(
    _cmd: &resp::DataType,
    stream: &mut TcpStream,
    _db: &Arc<db::DB>,
) -> std::io::Result<()> {
    let database = rdb::RDB::new();
    let mut response = vec![];
    response
        .extend_from_slice("+FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0\r\n".as_bytes());
    if let Ok(rdb_content) = database.get() {
        response.extend_from_slice(&rdb_content);
    } else {
        println!("---------- Error decoding RDB content ----------");
    }
    stream.write_all(&response)
}
