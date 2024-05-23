use crate::commands::incoming;
use crate::commands::getset;
use crate::store::db;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;
use crate::commands::array;

#[allow(dead_code)]

#[derive(Debug, Clone)]
pub struct Stream<'a> {
    cmd: &'a Vec<String>,
    replication_conn: bool,
}

impl<'a> Stream<'a> {
    pub fn new(cmd: &'a Vec<String>, replication_conn: bool) -> Self {
        Self {cmd, replication_conn}
    }
}

impl<'a> incoming::CommandHandler for Stream<'a> {
    fn handle(&self, stream: &mut TcpStream, db: &Arc<db::DB>) -> std::io::Result<()> {
        let mut response = String::new();
        if let Some(skey) = array::get_nth_arg(self.cmd, 1) {
            if skey.as_str() != "stream_key" { // return error 
            }
            if let Some(skey_id) = array::get_nth_arg(self.cmd, 2) {
                // save the key with value
                let value = self.cmd.iter().skip(2).fold(String::new(), |mut acc, opt| {
                    acc.push_str(opt);
                    acc.push(' '); // add separator
                    acc
                });

                let options = getset::SetOptions::new();
                let db_result = db.add(skey.clone(), db::KeyValueType::StreamType(value), &options);
                if db_result.is_err() {
                    println!("Error writing into the DB");
                    return Err(std::io::Error::new(std::io::ErrorKind::Other,
                    format!("failed set command: {:?}", self.cmd)));
                }
                let _ = std::fmt::write(&mut response,
                    format_args!("${}\r\n{}\r\n", skey_id.len(), skey_id));
            }
        } else {
            let _ = std::fmt::write(&mut response,
                format_args!("+invalid\r\n"));
        }
        stream.write_all(response.as_bytes())
    }

}
