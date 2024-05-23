use crate::commands::incoming;
use crate::store::db;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;
use crate::repl::repl;
use std::thread;
use std::time;
use crate::commands::array;

#[derive(Debug, Clone)]
pub struct TType<'a> {
    cmd: &'a Vec<String>,
    replication_conn: bool,
}

impl<'a> TType<'a> {
    pub fn new(cmd: &'a Vec<String>, replication_conn: bool) -> Self {
        Self {cmd, replication_conn}
    }
}

impl<'a> incoming::CommandHandler for TType<'a> {
    fn handle(&self, stream: &mut TcpStream, db: &Arc<db::DB>) -> std::io::Result<()> {
        let mut response = String::new();
        if let Some(key) = array::get_nth_arg(self.cmd, 1) {
            if let Some(_value) = db.get(key) {
                let _ = std::fmt::write(&mut response,
                    format_args!("+string\r\n"));
            } else {
                let _ = std::fmt::write(&mut response,
                    format_args!("+none\r\n"));
            }
        } else {
            let _ = std::fmt::write(&mut response,
                format_args!("+invalid\r\n"));
        }
        stream.write_all(response.as_bytes())
    }

}
