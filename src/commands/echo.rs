use crate::commands::incoming;
use crate::store::db;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Echo<'a> {
    cmd: &'a Vec<String>,
    replication_conn: bool,
}

impl<'a> Echo<'a> {
    pub fn new(cmd: &'a Vec<String>, replication_conn: bool) -> Self {
        Self { cmd, replication_conn }
    }
}

impl<'a> incoming::CommandHandler for Echo<'a> {
    fn handle(
        &self,
        stream: &mut TcpStream,
        _db: &Arc<db::DB>,
    ) -> std::io::Result<()> {
        // only be called when data type is appropriate
        if self.replication_conn { return Ok(()); }
        let mut response = String::new();
        if self.cmd.len() >= 2 {
            if self.cmd[0] == "echo" {
                self.cmd.iter().skip(1).for_each(|val| {
                    let _ = std::fmt::write(
                        &mut response,
                        format_args!("${}\r\n{}\r\n", val.chars().count(), val),
                    );
                });
            }
        }
        stream.write_all(response.as_bytes())
    }
}
