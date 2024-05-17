use crate::commands::array;
use crate::commands::incoming;
use crate::store::db;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Info<'a> {
    cmd: &'a Vec<String>
}

impl<'a> Info<'a> {
    pub fn new(cmd: &'a Vec<String>) -> Self {
        Self {cmd}
    }
}

impl<'a> incoming::CommandHandler for Info<'a> {
    fn handle(
        &self,
        stream: &mut TcpStream,
        db: &Arc<db::DB>,
    ) -> std::io::Result<()> {
        // only be called when data type is appropriate
        let mut response = String::new();
        let mut replication = false;
        if let Some(info_type) = array::get_nth_arg(self.cmd, 1) {
            if info_type.contains("replication") {
                replication = true;
            }
        }
        if replication {
            let mut role = "role:slave".to_string();
            if db.role_master() {
                role = "role:master".to_string();
                role.push_str("\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
                role.push_str("\r\nmaster_repl_offset:0");
            }
            let _ = std::fmt::write(
                &mut response,
                format_args!("${}\r\n{}\r\n", role.len(), role),
            );
        }
        stream.write_all(response.as_bytes())
    }
}
