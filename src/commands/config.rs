use crate::commands::array;
use crate::commands::incoming;
use crate::store::db;
use std::io::ErrorKind;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;

use super::ss::invalid;

#[derive(Debug, Clone)]
pub struct Config<'a> {
    cmd: &'a Vec<String>,
    replication_conn: bool,
}

impl<'a> Config<'a> {
    pub fn new(cmd: &'a Vec<String>, replication_conn: bool) -> Self {
        Self { cmd, replication_conn }
    }
}

impl<'a> incoming::CommandHandler for Config<'a> {
    fn handle(
        &self,
        stream: &mut TcpStream,
        db: &Arc<db::DB>,
    ) -> std::io::Result<()> {
        let mut response = String::new();
        // how do we know if this is master/slave connection

        let mut num_args = 0;
        let mut optidx = 1;
        let mut valid_command = false;
        if let Some(arg) = array::get_nth_arg(self.cmd, optidx) {
            if arg.contains("GET") || arg.contains("get") {
                valid_command = true;
            }
        }
        
        if !valid_command {
            return Err(std::io::Error::new(std::io::ErrorKind::Other,
                format!("unsupported config command: {:?}", self.cmd)));
        }

        optidx += 1;

        while let Some(arg) = array::get_nth_arg(self.cmd, optidx) {
            match arg.as_str() {
                 "dir" => {
                    num_args += 2;
                    optidx += 1;
                    let dir = db.rdb_directory();
                    let _ = std::fmt::write(&mut response,
                        format_args!("${}\r\n{}\r\n${}\r\n{}\r\n", arg.len(), arg, dir.len(), dir));
                },
                "dbfilename" => {
                    num_args += 2;
                    optidx += 1;
                    let filename = db.rdb_filename();
                    let _ = std::fmt::write(&mut response,
                        format_args!("${}\r\n{}\r\n${}\r\n{}\r\n", arg.len(), arg, filename.len(), filename));
                },
                _ => {
                    // return error
                    return Err(std::io::Error::new(std::io::ErrorKind::Other,
                        format!("Invalid arguements to config command: {}", arg)));
                }
            }
        }
        if num_args == 0 { 
            // error command
            return Err(std::io::Error::new(std::io::ErrorKind::Other,
                format!("No valid arguements to config command: {:?}", self.cmd)));
        }

        let final_response = format!("*{}\r\n{}", num_args, response);
        // is it dirname or filename -> next parameter
        stream.write_all(final_response.as_bytes())
    }

}
