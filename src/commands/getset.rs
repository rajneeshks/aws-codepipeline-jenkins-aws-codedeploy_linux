use crate::commands::array;
use crate::commands::incoming;
use crate::commands::ss;
use crate::store::db;
use bytes::BytesMut;
use clap::error::ErrorKind;
use std::io::Write;
use std::net::TcpStream;
use std::sync::mpsc::Sender;
use std::sync::Arc;

#[derive(Default, Debug)]
pub struct SetOptions {
    pub expiry_in_ms: u64,
}

impl SetOptions {
    fn new() -> Self {
        Self {
            ..Default::default()
        }
    }
}
#[derive(Debug, Clone)]
pub struct SetCommand <'a>{
    cmd: &'a Vec<String>,
    replication_conn: bool,
}

impl<'a> SetCommand<'a> {
    pub fn new(cmd: &'a Vec<String>, replication_conn: bool) -> Self {
        Self { cmd, replication_conn }
    }
}

impl<'a> incoming::CommandHandler for SetCommand<'a> {
    fn handle(
        &self,
        stream: &mut TcpStream,
        db: &Arc<db::DB>
    ) -> std::io::Result<()> {
        let cmd = &self.cmd;
        let mut response = String::new();
        let key_option = array::get_nth_arg(cmd, 1);
        let val_option = array::get_nth_arg(cmd, 2);
        if key_option.is_none() || val_option.is_none() {
            return ss::invalid(stream);
        }
        let key = key_option.unwrap();
        let val = val_option.unwrap();
        let mut options = SetOptions::new();
        // search for expiry option
        let mut argidx = 3;
        let px_option = "px".to_string();
        while let Some(opt) = array::get_nth_arg(cmd, argidx) {
            match opt {
                px_option => {
                    argidx += 1;
                    if let Some(expiry) = array::get_nth_arg(cmd, argidx) {
                        if let Ok(expiry_ms) = expiry.parse::<u64>() {
                            options.expiry_in_ms = expiry_ms;
                        }
                    }
                }
                _ => {}
            };
            argidx += 1;
        }
        let db_result = db.add(key.clone(), val.clone(), &options);
        if db_result.is_err() {
            println!("Error writing into the DB");
            return Err(std::io::Error::new(std::io::ErrorKind::Other,
                format!("failed set command: {:?}", self.cmd)));
        }
        if self.replication_conn { return Ok(()); }

        if let Ok(_o) =  db_result {
            // replicate it now
            let _ = std::fmt::write(&mut response, format_args!("+OK\r\n"));
        } else {
            let _ = std::fmt::write(&mut response, format_args!("-Error writing to DB\r\n"));
        }
        stream.write_all(response.as_bytes())
    }

    fn replicate(
            &self,
            buf: &BytesMut,
            tx_ch: &Sender<BytesMut>
        ) -> std::io::Result<()> {
        if self.replication_conn { return Ok(()); }
        match tx_ch.send(buf.clone()) {
            Ok(_) => Ok(()),
            Err(e) => 
                Err(std::io::Error::new(std::io::ErrorKind::Other,
                    format!("failed replication: {:?}", e))),
        }
    }
}


#[derive(Debug, Clone)]
pub struct GetCommand<'a> {
    cmd: &'a Vec<String>,
    replication_conn: bool,
}

impl<'a> GetCommand<'a> {
    pub fn new(cmd: &'a Vec<String>, replication_conn: bool) -> Self {
        Self { cmd, replication_conn }
    }
}

impl<'a> incoming::CommandHandler for GetCommand<'a> {
    fn handle(
        &self,
        stream: &mut TcpStream,
        db: &Arc<db::DB>,
    ) -> std::io::Result<()> {
        if self.replication_conn { return Ok(()); }
        
        let cmd = &self.cmd;
        let mut response = String::new();
        if let Some(key) = array::get_nth_arg(cmd, 1) {
            if let Some(val) = db.get(key) {
                let _ = std::fmt::write(
                    &mut response,
                    format_args!("${}\r\n{}\r\n", val.chars().count(), val),
                );
            } else {
                // did not find
                let _ = std::fmt::write(&mut response, format_args!("$-1\r\n"));
            }
        } else {
            return ss::invalid(stream);
        }
        stream.write_all(response.as_bytes())
    }
}