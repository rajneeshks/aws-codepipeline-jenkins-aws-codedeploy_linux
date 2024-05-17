// incoming command formatting
use crate::commands::array;
use crate::commands::resp;
use crate::commands::ss;
use crate::repl::repl;
use crate::store::db;
use bytes::BytesMut;
use std::io::Write;
use std::net::TcpStream;
use std::sync::mpsc::Sender;
use std::sync::Arc;

const COMMAND_DELIMITER: &str = "\r\n";

pub trait CommandHandler {
    fn handle(
        &self,
        stream: &mut TcpStream,
        db: &Arc<db::DB>,
    ) -> std::io::Result<()>;

    // if command requires replication - add its own implementation
    fn replicate(
        &self,
        _buf: &BytesMut,
        _tx_ch: &Sender<BytesMut>
    ) -> std::io::Result<()> {
        Ok(())
    }

    // if command is setting up replication config, add its implementation
    fn repl_config(
        &self,
        _stream: &mut TcpStream,
        _replcfg: &Arc<repl::ReplicationConfig>
    ) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct Incoming<'a> {
    pub buf: &'a BytesMut,
    pub commands: Vec<resp::DataType>,
    replication_conn: bool,
}

impl<'a, 'b> Incoming<'b> {
    pub fn new(buf: &'a BytesMut, replication_conn: bool) -> Incoming<'b>
    where
        'a: 'b,
    {
        let commands = resp::DataType::new(buf);
        Self {
            buf,
            commands,
            replication_conn,
        }
    }

    pub fn handle(
        &self,
        stream: &mut TcpStream,
        db: &Arc<db::DB>,
        replcfg: &Arc<repl::ReplicationConfig>,
        repl_ch: &Sender<BytesMut>,
    ) -> std::io::Result<()> {
        for command in &self.commands {
            println!("processing command: {}", command);
            let mut handler = None;
            match command {
                resp::DataType::SimpleString(ref cmd) => { // give the ownership away?
                    handler = Some(ss::simple_string_command_handler(cmd, self.replication_conn));
                },
                resp::DataType::Array(ref cmd) => {
                    handler = Some(array::array_type_handler(cmd, self.replication_conn));
                },
                _ => stream.write_all(format!("-{}\r\n", command).as_bytes())?,
            }
            if let Some(f) = handler {
                let result1 = f.handle(stream, db);
                let result2 = f.replicate(self.buf, repl_ch);
                let result3 = f.repl_config(stream, replcfg);

                if result1.is_err() { 
                    println!("Error processing command for {}", command);
                    // return result1; 
                }
                if result2.is_err() { 
                    println!("Error processing replication for {}", command);
                    //return result2; 
                }
                if result3.is_err() {
                    println!("Error updating repl configuration for {}", command);
                    //return result3;
                }
            }
        }
        Ok(())
    }

    // returns first token - could be command or a response
    pub fn get_command(&self, id: usize) -> String {
        if id >= self.commands.len() {
            return "invalid command index".to_string();
        }
        match self.commands[id] {
            resp::DataType::SimpleString(ref cmd) => cmd.clone(),
            resp::DataType::Array(ref cmd) => cmd[0].clone(),
            _ => return "not implemented".to_string(),
        }
    }
}

impl std::fmt::Display for Incoming<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut response = String::new();
        for i in 0..self.commands.len() {
            let _ = std::fmt::write(&mut response,
                format_args!("command {i}: {}", &self.commands[i]));
        }
        write!(f, "Incoming command(s): {}", response)
    }
}
