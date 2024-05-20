// incoming command formatting
use crate::commands::array;
use crate::commands::bulk;
use crate::commands::resp;
use crate::commands::ss;
use crate::repl::repl;
use crate::slave::slave;
use crate::store::db;
use bytes::BytesMut;
use std::io::Write;
use std::net::TcpStream;
use std::sync::mpsc::Sender;
use std::sync::Arc;


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

    // for tracking slave offset - per received buffer, its done in main thread
    // this is for any further processing or specific response
    fn track_offset(&self, slavecfg: &Option<slave::Config>, _stream: &mut TcpStream, length: usize) -> std::io::Result<()>{
        if let Some(cfg) = slavecfg {
            cfg.track_offset(length as u64);
        }
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
        slavecfg: &Option<slave::Config>,
    ) -> std::io::Result<()> {
        for command in &self.commands {
            println!("processing command: {}", command);
            let mut handler = None;
            let mut start = 0;
            let mut end = 0;
            match command {
                resp::DataType::SimpleString(ref cmd, _start, _end) => {
                    handler = Some(ss::simple_string_command_handler(cmd, self.replication_conn));
                    start = *_start;
                    end = *_end;
                },
                resp::DataType::Array(ref cmd, _start, _end) => {
                    handler = Some(array::array_type_handler(cmd, self.replication_conn));
                    start = *_start;
                    end = *_end;
                },
                resp::DataType::BulkString(ref cmd, _start, _end) => {
                    handler = Some(bulk::bulk_string_type_handler(cmd, self.replication_conn));
                    start = *_start;
                    end = *_end;
                },
                resp::DataType::SimpleError(ref _cmd, _start, _end) => { // received error message, may be log it for now
                    println!("Received Simple error command: {}", command);
                },
                _ => if !self.replication_conn {
                    stream.write_all(format!("-{}\r\n", command).as_bytes())?;
                } else {
                    println!("reported invalid command on replicastion connection!!");
                }
            }
            if let Some(f) = handler {
                let result1 = f.handle(stream, db);
                let result2 = f.replicate(self.buf, repl_ch);
                let result3 = f.repl_config(stream, replcfg);
                let result4 = f.track_offset(slavecfg, stream, command.len());

                if result1.is_err() { 
                    println!("Error processing command for {} - result1: {:?}", command, result1);
                    // return result1; 
                }
                if result2.is_err() { 
                    println!("Error processing replication for {} - result2: {:?}", command, result2);
                    //return result2; 
                }
                if result3.is_err() {
                    println!("Error updating repl configuration for {} - result3: {:?}", command, result3);
                    //return result3;
                }
                if result4.is_err() {
                    println!("Error updating slave offset for {} for result4: {:?}", command, result4);
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
            resp::DataType::SimpleString(ref cmd, _, _) => cmd.clone(),
            resp::DataType::Array(ref cmd, _, _) => cmd[0].clone(),
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
