use crate::commands::incoming;
use crate::store;
use crate::repl;
use bytes::BytesMut;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::RwLock;
use std::thread;
use std::time;
use std::sync::Arc;
use std::sync::mpsc::Sender;

#[allow(dead_code)]

const MAX_RETRIES: u8 = 5;

trait State {
    fn initiate(self: Box<Self>, stream: &mut TcpStream, config: &MasterNodeConfig) -> Box<dyn State>;
}

#[derive(Debug, Clone)]
struct Init {}

impl Init {
    pub fn new() -> Self {
        Self {}
    }
}

impl State for Init {
    fn initiate(self: Box<Self>, stream: &mut TcpStream, config: &MasterNodeConfig) -> Box<dyn State> {
        let new_state = Box::new(Ping::new());
        new_state.clone().initiate(stream, config);
        new_state
    }
}

#[derive(Debug, Clone)]
struct Ping {
    retries: u8,
}

impl Ping {
    pub fn new() -> Self {
        Self { retries: 0 }
    }

    fn initiate_internal(&mut self, stream: &mut TcpStream) -> Result<(), String> {
        // send SYNC command
        let command = "*1\r\n$4\r\nPING\r\n";
        let resp = stream.write(command.as_bytes());
        if resp.is_err() {
            return Err(format!("Error sending PING command"));
        }
        let mut buf = BytesMut::with_capacity(500);
        unsafe {
            buf.set_len(500);
        }
        if let Ok(len) = stream.read(&mut buf) {
            if len <= 0 {
                // sleep and retry
                return Err("Did not receive appropriate command".to_string());
            }
            unsafe {
                buf.set_len(len);
            }
            // verify that command contains +PONG
            let cmd = incoming::Incoming::new(&buf, true);
            if cmd.get_command(0).to_lowercase().contains("pong") {
                return Ok(());
            }
        }
        Err("Unable to move to next state - may be retrying!!".to_string())
    }
}

impl State for Ping {
    fn initiate(mut self: Box<Self>, stream: &mut TcpStream, config: &MasterNodeConfig) -> Box<dyn State> {
        while self.retries < MAX_RETRIES {
            self.retries += 1;
            if let Ok(_resp) = self.initiate_internal(stream) {
                let new_state = Box::new(ReplConf1::new());
                new_state.clone().initiate(stream, config);
                return new_state;
            } else {
                // sleep for sometime
                thread::sleep(time::Duration::from_millis(1000 * self.retries as u64));
            }
        }
        self
    }
}

#[derive(Debug, Clone)]
struct ReplConf1 {
    retries: u8,
}

impl ReplConf1 {
    pub fn new() -> Self {
        Self { retries: 0 }
    }

    fn initiate_internal(&mut self, stream: &mut TcpStream, config: &MasterNodeConfig) -> Result<(), String> {
        // send SYNC command
        //*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n
        let mut command = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n".to_string();
        command = format!(
            "{}${}\r\n{}\r\n",
            command,
            config.my_port.to_string().len(),
            config.my_port
        );
        let resp = stream.write(command.as_bytes());
        if resp.is_err() {
            return Err(format!("Error sending REPLCONF1 command"));
        }
        let mut buf = BytesMut::with_capacity(500);
        unsafe {
            buf.set_len(500);
        }
        if let Ok(len) = stream.read(&mut buf) {
            if len <= 0 {
                // sleep and retry
                return Err("Did not receive appropriate command response (REPLCONF1)".to_string());
            }
            unsafe {
                buf.set_len(len);
            }
            // verify that command contains +PONG
            let cmd = incoming::Incoming::new(&buf, true);
            if cmd.get_command(0).to_lowercase().contains("ok") {
                return Ok(());
            }
        }
        Err("Unable to move to next state - may be retrying!!".to_string())
    }
}

impl State for ReplConf1 {
    fn initiate(mut self: Box<Self>, stream: &mut TcpStream, config: &MasterNodeConfig) -> Box<dyn State> {
        while self.retries < MAX_RETRIES {
            self.retries += 1;
            if let Ok(_resp) = self.initiate_internal(stream, config) {
                let new_state = Box::new(ReplConf2::new());
                new_state.clone().initiate(stream, config);
                return new_state;
            } else {
                // sleep for sometime
                thread::sleep(time::Duration::from_millis(1000 * self.retries as u64));
            }
        }
        self
    }
}

#[derive(Debug, Clone)]
struct ReplConf2 {
    retries: u8,
}

impl ReplConf2 {
    fn new() -> Self {
        Self { retries: 0 }
    }

    fn initiate_internal(
        &mut self,
        stream: &mut TcpStream,
        _config: &MasterNodeConfig,
    ) -> Result<(), String> {
        // send SYNC command
        //*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n
        let command = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".to_string();
        let resp = stream.write(command.as_bytes());
        if resp.is_err() {
            return Err(format!("Error sending REPLCONF2 command"));
        }
        let mut buf = BytesMut::with_capacity(500);
        unsafe {
            buf.set_len(500);
        }
        if let Ok(len) = stream.read(&mut buf) {
            if len <= 0 {
                // sleep and retry
                return Err("Did not receive appropriate command response (REPLCONF2)".to_string());
            }
            unsafe {
                buf.set_len(len);
            }
            // verify that command contains +OK
            let cmd = incoming::Incoming::new(&buf, true);
            if cmd.get_command(0).to_lowercase().contains("ok") {
                return Ok(());
            }
        }
        Err("Unable to move to next state - may be retrying!!".to_string())
    }
}

impl State for ReplConf2 {
    fn initiate(mut self: Box<Self>, stream: &mut TcpStream, config: &MasterNodeConfig) -> Box<dyn State> {
        while self.retries < MAX_RETRIES {
            self.retries += 1;
            if let Ok(_resp) = self.initiate_internal(stream, config) {
                let new_state = Box::new(PSync::new());
                new_state.clone().initiate(stream, config);
                return new_state;
            } else {
                // sleep for sometime
                thread::sleep(time::Duration::from_millis(1000 * self.retries as u64));
            }
        }
        self
    }
}

#[derive(Debug, Clone)]
struct PSync {
    retries: u8,
}

impl PSync {
    fn new() -> Self {
        Self { retries: 0 }
    }

    fn initiate_internal(
        &mut self,
        stream: &mut TcpStream,
        _config: &MasterNodeConfig,
    ) -> Result<(), String> {
        let command = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
        let resp = stream.write(command.as_bytes());
        if resp.is_err() {
            return Err(format!("Error sending PSYNC command"));
        }
        // we will handle these commands as part of regular processing
/*
        let mut cmds_processed = 0;
        let mut buf = BytesMut::with_capacity(1500);
        // hack - may be sent over multiple commands - how to do we know its the end?
        while cmds_processed < 2 {
            unsafe {
                buf.set_len(1500);
            }
            if let Ok(len) = stream.read(&mut buf) {
                if len <= 0 {
                    // sleep and retry
                    return Err("Did not receive appropriate command response (PSYNC)".to_string());
                }
                unsafe {
                    buf.set_len(len);
                }
                println!("PSync2 Raw response of {len} bytes: {:?}", buf);
            }
            let cmd = incoming::Incoming::new(&buf, true);
            cmds_processed += cmd.commands.len();
            //cmd.handle(stream, None, None, None);
        }
        Err("Unable to move to next state (PSYNC) - may be retrying!!".to_string())
*/
        Ok(())
    }
}

impl State for PSync {
    fn initiate(mut self: Box<Self>, stream: &mut TcpStream, config: &MasterNodeConfig) -> Box<dyn State> {
        while self.retries < MAX_RETRIES {
            self.retries += 1;
            if let Ok(_resp) = self.initiate_internal(stream, config) {
                return Box::new(Complete::new());
            } else {
                // sleep for sometime
                thread::sleep(time::Duration::from_millis(1000 * self.retries as u64));
            }
        }
        self
    }
}

#[derive(Debug, Clone)]
struct Complete {}

impl Complete {
    fn new() -> Self {
        Self {}
    }
}

impl State for Complete {
    fn initiate(self: Box<Self>, _stream: &mut TcpStream, _config: &MasterNodeConfig) -> Box<dyn State> {
        self
    }
}

#[derive(Debug, Clone)]
struct MasterNodeConfig {
    master_ip_addr: String,
    master_port: u16,
    my_port: u16,
}

impl MasterNodeConfig {
    fn new(master_ip_addr: String, master_port: u16, my_port: u16) -> Self{
        Self {
            master_ip_addr,
            master_port,
            my_port,
        }
    }
}

pub struct Config {
    master_node: MasterNodeConfig,
    stream: Option<TcpStream>,
    state: Option<Box<dyn State>>,
    in_sync: RwLock<bool>,  // if replica is in sync
    offset: RwLock<u64>,
}

impl Config {
    pub fn new(master_ip_addr: String, master_port: u16, my_port: u16) -> Self {
        Self {
            master_node: MasterNodeConfig::new(master_ip_addr, master_port, my_port),
            stream: None,
            state: Some(Box::new(Init::new())),
            in_sync: RwLock::new(false),
            offset: RwLock::new(u64::MIN),
        }
    }

    pub fn initiate(&mut self) {
        // establish TCP connection with the master
        // save the socket stream

        if self.stream.is_none() {
            let stream =
                TcpStream::connect(format!("{}:{}", self.master_node.master_ip_addr, self.master_node.master_port));
            self.stream = stream.ok();
        }
        if let Some(mut stream) = self.stream.as_mut() {
            println!(
                "Connected to master at {}:{}",
                self.master_node.master_ip_addr, self.master_node.master_port
            );
            if let Some(s) = self.state.take() {
                self.state = Some(s.initiate(&mut stream, &self.master_node));
            }
        } else {
            println!("Slave is not connected to the master...");
        }
    }

    #[allow(dead_code)]
    pub fn shutdown(&mut self) {
        self.state = Some(Box::new(Init::new()));
        if let Some(conn) = &self.stream {
            let _ = conn.shutdown(Shutdown::Both);
        }
    }

    pub fn synced_in(&self) {
        *self.in_sync.write().unwrap() = true;
    }

    pub fn track_offset(&self, len: u64) {
        let synced = *self.in_sync.read().unwrap();
        if synced {
            *self.offset.write().unwrap() += len;
        }
    }

    pub fn get_offset(&self) -> u64 {
        *self.offset.read().unwrap()
    }
}


pub fn slave_thread(
    db: Arc<store::db::DB>,
    replcfg: Arc<repl::repl::ReplicationConfig>,
    repl_ch_tx: Sender<BytesMut>,
    master_ip_addr: String,
    master_port: u16,
    my_port: u16
) {
    let mut slave = Config::new(
        master_ip_addr,
        master_port,
        my_port,
    );
    slave.initiate(); // initiate the state machine.

    let mut buf = BytesMut::with_capacity(1500);
    unsafe {
        buf.set_len(1500);
    }

    // take out the stream from inside the config struct to be safe
    let mut stream = slave.stream.take().unwrap();

    let slavecfg = Some(slave);
    // read data from socket
    loop {
        if let Ok(len) = stream.read(&mut buf) {
            
            if len <= 0 {
                println!("read {len} bytes and hence existing...");
                break;
            }
            unsafe {
                buf.set_len(len);
            }
            let cmd = incoming::Incoming::new(&buf, true);
            if let Err(e) = cmd.handle(&mut stream, &db, &replcfg, &repl_ch_tx, &slavecfg) {
                println!("error handling incoming command on master-slave channel: {}, Error: {}", cmd, e);
                break;
            }
            unsafe {
                buf.set_len(1500);
            }
        }
    }

    println!("replication connection (slave thread): Done with this socket - closing....");
    let _ = stream.shutdown(Shutdown::Both);

}
