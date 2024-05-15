use crate::commands::incoming;
use bytes::BytesMut;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::thread;
use std::time;

const MAX_RETRIES: u8 = 5;

trait State {
    fn initiate(self: Box<Self>, stream: &mut TcpStream) -> Box<dyn State>;
}

#[derive(Debug, Clone)]
struct Init {}

impl Init {
    pub fn new() -> Self {
        Self {}
    }
}

impl State for Init {
    fn initiate(mut self: Box<Self>, stream: &mut TcpStream) -> Box<dyn State> {
        let new_state = Box::new(Ping::new());
        new_state.clone().initiate(stream);
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
            let cmd = incoming::Incoming::new(&buf, len);
            if cmd.get_command().to_lowercase().contains("pong") {
                return Ok(());
            }
        }
        Err("Unable to move to next state - may be retrying!!".to_string())
    }
}

impl State for Ping {
    fn initiate(mut self: Box<Self>, stream: &mut TcpStream) -> Box<dyn State> {
        while self.retries < MAX_RETRIES {
            self.retries += 1;
            if let Ok(resp) = self.initiate_internal(stream) {
                let mut new_state = Box::new(ReplConf1::new());
                new_state.clone().initiate(stream);
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
}

impl State for ReplConf1 {
    fn initiate(mut self: Box<Self>, stream: &mut TcpStream) -> Box<dyn State> {
        Box::new(ReplConf1::new())
    }
}

#[derive(Debug, Clone)]
struct ReplConf2 {}

impl ReplConf2 {
    fn new() -> Self {
        Self {}
    }
}

impl State for ReplConf2 {
    fn initiate(self: Box<Self>, stream: &mut TcpStream) -> Box<dyn State> {
        Box::new(ReplConf2::new())
    }
}

#[derive(Debug, Clone)]
struct PSync {}

impl PSync {
    fn new() -> Self {
        Self {}
    }
}

impl State for PSync {
    fn initiate(self: Box<Self>, stream: &mut TcpStream) -> Box<dyn State> {
        Box::new(PSync::new())
    }
}

pub struct Config {
    master_ip_addr: String,
    master_port: u16,
    stream: Option<TcpStream>,
    state: Option<Box<dyn State>>,
}

impl Config {
    pub fn new(master_ip_addr: String, master_port: u16) -> Self {
        Self {
            master_ip_addr,
            master_port,
            stream: None,
            state: Some(Box::new(Init::new())),
        }
    }

    pub fn initiate(&mut self) {
        // establish TCP connection with the master
        // save the socket stream
        if self.stream.is_none() {
            let stream =
                TcpStream::connect(format!("{}:{}", self.master_ip_addr, self.master_port));
            self.stream = stream.ok();
        }
        if let Some(mut stream) = self.stream.as_mut() {
            println!(
                "Connected to master at {}:{}",
                self.master_ip_addr, self.master_port
            );
            if let Some(s) = self.state.take() {
                self.state = Some(s.initiate(&mut stream));
            }
        } else {
            println!("Slave is not connected to the master...");
        }
    }

    pub fn shutdown(&mut self) {
        self.state = Some(Box::new(Init::new()));
        if let Some(conn) = &self.stream {
            conn.shutdown(Shutdown::Both);
        }
    }
}
