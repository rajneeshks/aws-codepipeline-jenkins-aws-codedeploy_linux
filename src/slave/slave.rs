use crate::commands::incoming;
use bytes::BytesMut;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::thread;
use std::time;

const MAX_RETRIES: u8 = 5;

trait State {
    fn initiate(self: Box<Self>, stream: &mut TcpStream, config: &Config) -> Box<dyn State>;
}

#[derive(Debug, Clone)]
struct Init {}

impl Init {
    pub fn new() -> Self {
        Self {}
    }
}

impl State for Init {
    fn initiate(self: Box<Self>, stream: &mut TcpStream, config: &Config) -> Box<dyn State> {
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
    fn initiate(mut self: Box<Self>, stream: &mut TcpStream, config: &Config) -> Box<dyn State> {
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

    fn initiate_internal(&mut self, stream: &mut TcpStream, config: &Config) -> Result<(), String> {
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
            println!("replconf1 response: {}", cmd.get_command(0));
            println!("raplconf1 raw buffer of len: {len} : {:?}", buf);
            if cmd.get_command(0).to_lowercase().contains("ok") {
                return Ok(());
            }
        }
        Err("Unable to move to next state - may be retrying!!".to_string())
    }
}

impl State for ReplConf1 {
    fn initiate(mut self: Box<Self>, stream: &mut TcpStream, config: &Config) -> Box<dyn State> {
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
        _config: &Config,
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
            println!("replconf2 raw buffer of length: {len}: {:?}", buf);
            // verify that command contains +OK
            let cmd = incoming::Incoming::new(&buf, true);
            println!("replconf2 incoming command parsing: {}", cmd);
            println!("replconf2 response: {}", cmd.get_command(0));
            if cmd.get_command(0).to_lowercase().contains("ok") {
                return Ok(());
            }
        }
        Err("Unable to move to next state - may be retrying!!".to_string())
    }
}

impl State for ReplConf2 {
    fn initiate(mut self: Box<Self>, stream: &mut TcpStream, config: &Config) -> Box<dyn State> {
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
        _config: &Config,
    ) -> Result<(), String> {
        let command = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
        let resp = stream.write(command.as_bytes());
        if resp.is_err() {
            return Err(format!("Error sending PSYNC command"));
        }
        let mut buf = BytesMut::with_capacity(1500);
        unsafe {
            buf.set_len(1500);
        }
        if let Ok(len) = stream.read(&mut buf) {
            println!("received response of {len} length for PSync2 command");
            if len <= 0 {
                // sleep and retry
                return Err("Did not receive appropriate command response (PSYNC)".to_string());
            }
            unsafe {
                buf.set_len(len);
            }
            if len > 10 {
                println!("PSync Assuming success - need to parse this input properly!!");
                return Ok(());
            }
            let cmd = incoming::Incoming::new(&buf, true);
            println!("PSYNC command response received: {}", cmd.get_command(0));
            if cmd.get_command(0).to_lowercase().contains("fullresync") {
                return Ok(());
            }
        }
        Err("Unable to move to next state (PSYNC) - may be retrying!!".to_string())
    }
}

impl State for PSync {
    fn initiate(mut self: Box<Self>, stream: &mut TcpStream, config: &Config) -> Box<dyn State> {
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
    fn initiate(self: Box<Self>, _stream: &mut TcpStream, _config: &Config) -> Box<dyn State> {
        self
    }
}

pub struct Config {
    master_ip_addr: String,
    master_port: u16,
    my_port: u16,
    pub stream: Option<TcpStream>,
    state: Option<Box<dyn State>>,
}

impl Config {
    pub fn new(master_ip_addr: String, master_port: u16, my_port: u16) -> Self {
        Self {
            master_ip_addr,
            master_port,
            my_port,
            stream: None,
            state: Some(Box::new(Init::new())),
        }
    }

    pub fn initiate(&mut self) {
        // establish TCP connection with the master
        // save the socket stream
        let config = Config {
            master_ip_addr: self.master_ip_addr.clone(),
            master_port: self.master_port,
            my_port: self.my_port,
            stream: None,
            state: None,
        };
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
                self.state = Some(s.initiate(&mut stream, &config));
            }
        } else {
            println!("Slave is not connected to the master...");
        }
    }

    pub fn shutdown(&mut self) {
        self.state = Some(Box::new(Init::new()));
        if let Some(conn) = &self.stream {
            let _ = conn.shutdown(Shutdown::Both);
        }
    }
}
