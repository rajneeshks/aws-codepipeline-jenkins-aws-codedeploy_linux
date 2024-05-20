use bytes::BytesMut;
use std::io::ErrorKind;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use crate::store::db;

#[derive(Debug)]
pub struct ReplicationNode {
    id: String, // any ID
    ip: String, // IP and port information
    peer_addr: String,
    //capa: String,
    port: u16,
    eof: bool,
    connection: Option<TcpStream>,
    ready: bool,
    repl_id: u64,
    ack_id: u64,
}

impl ReplicationNode {
    pub fn new(ip: &str, port: u16, peer_addr: &str) -> Self {
        Self {
            id: peer_addr.to_string(),
            ip: ip.to_string(),
            peer_addr: peer_addr.to_string(),
            port,
            eof: false,
            connection: None,
            ready: false,
            repl_id: 0,
            ack_id: 0,
        }
    }

    pub fn replicate(&mut self, buffers: &Vec<BytesMut>) -> std::io::Result<()> {
        if !self.ready {
            println!("node not ready for replication...");
            return Ok(());
        }
        if self.connection.is_none() {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "slave is not connected!",
            ));
        }
        if let Some(connection) = self.connection.as_mut() {
            while self.repl_id < buffers.len() as u64 {
                let rslt = connection.write_all(&buffers[self.repl_id as usize]);
                if rslt.is_err() {
                    return rslt;
                }
                self.repl_id += 1;
            }
        }
        Err(std::io::Error::new(
            ErrorKind::Other,
            "unable to send replication command!",
        ))
    }

    fn replication_acked(&mut self, id: u64) -> Result<(), String>{
        if !self.ready {
            println!("node not ready for replication...");
            return Ok(());
        }
        self.ack_id += 1;
        Ok(())
    }

    pub fn shutdown(&mut self) {
        if let Some(connection) = self.connection.take() {
            let _ = connection.shutdown(Shutdown::Both);
        }
    }

    pub fn mark_ready(&mut self) {
        self.ready = true;
    }

    fn get_ack(&mut self, _ack: u64) -> std::io::Result<()> {
        let cmd = b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
        if let Some(connection) = self.connection.as_mut() {
            println!("sending replconf getack request to slave: {}, ip: {}, port: {}", self.peer_addr, self.ip, self.port);
            connection.write_all(cmd)?
        }
        Err(std::io::Error::new(
            ErrorKind::Other,
            "No connection to the replica! we should not be here",
        ))
    }

    fn pending(&self) -> bool {
        self.ack_id < self.repl_id
    }
}

struct ReplicationConfigInternal {
    nodes: Vec<ReplicationNode>,
}

impl ReplicationConfigInternal {
    pub fn new() -> Self {
        Self { nodes: vec![] }
    }
}

struct ReplicationCommands {
    commands: Vec<BytesMut>,
}

impl ReplicationCommands {
    pub fn new() -> Self {
        Self { commands: vec![] }
    }
}

pub struct ReplicationConfig {
    replcfg: RwLock<ReplicationConfigInternal>,
    commands: RwLock<ReplicationCommands>,
}

impl ReplicationConfig {
    pub fn new() -> Self {
        Self {
            replcfg: RwLock::new(ReplicationConfigInternal::new()),
            commands: RwLock::new(ReplicationCommands::new()),
        }
    }

    // finds node by its peer address -> remote IP/port where
    // connection is made. Note that this port is different than the
    // port slave is listening on
    //fn find_node(&self, peer_addr: &str) -> Option<&ReplicationNode> {
    //}

    // adds a new node
    // at this stage, we have information about IP, Port and peer_address
    // called with REPLCONF1 command
    pub fn add_node(&self, ip: &str, port: u16, peer_addr: &str) -> Result<(), String> {
        {
            let config = self.replcfg.read().unwrap();
            let existing = config
                .nodes
                .iter()
                .find(|node| node.ip == *ip && node.port == port && node.peer_addr == *peer_addr);
            if existing.is_some() {
                return Ok(());
            }
        }

        let new_node = ReplicationNode::new(ip, port, peer_addr);
        self.replcfg.write().unwrap().nodes.push(new_node);
        println!("New node added with ip: {}, port: {}, total so far: {}", ip, port, self.replcfg.read().unwrap().nodes.len());
        Ok(())
    }

    pub fn _add_capabilities(&mut self, _peer_addr: &str) {
        todo!()
    }

    // when PSync command is invoked, we only know the peer address as part
    // of the command
    pub fn update_psync_repl_id(&self, peer_addr: &str, repl_id: i64, stream: &mut TcpStream) {
        let mut replcfg = self.replcfg.write().unwrap();
        for i in 0..replcfg.nodes.len() {
            if replcfg.nodes[i].peer_addr == *peer_addr {
                replcfg.nodes[i].repl_id = if repl_id <= 0 { 0 } else { repl_id as u64 };
                println!(
                    "updated node repl id with peer_addr: {} to {}",
                    peer_addr, replcfg.nodes[i].repl_id
                );
                // finally mark it ready
                replcfg.nodes[i].ready = true;
                if let Ok(cloned_stream) = stream.try_clone() {
                    println!("able to clone connection!!!!");
                    replcfg.nodes[i].connection = Some(cloned_stream);
                }
            }
        }
    }

    pub fn get_acks(&self, ackid: u64)-> std::io::Result<()>{
        let mut config = self.replcfg.write().unwrap();
        println!("*************  number of replication nodes: {}", config.nodes.len());
        for  i in 0..config.nodes.len() {
            let _ = config.nodes[i].get_ack(ackid);
        }
        if true { return Ok(()); }
        Err(std::io::Error::new(
            ErrorKind::Other,
            "No connection to the replica! we should not be here",
        ))
    }

    pub fn num_replicas(&self) -> usize {
        self.replcfg.read().unwrap().nodes.len()
    }

    pub fn num_replicas_acked(&self) -> usize {
        self.replcfg.read().unwrap().nodes.iter().filter(|node| !node.pending()).count()
    }

    pub fn replication_acked(&self, peer_addr: &str, _ack_id: u64) -> Result<(), String>{
        let mut replcfg = self.replcfg.write().unwrap();
        for i in 0..replcfg.nodes.len() {
            if replcfg.nodes[i].peer_addr == *peer_addr {
                println!(
                    "Replicatin node acked: {} to {}:{}",
                    peer_addr, replcfg.nodes[i].ip, replcfg.nodes[i].port
                );
                let _ = replcfg.nodes[i].replication_acked(_ack_id);
            }
        }
        Ok(())
    }

    pub fn replication_connection(&self, peer_addr: &str) -> bool {
        let replcfg = self.replcfg.read().unwrap();
        for i in 0..replcfg.nodes.len() {
            if replcfg.nodes[i].peer_addr == *peer_addr {
                return replcfg.nodes[i].ready;
            }
        }
        false
    }

}

pub fn replicator(
    replcfg: Arc<ReplicationConfig>,
    repl_ch_rx: Receiver<BytesMut>,
    db: Arc<db::DB>,
) {
    // replicates the commands
    while let Ok(data) = repl_ch_rx.recv() {
        println!("Received a command to replicate...");
        if db.role_master() {
            let mut config = replcfg.replcfg.write().unwrap();
            let mut commands = replcfg.commands.write().unwrap();
            if data.len() > 0 {
                commands.commands.push(data);
            }
            for i in 0..config.nodes.len() {
                let _ = config.nodes[i].replicate(&commands.commands);
            }
        }
    }
}

fn discard_incoming_data(stream: &mut TcpStream) {
    let mut response: [u8; 1500] = [0; 1500];
    let _ = stream.set_read_timeout(Some(Duration::from_millis(1)));
    let _ = stream.read(&mut response);
}
