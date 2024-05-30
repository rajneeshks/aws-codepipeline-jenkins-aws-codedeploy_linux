use bytes::BytesMut;
use clap::Parser;
use std::io::Read;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread;

mod commands;
mod rdb;
mod repl;
mod slave;
mod store;

const EXPIRY_LOOP_TIME: u64 = 500; // 500 milli seconds
const DEFAULT_LISTENING_PORT: u16 = 6379;

#[derive(Debug, Default, Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[clap(default_value_t=DEFAULT_LISTENING_PORT, short, long)]
    port: u16,
    #[clap(short, long)]
    replicaof: Option<String>,
    #[clap(long)]
    dir: Option<String>,
    #[clap(long)]
    dbfilename: Option<String>,
}

fn handle_connection(
    stream: TcpStream,
    db: Arc<store::db::DB>,
    replcfg: Arc<repl::repl::ReplicationConfig>,
    repl_ch_tx: Sender<BytesMut>,
) {
    let mut stream = stream;
    let mut buf = BytesMut::with_capacity(1500);
    unsafe {
        buf.set_len(1500);
    }
    // read data from socket

    while let Ok(len) = stream.read(&mut buf) {
        if len <= 0 {
            break;
        }
        unsafe {
            buf.set_len(len);
        }
        let cmd = commands::incoming::Incoming::new(&buf, false);
        if let Err(e) = cmd.handle(&mut stream, &db, &replcfg, &repl_ch_tx, &None) {
            println!("error handling incoming command: {}, Error: {}", cmd, e);
            break;
        }
        unsafe {
            buf.set_len(1500);
        }
    }

    let _ = stream.shutdown(Shutdown::Both);
}

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let args = Args::parse();
    println!("Listening port: {}", args.port);
    println!("replicaoff flag: {:?}", args.replicaof);

    let mut role_master = true;
    let mut master_ip_addr = "".to_string();
    let mut master_port = 6380;

    if let Some(replica) = args.replicaof {
        role_master = false; // slave node
        let args = replica
            .split(" ")
            .map(|s| {
                if s == "localhost" {
                    return "127.0.0.1".to_string();
                }
                s.to_string()
            })
            .collect::<Vec<String>>();
        if args.len() != 2 {
            // problematic
            println!("Invalid options for replicaof.... exiting");
            return;
        }
        master_ip_addr = args[0].clone();
        if let Ok(p) = args[1].parse::<u16>() {
            master_port = p;
        } else {
            println!("Invalid master port format!!... exiting");
            return;
        }
    }

    // Uncomment this block to pass the first stage
    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port)).unwrap();
    let db = Arc::new(store::db::DB::new(role_master, args.dir, args.dbfilename));

    // spawn expiry thread
    if true {
        let dbc = Arc::clone(&db);
        let _ = thread::spawn(move || store::db::key_expiry_thread(dbc, EXPIRY_LOOP_TIME));
    }

    let (repl_tx_ch, repl_rx_ch) = mpsc::channel();
    let replcfg = Arc::new(repl::repl::ReplicationConfig::new());

    // start replication thread - only needed on master
    // but slave can get promoted to a master
    if true {
        let dbc = Arc::clone(&db);
        let replcfg_cp = Arc::clone(&replcfg);
        let _ = thread::spawn(move || repl::repl::replicator(replcfg_cp, repl_rx_ch, dbc));
    }

    if !role_master {
        // new thread for master-slave communications
        let dbc = Arc::clone(&db);
        let tx_ch_clone = repl_tx_ch.clone();
        let replcfg_cp = Arc::clone(&replcfg);
        let _ =
            thread::spawn(move || slave::slave::slave_thread(dbc, replcfg_cp,
                    tx_ch_clone, master_ip_addr, master_port, args.port));
    }

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                let dbc = Arc::clone(&db);
                let tx_ch_clone = repl_tx_ch.clone();
                let replcfg_cp = Arc::clone(&replcfg);
                let _ =
                    thread::spawn(move || handle_connection(_stream, dbc, replcfg_cp, tx_ch_clone));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
