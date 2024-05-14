use bytes::BytesMut;
use clap::Parser;
use std::io::Read;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

mod commands;
mod store;

const EXPIRY_LOOP_TIME: u64 = 500; // 500 milli seconds
const DEFAULT_LISTENING_PORT: u16 = 6379;

#[derive(Debug, Default, Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[clap(default_value_t=DEFAULT_LISTENING_PORT, short, long)]
    port: u16,
}

fn handle_client(stream: TcpStream, db: Arc<store::db::DB>) {
    let mut stream = stream;
    let mut buf = BytesMut::with_capacity(1500);
    unsafe {
        buf.set_len(1500);
    }
    // read data from socket

    while let Ok(len) = stream.read(&mut buf) {
        if len <= 0 {
            println!("read {len} bytes and hence existing...");
            break;
        }
        unsafe {
            buf.set_len(len);
        }
        let cmd = commands::incoming::Incoming::new(&buf, len);
        if let Err(e) = cmd.handle(&mut stream, &db) {
            println!("error handling incoming command: {}, Error: {}", cmd, e);
            break;
        }
        unsafe {
            buf.set_len(1500);
        }
    }

    println!("Done with this socket - closing....");
    let _ = stream.shutdown(Shutdown::Both);
}

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let args = Args::parse();
    println!("Listening port: {}", args.port);
    // Uncomment this block to pass the first stage
    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port)).unwrap();
    let db = Arc::new(store::db::DB::new());

    // spawn expiry thread
    {
        let dbc = Arc::clone(&db);
        let _ = thread::spawn(move || store::db::key_expiry_thread(dbc, EXPIRY_LOOP_TIME));
    }

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                println!("accepted new connection");
                let dbc = Arc::clone(&db);
                let _ = thread::spawn(move || handle_client(_stream, dbc));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
