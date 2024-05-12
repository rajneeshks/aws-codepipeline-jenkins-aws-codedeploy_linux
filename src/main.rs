use bytes::BytesMut;
use std::io::Read;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::thread;

mod commands;

//use crate::commands;

fn handle_client(stream: TcpStream) {
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
        println!("Received {len} bytes from {}", stream.peer_addr().unwrap());
        unsafe {
            buf.set_len(len);
        }
        let cmd = commands::incoming::Incoming::new(&buf, len);
        println!("comamnd: {}", cmd);
        if let Err(e) = cmd.handle(&mut stream) {
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

    // Uncomment this block to pass the first stage
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                println!("accepted new connection");
                let _ = thread::spawn(move || handle_client(_stream));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
