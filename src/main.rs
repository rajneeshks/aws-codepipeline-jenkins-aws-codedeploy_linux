use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::thread;

fn handle_client(stream: TcpStream) {
    let mut stream = stream;
    let mut data: [u8; 1500] = [0; 1500];
    // read data from socket

    while let Ok(_n) = stream.read(&mut data) {
        if _n <= 0 {
            break;
        }
        println!("Received {_n} bytes from {}", stream.peer_addr().unwrap());
        let command = String::from_utf8(data[0.._n].to_vec());
        if command.is_ok() {
            let command = command.unwrap();
            println!("Incomign command: {command}");
            if command.contains("PING") {
                println!("responding with PONG\r\n");
                let _ = stream.write_all(b"+PONG\r\n");
            }
        } else {
            println!("not sure what we received...");
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
