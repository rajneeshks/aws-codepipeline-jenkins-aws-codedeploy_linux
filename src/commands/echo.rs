use crate::commands::resp;
use std::io::Write;
use std::net::TcpStream;

pub const COMMAND_NAME: &str = "echo";

pub fn handler(cmd: &resp::DataType, stream: &mut TcpStream) -> std::io::Result<()> {
    // only be called when data type is appropriate
    let mut response = String::new(); //("*\r\n");
    if let resp::DataType::Array(values) = cmd {
        if values.len() >= 2 {
            if values[0] == COMMAND_NAME {
                values.iter().skip(1).for_each(|val| {
                    println!("Encoding {} for echo command", val);
                    let _ = std::fmt::write(
                        &mut response,
                        format_args!("${}\r\n{}\r\n", val.chars().count(), val),
                    );
                });
            }
        }
    }
    println!("Echo response built: {}", response);
    stream.write_all(response.as_bytes())
}
