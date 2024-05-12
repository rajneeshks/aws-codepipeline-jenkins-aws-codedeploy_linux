use crate::commands::ping;
use crate::commands::resp;
use std::io::Write;
use std::net::TcpStream;

pub fn invalid(_: &resp::DataType, stream: &mut TcpStream) -> std::io::Result<()> {
    let d = resp::DataType::Invalid("invalid command".to_string());
    stream.write_all(format!("{}", d).as_bytes())
}

pub fn SimpleStringCommandHandler(
    cmd: &String,
) -> fn(&resp::DataType, &mut TcpStream) -> std::io::Result<()> {
    if cmd.contains("ping") {
        return ping::handler;
    }

    invalid
}
