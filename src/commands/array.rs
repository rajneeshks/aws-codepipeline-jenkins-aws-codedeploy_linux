use crate::commands::echo;
use crate::commands::ping;
use crate::commands::resp;
use crate::commands::ss;
use std::io::Write;
use std::net::TcpStream;

pub fn array_type_handler(
    cmd: &Vec<String>,
) -> fn(&resp::DataType, &mut TcpStream) -> std::io::Result<()> {
    if cmd[0].contains("echo") {
        return echo::handler;
    } else if cmd[0].contains("ping") {
        return ping::handler;
    }

    ss::invalid
}
