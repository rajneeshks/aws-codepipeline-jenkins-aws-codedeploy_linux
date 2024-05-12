use crate::commands::echo;
use crate::commands::ping;
use crate::commands::resp;
use crate::commands::ss;
use std::io::Write;
use std::net::TcpStream;

pub fn ArrayTypeHandler(
    cmd: &Vec<String>,
) -> fn(&resp::DataType, &mut TcpStream) -> std::io::Result<()> {
    if cmd[0].contains("echo") {
        return echo::handler;
    }

    ss::invalid
}
