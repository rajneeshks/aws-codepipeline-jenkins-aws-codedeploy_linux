use crate::commands::echo;
use crate::commands::getset;
use crate::commands::info;
use crate::commands::ping;
use crate::commands::repl;
use crate::commands::resp;
use crate::commands::ss;
use crate::store::db;
use std::net::TcpStream;
use std::sync::Arc;

pub fn get_nth_arg(cmd: &resp::DataType, id: usize) -> Option<&String> {
    if let resp::DataType::Array(values) = cmd {
        if values.len() <= id {
            return None;
        }
        return Some(&values[id]);
    }
    None
}

pub fn array_type_handler(
    cmd: &Vec<String>,
) -> fn(&resp::DataType, &mut TcpStream, &Arc<db::DB>) -> std::io::Result<()> {
    if cmd[0].contains("echo") {
        return echo::handler;
    } else if cmd[0].contains("ping") {
        return ping::handler;
    } else if cmd[0].contains("set") {
        return getset::set_handler;
    } else if cmd[0].contains("get") {
        return getset::get_handler;
    } else if cmd[0].contains("info") {
        return info::handler;
    } else if cmd[0].contains("replconf") {
        return repl::handler;
    }

    ss::invalid
}
