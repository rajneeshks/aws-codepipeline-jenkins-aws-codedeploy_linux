use crate::commands::echo;
use crate::commands::getset;
use crate::commands::incoming;
use crate::commands::info;
use crate::commands::ping;
use crate::commands::psync;
use crate::commands::replcmd;
use crate::commands::ss;

pub fn get_nth_arg(values: &Vec<String>, id: usize) -> Option<&String> {
    if values.len() <= id {
        return None;
    }
    return Some(&values[id]);
}

pub fn array_type_handler(
    cmd: &Vec<String>,
) -> Box<dyn incoming::CommandHandler + '_> {
    if cmd[0].contains("ok") {
        return Box::new(ss::OkResponse::new());
    } else if cmd[0].contains("info") {
        return Box::new(info::Info::new(cmd));
    } else if cmd[0].contains("echo") {
        return Box::new(echo::Echo::new(cmd));
    } else if cmd[0].contains("ping") {
        return Box::new(ping::Ping::new());
    } else if cmd[0].contains("set") {
        return Box::new(getset::SetCommand::new(cmd));
    } else if cmd[0].contains("get") {
        return Box::new(getset::GetCommand::new(cmd));
    } else if cmd[0].contains("replconf") {
        return Box::new(replcmd::ReplCommand::new(cmd));
    } else if cmd[0].contains("psync") {
        return Box::new(psync::PSync::new(cmd));
    }

    Box::new(ss::InvalidCommand::new())
}
