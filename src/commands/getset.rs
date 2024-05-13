use crate::commands::array;
use crate::commands::resp;
use crate::commands::ss;
use crate::store::db;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;

pub struct SetOptions {}

pub fn set_handler(
    cmd: &resp::DataType,
    stream: &mut TcpStream,
    db: &Arc<db::DB>,
) -> std::io::Result<()> {
    let mut response = String::new(); //("*\r\n");
    let key_option = array::get_nth_arg(cmd, 1);
    let val_option = array::get_nth_arg(cmd, 2);
    if key_option.is_none() || val_option.is_none() {
        return ss::invalid(cmd, stream, db);
    }
    let key = key_option.unwrap();
    let val = val_option.unwrap();

    let options = SetOptions {};
    db.add(key.clone(), val.clone(), &options);

    let _ = std::fmt::write(&mut response, format_args!("+OK\r\n"));
    stream.write_all(response.as_bytes())
}

pub fn get_handler(
    cmd: &resp::DataType,
    stream: &mut TcpStream,
    db: &Arc<db::DB>,
) -> std::io::Result<()> {
    let mut response = String::new(); //("*\r\n");
    if let Some(key) = array::get_nth_arg(cmd, 1) {
        if let Some(val) = db.get(key) {
            let _ = std::fmt::write(
                &mut response,
                format_args!("${}\r\n{}\r\n", val.chars().count(), val),
            );
        } else {
            // did not find
            let _ = std::fmt::write(&mut response, format_args!("$-1\r\n"));
        }
    } else {
        return ss::invalid(cmd, stream, db);
    }
    stream.write_all(response.as_bytes())
}
