use crate::commands::incoming;
use crate::store::db;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;
use crate::commands::array;
use crate::store::streams;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct XRange<'a> {
    cmd: &'a Vec<String>,
    replication_conn: bool,
}

impl<'a> XRange<'a> {
    pub fn new(cmd: &'a Vec<String>, replication_conn: bool) -> Self {
        Self {cmd, replication_conn}
    }

    fn parse_options(&self) -> Result<((u128, u64), (u128, u64)), String> {
        // XRANGE some_key 1526985054069 1526985054079
        // XRANGE some_key - 1526985054079
        // XRANGE some_key 1526985054069 +

        let mut start = u128::MIN;
        let mut end =  u128::MAX;
        let mut start_seq = u64::MIN;
        let mut end_seq = u64::MAX;

        if let Some(v) = array::get_nth_arg(self.cmd, 2) {
            if v == "-" {  
                start = u128::MIN; 
            } else {
                // see if this is complete key
                if v.contains("-") {
                    let ss = v.split('-').collect::<Vec<&str>>();
                    if let Ok(_base) = ss[0].parse::<u128>() {
                        start = _base;
                    } else {
                        return Err("Invalid timestamp".to_string());
                    }
                    if ss.len() >= 2 {
                        if let Ok(_seq) = ss[1].parse::<u64>() {
                            start_seq = _seq;
                        } else {
                            return Err("Invalid Sequence number to xrange command (start)".to_string());
                        }
                    }
                } else {
                    if let Ok(_base) = v.parse::<u128>() {
                        start = _base;
                    } else {
                        return Err("Invalid arguments to xrange command (start)".to_string());
                    }
                }
            }
        }

        if let Some(v) = array::get_nth_arg(self.cmd, 3) {
            if v == "+" {  
                end = u128::MAX; 
            } else {
                // see if this is complete key
                if v.contains("-") {
                    let ss = v.split('-').collect::<Vec<&str>>();
                    if let Ok(_base) = ss[0].parse::<u128>() {
                        end = _base;
                    } else {
                        return Err("Invalid timestamp".to_string());
                    }
                    if ss.len() >= 2 {
                        if let Ok(_seq) = ss[1].parse::<u64>() {
                            end_seq = _seq;
                        } else {
                            return Err("Invalid Sequence number to xrange command (end)".to_string());
                        }
                    }
                } else {
                    if let Ok(_base) = v.parse::<u128>() {
                        end = _base;
                    } else {
                        return Err("Invalid arguments to xrange command (end)".to_string());
                    }
                }
            }
        }

        Ok(((start, start_seq), (end, end_seq)))
    }

    fn build_response(&self, stream: &streams::Streams, start: u128, start_seq: u64, end: u128, end_seq: u64) -> Result<String, String> {
        let (count, response) = stream.streams.iter()
            .filter(|((ts, seq), _value)| *ts >= start && *seq >= start_seq && *ts <= end && *seq <= end_seq)
            .fold((0, String::new()), |(count, mut acc), ((ts, seq), value)| {
                let field = format!("{}-{}", ts, seq);
                let _ = std::fmt::write(&mut acc, format_args!("*2\r\n${}\r\n{}\r\n", field.len(), field));
                // format the internal array (string)
                let _ = std::fmt::write(&mut acc, format_args!("*{}\r\n", value.len()));
                value.iter().for_each(|s| {
                    let _ = std::fmt::write(&mut acc, format_args!("${}\r\n{}\r\n", s.len(), s));
                });
                (count+1, acc)
            });
        Ok(format!("*{}\r\n{}", count, response))
    }
}

impl<'a> incoming::CommandHandler for XRange<'a> {
    fn handle(&self, stream: &mut TcpStream, db: &Arc<db::DB>) -> std::io::Result<()> {
        let mut response = String::new();
        if let Some(skey) = array::get_nth_arg(self.cmd, 1) {
            // check if key already exists
            if let Some(existing_key) = db.get(skey) {
                match existing_key {
                    db::KeyValueType::StreamType(value) => {
                        // found one - lets validate the timestamp and seq
                        match self.parse_options() {
                            Ok(((start, start_seq), (end, end_seq))) => {
                                match self.build_response(&value, start, start_seq, end, end_seq) {
                                    Ok(res) => {
                                        let _ = std::fmt::write(&mut response,
                                            format_args!("{}", res));
                                    },
                                    Err(e) => {
                                        let _ = std::fmt::write(&mut response,
                                            format_args!("-Unable to put together response to xrange: {}\r\n", e));
                                    }
                                }
                            },
                            Err(e) => {
                                let _ = std::fmt::write(&mut response,
                                    format_args!("-Invalid start/end to xrange: {}\r\n", e));
                            }
                        }
                    },
                    _ => {
                        let _ = std::fmt::write(&mut response,
                            format_args!("-Invalid key type {} - not a stream type\r\n{}\r\n", skey.len(), skey));
                    }
                }
            } else {
                let _ = std::fmt::write(&mut response,
                    format_args!("-invalid stream key - does not exist\r\n"));
            }
        } else {
            let _ = std::fmt::write(&mut response,
                format_args!("-invalid command\r\n"));
        }
        stream.write_all(response.as_bytes())
    }
}
