use crate::commands::incoming;
use crate::commands::getset;
use crate::store::db;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;
use crate::commands::array;
use crate::store::streams;
use std::fmt;

#[allow(dead_code)]
enum XADDErrors {
    TimeStampOlder(u128),
    TimeStampInvalid(u128),
    InvalidArgs,
}

impl fmt::Display for XADDErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            XADDErrors::TimeStampOlder(_val) => write!(f, "ERR The ID specified in XADD is equal or smaller than the target stream top item"),
            XADDErrors::TimeStampInvalid(val) => write!(f, "ERR The ID specified in XADD must be greater than {}-0", val),
            XADDErrors::InvalidArgs => write!(f, "-invalid arguments"),
        }
    }
}

#[allow(dead_code)]

#[derive(Debug, Clone)]
pub struct Stream<'a> {
    cmd: &'a Vec<String>,
    replication_conn: bool,
}

impl<'a> Stream<'a> {
    pub fn new(cmd: &'a Vec<String>, replication_conn: bool) -> Self {
        Self {cmd, replication_conn}
    }

    fn extract_timestamp(&self) -> Result<(u128, u64), String> {
        if let Some(stamp) = array::get_nth_arg(self.cmd, 2) {
            let ss = stamp.split('-').collect::<Vec<&str>>();
            let base: u128;
            let seq: u64;
            if let Ok(_base) = ss[0].parse::<u128>() {
                base = _base;
            } else {
                return Err("Invalid timestamp".to_string());
            }
            
            if let Ok(_seq) = ss[1].parse::<u64>() {
                seq = _seq;
            } else {
                return Err("Invalid Sequence number".to_string());
            }
            return Ok((base, seq));
        }
        Err("Insufficient number of arguements to XADD command".to_string())
    }

    fn build(&self) -> Result<streams::Streams, XADDErrors> {
        // XADD stream_key 1526919030474-0 temperature 36 humidity 95
        // split 1526919030474-0 (time stamp and seq-id)
        if let Ok((timestamp, seq)) = self.extract_timestamp() {
            // if the stream is empty - only then we reach here
            if timestamp == 0 { return Err(XADDErrors::TimeStampInvalid(timestamp)); }

            // gather everything else and build an vector of strings
            let kvpairs = self.cmd.iter()
                .skip(2)
                .fold(Vec::<String>::new(), |mut acc, s| {
                    acc.push(s.clone()); // create a copy for now
                    acc
                });

            return Ok(streams::Streams::new(timestamp, seq, kvpairs));
        }
        Err(XADDErrors::InvalidArgs)
    }

    fn validate_timetamp(&self, value: &streams::Streams) -> Result<(), XADDErrors> {
        if let Ok((in_tstamp, in_seq)) = self.extract_timestamp() {
            if in_tstamp == 0 { return Err(XADDErrors::TimeStampInvalid(timestamp)); }
            for (tstamp, seq) in value.streams.keys() {
                println!("incoming tstamp: {} vs db: {}, in seq: {} vs db {}", in_tstamp, tstamp, in_seq, seq);
                if in_tstamp < *tstamp ||
                    in_tstamp == *tstamp && in_seq <= *seq {
                        return Err(XADDErrors::TimeStampOlder(in_tstamp));
                }
            }
        }
        Ok(())
    }

}

impl<'a> incoming::CommandHandler for Stream<'a> {
    fn handle(&self, stream: &mut TcpStream, db: &Arc<db::DB>) -> std::io::Result<()> {
        let mut response = String::new();
        if let Some(skey) = array::get_nth_arg(self.cmd, 1) {
            if let Some(skey_id) = array::get_nth_arg(self.cmd, 2) {
                // check if key already exists
                let mut valid = true;
                if let Some(existing_key) = db.get(skey) {
                    match existing_key {
                        db::KeyValueType::StreamType(value) => {
                            // found one - lets validate the timestamp and seq
                            match self.validate_timetamp(&value) {
                                Ok(()) => {},
                                Err(e) => {
                                    valid = false;
                                    let _ = std::fmt::write(&mut response,
                                        format_args!("-{}", e));
                                }
                            };
                        },
                        _ => {
                            valid = false;
                            let _ = std::fmt::write(&mut response,
                                format_args!("-Invalid Command {}\r\n{}\r\n", skey_id.len(), skey_id));
                        }
                    }
                    // validate
                } else {
                    println!("---------------- existing key not found, adding new one --------------- ");
                }
                // save the key with value
                if valid {
                    match self.build() {
                        Ok(value) => {
                            let options = getset::SetOptions::new();
                            let db_result = db.add(skey.clone(), db::KeyValueType::StreamType(value), &options);
                            if db_result.is_err() {
                                println!("Error writing into the DB");
                                return Err(std::io::Error::new(std::io::ErrorKind::Other,
                                format!("failed set command: {:?}", self.cmd)));
                            }
                            let _ = std::fmt::write(&mut response,
                                format_args!("${}\r\n{}\r\n", skey_id.len(), skey_id));
                        },
                        Err(e) => {
                            let _ = std::fmt::write(&mut response,
                                format_args!("-{}\r\n", e));
                        }
                    }
                }
            }
        } else {
            let _ = std::fmt::write(&mut response,
                format_args!("-invalid command\r\n"));
        }
        stream.write_all(response.as_bytes())
    }

}
