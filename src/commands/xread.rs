use crate::commands::incoming;
use crate::store::db;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;
use crate::commands::array;
use crate::store::streams;
use std::sync::RwLock;

#[derive(Debug, Clone)]
struct KeyOptions {
    key: String,
    timestamp: u128,
    seq: u64,
}

impl KeyOptions {
    fn new(key: String) -> Self {
        Self {
            key,
            timestamp: 0,
            seq: 0,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct XRead<'a> {
    cmd: &'a Vec<String>,
    replication_conn: bool,
    keys: RwLock<Vec<KeyOptions>>,
}

impl<'a> XRead<'a> {
    pub fn new(cmd: &'a Vec<String>, replication_conn: bool) -> Self {
        Self {
            cmd,
            replication_conn,
            keys: RwLock::new(vec![]),
        }
    }

    fn parse_options(&self) -> Result<(), String> {
        // XREAD streams stream_key 0-0
        // XREAD streams stream_key other_stream_key 0-0 0-1

        // get all the keys!
        let mut keys = self.keys.write().unwrap();
        let mut idx: usize = 2; // start of stream name
        let mut keyidx: usize = 0;
        loop {
            if let Some(v) = array::get_nth_arg(self.cmd, idx) {
                if v == "-" {  
                    keys[keyidx].timestamp = u128::MIN;
                    keyidx += 1;
                } else {
                    // see if this is complete key
                    if v.contains("-") {
                        let ss = v.split('-').collect::<Vec<&str>>();
                        if let Ok(_base) = ss[0].parse::<u128>() {
                            keys[keyidx].timestamp = _base;
                            keyidx += 1;
                        } else {
                            return Err("Invalid timestamp to xread command".to_string());
                        }
                        if ss.len() >= 2 {
                            if let Ok(_seq) = ss[1].parse::<u64>() {
                                keys[keyidx].seq = _seq;
                                keyidx += 1;
                            } else {
                                return Err("Invalid Sequence number to xread command (key)".to_string());
                            }
                        }
                    } else {
                        if let Ok(_base) = v.parse::<u128>() {
                            keys[keyidx].timestamp = _base;
                            keyidx += 1;
                        } else { // its a key
                            keys.push(KeyOptions::new(v.clone()));
                        }
                    }
                }
                idx += 1;
            } else {
                break;
            }
        }
        println!("After parsing options - keys and options: {:?}", keys);
        Ok(())
    }

    fn build_response(&self, stream: &streams::Streams, idx: usize) -> Result<String, String> {
        let keys = self.keys.read().unwrap();
        let (count, response) = stream.streams.iter()
            .filter(|((ts, seq), _value)| *ts > keys[idx].timestamp && *seq > keys[idx].seq)
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
        println!("key: {}, count: {count}, response: {response}", keys[idx].key);
        Ok(format!("*{}\r\n${}\r\n{}\r\n{}", count, keys[idx].key.len(), keys[idx].key, response))
    }
}

impl<'a> incoming::CommandHandler for XRead<'a> {
    fn handle(&self, stream: &mut TcpStream, db: &Arc<db::DB>) -> std::io::Result<()> {
        let mut response = String::new();
        if let Ok(_) = self.parse_options() {
            // check if key already exists
            let num_keys = self.keys.read().unwrap().len();
            let mut i_responses = Vec::with_capacity(num_keys);
            for i in 0..num_keys {
                if let Some(existing_key) = db.get(&self.keys.read().unwrap()[i].key) {
                    match existing_key {
                        db::KeyValueType::StreamType(value) => {
                            // found one - lets validate the timestamp and seq
                            match self.build_response(&value, i) {
                                Ok(res) => {
                                    println!("Response: {}", res);
                                    i_responses.push(res);
                                },
                                Err(e) => {
                                    return stream.write_all(format!("-Unable to put together response to xread: {}\r\n", e).as_bytes());
                                }
                            }
                        },
                        db::KeyValueType::StringType(_) => {
                            return stream.write_all(b"-Wrong type of key for xread command\r\n");
                        }
                    };
                } else {
                    return stream.write_all(b"-invalid stream key - does not exist (xread command)\r\n");
                }
            }
            let _ = std::fmt::write(&mut response, format_args!("*{}\r\n", num_keys));
            for i in 0..num_keys {
                let _ = std::fmt::write(&mut response,
                    format_args!("{}", i_responses[i]));
            }
        } else {
            let _ = std::fmt::write(&mut response,
                format_args!("-invalid command\r\n"));
        }
        stream.write_all(response.as_bytes())
    }
}
