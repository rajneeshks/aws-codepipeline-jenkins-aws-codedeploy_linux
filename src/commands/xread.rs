use crate::commands::incoming;
use crate::store::db;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;
use crate::commands::array;
use crate::store::streams;
use std::sync::RwLock;
use std::time::{Instant, Duration};
use std::thread;

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
    block: RwLock<Option<u64>>, // block for how long in seconds
}

impl<'a> XRead<'a> {
    pub fn new(cmd: &'a Vec<String>, replication_conn: bool) -> Self {
        Self {
            cmd,
            replication_conn,
            keys: RwLock::new(vec![]),
            block: RwLock::new(None),
        }
    }

    fn parse_options(&self) -> Result<(), String> {
        // XREAD streams stream_key 0-0
        // XREAD streams stream_key other_stream_key 0-0 0-1
        // XREAD block 1000 streams some_key 1526985054069-0

        // get all the keys!
        let mut keys = self.keys.write().unwrap();
        let mut idx: usize = 1; // start of block or stream name
        let mut keyidx: usize = 0;
        let mut block = self.block.write().unwrap();
        loop {
            if let Some(v) = array::get_nth_arg(self.cmd, idx) {
                match v.as_str() {
                    "streams" => {
                        // do nothing for now - ideally should validate
                    },
                    "block" => {
                        // read blocking interval
                        match array::get_nth_arg(self.cmd, idx+1) {
                            Some(b) => {
                                if let Ok(bi) = b.parse::<u64>() {
                                    *block = Some(bi);
                                } else {
                                    return Err("Invalid blocking interval number to xread command (key)".to_string());
                                }
                                idx += 1;
                            },
                            None => {
                                // invalid - return error
                                return Err("No time interval provided to blocking xread command".to_string());
                            }
                        }
                    },
                    _ => {
                        if v == "-" {  
                            keys[keyidx].timestamp = u128::MIN;
                            keyidx += 1;
                        } else {
                            // see if this is complete key
                            if v.contains("-") {
                                let ss = v.split('-').collect::<Vec<&str>>();
                                if let Ok(_base) = ss[0].parse::<u128>() {
                                    keys[keyidx].timestamp = _base;
                                } else {
                                    return Err("Invalid timestamp to xread command".to_string());
                                }
                                if ss.len() >= 2 {
                                    if let Ok(_seq) = ss[1].parse::<u64>() {
                                        keys[keyidx].seq = _seq;
                                    } else {
                                        return Err("Invalid Sequence number to xread command (key)".to_string());
                                    }
                                }
                                keyidx += 1;
                            } else {
                                if let Ok(_base) = v.parse::<u128>() {
                                    keys[keyidx].timestamp = _base;
                                    keyidx += 1;
                                } else { // its a key
                                    keys.push(KeyOptions::new(v.clone()));
                                }
                            }
                        }
                    }
                }
                idx += 1;
            } else {
                break;
            }
        }
        Ok(())
    }

    fn build_response(&self, stream: &streams::Streams, idx: usize) -> Result<String, String> {
        let keys = self.keys.read().unwrap();
        build_response_internal(stream, &keys[idx].key, keys[idx].timestamp, keys[idx].seq)
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
                let k = self.keys.read().unwrap()[i].key.clone();
                if let Some(existing_key) = db.get(&k) {
                    match existing_key {
                        db::KeyValueType::StreamType(value) => {
                            // found one - lets validate the timestamp and seq
                            match self.build_response(&value, i) {
                                Ok(res) => {
                                    i_responses.push(res);
                                },
                                Err(_e) => { // possibly blocking read command
                                    let block = *self.block.read().unwrap();
                                    if let Some(interval) = block {
                                        let db_clone = Arc::clone(&db);
                                        let stream_cloned: TcpStream;
                                        match stream.try_clone() {
                                            Ok(c) => stream_cloned = c,
                                            Err(e) => {
                                                return stream.write_all(format!("-Unable to clone stream for xread block: {}\r\n", e).as_bytes());
                                            }
                                        };                                        
                                        let timestamp = self.keys.read().unwrap()[i].timestamp;
                                        let cmd_seq = self.keys.read().unwrap()[i].seq;
                                        let _ = thread::spawn(move || 
                                            blocking_xread_thread(db_clone, k, stream_cloned, interval,
                                                timestamp, cmd_seq));
                                        // thread will handle the response - this CLI is returning
                                        return Ok(());
                                    };
                                    return stream.write_all(b"-1\r\n");
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


// build_response_internal
//
// puts together response to xread command
fn build_response_internal(stream: &streams::Streams, key: &str, timestamp: u128, cmd_seq: u64) -> Result<String, String> {
    let (count, response) = stream.streams.iter()
        .filter(|((ts, seq), _value)| (*ts == timestamp && *seq > cmd_seq) || *ts > timestamp)
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
    // no entries found
    if count == 0 {
        return Err("$-1\r\n".to_string());
    }
    Ok(format!("*{}\r\n${}\r\n{}\r\n*{}\r\n{}", count+1, key.len(), key, count, response))
}

// blocking_read_thread
//
// thread that blocks until given timeout 
pub fn blocking_xread_thread(db: Arc<db::DB>, key: String, mut stream: TcpStream, timeout: u64, timestamp: u128, cmd_seq: u64) {
    // TODO - make it channel receiver - publisher sends a message when a new key is added
    let sleep_duration = Duration::from_millis(100); // check every 100 milliseconds
    let wait_time = Duration::from_millis(timeout);
    let now = Instant::now();
    let mut responded = false;

    println!("Im in blocking xread thread with timeout: {timeout} milli-seconds");

    while now.elapsed() < wait_time || timeout == 0 {
        thread::sleep(sleep_duration);

        // check DB if additional entries are added
        if let Some(existing_key) = db.get(&key) {
            match existing_key {
                db::KeyValueType::StreamType(value) => {
                    // found one - lets validate the timestamp and seq
                    match build_response_internal(&value, &key, timestamp, cmd_seq) {
                        Ok(res) => {
                            println!("block thread responding with: {}", res);
                            let _ = stream.write_all(res.as_bytes());
                            responded = true;
                        },
                        Err(_e) => {},
                    };
                },
                _ => {},
            };
        };
    }

    if !responded {
       let _ = stream.write_all(b"$-1\r\n");
    }
    //println!("xread blocking thread exiting........");
}