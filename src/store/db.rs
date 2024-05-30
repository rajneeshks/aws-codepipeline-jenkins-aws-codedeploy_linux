// maintain in memory DB
use crate::commands::getset;
use crate::rdb::rdb;
use crate::store::node_info;
use crate::store::streams;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::thread;
use std::time::{Duration, Instant};

#[allow(dead_code)]

#[derive(Debug, Clone)]
pub enum KeyValueType {
    StringType(String),
    StreamType(streams::Streams),
}

#[derive(Debug, Clone)]
struct KeyValueData {
    key: String,
    value: KeyValueType,
    expires: bool,
    expiring_at: Instant,
}

impl KeyValueData {
    fn new(key: String, value: KeyValueType, options: &getset::SetOptions) -> Self {
        let now = Instant::now();
        let mut expires = false;
        if options.expiry_in_ms > 0 {
            expires = true;
        }
        Self {
            key,
            value,
            expires,
            expiring_at: now + Duration::from_millis(options.expiry_in_ms),
        }
    }
}

struct DBInternal {
    db: HashMap<String, KeyValueData>,
}

impl DBInternal {
    fn new() -> Self {
        Self {
            db: HashMap::new(),
        }
    }

    // adds key into the store
    fn add(
        &mut self,
        key: String,
        value: KeyValueType,
        options: &getset::SetOptions,
    ) -> Result<(), String> {
        let v = KeyValueData::new(key.clone(), value.clone(), options);
        let _ = self
            .db
            .entry(key)
            .and_modify(|val| *val = v.clone())
            .or_insert(v);
        //TODO: return appropriately
        Ok(())
    }

    // adds entry into stream
    fn xadd(
        &mut self,
        key: String,
        value: KeyValueType,
        options: &getset::SetOptions,
        timestamp: u128,
        seq: u64,
        kvpairs: Vec<String>,
    ) -> Result<(), String> {
        let _ = self
            .db
            .entry(key.clone())
            .and_modify(|val| {
                match &mut val.value {
                    KeyValueType::StreamType(s) => {
                        // add the key into streams
                        s.streams.insert((timestamp, seq), kvpairs.clone());
                    },
                    KeyValueType::StringType(_s) => {
                        let v = KeyValueData::new(key.clone(), value.clone(), options);
                        *val = v.clone();
                    }
                }
            })
            .or_insert_with(||{
                let vv: streams::Streams = streams::Streams::new(timestamp, seq, kvpairs);
                let v = KeyValueData::new(key.clone(), KeyValueType::StreamType(vv.clone()), options);
                v
            });
        //TODO: return appropriately
        Ok(())
    }
}

pub struct DB {
    store: RwLock<DBInternal>,
    node_info: node_info::NodeInfo,
    rdb: rdb::RDB,
}

impl DB {
    pub fn new(role_master: bool, dir: Option<String>, db_filename: Option<String>) -> Self {
        let instance = Self {
            store: RwLock::new(DBInternal::new()),
            node_info: node_info::NodeInfo::new(role_master),
            rdb: rdb::RDB::new(dir, db_filename),
        };

        // if rdb DB file has been specified, read/load the DB
      if let Err(e) = instance.rdb.load_rdb(&instance) {
        println!("Error loading RDB for this slave device: {:?}", e);
      }
        instance
    }

    pub fn add(
        &self,
        key: String,
        value: KeyValueType,
        options: &getset::SetOptions,
    ) -> Result<(), String> {
        println!("Adding {key} with value: {:?}", value);
        let retval;
        {
            let mut db = self.store.write().unwrap();
            retval = db.add(key, value, options);
        }
        println!("DB at this stage: {:?}", self.store.read().unwrap().db);
        retval
    }

    pub fn xadd(
        &self,
        key: String,
        value: KeyValueType,
        options: &getset::SetOptions,
        timestamp: u128,
        seq: u64,
        kvpairs: Vec<String>,
    ) -> Result<(), String> {
        println!("Adding {key}/{timestamp}-{seq} with value: {:?}", kvpairs);
        let retval;
        {
            let mut db = self.store.write().unwrap();
            retval = db.xadd(key, value, options, timestamp, seq, kvpairs);
        }
        println!("DB at this stage: {:?}", self.store.read().unwrap().db);
        retval
    }

    pub fn remove(&self, key: &String) -> Option<KeyValueType> {
        if let Some(result) = self.store.write().unwrap().db.remove(key) {
            return Some(result.value);
        }
        None
    }

    pub fn get(&self, key: &str) -> Option<KeyValueType> {
        let mut value = None;
        {
            if let Some(result) = self.store.read().unwrap().db.get(key) {
                // clone so that we can release the lock
                value = Some(result.clone());
            }
        }
        if let Some(res) = value {
            // race condition - if this key is due for cleanup, check
            if res.expires && res.expiring_at < Instant::now() {
                self.remove(&res.key);
            } else {
                return Some(res.value.clone());
            }
        }
        None
    }

    pub fn role_master(&self) -> bool {
        self.node_info.master
    }

    pub fn rdb_directory(&self) -> &str {
        self.rdb.get_rdb_directory()
    }

    pub fn rdb_filename(&self) -> &str {
        self.rdb.get_rdb_filename()
    }

    pub fn keys(&self) -> (String, u64) {
        let mut response = String::from("");
        let mut count: u64 = 0;
        let db = self.store.read().unwrap();
        for (k, _v) in db.db.iter() {
            count += 1;
            let _ = std::fmt::write(&mut response,
                format_args!("${}\r\n{}\r\n", k.len(), k));
        }
        if count == 0 {
            return (format!("$-1\r\n"), count);
        }
        (format!("*{}\r\n{}", count, response), count)
    }

}

pub fn key_expiry_thread(db: Arc<DB>, loop_every_in_ms: u64) {
    let sleep_duration = Duration::from_millis(loop_every_in_ms);
    let mut expired_keys: Vec<String> = Vec::new();
    loop {
        let now = Instant::now();
        {
            let store = db.store.read().unwrap();
            for (key, value) in store.db.iter() {
                if value.expires && value.expiring_at < now {
                    expired_keys.push(key.clone());
                }
            }
        }

        /*
        ** go over expiring entries
        */
        for key in expired_keys.iter() {
            println!("----- timer thread: Removing {key} -------");
            db.remove(key);
        }
        expired_keys.clear();

        thread::sleep(sleep_duration);
    }
}
