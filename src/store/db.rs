// maintain in memory DB

use crate::commands::getset;
use crate::store::node_info;
use std::collections::HashMap;
use std::collections::LinkedList;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

type KeyValueType = String;

#[derive(Debug, Clone)]
struct KeyValueData {
    key: String,
    value: String,
    expires: bool,
    inserted_at: Instant, // time stamp when this key was inserted
    expiring_at: Instant,
}

impl KeyValueData {
    fn new(key: String, value: String, options: &getset::SetOptions) -> Self {
        let now = Instant::now();
        let mut expires = false;
        if options.expiry_in_ms > 0 {
            expires = true;
        }
        Self {
            key,
            value,
            expires,
            inserted_at: now,
            expiring_at: now + Duration::from_millis(options.expiry_in_ms),
        }
    }
}

struct DBInternal {
    db: HashMap<String, KeyValueData>,
    expiry: LinkedList<KeyValueData>,
}

impl DBInternal {
    fn new() -> Self {
        Self {
            db: HashMap::new(),
            expiry: LinkedList::new(),
        }
    }

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
}

pub struct DB {
    store: Mutex<DBInternal>,
    node_info: node_info::NodeInfo,
}

impl DB {
    pub fn new(role_master: bool) -> Self {
        Self {
            store: Mutex::new(DBInternal::new()),
            node_info: node_info::NodeInfo::new(role_master),
        }
    }

    pub fn add(
        &self,
        key: KeyValueType,
        value: String,
        options: &getset::SetOptions,
    ) -> Result<(), String> {
        // TODO: convert into result type appropriately
        println!("Adding {key} with value: {value}");
        let mut retval = Ok(());
        {
            let mut db = self.store.lock().unwrap();
            retval = db.add(key, value, options);
        }
        println!("DB at this stage: {:?}", self.store.lock().unwrap().db);
        retval
    }

    pub fn remove(&self, key: &KeyValueType) -> Option<KeyValueType> {
        if let Some(result) = self.store.lock().unwrap().db.remove(key) {
            return Some(result.value);
        }
        None
    }

    pub fn get(&self, key: &String) -> Option<KeyValueType> {
        let mut value = None;
        {
            if let Some(result) = self.store.lock().unwrap().db.get(key) {
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
}

pub fn key_expiry_thread(db: Arc<DB>, loop_every_in_ms: u64) {
    let sleep_duration = Duration::from_millis(loop_every_in_ms);
    let mut expired_keys: Vec<String> = Vec::new();
    loop {
        let now = Instant::now();
        {
            let store = db.store.lock().unwrap();
            for (key, value) in store.db.iter() {
                if value.expires && value.expiring_at < now {
                    expired_keys.push(key.clone());
                }
            }
        }

        // go over expiring entries
        for key in expired_keys.iter() {
            println!("----- timer thread: Removing {key} -------");
            db.remove(key);
        }
        expired_keys.clear();

        thread::sleep(sleep_duration);
    }
}