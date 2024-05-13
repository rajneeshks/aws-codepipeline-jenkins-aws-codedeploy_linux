// maintain in memory DB

use crate::commands::getset;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Mutex;
use std::time::Duration;

type KeyValueType = String;

struct DBInternal {
    db: HashMap<String, String>,
    expiry: HashSet<(String, Duration)>,
}

impl DBInternal {
    fn new() -> Self {
        Self {
            db: HashMap::new(),
            expiry: HashSet::new(),
            // start expiry monitor thread
        }
    }
}

pub struct DB {
    store: Mutex<DBInternal>,
}

impl DB {
    pub fn new() -> Self {
        Self {
            store: Mutex::new(DBInternal::new()),
        }
    }

    pub fn add(
        &self,
        key: KeyValueType,
        value: String,
        _options: &getset::SetOptions,
    ) -> Result<(), String> {
        // TODO: convert into result type appropriately
        let _ = self
            .store
            .lock()
            .unwrap()
            .db
            .entry(key)
            .and_modify(|val| *val = value.clone())
            .or_insert(value);
        Ok(())
    }

    pub fn remove(&mut self, key: &KeyValueType) -> Option<KeyValueType> {
        self.store.lock().unwrap().db.remove(key)
    }

    pub fn get(&self, key: &String) -> Option<KeyValueType> {
        let store = self.store.lock().unwrap();
        let result = store.db.get(key);

        if result.is_some() {
            return Some(result.unwrap().clone());
        }
        None
    }
}
