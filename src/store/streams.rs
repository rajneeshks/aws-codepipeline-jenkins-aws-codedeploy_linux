// maintain in memory DB for streams
use std::collections::BTreeMap;

#[allow(dead_code)]
/*
#[derive(Debug, Clone)]
struct StreamData {
    data: String,
} */

#[derive(Debug, Clone)]
pub struct Streams {
    pub streams: BTreeMap<(u128, u64), Vec<String>>,
}

impl Streams {
   pub fn new(timestamp: u128, seq: u64, kvpairs: Vec<String>) -> Self {
        let mut map = BTreeMap::new();
        map.insert((timestamp, seq), kvpairs);
        Self {
            streams: map,
        }
    }
}
