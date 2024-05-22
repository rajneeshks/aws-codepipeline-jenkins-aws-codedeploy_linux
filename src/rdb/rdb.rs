use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use crate::store::db;
use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use crate::commands::getset;

enum RDBDataType {

}
pub struct RDB {
    directory: String,
    rdb_file: String,
}

impl RDB {
    pub fn new(dir: Option<String>, rdb_file: Option<String>) -> Self {
        Self {
            directory: dir.unwrap_or("".to_string()),
            rdb_file: rdb_file.unwrap_or("".to_string()),
        }
    }

    pub fn get_rdb_directory(&self) -> &str {
        &self.directory
    }

    pub fn get_rdb_filename(&self) -> &str {
        &self.rdb_file
    }

    fn checksum(&self) -> bool {
        // open file, read entire file and compute checksum
        // compare with checksum which is the last byte in the file
        true
    }
/*
    fn int_value(len: usize, value: &Vec<u8>) -> usize {
        match len {
            1 => value[0] as usize,
            2 => u16::from_ne_bytes(value[0..=1]) as usize,
            4 => u32::from_ne_bytes(value[0..=3]) as usize,
            _ => 0,
        }
    }
*/

    fn read_key_value(opcode: Option<u8>, reader: &mut BufReader<File>) -> Result<(Vec<u8>, Vec<u8>),  String> {
        if let Ok(length) = Self::encoded_length(opcode, reader) {
            let mut key = vec!(0; length);
            if let Err(e) = reader.read_exact(&mut key) {
                return Err(format!("Failed to read Key from RDB file, error: {}", e));
            }
            if let Ok(val_len) = Self::encoded_length(None, reader) {
                let mut value = vec!(0; val_len);
                if let Err(e) = reader.read_exact(&mut value) {
                    return Err(format!("Failed to read value for key {} from RDB file, error: {}",
                        String::from_utf8_lossy(&key), e));
                }
                println!("read_key_value, key: {:?}, value: {:?}",
                    String::from_utf8(key.clone()).unwrap_or("unable to unwrap key".to_string()),
                    String::from_utf8(value.clone()).unwrap_or("unwrap error value".to_string()));
                return Ok((key, value));
            }
        }
        Err("invalid string - its all messed up!!".to_string())
    }

    fn read_key_value_with_type(vtype: Option<u8>, reader: &mut BufReader<File>) -> Result<(Vec<u8>, Vec<u8>),  String> {
        // read 1 byte value type
        let opcode: u8;
        if vtype.is_none() {
            let mut value_type = [0; 1];
            let result = reader.read_exact(&mut value_type);
            if result.is_err() {
                return Err(format!("Unable to read the value type: {:?}", result))
            }
            opcode = value_type[0];
        } else {
            opcode = vtype.unwrap();
        }

        match opcode {
            0 => {// string encoding
                // read length, and then key
                return Self::read_key_value(None, reader);
            },
            _ => {
                Err(format!("Value type: {} is not yet supported!", opcode))
            }
        }
    }

    fn encoded_length(opcode_option: Option<u8>, reader: &mut BufReader<File>) -> Result<usize,  String> {
        let mut opcode = [0; 1];
        if opcode_option.is_some() {
            opcode[0] = opcode_option.unwrap();
        } else {
            let result = reader.read_exact(&mut opcode);
            if result.is_err() {
                return Err(format!("Unable to read the opcode: {:?}", result))
            }
        }

        match (opcode[0] & 0xC0) >> 6 {
            0x0 => Ok(opcode[0] as usize & 0x3F),
            0x1 => {
                // read one more byte
                let mut length = [0; 1];
                if let Ok(_) = reader.read_exact(&mut length) {
                    return Err("Unable to read next byte from stream!".to_string());
                }
                return Ok(((opcode[0] as usize & 0x3F) << 8) | length[0] as usize);
            },
            0x2 => {
                let mut length = [0; 4];
                if let Ok(_) = reader.read_exact(&mut length) {
                    return Err("Unable to read next byte from stream!".to_string());
                }
                return Ok(u32::from_ne_bytes(length) as usize);
            },
            0x3 => {
                match opcode[0] & 0x3F {
                    0 => {
                        Ok(1)
                    },
                    1 => { // 2 bytes integer
                        Ok(2)
                    },
                    2 => {  // 4 byte integers
                        Ok(4)
                    },
                    _ => {
                        return Err("Invalid integer encoding!".to_string());
                    }
                }
            },
            _ => Err(format!("Invaid encoding!!! {}", opcode[0])),
        }

        //Err(format!("Invaid encoding!!! {}", byte))
    }

    pub fn load_rdb(&self, db: &db::DB) -> std::io::Result<()> {
        if self.directory.len() == 0 || self.rdb_file.len() == 0 {
            println!("RDB prameters are invalid - so can not parse!!");
            return Ok(());
        }

        if !self.checksum() {
            println!("invalid checksum.........");
            return Err(std::io::Error::new(std::io::ErrorKind::Other,
                format!("RDB checksum does not match!!!")));
        }
        let filename = format!("{}/{}", self.directory, self.rdb_file);
        println!("reading file: {}", &filename);
        // directory and filename are specified.
        let f1 = File::open(filename)?;
        let mut reader = BufReader::new(f1);

        // check the signature of this file
        let mut signature = [0; 9];
        reader.read_exact(&mut signature)?;
        print!("Signature masmatch: ");
        for i in 0..5 {
            print!("{} ", signature[i]);
        }
        println!();
        // if we need to check a specific signature
        //return Err(std::io::Error::new(std::io::ErrorKind::Other,
        //    format!("RDB signature does not match!!!")));

        println!("REDIS version: {}", String::from_utf8(signature[5..].to_vec()).unwrap());
        let mut db_num = [0; 1];
        loop {
            //read next byte to interpret what type it is?
            let mut opcode = [0; 1];
            reader.read_exact(&mut opcode)?;
            match opcode[0] {
                0xFA => {
                    if let Ok((key, value)) = Self::read_key_value(None, &mut reader) {
                        println!("0xFA block, key: {:?}, value: {:?}",
                            String::from_utf8_lossy(&key),
                            String::from_utf8_lossy(&value));
                    }
                },
                0xFB => {
                    // read length encoded int
                    if let Ok(len1) = Self::encoded_length(None, &mut reader) {
                        println!("Found first integer of length: {}", len1);
                        let mut value1 = vec![0; len1];
                        reader.read_exact(&mut value1)?;
                        let hash_table_sz = match len1 {
                            1 => value1[0] as usize,
                            2 => 110, //u16::from_ne_bytes(value1) as usize,
                            4 => 220, //u32::from_ne_bytes(value1) as usize,
                            _ => 0,
                        };
                        if let Ok(len2) = Self::encoded_length(None, &mut reader) {
                            println!("Found second integer of length: {}", len2);
                            let mut value2 = vec![0; len2];
                            reader.read_exact(&mut value2)?;
                            let expire_hash_tbl_sz = match len2 {
                                1 => value2[0] as usize,
                                2 => 110, //u16::from_ne_bytes(value2) as usize,
                                4 => 220, //u32::from_ne_bytes(value2) as usize,
                                _ => 0,
                            };
                            println!("0xFB block, value1: {:?}, value2: {:?}", hash_table_sz, expire_hash_tbl_sz);
                        }
                    }
                },
                0xFC => {
                    println!("FC Block - please decode me!");
                    let mut expiry_time_ms_db = [0; 8];
                    reader.read_exact(&mut expiry_time_ms_db)?;
                    let expiry_time_ms = u64::from_ne_bytes(expiry_time_ms_db);
                    if let Ok((key, value)) = Self::read_key_value_with_type(None, &mut reader) {
                        println!("0xFC block - key: {}, value: {}, expiry: {} ms to be added in DB",
                        String::from_utf8_lossy(&key), String::from_utf8_lossy(&value), expiry_time_ms);
                    }
                },
                0xFD => {
                    println!("FD Block - please decode me!");
                    let mut expiry_time_sec_db = [0; 4];
                    reader.read_exact(&mut expiry_time_sec_db)?;
                    let expiry_time_sec = u32::from_ne_bytes(expiry_time_sec_db) as usize;
                    if let Ok((key, value)) = Self::read_key_value_with_type(None, &mut reader) {
                        println!("0xFD block - key: {}, value: {} with expiry {} sec to be added in DB",
                        String::from_utf8_lossy(&key), String::from_utf8_lossy(&value), expiry_time_sec);
                    }
                },
                0xFE => {
                    reader.read_exact(&mut db_num)?;
                    println!("0XFE block - DB Number: {}", db_num[0]);
                },
                0xFF => {
                    println!("0xFE block - received EOF opcode!!!");
                    break;
                },
                _ => {
                    println!("Invalid Opcode found, lets try reading the key and value pair: {}", opcode[0]);
                    if let Ok((k, v)) = Self::read_key_value(Some(opcode[0]), &mut reader) {
                        let key = String::from_utf8_lossy(&k);
                        let value = String::from_utf8_lossy(&v);
                        let options = getset::SetOptions::new();
                        if let Err(e) = db.add(key.into_owned(), value.into_owned(), &options) {
                            println!("Error adding key to the DB");
                        }
                    }
                    //return Err(std::io::Error::new(std::io::ErrorKind::Other,
                    //    format!("Invalid RDB Opcode found: {}", opcode[0])));
                }
            }
        }

        Ok(())
    }

    pub fn empty() -> Result<Vec<u8>, base64::DecodeError> {
        let mut buffer = vec![];
        let empty_rdb = BASE64_STANDARD.decode(b"UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")?;
        buffer.extend_from_slice(format!("${}\r\n", empty_rdb.len()).as_bytes());
        buffer.extend_from_slice(&empty_rdb);
        Ok(buffer)
    }
}
