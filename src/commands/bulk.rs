use crate::commands::incoming;
use crate::commands::rdbfile;


pub fn bulk_string_type_handler<'a>(
    cmd: &'a Vec<u8>,
    replication_conn: bool,
) -> Box<dyn incoming::CommandHandler + 'a> 
{
    Box::new(rdbfile::RDBFile::new(cmd, replication_conn))
}
