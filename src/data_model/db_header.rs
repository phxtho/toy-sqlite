use std::io::Read;

use crate::data_model::serialisation::Deserialize;

pub struct Dbheader {
    pub page_size: u16,
}
pub const DB_HEADER_SIZE: usize = 100;

impl Deserialize for Dbheader {
    fn deserialize<T: Read>(reader: &mut T) -> Dbheader {
        let mut buf = [0; DB_HEADER_SIZE];
        reader
            .read_exact(&mut buf)
            .expect("failed to read Dbheader");
        Dbheader {
            page_size: u16::from_be_bytes([buf[16], buf[17]]),
        }
    }
}
