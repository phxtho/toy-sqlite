use crate::serialisation::{deserialize::Deserialize, varint::read_varint};

pub struct TableInteriorCell {
    pub left_child: u32, // page number of the left subtree
    pub row_id: u64,     // all keys in the subtree are less than this key
}

impl Deserialize for TableInteriorCell {
    fn deserialize<T: std::io::Read>(reader: &mut T) -> Self {
        let mut buf: [u8; 4] = [0; 4];
        reader
            .read(&mut buf)
            .expect("Failed to read 4 bytes for left child pointer");
        let left_child = u32::from_be_bytes(buf);
        let (row_id, _) = read_varint(reader);

        TableInteriorCell { left_child, row_id }
    }
}
