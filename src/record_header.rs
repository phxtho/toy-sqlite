#[allow(dead_code)]
#[derive(Clone)]
pub struct RecordHeader {
    // size of the row excluding the bytes to represent the header
    pub size: u64,
    pub row_id: u64,
}
