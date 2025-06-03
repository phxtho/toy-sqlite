use std::io::Read;

pub trait Deserialize {
    fn deserialize<T: Read>(reader: &mut T) -> Self;
}
