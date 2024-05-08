use std::io::Read;

pub trait Parse {
    fn parse<T: Read>(reader: &mut T) -> Self;
}
