/// Set which maintains insertion order
pub struct Set<T: PartialOrd> {
    vec: Vec<T>,
}

impl<T: PartialOrd> Set<T> {
    pub fn new() -> Self {
        Self { vec: vec![] }
    }

    pub fn push(&mut self, idx: T) {
        if !self.vec.contains(&idx) {
            self.vec.push(idx);
        }
    }

    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.vec.iter()
    }
}

impl<T: PartialOrd> IntoIterator for Set<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.vec.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_and_iter() {
        let mut set = Set::new();

        // Insert some elements.
        set.push(3);
        set.push(1);
        set.push(2);
        // Attempt to push a duplicate.
        set.push(3);

        // Collect items from the iterator.
        let items: Vec<_> = set.iter().cloned().collect();
        // Check that the set maintains insertion order and does not duplicate elements.
        assert_eq!(items, vec![3, 1, 2]);
    }

    #[test]
    fn test_into_iter() {
        let mut set = Set::new();
        set.push('a');
        set.push('b');
        set.push('c');
        // Attempt to push a duplicate.
        set.push('a');

        // Consume the set into an iterator.
        let items: Vec<_> = set.into_iter().collect();
        // Verify that the consumed set retains insertion order and duplicates were not added.
        assert_eq!(items, vec!['a', 'b', 'c']);
    }
}
