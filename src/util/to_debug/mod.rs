use std::fmt::Debug;

// Convenience trait to add to_debug() to a type which is std::fmt::Debug.
pub trait ToDebug: Debug {
    fn to_debug(&self) -> String {
        to_debug(&self)
    }
}

// Convenience bare function, useful for e.g., map() and map_err().
pub fn to_debug<T: Debug>(thing: T) -> String {
    format!("{:?}", thing)
}
