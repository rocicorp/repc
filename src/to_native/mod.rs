pub trait ToNativeValue<T> {
    fn to_native(&self) -> Option<&T>;
}
