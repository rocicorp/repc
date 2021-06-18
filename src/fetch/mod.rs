#[cfg(not(target_arch = "wasm32"))]
const DEFAULT_FETCH_TIMEOUT_SECS: u64 = 10;

#[cfg(not(target_arch = "wasm32"))]
#[cfg_attr(not(target_arch = "wasm32"), path = "rust_client.rs")]
pub mod client;

pub mod errors;
mod timeout;
