mod errors;
mod logger;
#[cfg_attr(target_arch = "wasm32", path = "browser_timer.rs")]
#[cfg_attr(not(target_arch = "wasm32"), path = "rust_timer.rs")]
mod timer;

pub use errors::TimerError;
pub use logger::log;
pub use logger::Logger;
pub use timer::Timer;
