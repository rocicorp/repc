pub mod wasm;

extern crate async_std;
#[macro_use]
extern crate lazy_static;
extern crate log;

mod dag;
mod dispatch;
mod hash;
pub mod util;

#[cfg(not(default))]
pub mod kv;

#[cfg(default)]
mod kv;

mod prolly;
