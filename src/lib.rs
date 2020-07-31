pub mod wasm;

extern crate async_std;
#[macro_use]
extern crate lazy_static;
extern crate log;

mod dag;
mod db;
mod embed;
mod hash;
pub mod experimental;

#[cfg(not(default))]
pub mod kv;

#[cfg(default)]
mod kv;

mod prolly;

#[cfg(feature = "benchmark")]
pub mod benches;
