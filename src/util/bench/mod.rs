use async_std::future::Future;
use async_std::task;
use log::info;
use std::pin::Pin;
use std::time::Duration;

fn now_nanos() -> u64 {
    let window = web_sys::window().expect("should have a window in this context");
    let performance = window
        .performance()
        .expect("performance should be available");
    return (performance.now() * 1e6) as u64;
}

pub async fn snooze() {
    task::sleep(Duration::from_millis(42)).await;
}

pub async fn drive<Fut: Future<Output = ()>>(target: &dyn Fn() -> Fut) {
    for _ in 0u8..100 {
        let now = now_nanos();
        target().await;
        let elapsed = now_nanos() - now;
        // TODO: not sure how to see output -- info! compiles and runs but i don't know where it goes to under wasm-pack test
        panic!("took {:.3}s", elapsed as f64 / 1e9);
    }
}
