use crate::kv::idbstore::IdbStore;
use crate::kv::Store;
use rand::Rng;
use wasm_bench::*;

fn rand_bytes(len: usize) -> Vec<u8> {
    (0..len).map(|_| rand::random::<u8>()).collect()
}

fn rand_string(len: usize) -> String {
    let mut rng = rand::thread_rng();
    std::iter::repeat(())
        .map(|_| rng.sample(rand::distributions::Alphanumeric))
        .take(len)
        .collect()
}

#[wasm_bench]
async fn read1x256(b: &mut Bench) {
    read1x(b, 256).await
}

#[wasm_bench]
async fn read1x1024(b: &mut Bench) {
    read1x(b, 1 * 1024).await
}

#[wasm_bench]
async fn read1x4096(b: &mut Bench) {
    read1x(b, 4 * 1024).await
}

#[wasm_bench]
async fn read1x16384(b: &mut Bench) {
    read1x(b, 16 * 1024).await
}

#[wasm_bench]
async fn read1x65536(b: &mut Bench) {
    read1x(b, 64 * 1024).await
}

async fn read1x(b: &mut Bench, size: u64) {
    let store = IdbStore::new(&rand_string(12)[..]).await.unwrap().unwrap();
    let key = rand_string(12);
    let wt = store.write().await.unwrap();
    wt.put(&key, &rand_bytes(size as usize)).await.unwrap();
    wt.commit().await.unwrap();

    let rt = store.read().await.unwrap();
    b.bytes = size;
    let n = b.iterations();
    b.reset_timer();
    for _ in 0..n {
        rt.get(&key).await.unwrap();
    }
}

#[wasm_bench]
async fn write1x256(b: &mut Bench) {
    write(b, 1, 256).await
}

#[wasm_bench]
async fn write1x4096(b: &mut Bench) {
    write(b, 1, 4 * 1024).await
}

#[wasm_bench]
async fn write4x4096(b: &mut Bench) {
    write(b, 4, 4 * 1024).await
}

#[wasm_bench]
async fn write16x4096(b: &mut Bench) {
    write(b, 16, 4 * 1024).await
}

#[wasm_bench]
async fn write1x65536(b: &mut Bench) {
    write(b, 1, 64 * 1024).await
}

async fn write(b: &mut Bench, writes: u32, size: u64) {
    let store = IdbStore::new(&rand_string(12)[..]).await.unwrap().unwrap();
    let key = rand_string(12);

    b.bytes = writes as u64 * size;
    let n = b.iterations();
    b.reset_timer();
    for _ in 0..n {
        let wt = store.write().await.unwrap();
        for _ in 0..writes {
            wt.put(&key, &rand_bytes(size as usize)).await.unwrap();
        }
        wt.commit().await.unwrap();
    }
}
