use wasm_bindgen::prelude::*;

mod uuid;

#[wasm_bindgen]
pub fn uuid() -> String {
    let mut numbers = [0u8; 36];
    make_random_numbers(&mut numbers);
    uuid::uuid_from_numbers(&numbers)
}

#[cfg(target_arch = "wasm32")]
fn make_random_numbers(number: &mut [u8]) {
    web_sys::window()
        .expect("window is not available")
        .crypto()
        .expect("window.crypto is not available")
        .get_random_values_with_u8_array(&mut numbers)
        .expect("window.crypto.getRandomValues not available");
}

#[cfg(not(target_arch = "wasm32"))]
fn make_random_numbers(numbers: &mut [u8]) {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    for v in numbers.iter_mut() {
        *v = rng.gen();
    }
}
