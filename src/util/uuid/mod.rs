use std::char;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(catch, js_name = getRandomValues, js_namespace = crypto)]
    fn get_random_values(arr: &mut [u8]) -> std::result::Result<(), JsValue>;
}

#[derive(Debug)]
pub enum UuidError {
    NoCryptoGetRandomValues(JsValue),
}

pub fn uuid() -> Result<String, UuidError> {
    let mut numbers = [0u8; 36];
    make_random_numbers(&mut numbers)?;
    Ok(uuid_from_numbers(&numbers))
}

#[cfg(target_arch = "wasm32")]
pub fn make_random_numbers(numbers: &mut [u8]) -> Result<(), UuidError> {
    get_random_values(numbers).map_err(UuidError::NoCryptoGetRandomValues)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn make_random_numbers(numbers: &mut [u8]) -> Result<(), UuidError> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    for v in numbers.iter_mut() {
        *v = rng.gen();
    }
    Ok(())
}

enum UuidElements {
    Random09AF,
    Random89AB,
    Hyphen,
    Version,
}

const UUID_V4_FORMAT: [UuidElements; 36] = [
    UuidElements::Random09AF,
    UuidElements::Random09AF,
    UuidElements::Random09AF,
    UuidElements::Random09AF,
    UuidElements::Random09AF,
    UuidElements::Random09AF,
    UuidElements::Random09AF,
    UuidElements::Random09AF,
    UuidElements::Hyphen,
    UuidElements::Random09AF,
    UuidElements::Random09AF,
    UuidElements::Random09AF,
    UuidElements::Random09AF,
    UuidElements::Hyphen,
    UuidElements::Version,
    UuidElements::Random09AF,
    UuidElements::Random09AF,
    UuidElements::Random09AF,
    UuidElements::Hyphen,
    UuidElements::Random89AB,
    UuidElements::Random09AF,
    UuidElements::Random09AF,
    UuidElements::Random09AF,
    UuidElements::Hyphen,
    UuidElements::Random09AF,
    UuidElements::Random09AF,
    UuidElements::Random09AF,
    UuidElements::Random09AF,
    UuidElements::Random09AF,
    UuidElements::Random09AF,
    UuidElements::Random09AF,
    UuidElements::Random09AF,
    UuidElements::Random09AF,
    UuidElements::Random09AF,
    UuidElements::Random09AF,
    UuidElements::Random09AF,
];

const ERROR_MAKE_CHAR: &str = "Error in making char";

pub fn uuid_from_numbers(random_numbers: &[u8; 36]) -> String {
    UUID_V4_FORMAT
        .iter()
        .enumerate()
        .map(|(i, kind)| match kind {
            UuidElements::Random09AF => {
                char::from_digit((random_numbers[i] & 0b1111) as u32, 16).expect(ERROR_MAKE_CHAR)
            }
            UuidElements::Random89AB => {
                char::from_digit((random_numbers[i] & 0b11) as u32 + 8, 16).expect(ERROR_MAKE_CHAR)
            }
            UuidElements::Version => '4',
            UuidElements::Hyphen => '-',
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use regex::Regex;

    #[test]
    fn test_uuid() {
        let uuid = uuid_from_numbers(&[0u8; 36]);
        assert_eq!(uuid, "00000000-0000-4000-8000-000000000000");
        let re =
            Regex::new(r"^[0-9:A-z]{8}-[0-9:A-z]{4}-4[0-9:A-z]{3}-[0-9:A-z]{4}-[0-9:A-z]{12}$")
                .unwrap();

        assert!(re.is_match(&uuid));
    }
}
