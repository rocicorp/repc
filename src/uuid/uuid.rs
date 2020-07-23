use std::char;

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

pub fn gen_uuid_with_xorshift(random_numbers: &[u8; 36]) -> String {
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
        let uuid = gen_uuid_with_xorshift(&[0u8; 36]);
        assert_eq!(uuid, "00000000-0000-4000-8000-000000000000");
        let re =
            Regex::new(r"^[0-9:A-z]{8}-[0-9:A-z]{4}-4[0-9:A-z]{3}-[0-9:A-z]{4}-[0-9:A-z]{12}$")
                .unwrap();

        assert!(re.is_match(&uuid));
    }
}
