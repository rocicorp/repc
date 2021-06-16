use wasm_bindgen::JsValue;

pub trait ToJsValue {
    fn to_js(&self) -> Option<&JsValue>;
}
