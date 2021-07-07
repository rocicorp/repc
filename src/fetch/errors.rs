#[cfg(not(target_arch = "wasm32"))]
use crate::to_native::ToNativeValue;
#[cfg(not(target_arch = "wasm32"))]
use http::header::ToStrError;
#[cfg(not(target_arch = "wasm32"))]
use std::string::FromUtf8Error;
#[cfg(not(target_arch = "wasm32"))]
use wasm_bindgen::JsValue;

// FetchErrors are returned by both the rust and browser versions of fetch. Since
// lower level errors in each case will be coming from two different places,
// FetchError is lossy of the error types underneath: it holds an error string.
#[derive(Debug)]
#[cfg(not(target_arch = "wasm32"))]
pub enum FetchError {
    ErrorReadingResponseBody(String),
    ErrorReadingResponseBodyAsString(FromUtf8Error),
    FailedToWrapHttpResponse(http::Error),
    InvalidRequestBody(http::Error),
    InvalidRequestHeader(ToStrError),
    RequestFailed(String),
    RequestTimeout(async_std::future::TimeoutError),
}

#[cfg(not(target_arch = "wasm32"))]
impl ToNativeValue<JsValue> for FetchError {
    fn to_native(&self) -> Option<&JsValue> {
        match self {
            FetchError::ErrorReadingResponseBody(_) => None,
            FetchError::ErrorReadingResponseBodyAsString(_) => None,
            FetchError::FailedToWrapHttpResponse(_) => None,
            FetchError::InvalidRequestBody(_) => None,
            FetchError::InvalidRequestHeader(_) => None,
            FetchError::RequestFailed(_) => None,
            FetchError::RequestTimeout(_) => None,
        }
    }
}
