use crate::to_js::ToJsValue;
use http::header::ToStrError;
#[cfg(not(target_arch = "wasm32"))]
use std::string::FromUtf8Error;
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
impl ToJsValue for FetchError {
    fn to_js(&self) -> Option<&JsValue> {
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

#[derive(Debug)]
#[cfg(target_arch = "wasm32")]
pub enum FetchError {
    ErrorReadingResponseBodyJs(JsValue),
    ErrorReadingResponseBodyAsStringJs(JsValue),
    FailedToWrapHttpResponse(http::Error),
    FetchFailed(JsValue),
    InvalidRequestHeader(ToStrError),
    InvalidResponseFromJs(JsValue),
    RequestFailedJs(JsValue),
    RequestTimeout(async_std::future::TimeoutError),
    UnableToCreateRequest(JsValue),
    UnableToSetRequestHeader(JsValue),
}

#[cfg(target_arch = "wasm32")]
impl ToJsValue for FetchError {
    fn to_js(&self) -> Option<&JsValue> {
        match self {
            FetchError::ErrorReadingResponseBodyJs(v) => Some(v),
            FetchError::ErrorReadingResponseBodyAsStringJs(v) => Some(v),
            FetchError::FailedToWrapHttpResponse(_) => None,
            FetchError::FetchFailed(v) => Some(v),
            FetchError::InvalidRequestHeader(_) => None,
            FetchError::InvalidResponseFromJs(v) => Some(v),
            FetchError::RequestFailedJs(v) => Some(v),
            FetchError::RequestTimeout(_) => None,
            FetchError::UnableToCreateRequest(v) => Some(v),
            FetchError::UnableToSetRequestHeader(v) => Some(v),
        }
    }
}
