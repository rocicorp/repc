use std::convert::TryFrom;
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;
use web_sys::{RequestInit, RequestMode};

// s makes map_err calls nicer by mapping a error to its debug-printed string.
fn s<D: std::fmt::Debug>(err: D) -> String {
    format!("{:?}", err)
}

// rust_fetch fetches using a rust (as opposed to browser Fetch) http client.
// It consumes the input request (it needs to own the request body). The response
// returned will have status and body filled (doesn't do headers at this point).
// Non-200 status code does not constitute an Err Result.
//
// TODO TLS
// TODO timeout/abort
// TODO log req/resp
// TODO understand what if any tokio runtime assumptions are implied here
pub async fn rust_fetch(
    hyper_client: &hyper::Client<hyper::client::HttpConnector>,
    http_req: http::Request<String>,
) -> Result<http::Response<String>, FetchError> {
    let (parts, req_body) = http_req.into_parts();
    let mut builder = hyper::Request::builder()
        .method(parts.method.as_str())
        .uri(&parts.uri.to_string());
    for (k, v) in parts.headers.iter() {
        builder = builder.header(
            k,
            v.to_str()
                .map_err(|e| FetchError::InvalidRequestHeader(s(e)))?,
        );
    }
    let hyper_req = builder
        .body(hyper::Body::from(req_body))
        .map_err(|e| FetchError::InvalidRequestBody(s(e)))?;

    let mut hyper_resp = hyper_client
        .request(hyper_req)
        .await
        .map_err(|e| FetchError::RequestFailed(s(e)))?;
    let http_resp_builder = http::response::Builder::new();
    let http_resp_bytes = hyper::body::to_bytes(hyper_resp.body_mut())
        .await
        .map_err(|e| FetchError::ErrorReadingResponseBody(s(e)))?;
    let http_resp_string =
        String::from_utf8(http_resp_bytes.to_vec()) // Copies :(
            .map_err(|e| FetchError::ErrorReadingResponseBodyAsString(s(e)))?;
    let http_resp = http_resp_builder
        .status(hyper_resp.status())
        .body(http_resp_string)
        .map_err(|e| FetchError::FailedToWrapHttpResponse(s(e)))?;
    Ok(http_resp)
}

// js makes browser_fetch map_err calls nicer by converting opaque JsValue errors
// into js_sys::Error's and debug-printing their content.
fn js(err: JsValue) -> String {
    match js_sys::Error::try_from(err) {
        Ok(e) => s(e),
        Err(_) => "unknown JS error: could not conver to js_sys::Error".to_string(),
    }
}

// browser_fetch makes and HTTP request over the network via the browser's Fetch API.
// It is intended to be used in wasm; it won't work in regular rust. The response contains
// the status and body, it doesn't populate response headers (though it easily could).
// Since we can't run a web server in wasm land and this fn doesn't work in rust land
// this fn is tested manually in tests/wasm.rs.
//
// Note: consumes the input request.
//
// TODO timeout/abort
// TODO log request/response
pub async fn browser_fetch(
    http_req: http::Request<String>,
) -> Result<http::Response<String>, FetchError> {
    let mut opts = RequestInit::new();
    opts.method(http_req.method().as_str());
    opts.mode(RequestMode::Cors);
    let js_body = JsValue::from_str(http_req.body());
    opts.body(Some(&js_body));
    let web_sys_req = web_sys::Request::new_with_str_and_init(&http_req.uri().to_string(), &opts)
        .map_err(|e| FetchError::UnableToCreateRequest(js(e)))?;
    let h = web_sys_req.headers();
    for (k, v) in http_req.headers().iter() {
        h.set(
            k.as_ref(),
            v.to_str()
                .map_err(|e| FetchError::InvalidRequestHeader(s(e)))?,
        )
        .map_err(|e| FetchError::UnableToSetRequestHeader(s(e)))?;
    }

    let window = web_sys::window().ok_or_else(|| FetchError::NoWindow)?;
    let http_req_promise = window.fetch_with_request(&web_sys_req);
    let http_req_future = JsFuture::from(http_req_promise);
    let js_web_sys_resp = http_req_future
        .await
        .map_err(|e| FetchError::RequestFailed(js(e)))?;
    if !js_web_sys_resp.is_instance_of::<web_sys::Response>() {
        return Err(FetchError::InvalidResponseFromJS);
    }
    let web_sys_resp: web_sys::Response = js_web_sys_resp.dyn_into().unwrap();
    let body_js_value = JsFuture::from(
        web_sys_resp
            .text()
            .map_err(|e| FetchError::ErrorReadingResponseBodyAsString(js(e)))?,
    )
    .await
    .map_err(|e| FetchError::ErrorReadingResponseBody(js(e)))?;
    let resp_body = body_js_value
        .as_string()
        .ok_or_else(|| FetchError::ErrorReadingResponseBodyAsString("".to_string()))?;

    let builder = http::response::Builder::new();
    let http_resp = builder
        .status(web_sys_resp.status())
        .body(resp_body)
        .map_err(|e| FetchError::FailedToWrapHttpResponse(s(e)))?;
    Ok(http_resp)
}

// FetchErrors are returned by both the rust and browser versions of fetch. Since
// lower level errors in each case will be coming from two different places,
// FetchError is lossy of the error types underneath: it holds an error string.
#[derive(Debug, PartialEq)]
pub enum FetchError {
    ErrorReadingResponseBodyAsString(String),
    ErrorReadingResponseBody(String),
    FailedToWrapHttpResponse(String),
    InvalidRequestBody(String),
    InvalidRequestHeader(String),
    InvalidResponseFromJS,
    NoWindow,
    RequestFailed(String),
    UnableToCreateRequest(String),
    UnableToSetRequestHeader(String),
}

// Note: browser_fetch manually tested in tests/wasm.rs.
#[cfg(test)]
#[cfg(default)]
mod tests {
    use super::*;
    use httptest::{matchers::*, Expectation, Server};

    #[tokio::test]
    async fn test_rust_fetch() {
        let path = "/test";
        struct Case<'a> {
            pub name: &'a str,
            pub req_body: &'a str,
            pub resp_status: u16,
            pub resp_body: &'a str,
            pub exp_error: Option<FetchError>,
            pub exp_status: u16,
            pub exp_body: &'a str,
        }
        let cases = [
            Case {
                name: "ok",
                req_body: "body",
                resp_status: 200,
                resp_body: "hello",
                exp_error: None,
                exp_status: 200,
                exp_body: "hello",
            },
            Case {
                name: "ok no body",
                req_body: "",
                resp_status: 200,
                resp_body: "",
                exp_error: None,
                exp_status: 200,
                exp_body: "",
            },
            Case {
                name: "404",
                req_body: "",
                resp_status: 404,
                resp_body: "",
                exp_error: None,
                exp_status: 404,
                exp_body: "",
            },
        ];
        for c in cases.iter() {
            let server = Server::run();
            let resp = http::Response::builder()
                .status(c.resp_status)
                .body(c.resp_body)
                .unwrap();
            server.expect(
                Expectation::matching(all_of![
                    request::method_path("POST", path),
                    // sic lowercase header below; it does that apparently though http request
                    // headers are supposed to be case-insensitive.
                    request::headers(contains(("x-header-name", "Header Value"))),
                    request::body(c.req_body.to_string()),
                ])
                .respond_with(resp),
            );

            let client = hyper::Client::new();
            let mut req_builder = http::request::Builder::new();
            req_builder = req_builder
                .method("POST")
                .uri(server.url(path))
                .header("X-Header-Name", "Header Value");
            let req = req_builder.body(c.req_body.to_string()).unwrap();
            let resp = rust_fetch(&client, req).await;

            // Is there a simpler way to write this?
            match &c.exp_error {
                None => {
                    if let Err(e) = resp {
                        assert!(false, "expected no error, got {:?}", e);
                        continue;
                    }
                    let got = resp.unwrap();
                    assert_eq!(c.exp_status, got.status());
                    assert_eq!(c.exp_body, got.body());
                }
                Some(e) => {
                    if let Ok(_) = resp {
                        assert!(false, "expected {:?}, got Ok", e);
                        continue;
                    }
                }
            }
        }
    }
}
