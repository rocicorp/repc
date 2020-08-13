use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;
use web_sys::{RequestInit, RequestMode};

macro_rules! box_err {
    ($ex:expr, $variant:ident) => {
        $ex.map_err(|e| $variant(Box::new(e)))
    };
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
    use FetchError::*;
    let (parts, req_body) = http_req.into_parts();
    let mut builder = hyper::Request::builder()
        .method(parts.method.as_str())
        .uri(&parts.uri.to_string());
    for (k, v) in parts.headers.iter() {
        builder = builder.header(k, box_err!(v.to_str(), InvalidRequestHeader)?);
    }
    let hyper_req = box_err!(
        builder.body(hyper::Body::from(req_body)),
        InvalidRequestBody
    )?;

    let mut hyper_resp = box_err!(hyper_client.request(hyper_req).await, RequestFailed)?;
    let http_resp_builder = http::response::Builder::new();
    let http_resp_bytes = box_err!(
        hyper::body::to_bytes(hyper_resp.body_mut()).await,
        ErrorReadingResponseBody
    )?;
    let http_resp_string = box_err!(
        String::from_utf8(http_resp_bytes.to_vec()),
        ErrorReadingResponseBodyAsString
    )?; // Copies :(
    let http_resp = box_err!(
        http_resp_builder
            .status(hyper_resp.status())
            .body(http_resp_string),
        FailedToWrapHttpResponse
    )?;
    Ok(http_resp)
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
    use FetchError::*;
    let mut opts = RequestInit::new();
    opts.method(http_req.method().as_str());
    opts.mode(RequestMode::Cors);
    let js_body = JsValue::from_str(http_req.body());
    opts.body(Some(&js_body));
    let web_sys_req = box_err!(
        web_sys::Request::new_with_str_and_init(&http_req.uri().to_string(), &opts),
        UnableToCreateRequest
    )?;
    let h = web_sys_req.headers();
    for (k, v) in http_req.headers().iter() {
        box_err!(
            h.set(k.as_ref(), box_err!(v.to_str(), InvalidRequestHeader)?),
            UnableToSetRequestHeader
        )?;
    }

    let window = web_sys::window().ok_or_else(|| NoWindow)?;
    let http_req_promise = window.fetch_with_request(&web_sys_req);
    let http_req_future = JsFuture::from(http_req_promise);
    let js_web_sys_resp = box_err!(http_req_future.await, RequestFailed)?;
    if !js_web_sys_resp.is_instance_of::<web_sys::Response>() {
        return Err(InvalidResponseFromJS);
    }
    let web_sys_resp: web_sys::Response = js_web_sys_resp.dyn_into().unwrap();
    let body_js_value = box_err!(web_sys_resp.text(), ErrorReadingResponseBodyAsString)?;
    //.await;
    let resp_body = body_js_value
        .as_string()
        .ok_or_else(|| ErrorReadingResponseBodyAsString(Box::new("".to_string())))?;

    let builder = http::response::Builder::new();
    let http_resp = box_err!(
        builder.status(web_sys_resp.status()).body(resp_body),
        FailedToWrapHttpResponse
    )?;
    Ok(http_resp)
}

// FetchErrors are returned by both the rust and browser versions of fetch. Since
// lower level errors in each case will be coming from two different places,
// FetchError is lossy of the error types underneath: it holds an error string.
#[derive(Debug)]
pub enum FetchError {
    ErrorReadingResponseBodyAsString(Box<dyn std::fmt::Debug>),
    ErrorReadingResponseBody(Box<dyn std::fmt::Debug>),
    FailedToWrapHttpResponse(Box<dyn std::fmt::Debug>),
    InvalidRequestBody(Box<dyn std::fmt::Debug>),
    InvalidRequestHeader(Box<dyn std::fmt::Debug>),
    InvalidResponseFromJS,
    NoWindow,
    RequestFailed(Box<dyn std::fmt::Debug>),
    UnableToCreateRequest(Box<dyn std::fmt::Debug>),
    UnableToSetRequestHeader(Box<dyn std::fmt::Debug>),
}

// Note: browser_fetch manually tested in tests/wasm.rs.
#[cfg(test)]
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
                    if let Err(e) = &resp {
                        assert!(false, "expected no error, got {:?}", e);
                    }
                    let got = resp.unwrap();
                    assert_eq!(c.exp_status, got.status());
                    assert_eq!(c.exp_body, got.body());
                }
                Some(e) => {
                    if let Ok(_) = resp {
                        assert!(false, "expected {:?}, got Ok", e);
                    }
                }
            }
        }
    }
}
