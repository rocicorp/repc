use serde::Serialize;

#[derive(Debug)]
pub enum RequestError {
    InvalidRequest(http::Error),
    SerializeRequestError(serde_json::error::Error),
}

pub fn new_http_request<Request>(
    req: &Request,
    url: &str,
    auth: &str,
    request_id: &str,
    overlapping_requests: bool,
) -> Result<http::Request<String>, RequestError>
where
    Request: Serialize,
{
    use RequestError::*;
    let body = serde_json::to_string(req).map_err(SerializeRequestError)?;
    let builder = http::request::Builder::new();
    let http_req = builder
        .version(if overlapping_requests {
            http::Version::HTTP_2
        } else {
            http::Version::default()
        })
        .method("POST")
        .uri(url)
        .header("Content-type", "application/json")
        .header("Authorization", auth)
        .header("X-Replicache-RequestID", request_id)
        .body(body)
        .map_err(InvalidRequest)?;
    Ok(http_req)
}
