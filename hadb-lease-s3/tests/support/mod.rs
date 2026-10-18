//! Test-only HTTP fault adapter. Every accepted request reaches real S3;
//! selected responses model an unsupported CAS or an ambiguous commit.

use axum::{
    body::{to_bytes, Body},
    extract::{Request, State},
    http::{Method, Response, StatusCode},
    Router,
};
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};

struct ProxyState {
    upstream: String,
    client: reqwest::Client,
    fault: Arc<AtomicU8>,
}

pub struct FaultProxy {
    pub endpoint: String,
    pub fault: Arc<AtomicU8>,
    task: tokio::task::JoinHandle<()>,
}

impl FaultProxy {
    pub async fn new(upstream: &str) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let fault = Arc::new(AtomicU8::new(0));
        let state = Arc::new(ProxyState {
            upstream: upstream.to_owned(),
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .unwrap(),
            fault: fault.clone(),
        });
        let router = Router::new().fallback(forward).with_state(state);
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            endpoint,
            fault,
            task,
        }
    }
}

impl Drop for FaultProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn s3_error(status: StatusCode, code: &str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "application/xml")
        .body(Body::from(format!(
            "<Error><Code>{code}</Code><Message>Injected transport failure</Message></Error>"
        )))
        .unwrap()
}

async fn forward(
    State(state): State<Arc<ProxyState>>,
    request: Request,
) -> Result<Response<Body>, StatusCode> {
    let (parts, body) = request.into_parts();
    let conditional_create =
        parts.method == Method::PUT && parts.headers.contains_key("if-none-match");
    if conditional_create {
        match state.fault.load(Ordering::SeqCst) {
            1 => return Ok(s3_error(StatusCode::NOT_IMPLEMENTED, "NotImplemented")),
            4 => return Ok(s3_error(StatusCode::CONFLICT, "ConditionalRequestConflict")),
            5 => return Ok(s3_error(StatusCode::CONFLICT, "UnknownConflict")),
            _ => {}
        }
    }
    let lose_receipt = parts.method == Method::PUT
        && parts.headers.contains_key("if-match")
        && state
            .fault
            .compare_exchange(2, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok();
    let bytes = to_bytes(body, 4 * 1024 * 1024)
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let upstream = state
        .client
        .request(parts.method, format!("{}{}", state.upstream, parts.uri))
        .headers(parts.headers)
        .body(bytes)
        .send()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let status = upstream.status();
    let headers = upstream.headers().clone();
    let bytes = upstream
        .bytes()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    if lose_receipt && status.is_success() {
        return Ok(s3_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailable",
        ));
    }
    let mut response = Response::builder()
        .status(status)
        .body(Body::from(bytes))
        .unwrap();
    *response.headers_mut() = headers;
    Ok(response)
}
