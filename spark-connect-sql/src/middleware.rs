use std::{collections::HashMap, str::FromStr, task::{Context, Poll}};
use futures_util::future::BoxFuture;
use http_body::Body;
use tonic::codegen::http::{Request, HeaderName, HeaderValue};
use tower::Service;


/// Layer to inject headers into gRPC requests
#[derive(Debug, Clone)]
pub struct HeadersLayer {
    headers: HashMap<String, String>,
}

impl HeadersLayer {
    pub fn new(headers: HashMap<String, String>) -> Self {
        Self { headers }
    }
}

impl<S> tower::Layer<S> for HeadersLayer {
    type Service = HeadersMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        HeadersMiddleware::new(inner, self.headers.clone())
    }
}

/// Middleware that applies headers to outgoing gRPC requests
#[derive(Clone, Debug)]
pub struct HeadersMiddleware<S> {
    inner: S,
    headers: HashMap<String, String>,
}

impl<S> HeadersMiddleware<S> {
    pub fn new(inner: S, headers: HashMap<String, String>) -> Self {
        Self { inner, headers }
    }
}

impl<S, B> Service<Request<B>> for HeadersMiddleware<S>
where
    S: Service<Request<B>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    S::Error: Send + std::fmt::Debug + 'static,
    B: Body + Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<B>) -> Self::Future {
        let mut inner = std::mem::replace(&mut self.inner, self.inner.clone());
        let headers = self.headers.clone();

        Box::pin(async move {
            for (key, value) in headers {
                let key = HeaderName::from_str(&key).expect("Invalid header name");
                let value = HeaderValue::from_str(&value).expect("Invalid header value");
                req.headers_mut().insert(key, value);
            }

            inner.call(req).await
        })
    }
}
