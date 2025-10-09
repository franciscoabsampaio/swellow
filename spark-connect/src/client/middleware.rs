use std::collections::HashMap;
use tonic::{Request, Status};
use tonic::metadata::{MetadataKey, MetadataValue};
use tonic::service::Interceptor;

/// A lightweight gRPC [`Interceptor`] for injecting static headers into outgoing requests.
///
/// Unlike a [`tower::Layer`], this type operates at the client level — it wraps
/// gRPC calls to attach metadata before transmission, without altering the service stack.
///
/// Used internally by [`SparkClient`](crate::SparkClient) to attach
/// authentication tokens or user context to every request.
///
/// # Notes
/// - All headers must be valid gRPC metadata keys and values.
/// - This interceptor is **cloneable** and cheap to reuse across channels.
#[derive(Clone, Debug)]
pub struct HeaderInterceptor {
    headers: HashMap<String, String>,
}

impl HeaderInterceptor {
    pub(crate) fn new(headers: HashMap<String, String>) -> Self {
        Self { headers }
    }
}

impl Interceptor for HeaderInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        for (key, value) in &self.headers {
            let metadata_key = MetadataKey::from_bytes(key.as_bytes())
                .map_err(|_| Status::invalid_argument(format!("Invalid header key: {}", key)))?;
            let metadata_value = MetadataValue::try_from(value.as_str())
                .map_err(|_| Status::invalid_argument(format!("Invalid header value: {}", value)))?;

            req.metadata_mut().insert(metadata_key, metadata_value);
        }
        Ok(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataMap;

    #[test]
    fn test_headers_are_inserted() {
        let mut headers = std::collections::HashMap::new();
        headers.insert("x-test-key".to_string(), "test-value".to_string());
        headers.insert("authorization".to_string(), "Bearer token".to_string());

        let mut interceptor = HeaderInterceptor::new(headers);

        let req = tonic::Request::new(());
        let req = interceptor.call(req).expect("Interceptor failed");

        let metadata: &MetadataMap = req.metadata();

        assert_eq!(metadata.get("x-test-key").unwrap(), "test-value");
        assert_eq!(metadata.get("authorization").unwrap(), "Bearer token");
    }

    #[test]
    fn test_invalid_key_returns_error() {
        let mut headers = std::collections::HashMap::new();
        headers.insert("invalid key!".to_string(), "value".to_string());

        let mut interceptor = HeaderInterceptor::new(headers);
        let req = tonic::Request::new(());

        let result = interceptor.call(req);
        assert!(result.is_err(), "Expected error for invalid header key");
    }

    #[test]
    fn test_invalid_value_returns_error() {
        let mut headers = std::collections::HashMap::new();
        // Control characters are invalid in MetadataValue
        headers.insert("x-test".to_string(), "\u{7f}".to_string());

        let mut interceptor = HeaderInterceptor::new(headers);
        let req = tonic::Request::new(());

        let result = interceptor.call(req);
        assert!(result.is_err(), "Expected error for invalid header value");
    }

    #[test]
    fn test_existing_metadata_preserved() {
        let mut headers = std::collections::HashMap::new();
        headers.insert("x-new-header".to_string(), "new-value".to_string());

        let mut interceptor = HeaderInterceptor::new(headers);

        let mut req = tonic::Request::new(());
        req.metadata_mut().insert(
            "x-existing",
            tonic::metadata::MetadataValue::try_from("old-value").unwrap(),
        );

        let req = interceptor.call(req).expect("Interceptor failed");
        let metadata = req.metadata();

        assert_eq!(metadata.get("x-existing").unwrap(), "old-value");
        assert_eq!(metadata.get("x-new-header").unwrap(), "new-value");
    }
}
