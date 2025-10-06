use std::collections::HashMap;
use tonic::{Request, Status};
use tonic::metadata::{MetadataKey, MetadataValue};
use tonic::service::Interceptor;


/// Interceptor that injects headers into gRPC requests
#[derive(Clone, Debug)]
pub struct HeaderInterceptor {
    headers: HashMap<String, String>,
}

impl HeaderInterceptor {
    pub fn new(headers: HashMap<String, String>) -> Self {
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
