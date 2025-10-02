mod builder;
mod client;
mod error;
mod middleware;
pub mod session;

/// Spark Connect gRPC protobuf translated using [tonic]
pub mod spark {
    tonic::include_proto!("spark.connect");
}

pub use session::SparkSessionBuilder;
