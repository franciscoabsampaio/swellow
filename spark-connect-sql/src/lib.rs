mod builder;
mod client;
pub mod error;
mod middleware;
mod session;

/// Spark Connect gRPC protobuf translated using [tonic]
pub mod spark {
    tonic::include_proto!("spark.connect");
}

pub use client::SparkClient;
pub use session::{SparkSessionBuilder, SparkSession};
