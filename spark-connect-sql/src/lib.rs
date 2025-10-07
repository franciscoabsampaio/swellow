mod io;
mod builder;
mod client;
pub mod error;
mod handlers;
mod middleware;
mod session;
pub mod sql;

/// Spark Connect gRPC protobuf translated using [tonic]
pub mod spark {
    tonic::include_proto!("spark.connect");
}

pub use client::SparkClient;
pub use session::{SparkSessionBuilder, SparkSession};

#[cfg(test)]
mod test_utils;