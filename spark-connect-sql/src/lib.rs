/*!
*/

mod io;
pub mod client;
mod error;
mod literal;
pub mod query;
mod session;

/// Spark Connect gRPC protobuf translated using [tonic].
pub mod spark {
    tonic::include_proto!("spark.connect");
}

pub use client::SparkClient;
pub use error::SparkError;
pub use session::{SparkSessionBuilder, SparkSession};
pub use literal::ToLiteral;

#[cfg(test)]
mod test_utils;