/*!
# spark-connect

<b>An idiomatic, SQL-first Rust client for Apache Spark Connect.</b>

This crate provides a fully asynchronous, strongly typed API for interacting
with a remote Spark Connect server over gRPC.

It allows you to build and execute SQL queries, bind parameters safely,
and collect Arrow `RecordBatch` results - just like any other SQL toolkit -
all in native Rust.

## ✨ Features

- ⚙️ **Spark-compatible connection builder** (`sc://host:port` format);
- 🪶 **Async execution** using `tokio` and `tonic`;
- 🧩 **Parameterized queries**;
- 🧾 **Arrow-native results** returned as `Vec<RecordBatch>`;

## Getting Started
 
```
use spark_connect::SparkSessionBuilder;

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
// 1️⃣ Connect to a Spark Connect endpoint
let session = SparkSessionBuilder::new("sc://localhost:15002")
    .build()
    .await?;

// 2️⃣ Execute a simple SQL query and receive a Vec<RecordBatches>
let batches = session
    .query("SELECT ? AS rule, ? AS text")
    .bind(42)
    .bind("world")
    .execute()
    .await?;

# Ok(())
# }
```

It's that simple!

## 🧩 Parameterized Queries

Behind the scenes, the [`SparkSession::query`] method
uses the [`ToLiteral`] trait to safely bind parameters
before execution:

```ignore
use spark_connect::ToLiteral;
 
// This is
 
let batches = session
    .query("SELECT ? AS id, ? AS text")
    .bind(42)
    .bind("world")
    .await?;

// the same as this

let lazy_plan = session.sql(
    "SELECT ? AS id, ? AS text",
    vec![42.to_literal(), "world".to_literal()]
).await?;
let batches = session.collect(lazy_plan);
```
 
## 😴 Lazy Execution

The biggest advantage to using the [`sql()`](SparkSession::sql) method
instead of [`query()`](SparkSession::query) is lazy execution -
queries can be lazily evaluated and collected afterwards.
If you're coming from PySpark or Scala, this should be the familiar interface.

## 🧠 Concepts

- <b>[`SparkSession`](crate::SparkSession)</b> — the main entry point for executing
  SQL queries and managing a session.
- <b>[`SparkClient`](crate::SparkClient)</b> — low-level gRPC client (used internally).
- <b>[`SqlQueryBuilder`](crate::query::SqlQueryBuilder)</b> — helper for binding parameters
  and executing queries.

## ⚙️ Requirements

- A running **Spark Connect server** (Spark 3.4+);
- Network access to the configured `sc://` endpoint;
- `tokio` runtime.

## 🔒 Example Connection Strings

```text
sc://localhost:15002
sc://spark-cluster:15002/?user_id=francisco
sc://10.0.0.5:15002;session_id=abc123;user_agent=my-app
```

## 📘 Learn More

- [Apache Spark Connect documentation](https://spark.apache.org/docs/latest/spark-connect.html);
- [Apache Arrow RecordBatch specification](https://arrow.apache.org/docs/format/Columnar.html).

## Disclaimer:

This project is not affiliated with, endorsed by, or sponsored by the Apache Software Foundation.
“Apache”, “Apache Spark”, and “Spark Connect” are trademarks of the Apache Software Foundation.

---
© 2025 Francisco A. B. Sampaio. Licensed under the Apache 2.0 License.

This project is not affiliated with, endorsed by, or sponsored by the Apache Software Foundation.
“Apache”, “Apache Spark”, and “Spark Connect” are trademarks of the Apache Software Foundation.
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