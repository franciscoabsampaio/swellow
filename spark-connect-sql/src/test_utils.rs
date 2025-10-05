#[cfg(test)]
pub mod test_utils {
    /// Test fixture to set up a Spark Connect server in a container and create a session.
    use crate::{SparkSession, SparkSessionBuilder, error::SparkError};

    pub async fn setup_session() -> Result<SparkSession, SparkError> {
        // Testcontainers is disabled for the moment.
        // For some still unknown reason,
        // the container created using testcontainers is unreachable,
        // despite having the same settings as a locally deployed container.
        
        // use testcontainers::{core::WaitFor, runners::AsyncRunner, GenericImage};
        
        // let container = GenericImage::new("franciscoabsampaio/spark-connect", "latest")
        //     .with_exposed_port(15002.into())
        //     .with_wait_for(WaitFor::message_on_either_std("Spark Connect server started at"))
        //     .start()
        //     .await
        //     .expect("Failed to start Spark Connect server container.");
        
        // let host = container.get_host()
        //     .await
        //     .expect("Failed to get container host.");
        // let port = container
        //     .get_host_port_ipv4(15002)
        //     .await
        //     .expect("Failed to get container port.");

        // let connection = format!("sc://{host}:{port}/");
        let connection = format!("sc://localhost:15002/");

        SparkSessionBuilder::new(&connection).build().await
    }
}