// mod swellow {
//     use pyo3::prelude::*;
//     pyo3::create_exception!(swellow, SwellowError, pyo3::exceptions::PyException);


//     #[pyfunction]
//     async fn up(
//         db_connection_string: String,
//         migration_directory: String,
//         current_version_id: Option<i64>,
//         target_version_id: Option<i64>,
//         flag_plan: bool,
//         flag_dry_run: bool,
//     ) -> PyResult<()> {
//         crate::commands::migrate(
//             db_connection_string,
//             migration_directory,
//             current_version_id, 
//             target_version_id, 
//             crate::commands::MigrationDirection::Up,
//             flag_plan,
//             flag_dry_run,
//         )
//             .await
//             .map_err(|e| SwellowError::new_err(e.to_string()))?;

//         Ok(())
//     }

//     #[pyfunction]
//     async fn down(
//         db_connection_string: String,
//         migration_directory: String,
//         current_version_id: Option<i64>,
//         target_version_id: Option<i64>,
//         flag_plan: bool,
//         flag_dry_run: bool,
//     ) -> PyResult<()> {
//         crate::commands::migrate(
//             db_connection_string,
//             migration_directory,
//             current_version_id, 
//             target_version_id, 
//             crate::commands::MigrationDirection::Down,
//             flag_plan,
//             flag_dry_run,
//         )
//             .await
//             .map_err(|e| SwellowError::new_err(e.to_string()))?;
//         Ok(())
//     }
// }
