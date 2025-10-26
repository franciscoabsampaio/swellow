// {
//   "status": "ok",
//   "command": "plan",
//   "message": "Migration plan generated",
//   "warnings": [
//     {"code": "destructive_drop", "message": "Table 'users' will be dropped"},
//     {"code": "potential_data_loss", "message": "Altering column type from INT to TEXT"}
//   ],
//   "data": {
//     "plan": [
//       {"version": 5, "action": "create_table", "table": "users"},
//       {"version": 6, "action": "alter_column", "table": "users", "column": "id"}
//     ]
//   },
//   "timestamp": "2025-10-17T15:52:12Z"
// }
use crate::{db::EngineError, error::{SwellowError, SwellowErrorKind}, parser::ParseErrorKind};
use serde::Serialize;
use serde_json::Value;


#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SwellowErrorJson {
    Engine { message: String },
    FileNotFound { message: String },
    Io { message: String },
    Parser { message: String },
    Version { message: String },
}

impl From<&SwellowError> for SwellowErrorJson {
    fn from(error: &SwellowError) -> Self {
        let stderr = format!("{error}");

        match &error.kind {
            SwellowErrorKind::DryRunUnsupportedEngine(_) => Self::Engine { message: stderr },
            SwellowErrorKind::Engine(_) => Self::Engine { message: stderr },
            SwellowErrorKind::InvalidVersionInterval(..) => Self::Version { message: stderr },
            SwellowErrorKind::IoDirectoryCreate {..} | SwellowErrorKind::IoFileWrite {..} => {
                Self::Io { message: stderr }
            }
            SwellowErrorKind::Parse(e) => match &e.kind {
                ParseErrorKind::FileNotFound {..} => Self::FileNotFound { message: stderr },
                ParseErrorKind::InvalidDirectory(_) => Self::Io { message: stderr },
                ParseErrorKind::InvalidVersionFormat(_) => Self::Version { message: stderr },
                ParseErrorKind::InvalidVersionNumber(_) => Self::Version { message: stderr },
                ParseErrorKind::Io {..} => Self::Io { message: stderr },
                ParseErrorKind::NoMigrationsInRange(..) => Self::FileNotFound { message: stderr },
                ParseErrorKind::Statement(_) | ParseErrorKind::Tokens(_) => Self::Parser { message: stderr },
            }
        }
    }
}

impl From<&EngineError> for SwellowErrorJson {
    fn from(e: &EngineError) -> Self {
        Self::Engine { message: format!("{e}") }
    }
}

#[derive(PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SwellowStatus {
    Success,
    Error,
}

#[derive(Serialize)]
pub struct SwellowOutput<T: Serialize> {
    pub command: String,
    pub status: SwellowStatus,
    pub data: Option<T>,
    pub error: Option<SwellowErrorJson>,
}

impl SwellowOutput<Value> {
    pub fn from_result(
        command: impl Into<String>,
        result: Result<(), SwellowError>,
    ) -> Self {
        match result {
            Ok(_) => Self {
                command: command.into(),
                status: SwellowStatus::Success,
                data: None,
                error: None,
            },
            Err(e) => Self {
                command: command.into(),
                status: SwellowStatus::Error,
                data: None,
                error: Some((&e).into()),
            },
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn wraps_output_as_error_correctly() {
        let err = SwellowError {
            kind: SwellowErrorKind::InvalidVersionInterval(5, 3),
        };

        let output = SwellowOutput::<()> {
            command: "plan".to_string(),
            status: SwellowStatus::Error,
            data: None,
            error: Some((&err).into()),
        };

        let s = serde_json::to_string(&output).unwrap();
        let v: Value = serde_json::from_str(&s).unwrap();

        assert_eq!(v["status"], "error");
        assert_eq!(v["command"], "plan");
        assert_eq!(v["error"]["type"], "version");
    }
}
