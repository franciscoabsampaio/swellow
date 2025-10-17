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
use crate::error::{SwellowError, SwellowErrorKind};
use serde::Serialize;


#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SwellowErrorJson {
    Engine(String),
    InvalidVersionInterval(String),
    IoDirectoryCreate(String),
    IoFileWrite(String),
    Parse(String),
}

impl From<&SwellowError> for SwellowErrorJson {
    fn from(e: &SwellowError) -> Self {
        // The pretty CLI output (stderr-like message)
        let stderr = format!("{e}");

        match &e.kind {
            SwellowErrorKind::Engine(_) => {
                Self::Engine(stderr)
            }
            SwellowErrorKind::InvalidVersionInterval(..) => {
                Self::InvalidVersionInterval(stderr)
            }
            SwellowErrorKind::IoDirectoryCreate {..} => {
                Self::IoDirectoryCreate(stderr)
            }
            SwellowErrorKind::IoFileWrite {..} => {
                Self::IoFileWrite(stderr)
            }
            SwellowErrorKind::Parse(_) => {
                Self::Parse(stderr)
            }
        }
    }
}


#[derive(Serialize)]
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


#[cfg(test)]
mod tests {
    use crate::{SwellowError, error::SwellowErrorKind, output::SwellowErrorJson};
    
    #[test]
    fn serializes_to_expected_json() {
        let err = SwellowError { kind: SwellowErrorKind::InvalidVersionInterval(3, 7) };
        let json = SwellowErrorJson::from(&err);
        let s = serde_json::to_string(&json).unwrap();
        assert_eq!(s, r#"{"type":"invalid_version_interval",,"from":3,"to":7}"#);
    }
}