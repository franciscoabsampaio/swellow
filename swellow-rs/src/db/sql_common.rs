pub const QUERY_LATEST_VERSION: &str = r#"
    SELECT MAX(version_id)
    FROM swellow.records
    WHERE status IN ('APPLIED', 'TESTED')
"#;
pub const QUERY_LOCK_EXISTS: &str = r#"
    SELECT 1
    FROM swellow.records
    WHERE version_id = 0
        AND object_type = 'LOCK'
        AND object_name_before = 'LOCK'
        AND object_name_after = 'LOCK'
        AND status = 'LOCKED'
    LIMIT 1
"#;
pub const QUERY_DELETE_LOCK: &str = r#"
    DELETE FROM swellow.records
    WHERE version_id = 0
        AND object_type = 'LOCK'
        AND object_name_before = 'LOCK'
        AND object_name_after = 'LOCK'
"#;