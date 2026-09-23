use chrono::NaiveDateTime;
use serde::Deserialize;

pub const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

const KNOWN_ENVS: &[&str] = &[
    "dev", "sit", "qa", "uat", "nonprod", "preprod", "staging", "prod",
];

#[derive(Debug, Deserialize)]
pub struct RawRow {
    pub dag_id: String,
    pub fileloc: String,
    #[serde(deserialize_with = "csv::invalid_option")]
    pub last_parsed_time: Option<String>,
    #[serde(deserialize_with = "csv::invalid_option")]
    pub parse_seconds: Option<f64>,
    // Export shapes differ: all-versions rows carry `version_number` (and optionally
    // `latest_version`/`version_count`); the latest-only export carries just `latest_version`.
    #[serde(default, deserialize_with = "csv::invalid_option")]
    pub version_number: Option<u32>,
    #[serde(default, deserialize_with = "csv::invalid_option")]
    pub latest_version: Option<u32>,
    #[serde(default, deserialize_with = "csv::invalid_option")]
    pub version_count: Option<u32>,
    #[serde(default, deserialize_with = "csv::invalid_option")]
    pub is_stale: Option<String>,
    #[serde(default, deserialize_with = "csv::invalid_option")]
    pub version_created_at: Option<String>,
}

impl RawRow {
    /// Orders rows of the same DAG so the newest version wins.
    pub fn version_key(&self) -> Option<u32> {
        self.version_number.or(self.latest_version)
    }

    /// Exact count when exported, else the latest version number (assumes no gaps).
    pub fn versions(&self) -> u32 {
        self.version_count
            .or(self.latest_version)
            .or(self.version_number)
            .unwrap_or(0)
    }
}

/// One DAG, deduplicated from its per-version rows.
#[derive(Debug, Clone, PartialEq)]
pub struct Dag {
    pub dag_id: String,
    pub fileloc: String,
    pub application: String,
    pub environment: String,
    pub parse_seconds: Option<f64>,
    pub last_parsed: Option<NaiveDateTime>,
    pub versions: u32,
    /// `None` when the export has no `is_stale` column.
    pub is_stale: Option<bool>,
    /// `created_at` of every exported version, ascending. Complete only for the all-versions export.
    pub version_times: Vec<NaiveDateTime>,
}

/// First directory named like `AP1234-...`, else the file's parent directory.
pub fn application_from_fileloc(fileloc: &str) -> String {
    let relative = fileloc
        .split_once("/dags/")
        .map_or(fileloc, |(_, rest)| rest);
    let dirs: Vec<&str> = relative.split('/').collect();
    let dirs = &dirs[..dirs.len().saturating_sub(1)];

    dirs.iter()
        .find(|d| is_application_code(d))
        .or_else(|| dirs.last())
        .map_or_else(|| "(root)".to_string(), |d| d.to_string())
}

fn is_application_code(dir: &str) -> bool {
    let bytes = dir.as_bytes();
    bytes.len() > 2 && bytes[..2].eq_ignore_ascii_case(b"AP") && bytes[2].is_ascii_digit()
}

pub fn environment_from_dag_id(dag_id: &str) -> String {
    let suffix = dag_id.rsplit(['-', '_']).next().unwrap_or_default();
    let suffix = suffix.to_ascii_lowercase();
    if KNOWN_ENVS.contains(&suffix.as_str()) {
        suffix
    } else {
        "other".to_string()
    }
}

/// Postgres exports booleans as `true`/`false` or `t`/`f` depending on the client.
pub fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "t" | "1" | "yes" => Some(true),
        "false" | "f" | "0" | "no" => Some(false),
        _ => None,
    }
}

pub fn parse_timestamp(value: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(value.trim(), TIMESTAMP_FORMAT).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn application_prefers_ap_code() {
        assert_eq!(
            application_from_fileloc("/opt/airflow/dags/org/AP1234-ALPHA/qa_x.py"),
            "AP1234-ALPHA"
        );
        assert_eq!(
            application_from_fileloc("/opt/airflow/dags/AP9000-beta/factory/x.py"),
            "AP9000-beta"
        );
    }

    #[test]
    fn application_falls_back_to_parent_dir() {
        assert_eq!(
            application_from_fileloc("/opt/airflow/dags/org/TEAM-SANDBOX/x.py"),
            "TEAM-SANDBOX"
        );
        assert_eq!(application_from_fileloc("/opt/airflow/dags/x.py"), "(root)");
    }

    #[test]
    fn environment_from_suffix() {
        assert_eq!(environment_from_dag_id("org-report-dag-qa"), "qa");
        assert_eq!(environment_from_dag_id("org-report-dag-SIT"), "sit");
        assert_eq!(environment_from_dag_id("load_transfers"), "other");
    }
}
