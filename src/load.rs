use std::collections::HashMap;

use chrono::NaiveDateTime;
use std::io::Read;
use std::path::Path;

use anyhow::{Context, Result};

use crate::model::{
    Dag, RawRow, application_from_fileloc, environment_from_dag_id, parse_bool, parse_timestamp,
};

pub fn load_dags(path: &Path) -> Result<Vec<Dag>> {
    let file = std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    read_dags(file).with_context(|| format!("reading {}", path.display()))
}

/// The export has one row per DAG version; parse fields repeat the current snapshot,
/// so we keep the row with the highest version per DAG plus every version's timestamp.
pub fn read_dags<R: Read>(reader: R) -> Result<Vec<Dag>> {
    let mut latest: HashMap<String, (RawRow, Vec<NaiveDateTime>)> = HashMap::new();

    for (index, record) in csv::Reader::from_reader(reader).deserialize().enumerate() {
        let row: RawRow = record.with_context(|| format!("CSV record {}", index + 1))?;
        let created = row.version_created_at.as_deref().and_then(parse_timestamp);
        match latest.get_mut(&row.dag_id) {
            Some((existing, times)) => {
                times.extend(created);
                if row.version_key() > existing.version_key() {
                    *existing = row;
                }
            }
            None => {
                latest.insert(row.dag_id.clone(), (row, created.into_iter().collect()));
            }
        }
    }

    let mut dags: Vec<Dag> = latest
        .into_values()
        .map(|(row, times)| to_dag(row, times))
        .collect();
    dags.sort_by(|a, b| a.dag_id.cmp(&b.dag_id));
    Ok(dags)
}

fn to_dag(row: RawRow, mut version_times: Vec<NaiveDateTime>) -> Dag {
    version_times.sort();
    Dag {
        version_times,
        application: application_from_fileloc(&row.fileloc),
        environment: environment_from_dag_id(&row.dag_id),
        parse_seconds: row.parse_seconds,
        last_parsed: row.last_parsed_time.as_deref().and_then(parse_timestamp),
        // LEFT JOIN: a DAG without any dag_version row has no version fields.
        versions: row.versions(),
        is_stale: row.is_stale.as_deref().and_then(parse_bool),
        dag_id: row.dag_id,
        fileloc: row.fileloc,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CSV: &str = "\
\"dag_id\",\"fileloc\",\"last_parsed_time\",\"parse_seconds\",\"version_number\",\"bundle_name\",\"bundle_version\"
a-dev,/opt/airflow/dags/org/AP1-X/a.py,2026-09-23 10:00:00,4.5,3,dags-folder,
a-dev,/opt/airflow/dags/org/AP1-X/a.py,2026-09-23 10:00:00,4.5,2,dags-folder,
a-dev,/opt/airflow/dags/org/AP1-X/a.py,2026-09-23 10:00:00,4.5,1,dags-folder,
b-sit,/opt/airflow/dags/org/AP2-Y/b.py,,,1,dags-folder,
c-qa,/opt/airflow/dags/org/AP2-Y/c.py,2026-09-23 10:00:00,1.5,,,
";

    #[test]
    fn reads_latest_version_export() {
        let csv = "\
dag_id,fileloc,last_parsed_time,parse_seconds,is_stale,latest_version,version_created_at,bundle_name,bundle_version
a-dev,/opt/airflow/dags/org/AP1-X/a.py,2026-09-23 10:00:00,4.5,f,7,2026-09-23 09:59:00,dags-folder,
";
        let dags = read_dags(csv.as_bytes()).unwrap();
        assert_eq!(dags[0].versions, 7);
        assert_eq!(dags[0].is_stale, Some(false));
        assert_eq!(dags[0].parse_seconds, Some(4.5));
    }

    #[test]
    fn reads_all_versions_export_with_extra_columns() {
        let csv = "\
dag_id,fileloc,last_parsed_time,parse_seconds,is_stale,bundle_name,bundle_version,version_number,version_created_at,latest_version,version_count,is_latest,seconds_since_prev_version
a-dev,/opt/airflow/dags/org/AP1-X/a.py,2026-09-23 10:00:00,4.5,f,dags-folder,,2,2026-09-23 09:59:00,2,2,t,3600.0
a-dev,/opt/airflow/dags/org/AP1-X/a.py,2026-09-23 10:00:00,4.5,f,dags-folder,,1,2026-09-23 08:59:00,2,2,f,
";
        let dags = read_dags(csv.as_bytes()).unwrap();
        assert_eq!(dags.len(), 1);
        assert_eq!(dags[0].versions, 2);
        assert_eq!(dags[0].version_times.len(), 2);
        assert!(dags[0].version_times[0] < dags[0].version_times[1]);
    }

    #[test]
    fn deduplicates_versions_and_keeps_missing_parse_time() {
        let dags = read_dags(CSV.as_bytes()).unwrap();
        assert_eq!(dags.len(), 3);

        let a = &dags[0];
        assert_eq!(a.dag_id, "a-dev");
        assert_eq!(a.versions, 3);
        assert_eq!(a.parse_seconds, Some(4.5));
        assert_eq!(a.application, "AP1-X");
        assert_eq!(a.environment, "dev");

        let b = &dags[1];
        assert_eq!(b.parse_seconds, None);
        assert_eq!(b.last_parsed, None);

        let c = &dags[2];
        assert_eq!(c.versions, 0);
    }
}
