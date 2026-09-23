use std::collections::{BTreeMap, HashSet};

use chrono::{Datelike, Duration, NaiveDateTime, Timelike, Weekday};
use serde::Serialize;

use super::stats::{mean, percentile, sorted_ascending};
use crate::model::{Dag, TIMESTAMP_FORMAT};

const HISTOGRAM_BINS_PER_DECADE: f64 = 5.0;
const HISTOGRAM_MIN_SECONDS: f64 = 0.01;
pub const AIRFLOW_DEFAULT_PARSING_PROCESSES: u32 = 2;
pub const AIRFLOW_DEFAULT_IMPORT_TIMEOUT: f64 = 30.0;
pub const AIRFLOW_DEFAULT_FILE_PROCESSOR_TIMEOUT: f64 = 50.0;

const ACTIVITY_BIN_MINUTES: i64 = 10;
/// Upper bound on the activity window; it normally starts at the oldest recent parse.
const ACTIVITY_MAX_WINDOW_HOURS: i64 = 12;
/// A version saved within this long after its parse started counts as created by that parse.
const CHANGED_AT_PARSE_SLACK_SECONDS: f64 = 120.0;
/// Averaging a new version per day for a week, or three in one day, rarely comes from deploys alone.
const SUSPECT_MIN_VERSIONS_7D: u32 = 7;
const SUSPECT_MIN_VERSIONS_24H: u32 = 3;

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Settings {
    pub top_n: usize,
    pub parsing_processes: u32,
    pub import_timeout: f64,
    pub file_processor_timeout: f64,
    /// Active DAGs not re-parsed for longer than this (relative to the newest parse) are overdue.
    pub overdue_hours: f64,
    /// True when the value is Airflow's default rather than the server's real config.
    pub parsing_processes_assumed: bool,
    pub import_timeout_assumed: bool,
    pub file_processor_timeout_assumed: bool,
}

impl Settings {
    /// A file is cut off by whichever timeout fires first.
    pub fn timeout_limit(&self) -> f64 {
        self.import_timeout.min(self.file_processor_timeout)
    }
}

#[derive(Debug, Serialize)]
pub struct DagProcessorReport {
    pub settings: Settings,
    /// Parse time above which a DAG file is cut off by a timeout.
    pub timeout_limit: f64,
    /// Newest `last_parsed_time` in the export; age is measured against it.
    pub reference_time: Option<String>,
    /// False for exports without the `is_stale` column: every DAG is then treated as active.
    pub has_removed_flag: bool,
    /// Statistics below cover active DAGs only; removed ones are listed separately.
    pub summary: Summary,
    /// Every DAG in the export, active and removed.
    pub dags: Vec<DagRow>,
    /// Indices into `dags`, slowest first.
    pub slowest: Vec<usize>,
    pub histogram: Vec<HistogramBin>,
    /// `[seconds, % of parsed DAGs at or below]`
    pub ecdf: Vec<[f64; 2]>,
    pub pareto: Pareto,
    pub by_application: Vec<GroupStats>,
    pub application_count: usize,
    pub by_environment: Vec<GroupStats>,
    /// Indices into `dags`, most versions first.
    pub churn: Vec<usize>,
    pub freshness: Vec<FreshnessBucket>,
    /// Indices into `dags` of overdue active DAGs, oldest parse first.
    pub overdue: Vec<usize>,
    /// Indices into `dags` of removed DAGs (`is_stale`), oldest parse first.
    pub removed: Vec<usize>,
    pub removed_by_application: Vec<RemovedGroup>,
    pub observed_loop: ObservedLoop,
    /// Active DAGs whose `last_parsed_time` falls in each 10-minute bin, oldest first.
    pub parse_activity: Vec<ActivityBin>,
    /// False unless the export has every version's `version_created_at`.
    pub has_version_history: bool,
    /// Indices into `dags` of likely non-deterministic DAGs, most new versions first.
    pub suspects: Vec<usize>,
}

#[derive(Debug, Serialize)]
pub struct ObservedLoop {
    /// Active, non-overdue DAGs with a parse time; overdue ones measure a stuck file, not the loop.
    pub sample: usize,
    pub age_p50_hours: f64,
    pub age_p90_hours: f64,
    /// Nearly every file has been re-parsed within this long: the practical loop length.
    pub age_p99_hours: f64,
    pub parsed_last_hour: usize,
    /// Active DAGs ÷ DAGs parsed in the last hour.
    pub loop_from_rate_hours: Option<f64>,
    /// `age_p99_hours` ÷ the config-based estimate.
    pub ratio_to_estimate: Option<f64>,
}

#[derive(Debug, PartialEq, Serialize)]
pub struct ActivityBin {
    pub start: String,
    pub count: usize,
}

struct VersionActivity {
    versions_24h: u32,
    versions_7d: u32,
    weekend_versions_7d: u32,
    last_changed: Option<String>,
    changed_at_last_parse: bool,
    hours_since_change: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct Summary {
    /// Active DAGs; the fields below describe these unless named otherwise.
    pub dag_count: usize,
    pub all_dag_count: usize,
    pub removed_count: usize,
    pub removed_missing_count: usize,
    pub file_count: usize,
    pub application_count: usize,
    pub parsed_count: usize,
    pub missing_count: usize,
    pub total_parse_seconds: f64,
    pub estimated_loop_seconds: f64,
    pub mean: f64,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
    pub max: f64,
    pub over_timeout_count: usize,
    pub overdue_count: usize,
    pub changed_24h_count: usize,
    pub changed_7d_count: usize,
    pub suspect_count: usize,
    /// Active DAGs whose latest parse saved a new version, and those whose latest parse didn't.
    pub changed_at_last_parse_count: usize,
    pub unchanged_at_last_parse_count: usize,
    /// Across all DAGs, including removed ones.
    pub version_rows: u64,
    pub max_versions: u32,
}

#[derive(Debug, Serialize)]
pub struct DagRow {
    pub dag_id: String,
    pub fileloc: String,
    pub application: String,
    pub environment: String,
    pub parse_seconds: Option<f64>,
    pub last_parsed: Option<String>,
    /// Hours from this DAG's last parse to the newest parse in the export.
    pub age_hours: Option<f64>,
    pub versions: u32,
    pub over_timeout: bool,
    /// Airflow marked the DAG stale: its file is gone from the bundle, so it is no longer parsed.
    pub removed: bool,
    pub overdue: bool,
    pub versions_24h: u32,
    pub versions_7d: u32,
    /// New versions created on Saturday or Sunday within the last 7 days.
    pub weekend_versions_7d: u32,
    pub last_changed: Option<String>,
    /// The latest version was saved by the latest parse.
    pub changed_at_last_parse: bool,
    pub suspect: bool,
    /// Hours from the latest version to the newest parse in the export.
    pub hours_since_change: Option<f64>,
    /// % of active parsed DAGs that parse at or faster than this one.
    pub parse_rank_pct: Option<f64>,
    /// Last parse + observed loop: roughly the latest time the next parse should happen.
    /// Only for active, non-overdue DAGs.
    pub next_parse_by: Option<String>,
}

#[derive(Debug, PartialEq, Serialize)]
pub struct HistogramBin {
    pub lo: f64,
    pub hi: f64,
    pub count: usize,
}

#[derive(Debug, Serialize)]
pub struct Pareto {
    /// Cumulative % of total parse time covered by the top 1..=n DAGs.
    pub cumulative_share: Vec<f64>,
    pub dags_for_50: usize,
    pub dags_for_80: usize,
    pub top_10pct_share: f64,
}

#[derive(Debug, Serialize)]
pub struct GroupStats {
    pub name: String,
    pub dags: usize,
    pub parsed: usize,
    pub total_seconds: f64,
    pub mean: f64,
    pub min: f64,
    pub q1: f64,
    pub median: f64,
    pub q3: f64,
    pub p90: f64,
    pub max: f64,
    pub over_timeout: usize,
    pub missing: usize,
    pub overdue: usize,
}

#[derive(Debug, Serialize)]
pub struct RemovedGroup {
    pub name: String,
    pub dags: usize,
    pub files: usize,
}

#[derive(Debug, PartialEq, Serialize)]
pub struct FreshnessBucket {
    pub label: String,
    pub count: usize,
}

pub fn analyze(dags: &[Dag], settings: Settings) -> DagProcessorReport {
    let reference = dags.iter().filter_map(|d| d.last_parsed).max();
    let mut rows: Vec<DagRow> = dags
        .iter()
        .map(|d| to_row(d, reference, &settings))
        .collect();
    rank_parse_times(&mut rows);
    let active: Vec<&DagRow> = rows.iter().filter(|r| !r.removed).collect();

    let parsed = sorted_ascending(active.iter().filter_map(|r| r.parse_seconds));
    let slowest = ranked(&rows, |r| r.parse_seconds);
    let churn = ranked(&rows, |r| (r.versions > 1).then_some(f64::from(r.versions)));
    let overdue = ranked(&rows, |r| r.age_hours.filter(|_| r.overdue));
    let removed = ranked_all(&rows, |r| {
        r.removed.then_some(r.age_hours.unwrap_or(f64::MAX))
    });
    let suspects = ranked(&rows, |r| r.suspect.then_some(f64::from(r.versions_7d)));

    let by_application = group_stats(&active, |r| &r.application, &settings);
    let application_count = by_application.len();

    let summary = summarize(&rows, &active, &parsed, application_count, settings);
    let mut report = DagProcessorReport {
        observed_loop: observed_loop(&active, summary.estimated_loop_seconds),
        parse_activity: reference
            .map_or_else(Vec::new, |r| parse_activity(active_parse_times(dags), r)),
        has_version_history: dags.iter().any(|d| d.version_times.len() > 1),
        suspects,
        summary,
        reference_time: reference.map(|t| t.format(TIMESTAMP_FORMAT).to_string()),
        has_removed_flag: dags.iter().any(|d| d.is_stale.is_some()),
        slowest: truncate(slowest, settings.top_n),
        histogram: log_histogram(&parsed),
        ecdf: ecdf(&parsed),
        pareto: pareto(&parsed),
        by_application: truncate(by_application, settings.top_n),
        application_count,
        by_environment: group_stats(&active, |r| &r.environment, &settings),
        churn: truncate(churn, settings.top_n),
        freshness: freshness(&active),
        overdue,
        removed_by_application: removed_groups(&rows),
        removed,
        dags: rows,
        timeout_limit: settings.timeout_limit(),
        settings,
    };
    estimate_next_parse(&mut report.dags, dags, &report.observed_loop);
    report
}

fn to_row(dag: &Dag, reference: Option<NaiveDateTime>, settings: &Settings) -> DagRow {
    let age_hours = dag
        .last_parsed
        .zip(reference)
        .map(|(parsed, newest)| (newest - parsed).num_seconds() as f64 / 3600.0);
    let removed = dag.is_stale == Some(true);
    let activity = version_activity(dag, reference);

    DagRow {
        dag_id: dag.dag_id.clone(),
        fileloc: dag.fileloc.clone(),
        application: dag.application.clone(),
        environment: dag.environment.clone(),
        parse_seconds: dag.parse_seconds,
        last_parsed: dag
            .last_parsed
            .map(|t| t.format(TIMESTAMP_FORMAT).to_string()),
        age_hours,
        versions: dag.versions,
        over_timeout: !removed
            && dag
                .parse_seconds
                .is_some_and(|s| s > settings.timeout_limit()),
        overdue: !removed && age_hours.is_some_and(|h| h > settings.overdue_hours),
        removed,
        suspect: !removed
            && (activity.versions_7d >= SUSPECT_MIN_VERSIONS_7D
                || activity.versions_24h >= SUSPECT_MIN_VERSIONS_24H),
        versions_24h: activity.versions_24h,
        versions_7d: activity.versions_7d,
        weekend_versions_7d: activity.weekend_versions_7d,
        last_changed: activity.last_changed,
        changed_at_last_parse: activity.changed_at_last_parse,
        hours_since_change: activity.hours_since_change,
        parse_rank_pct: None,
        next_parse_by: None,
    }
}

fn version_activity(dag: &Dag, reference: Option<NaiveDateTime>) -> VersionActivity {
    let within = |t: &NaiveDateTime, window: Duration| reference.is_some_and(|r| *t > r - window);
    let last_7d: Vec<&NaiveDateTime> = dag
        .version_times
        .iter()
        .filter(|t| within(t, Duration::days(7)))
        .collect();
    let latest = dag.version_times.last();

    VersionActivity {
        versions_24h: last_7d
            .iter()
            .filter(|t| within(t, Duration::hours(24)))
            .count() as u32,
        versions_7d: last_7d.len() as u32,
        weekend_versions_7d: last_7d
            .iter()
            .filter(|t| matches!(t.weekday(), Weekday::Sat | Weekday::Sun))
            .count() as u32,
        last_changed: latest.map(|t| t.format(TIMESTAMP_FORMAT).to_string()),
        hours_since_change: latest
            .zip(reference)
            .map(|(changed, newest)| (newest - *changed).num_seconds() as f64 / 3600.0),
        changed_at_last_parse: latest
            .zip(dag.last_parsed)
            .is_some_and(|(created, parsed)| {
                let gap = (parsed - *created).num_seconds() as f64;
                (0.0..=dag.parse_seconds.unwrap_or(0.0) + CHANGED_AT_PARSE_SLACK_SECONDS)
                    .contains(&gap)
            }),
    }
}

fn summarize(
    rows: &[DagRow],
    active: &[&DagRow],
    parsed: &[f64],
    application_count: usize,
    settings: Settings,
) -> Summary {
    let total: f64 = parsed.iter().sum();
    let files: HashSet<&str> = active.iter().map(|r| r.fileloc.as_str()).collect();
    let removed = || rows.iter().filter(|r| r.removed);

    Summary {
        dag_count: active.len(),
        all_dag_count: rows.len(),
        removed_count: removed().count(),
        removed_missing_count: removed().filter(|r| r.parse_seconds.is_none()).count(),
        file_count: files.len(),
        application_count,
        parsed_count: parsed.len(),
        missing_count: active.len() - parsed.len(),
        total_parse_seconds: total,
        estimated_loop_seconds: total / f64::from(settings.parsing_processes.max(1)),
        mean: mean(parsed),
        p50: percentile(parsed, 0.50),
        p90: percentile(parsed, 0.90),
        p99: percentile(parsed, 0.99),
        max: parsed.last().copied().unwrap_or(0.0),
        over_timeout_count: active.iter().filter(|r| r.over_timeout).count(),
        overdue_count: active.iter().filter(|r| r.overdue).count(),
        changed_24h_count: active.iter().filter(|r| r.versions_24h > 0).count(),
        changed_7d_count: active.iter().filter(|r| r.versions_7d > 0).count(),
        suspect_count: active.iter().filter(|r| r.suspect).count(),
        changed_at_last_parse_count: active.iter().filter(|r| r.changed_at_last_parse).count(),
        unchanged_at_last_parse_count: active
            .iter()
            .filter(|r| {
                r.last_changed.is_some() && r.last_parsed.is_some() && !r.changed_at_last_parse
            })
            .count(),
        version_rows: rows.iter().map(|r| u64::from(r.versions)).sum(),
        max_versions: active.iter().map(|r| r.versions).max().unwrap_or(0),
    }
}

/// Indices of active rows with a key, sorted by key descending (ties by dag_id).
fn ranked(rows: &[DagRow], key: impl Fn(&DagRow) -> Option<f64>) -> Vec<usize> {
    ranked_all(rows, |r| if r.removed { None } else { key(r) })
}

fn ranked_all(rows: &[DagRow], key: impl Fn(&DagRow) -> Option<f64>) -> Vec<usize> {
    let mut keyed: Vec<(usize, f64)> = rows
        .iter()
        .enumerate()
        .filter_map(|(i, r)| key(r).map(|k| (i, k)))
        .collect();
    keyed.sort_by(|a, b| {
        b.1.total_cmp(&a.1)
            .then_with(|| rows[a.0].dag_id.cmp(&rows[b.0].dag_id))
    });
    keyed.into_iter().map(|(i, _)| i).collect()
}

fn truncate<T>(mut items: Vec<T>, n: usize) -> Vec<T> {
    items.truncate(n);
    items
}

fn log_histogram(sorted: &[f64]) -> Vec<HistogramBin> {
    let (Some(&min), Some(&max)) = (sorted.first(), sorted.last()) else {
        return Vec::new();
    };
    let bin_of =
        |v: f64| (v.max(HISTOGRAM_MIN_SECONDS).log10() * HISTOGRAM_BINS_PER_DECADE).floor() as i32;
    let first = bin_of(min);
    let last = bin_of(max);
    let edge = |k: i32| 10f64.powf(f64::from(k) / HISTOGRAM_BINS_PER_DECADE);

    let mut bins: Vec<HistogramBin> = (first..=last)
        .map(|k| HistogramBin {
            lo: edge(k),
            hi: edge(k + 1),
            count: 0,
        })
        .collect();
    for &v in sorted {
        let index = (bin_of(v) - first).clamp(0, last - first) as usize;
        bins[index].count += 1;
    }
    bins
}

fn ecdf(sorted: &[f64]) -> Vec<[f64; 2]> {
    let n = sorted.len() as f64;
    sorted
        .iter()
        .enumerate()
        .map(|(i, &v)| [v, (i + 1) as f64 / n * 100.0])
        .collect()
}

fn pareto(sorted: &[f64]) -> Pareto {
    let total: f64 = sorted.iter().sum();
    let mut running = 0.0;
    let cumulative_share: Vec<f64> = sorted
        .iter()
        .rev()
        .map(|v| {
            running += v;
            if total > 0.0 {
                running / total * 100.0
            } else {
                0.0
            }
        })
        .collect();

    let dags_for = |pct: f64| {
        cumulative_share
            .iter()
            .position(|&s| s >= pct)
            .map_or(cumulative_share.len(), |i| i + 1)
    };
    let top_10pct = (sorted.len() as f64 * 0.1).ceil() as usize;

    Pareto {
        dags_for_50: dags_for(50.0),
        dags_for_80: dags_for(80.0),
        top_10pct_share: top_10pct
            .checked_sub(1)
            .and_then(|i| cumulative_share.get(i))
            .copied()
            .unwrap_or(0.0),
        cumulative_share,
    }
}

/// Sorted by total parse time descending.
fn group_stats<'a>(
    rows: &[&'a DagRow],
    key: impl Fn(&'a DagRow) -> &'a str,
    settings: &Settings,
) -> Vec<GroupStats> {
    let limit = settings.timeout_limit();
    let mut groups: BTreeMap<&str, Vec<&DagRow>> = BTreeMap::new();
    for &row in rows {
        groups.entry(key(row)).or_default().push(row);
    }

    let mut stats: Vec<GroupStats> = groups
        .into_iter()
        .map(|(name, members)| {
            let values = sorted_ascending(members.iter().filter_map(|r| r.parse_seconds));
            GroupStats {
                name: name.to_string(),
                dags: members.len(),
                parsed: values.len(),
                total_seconds: values.iter().sum(),
                mean: mean(&values),
                min: values.first().copied().unwrap_or(0.0),
                q1: percentile(&values, 0.25),
                median: percentile(&values, 0.50),
                q3: percentile(&values, 0.75),
                p90: percentile(&values, 0.90),
                max: values.last().copied().unwrap_or(0.0),
                over_timeout: values.iter().filter(|v| **v > limit).count(),
                missing: members.len() - values.len(),
                overdue: members.iter().filter(|r| r.overdue).count(),
            }
        })
        .collect();
    stats.sort_by(|a, b| {
        b.total_seconds
            .total_cmp(&a.total_seconds)
            .then_with(|| a.name.cmp(&b.name))
    });
    stats
}

fn rank_parse_times(rows: &mut [DagRow]) {
    let parsed = sorted_ascending(
        rows.iter()
            .filter(|r| !r.removed)
            .filter_map(|r| r.parse_seconds),
    );
    if parsed.is_empty() {
        return;
    }
    for row in rows.iter_mut().filter(|r| !r.removed) {
        row.parse_rank_pct = row.parse_seconds.map(|s| {
            let at_or_below = parsed.partition_point(|v| *v <= s);
            at_or_below as f64 / parsed.len() as f64 * 100.0
        });
    }
}

fn estimate_next_parse(rows: &mut [DagRow], dags: &[Dag], observed: &ObservedLoop) {
    if observed.sample == 0 {
        return;
    }
    let loop_length = Duration::seconds((observed.age_p99_hours * 3600.0).round() as i64);
    for (row, dag) in rows.iter_mut().zip(dags) {
        if row.removed || row.overdue {
            continue;
        }
        row.next_parse_by = dag
            .last_parsed
            .map(|t| (t + loop_length).format(TIMESTAMP_FORMAT).to_string());
    }
}

fn observed_loop(active: &[&DagRow], estimated_loop_seconds: f64) -> ObservedLoop {
    let ages = sorted_ascending(
        active
            .iter()
            .filter(|r| !r.overdue)
            .filter_map(|r| r.age_hours),
    );
    let parsed_last_hour = ages.iter().filter(|h| **h <= 1.0).count();
    let age_p99_hours = percentile(&ages, 0.99);
    let estimated_hours = estimated_loop_seconds / 3600.0;

    ObservedLoop {
        sample: ages.len(),
        age_p50_hours: percentile(&ages, 0.50),
        age_p90_hours: percentile(&ages, 0.90),
        age_p99_hours,
        parsed_last_hour,
        loop_from_rate_hours: (parsed_last_hour > 0)
            .then(|| active.len() as f64 / parsed_last_hour as f64),
        ratio_to_estimate: (estimated_hours > 0.0 && !ages.is_empty())
            .then(|| age_p99_hours / estimated_hours),
    }
}

/// Counts over the last few hours before `reference`; bins are aligned to the clock.
fn active_parse_times(dags: &[Dag]) -> impl Iterator<Item = NaiveDateTime> + '_ {
    dags.iter()
        .filter(|d| d.is_stale != Some(true))
        .filter_map(|d| d.last_parsed)
}

fn parse_activity(
    parse_times: impl Iterator<Item = NaiveDateTime>,
    reference: NaiveDateTime,
) -> Vec<ActivityBin> {
    let bin = Duration::minutes(ACTIVITY_BIN_MINUTES);
    let floor = |t: NaiveDateTime| {
        t.with_second(0)
            .and_then(|t| {
                t.with_minute(
                    t.minute() / ACTIVITY_BIN_MINUTES as u32 * ACTIVITY_BIN_MINUTES as u32,
                )
            })
            .unwrap_or(t)
    };
    let recent: Vec<NaiveDateTime> = parse_times
        .filter(|t| *t > reference - Duration::hours(ACTIVITY_MAX_WINDOW_HOURS))
        .collect();
    let Some(&oldest) = recent.iter().min() else {
        return Vec::new();
    };
    let first_start = floor(oldest);
    let bins = (floor(reference) - first_start).num_minutes() / ACTIVITY_BIN_MINUTES + 1;

    let mut counts = vec![0usize; bins as usize];
    for parsed in recent {
        let index = ((parsed - first_start).num_minutes() / ACTIVITY_BIN_MINUTES) as usize;
        if let Some(count) = counts.get_mut(index) {
            *count += 1;
        }
    }

    counts
        .into_iter()
        .enumerate()
        .map(|(i, count)| ActivityBin {
            start: (first_start + bin * i as i32).format("%H:%M").to_string(),
            count,
        })
        .collect()
}

/// Removed DAGs per application, most first.
fn removed_groups(rows: &[DagRow]) -> Vec<RemovedGroup> {
    let mut groups: BTreeMap<&str, (usize, HashSet<&str>)> = BTreeMap::new();
    for row in rows.iter().filter(|r| r.removed) {
        let (dags, files) = groups.entry(&row.application).or_default();
        *dags += 1;
        files.insert(&row.fileloc);
    }
    let mut result: Vec<RemovedGroup> = groups
        .into_iter()
        .map(|(name, (dags, files))| RemovedGroup {
            name: name.to_string(),
            dags,
            files: files.len(),
        })
        .collect();
    result.sort_by(|a, b| b.dags.cmp(&a.dags).then_with(|| a.name.cmp(&b.name)));
    result
}

fn freshness(rows: &[&DagRow]) -> Vec<FreshnessBucket> {
    const BUCKETS: [(&str, f64); 5] = [
        ("< 1 h", 1.0),
        ("1–6 h", 6.0),
        ("6–24 h", 24.0),
        ("1–7 d", 168.0),
        ("> 7 d", f64::INFINITY),
    ];

    let mut counts = [0usize; BUCKETS.len()];
    let mut never = 0;
    for row in rows {
        match row.age_hours {
            Some(h) => {
                let index = BUCKETS
                    .iter()
                    .position(|(_, upper)| h < *upper)
                    .unwrap_or(BUCKETS.len() - 1);
                counts[index] += 1;
            }
            None => never += 1,
        }
    }

    BUCKETS
        .iter()
        .zip(counts)
        .map(|((label, _), count)| FreshnessBucket {
            label: label.to_string(),
            count,
        })
        .chain((never > 0).then(|| FreshnessBucket {
            label: "Never parsed".to_string(),
            count: never,
        }))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::load::read_dags;

    const CSV: &str = "\
dag_id,fileloc,last_parsed_time,parse_seconds,version_number,bundle_name,bundle_version,is_stale
a-dev,/d/dags/AP1-X/a.py,2026-09-23 10:00:00,1.0,1,f,,false
b-dev,/d/dags/AP1-X/b.py,2026-09-23 09:30:00,2.0,5,f,,false
c-sit,/d/dags/AP2-Y/c.py,2026-09-21 10:00:00,40.0,1,f,,false
d-sit,/d/dags/AP2-Y/d.py,2026-09-23 10:00:00,57.0,2,f,,false
e-qa,/d/dags/AP2-Y/e.py,,,1,f,,false
f-sit,/d/dags/AP3-Z/f.py,2026-01-01 10:00:00,300.0,9,f,,true
g-dev,/d/dags/AP3-Z/g.py,,,1,f,,true
";

    fn report() -> DagProcessorReport {
        let settings = Settings {
            top_n: 2,
            parsing_processes: 2,
            import_timeout: 30.0,
            file_processor_timeout: 50.0,
            overdue_hours: 24.0,
            parsing_processes_assumed: false,
            import_timeout_assumed: false,
            file_processor_timeout_assumed: false,
        };
        analyze(&read_dags(CSV.as_bytes()).unwrap(), settings)
    }

    #[test]
    fn summary_counts_and_totals() {
        let s = report().summary;
        assert_eq!(s.dag_count, 5);
        assert_eq!(s.all_dag_count, 7);
        assert_eq!(s.removed_count, 2);
        assert_eq!(s.removed_missing_count, 1);
        assert_eq!(s.parsed_count, 4);
        assert_eq!(s.missing_count, 1);
        assert_eq!(s.total_parse_seconds, 100.0);
        assert_eq!(s.estimated_loop_seconds, 50.0);
        assert_eq!(s.over_timeout_count, 2);
        assert_eq!(s.overdue_count, 1);
        assert_eq!(s.max, 57.0);
        let r = report();
        let rank = |id: &str| {
            r.dags
                .iter()
                .find(|d| d.dag_id == id)
                .unwrap()
                .parse_rank_pct
        };
        assert_eq!(rank("d-sit"), Some(100.0));
        assert_eq!(rank("a-dev"), Some(25.0));
        assert_eq!(rank("f-sit"), None);
    }

    #[test]
    fn rankings_are_truncated_and_ordered() {
        let r = report();
        let ids = |idx: &[usize]| {
            idx.iter()
                .map(|&i| r.dags[i].dag_id.as_str())
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(&r.slowest), ["d-sit", "c-sit"]);
        assert_eq!(ids(&r.churn), ["b-dev", "d-sit"]);
        assert_eq!(ids(&r.overdue), ["c-sit"]);
        assert_eq!(ids(&r.removed), ["g-dev", "f-sit"]);
    }

    #[test]
    fn pareto_shares() {
        let p = report().pareto;
        let rounded: Vec<f64> = p.cumulative_share.iter().map(|s| s.round()).collect();
        assert_eq!(rounded, [57.0, 97.0, 99.0, 100.0]);
        assert_eq!(p.dags_for_50, 1);
        assert_eq!(p.dags_for_80, 2);
        assert!((p.top_10pct_share - 57.0).abs() < 1e-9);
    }

    #[test]
    fn histogram_covers_every_parsed_dag() {
        let bins = report().histogram;
        assert_eq!(bins.iter().map(|b| b.count).sum::<usize>(), 4);
        assert!(bins.first().unwrap().lo <= 1.0 && bins.last().unwrap().hi > 57.0);
    }

    #[test]
    fn groups_sorted_by_total() {
        let r = report();
        assert_eq!(r.by_application[0].name, "AP2-Y");
        assert_eq!(r.by_application[0].dags, 3);
        assert_eq!(r.by_application[0].parsed, 2);
        assert_eq!(r.by_application[0].total_seconds, 97.0);
        assert_eq!(r.by_application[0].missing, 1);
        assert_eq!(r.by_application[0].overdue, 1);
        assert!(r.by_application.iter().all(|g| g.name != "AP3-Z"));
        assert_eq!(r.removed_by_application[0].name, "AP3-Z");
        assert_eq!(r.removed_by_application[0].dags, 2);
    }

    const HISTORY_CSV: &str = "\
dag_id,fileloc,last_parsed_time,parse_seconds,is_stale,version_number,version_created_at
h-dev,/d/dags/AP1-X/h.py,2026-09-23 09:59:00,1.0,false,1,2026-09-17 09:00:00
h-dev,/d/dags/AP1-X/h.py,2026-09-23 09:59:00,1.0,false,2,2026-09-19 09:00:00
h-dev,/d/dags/AP1-X/h.py,2026-09-23 09:59:00,1.0,false,3,2026-09-20 09:00:00
h-dev,/d/dags/AP1-X/h.py,2026-09-23 09:59:00,1.0,false,4,2026-09-22 12:00:00
h-dev,/d/dags/AP1-X/h.py,2026-09-23 09:59:00,1.0,false,5,2026-09-23 09:00:00
h-dev,/d/dags/AP1-X/h.py,2026-09-23 09:59:00,1.0,false,6,2026-09-23 09:58:30
i-dev,/d/dags/AP1-X/i.py,2026-09-23 10:00:00,2.0,false,1,2026-01-01 00:00:00
j-dev,/d/dags/AP1-X/j.py,2026-09-23 08:05:00,2.0,false,1,2026-01-01 00:00:00
k-dev,/d/dags/AP1-X/k.py,2026-09-23 09:00:00,2.0,true,1,2026-09-23 09:00:00
k-dev,/d/dags/AP1-X/k.py,2026-09-23 09:00:00,2.0,true,2,2026-09-23 09:01:00
k-dev,/d/dags/AP1-X/k.py,2026-09-23 09:00:00,2.0,true,3,2026-09-23 09:02:00
";

    fn history_report() -> DagProcessorReport {
        let settings = Settings {
            top_n: 10,
            parsing_processes: 1,
            import_timeout: 600.0,
            file_processor_timeout: 600.0,
            overdue_hours: 24.0,
            parsing_processes_assumed: false,
            import_timeout_assumed: false,
            file_processor_timeout_assumed: false,
        };
        analyze(&read_dags(HISTORY_CSV.as_bytes()).unwrap(), settings)
    }

    #[test]
    fn version_activity_and_suspects() {
        let r = history_report();
        let h = r.dags.iter().find(|d| d.dag_id == "h-dev").unwrap();
        assert_eq!(h.versions_7d, 6);
        assert_eq!(h.versions_24h, 3);
        assert_eq!(h.weekend_versions_7d, 2);
        assert!(h.changed_at_last_parse);
        assert!(h.suspect);
        assert!((h.hours_since_change.unwrap() - 0.025).abs() < 0.01);
        assert!(r.has_version_history);

        let i = r.dags.iter().find(|d| d.dag_id == "i-dev").unwrap();
        assert!(!i.changed_at_last_parse && !i.suspect);
        // k-dev changes often but is removed, so it is never a suspect.
        let suspects: Vec<&str> = r
            .suspects
            .iter()
            .map(|&x| r.dags[x].dag_id.as_str())
            .collect();
        assert_eq!(suspects, ["h-dev"]);
        assert_eq!(r.summary.suspect_count, 1);
        assert_eq!(r.summary.changed_at_last_parse_count, 1);
        assert_eq!(r.summary.unchanged_at_last_parse_count, 2);
    }

    #[test]
    fn observed_loop_and_activity() {
        let r = history_report();
        let o = &r.observed_loop;
        assert_eq!(o.sample, 3);
        assert_eq!(o.parsed_last_hour, 2);
        assert_eq!(o.loop_from_rate_hours, Some(1.5));
        assert!((o.age_p99_hours - 1.9).abs() < 0.05);

        let next = |id: &str| {
            r.dags
                .iter()
                .find(|d| d.dag_id == id)
                .unwrap()
                .next_parse_by
                .clone()
        };
        let i_next = next("i-dev").unwrap();
        assert!(i_next.starts_with("2026-09-23 11:5"), "{i_next}");
        assert_eq!(next("k-dev"), None);

        // From the oldest recent parse (08:05 → 08:00 bin) up to the reference (10:00).
        assert_eq!(r.parse_activity.len(), 13);
        assert_eq!(r.parse_activity[0].start, "08:00");
        assert_eq!(r.parse_activity.last().unwrap().start, "10:00");
        assert_eq!(r.parse_activity.iter().map(|b| b.count).sum::<usize>(), 3);
    }

    #[test]
    fn freshness_buckets() {
        let f = report().freshness;
        let count = |label: &str| f.iter().find(|b| b.label == label).map(|b| b.count);
        assert_eq!(count("< 1 h"), Some(3));
        assert_eq!(count("1–7 d"), Some(1));
        assert_eq!(count("Never parsed"), Some(1));
    }
}
