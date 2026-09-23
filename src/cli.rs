use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    about = "Generate a static HTML report from Airflow metadata exports",
    args_override_self = true
)]
pub struct Args {
    /// DAG processor export (dag + dag_version join)
    #[arg(long, default_value = "dataset/DAG_processing_time.csv")]
    pub input: PathBuf,

    #[arg(long, default_value = "report/dag_processor_report.html")]
    pub output: PathBuf,

    /// Number of DAGs / applications shown in ranked charts
    #[arg(long, default_value_t = 30)]
    pub top_n: usize,

    /// Airflow `[dag_processor] parsing_processes`; Airflow's default (2) is assumed when omitted
    #[arg(long)]
    pub parsing_processes: Option<u32>,

    /// Airflow `[core] dagbag_import_timeout` in seconds; Airflow's default (30) is assumed when omitted
    #[arg(long)]
    pub import_timeout: Option<f64>,

    /// Airflow `[dag_processor] dag_file_processor_timeout` in seconds; Airflow's default (50) is assumed when omitted
    #[arg(long)]
    pub file_processor_timeout: Option<f64>,

    /// Active DAGs not re-parsed for longer than this (relative to the newest parse) are flagged overdue
    #[arg(long, alias = "stale-hours", default_value_t = 24.0)]
    pub overdue_hours: f64,
}
