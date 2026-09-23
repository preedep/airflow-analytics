mod analysis;
mod cli;
mod load;
mod model;
mod report;

use anyhow::Result;
use clap::Parser;

use analysis::dag_processor::{
    self, AIRFLOW_DEFAULT_FILE_PROCESSOR_TIMEOUT, AIRFLOW_DEFAULT_IMPORT_TIMEOUT,
    AIRFLOW_DEFAULT_PARSING_PROCESSES, Settings,
};

fn main() -> Result<()> {
    let args = cli::Args::parse();

    let dags = load::load_dags(&args.input)?;
    let settings = Settings {
        top_n: args.top_n,
        parsing_processes: args
            .parsing_processes
            .unwrap_or(AIRFLOW_DEFAULT_PARSING_PROCESSES),
        import_timeout: args
            .import_timeout
            .unwrap_or(AIRFLOW_DEFAULT_IMPORT_TIMEOUT),
        file_processor_timeout: args
            .file_processor_timeout
            .unwrap_or(AIRFLOW_DEFAULT_FILE_PROCESSOR_TIMEOUT),
        overdue_hours: args.overdue_hours,
        parsing_processes_assumed: args.parsing_processes.is_none(),
        import_timeout_assumed: args.import_timeout.is_none(),
        file_processor_timeout_assumed: args.file_processor_timeout.is_none(),
    };
    let dag_processor = dag_processor::analyze(&dags, settings);

    report::write_report(&args.input, &dag_processor, &args.output)?;
    let summary = &dag_processor.summary;
    println!(
        "Wrote {} ({} active DAGs, {} parsed, {} removed)",
        args.output.display(),
        summary.dag_count,
        summary.parsed_count,
        summary.removed_count
    );
    Ok(())
}
