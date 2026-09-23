use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;

use crate::analysis::dag_processor::DagProcessorReport;

const TEMPLATE: &str = include_str!("template.html");
const ECHARTS: &str = include_str!("../../assets/echarts.min.js");

#[derive(Serialize)]
struct ReportData<'a> {
    source: String,
    dag_processor: &'a DagProcessorReport,
}

pub fn write_report(
    source: &Path,
    dag_processor: &DagProcessorReport,
    output: &Path,
) -> Result<()> {
    let html = render(source, dag_processor)?;
    if let Some(parent) = output.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    std::fs::write(output, html).with_context(|| format!("writing {}", output.display()))
}

fn render(source: &Path, dag_processor: &DagProcessorReport) -> Result<String> {
    let data = ReportData {
        source: source
            .file_name()
            .map_or_else(String::new, |n| n.to_string_lossy().into_owned()),
        dag_processor,
    };
    let json = serde_json::to_string(&data).context("serializing report data")?;

    Ok(TEMPLATE
        .replace("/*__ECHARTS__*/", ECHARTS)
        .replace("\"__DATA__\"", &escape_for_script(&json)))
}

/// `<` only occurs inside JSON strings, where `\u003c` is equivalent; this keeps
/// DAG ids like `</script>` from terminating the embedding tag.
fn escape_for_script(json: &str) -> String {
    json.replace('<', "\\u003c")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_script_terminators() {
        assert_eq!(
            escape_for_script(r#"{"id":"</script>"}"#),
            r#"{"id":"\u003c/script>"}"#
        );
    }
}
