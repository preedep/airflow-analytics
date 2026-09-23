#!/bin/sh
# Build and generate the static HTML report.
# Override defaults via env vars, e.g.:
#   PARSING_PROCESSES=16 IMPORT_TIMEOUT=480 ./generate-report.sh
# Extra arguments are passed through to the binary; --open opens the report afterwards.
set -eu

cd "$(dirname "$0")"

INPUT="${INPUT:-dataset/DAG_processing_time.csv}"
OUTPUT="${OUTPUT:-report/dag_processor_report.html}"
# dev/sit server config (confirmed 2026-09-23):
#   dagbag_import_timeout=600, dag_file_processor_timeout=600,
#   parsing_processes=16 per dag-processor pod x 2 pods = 32
IMPORT_TIMEOUT="${IMPORT_TIMEOUT:-600}"
FILE_PROCESSOR_TIMEOUT="${FILE_PROCESSOR_TIMEOUT:-600}"
PARSING_PROCESSES="${PARSING_PROCESSES:-32}"
TOP_N="${TOP_N:-30}"
OVERDUE_HOURS="${OVERDUE_HOURS:-${STALE_HOURS:-24}}"

open_report=false
for arg in "$@"; do
  shift
  if [ "$arg" = "--open" ]; then
    open_report=true
  else
    set -- "$@" "$arg"
  fi
done

set -- --import-timeout "$IMPORT_TIMEOUT" \
  --file-processor-timeout "$FILE_PROCESSOR_TIMEOUT" \
  --parsing-processes "$PARSING_PROCESSES" "$@"

if [ ! -f "$INPUT" ]; then
  echo "error: input not found: $INPUT" >&2
  exit 1
fi

cargo build --release --quiet

./target/release/airflow-analytics \
  --input "$INPUT" \
  --output "$OUTPUT" \
  --top-n "$TOP_N" \
  --overdue-hours "$OVERDUE_HOURS" \
  "$@"

if [ "$open_report" = true ]; then
  if command -v open >/dev/null 2>&1; then
    open "$OUTPUT"
  elif command -v xdg-open >/dev/null 2>&1; then
    xdg-open "$OUTPUT"
  else
    echo "Report: $OUTPUT"
  fi
fi
