#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
API_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${API_DIR}/.." && pwd)"
SEMANTIC_SCRIPT="${SCRIPT_DIR}/semantic_query_cache_benchmark.sh"
SUREFIRE_DIR="${API_DIR}/target/surefire-reports"
BENCHMARK_DIR="${API_DIR}/benchmark-reports"

usage() {
  cat <<'EOF'
Usage:
  run_backend_checks.sh run
  run_backend_checks.sh run-live
  run_backend_checks.sh reports
  run_backend_checks.sh latest-benchmark
  run_backend_checks.sh help

Commands:
  run               Run the backend Maven test suite, then print a clean summary.
  run-live          Run the backend Maven test suite, print the summary, then run the live Gemini semantic benchmark.
  reports           Print the clean backend test summary from surefire reports.
  latest-benchmark  Render the latest saved semantic benchmark report, if one exists.
EOF
}

require_file() {
  local file_path="$1"
  if [[ ! -f "${file_path}" ]]; then
    printf 'Required file not found: %s\n' "${file_path}" >&2
    exit 1
  fi
}

latest_benchmark_json() {
  if [[ ! -d "${BENCHMARK_DIR}" ]]; then
    return 1
  fi

  find "${BENCHMARK_DIR}" -maxdepth 1 -type f -name 'semantic-query-cache-*.json' | sort | tail -n 1
}

app_reachable() {
  local base_url="${BASE_URL:-http://localhost:8080}"
  curl -fsS "${base_url}/api/query/query-cache/metrics" >/dev/null 2>&1
}

run_checks() {
  require_file "${API_DIR}/mvnw"
  require_file "${SEMANTIC_SCRIPT}"

  printf 'Running backend Maven test suite...\n\n'
  (
    cd "${API_DIR}"
    ./mvnw test
  )

  printf '\nRendering clean backend test summary...\n\n'
  "${SEMANTIC_SCRIPT}" tests
}

run_live_checks() {
  require_file "${API_DIR}/mvnw"
  require_file "${SEMANTIC_SCRIPT}"

  run_checks

  printf '\nPreparing live Gemini semantic benchmark...\n\n'
  if ! app_reachable; then
    printf 'Backend app is not reachable at %s, so the live benchmark was skipped.\n' "${BASE_URL:-http://localhost:8080}" >&2
    printf 'Start the backend app, then run: %s run-live\n' "${SCRIPT_DIR}/run_backend_checks.sh" >&2
    exit 1
  fi

  "${SEMANTIC_SCRIPT}" run
}

show_reports() {
  require_file "${SEMANTIC_SCRIPT}"
  "${SEMANTIC_SCRIPT}" tests

  printf '\nSurefire reports directory: %s\n' "${SUREFIRE_DIR}"
  if [[ -d "${BENCHMARK_DIR}" ]]; then
    printf 'Benchmark reports directory: %s\n' "${BENCHMARK_DIR}"
  fi
}

show_latest_benchmark() {
  require_file "${SEMANTIC_SCRIPT}"

  local latest_json
  latest_json="$(latest_benchmark_json || true)"
  if [[ -z "${latest_json}" ]]; then
    printf 'No semantic benchmark JSON reports found in %s\n' "${BENCHMARK_DIR}" >&2
    exit 1
  fi

  "${SEMANTIC_SCRIPT}" report "${latest_json}"
}

main() {
  local command="${1:-run}"
  case "${command}" in
    run)
      run_checks
      ;;
    run-live)
      run_live_checks
      ;;
    reports)
      show_reports
      ;;
    latest-benchmark)
      show_latest_benchmark
      ;;
    help|-h|--help)
      usage
      ;;
    *)
      printf 'Unknown command: %s\n\n' "${command}" >&2
      usage
      exit 1
      ;;
  esac
}

main "$@"
