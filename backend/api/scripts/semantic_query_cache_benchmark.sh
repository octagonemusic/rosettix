#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
API_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPORT_DIR_DEFAULT="${API_DIR}/benchmark-reports"
BASE_URL="${BASE_URL:-http://localhost:8080}"
POSTGRES_URI="${POSTGRES_URI:-postgresql://postgres:root@localhost:5432/postgres}"
MONGO_URI="${MONGO_URI:-mongodb://localhost:27017/test}"
REQUESTS_PER_MINUTE_LIMIT="${REQUESTS_PER_MINUTE_LIMIT:-10}"
THROTTLE_SECONDS="${THROTTLE_SECONDS:-7}"
MAX_RETRIES="${MAX_RETRIES:-3}"

usage() {
  cat <<'EOF'
Usage:
  semantic_query_cache_benchmark.sh run
  semantic_query_cache_benchmark.sh live
  semantic_query_cache_benchmark.sh report <benchmark-json>
  semantic_query_cache_benchmark.sh tests

Environment overrides:
  BASE_URL                    App base URL (default: http://localhost:8080)
  POSTGRES_URI                PostgreSQL URI for schema discovery
  MONGO_URI                   MongoDB URI for collection discovery
  REQUESTS_PER_MINUTE_LIMIT   Gemini request budget to respect (default: 10)
  THROTTLE_SECONDS            Delay between LLM-backed requests (default: 7)
  MAX_RETRIES                 Retries on Gemini 429 responses (default: 3)

Notes:
  - The `run` workload uses 6 LLM-backed requests total: 3 for PostgreSQL and 3 for MongoDB.
  - The script prints a formatted report and saves both JSON and Markdown outputs.
EOF
}

require_cmd() {
  local missing=()
  for cmd in "$@"; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
      missing+=("$cmd")
    fi
  done
  if ((${#missing[@]} > 0)); then
    printf 'Missing required command(s): %s\n' "${missing[*]}" >&2
    exit 1
  fi
}

ensure_report_dir() {
  mkdir -p "${REPORT_DIR_DEFAULT}"
}

check_app() {
  curl -fsS "${BASE_URL}/api/query/query-cache/metrics" >/dev/null
}

reset_query_cache() {
  curl -fsS -X POST "${BASE_URL}/api/query/query-cache/reset"
}

fetch_query_cache_metrics() {
  curl -fsS "${BASE_URL}/api/query/query-cache/metrics"
}

fetch_schema_cache_metrics() {
  curl -fsS "${BASE_URL}/api/query/schema-cache/metrics"
}

discover_postgres_table() {
  psql "${POSTGRES_URI}" -Atqc \
    "select tablename from pg_tables where schemaname='public' order by case when tablename='users' then 0 else 1 end, tablename limit 1;"
}

discover_mongo_collection() {
  mongosh "${MONGO_URI}" --quiet --eval '
    const collections = db.getCollectionNames().sort((left, right) => {
      const preferred = (name) => name === "products" ? 0 : 1;
      return preferred(left) - preferred(right) || left.localeCompare(right);
    });
    if (!collections.length) {
      quit(2);
    }
    print(collections[0]);
  '
}

extract_retry_seconds() {
  local message="$1"
  local seconds
  seconds="$(printf '%s' "${message}" | sed -n 's/.*Please retry in \([0-9][0-9]*\)\(\.[0-9]*\)\?s\..*/\1/p' | head -n 1)"
  if [[ -z "${seconds}" ]]; then
    seconds=60
  fi
  printf '%s\n' "${seconds}"
}

ms_between() {
  local start_ns="$1"
  local end_ns="$2"
  awk -v start="${start_ns}" -v end="${end_ns}" 'BEGIN { printf "%.2f", (end - start) / 1000000 }'
}

api_query() {
  local database="$1"
  local question="$2"
  local kind="$3"
  local request_index="$4"
  local response duration_ms start_ns end_ns message retry_after result_count
  local payload
  payload="$(jq -cn --arg q "${question}" --arg db "${database}" '{question: $q, database: $db}')"

  local attempt=1
  while (( attempt <= MAX_RETRIES )); do
    start_ns="$(date +%s%N)"
    response="$(curl -sS -X POST "${BASE_URL}/api/query" \
      -H 'Content-Type: application/json' \
      -d "${payload}")"
    end_ns="$(date +%s%N)"
    duration_ms="$(ms_between "${start_ns}" "${end_ns}")"

    if printf '%s' "${response}" | jq -e '.results' >/dev/null 2>&1; then
      result_count="$(printf '%s' "${response}" | jq '.results | length')"
      jq -cn \
        --arg strategy "${database}" \
        --arg kind "${kind}" \
        --arg question "${question}" \
        --argjson duration_ms "${duration_ms}" \
        --argjson result_count "${result_count}" \
        --argjson request_index "${request_index}" \
        '{
          strategy: $strategy,
          kind: $kind,
          question: $question,
          duration_ms: $duration_ms,
          result_count: $result_count,
          request_index: $request_index
        }'
      return 0
    fi

    message="$(printf '%s' "${response}" | jq -r '.message // .error // "Unknown error"')"
    if [[ "${message}" == *"429 Too Many Requests"* ]] && (( attempt < MAX_RETRIES )); then
      retry_after="$(extract_retry_seconds "${message}")"
      printf 'Gemini quota hit during %s %s request. Waiting %ss before retry %s/%s...\n' \
        "${database}" "${kind}" "${retry_after}" "${attempt}" "${MAX_RETRIES}" >&2
      sleep "${retry_after}"
      attempt=$((attempt + 1))
      continue
    fi

    printf 'Request failed for database %s (%s).\nQuestion: %s\nResponse: %s\n' \
      "${database}" "${kind}" "${question}" "${response}" >&2
    return 1
  done

  printf 'Exceeded retry budget for database %s (%s).\n' "${database}" "${kind}" >&2
  return 1
}

metric_delta() {
  local report_file="$1"
  local strategy="$2"
  local field="$3"
  jq -r --arg strategy "${strategy}" --arg field "${field}" '
    (((.metrics_after.strategies[$strategy] // {})[$field] // 0) - ((.metrics_before.strategies[$strategy] // {})[$field] // 0))
  ' "${report_file}"
}

metric_after() {
  local report_file="$1"
  local strategy="$2"
  local field="$3"
  jq -r --arg strategy "${strategy}" --arg field "${field}" '
    (((.metrics_after // .).strategies[$strategy] // {})[$field] // null)
  ' "${report_file}"
}

request_duration() {
  local report_file="$1"
  local strategy="$2"
  local kind="$3"
  jq -r --arg strategy "${strategy}" --arg kind "${kind}" '
    (.requests[] | select(.strategy == $strategy and .kind == $kind) | .duration_ms) // null
  ' "${report_file}"
}

request_avg() {
  local report_file="$1"
  local strategy="$2"
  jq -r --arg strategy "${strategy}" '
    [ .requests[] | select(.strategy == $strategy) | .duration_ms ] as $values
    | if ($values | length) == 0 then null else (($values | add) / ($values | length)) end
  ' "${report_file}"
}

format_number() {
  local value="$1"
  if [[ "${value}" == "null" || -z "${value}" ]]; then
    printf 'n/a'
  else
    awk -v value="${value}" 'BEGIN { printf "%.3f", value }'
  fi
}

emit_benchmark_report() {
  local report_file="$1"
  local output_file="${2:-}"
  local postgres_table mongo_collection throttle timestamp
  postgres_table="$(jq -r '.workload.postgres.target' "${report_file}")"
  mongo_collection="$(jq -r '.workload.mongodb.target' "${report_file}")"
  throttle="$(jq -r '.throttle_seconds' "${report_file}")"
  timestamp="$(jq -r '.captured_at' "${report_file}")"

  printf 'Semantic Query Cache Benchmark\n'
  printf '==============================\n\n'
  printf 'Captured at: %s\n' "${timestamp}"
  printf 'Base URL: %s\n' "$(jq -r '.base_url' "${report_file}")"
  printf 'Gemini throttle: %ss between requests (budget %s req/min)\n\n' \
    "${throttle}" "$(jq -r '.requests_per_minute_limit' "${report_file}")"

  printf 'Workload\n'
  printf '%s\n' '--------'
  printf -- '- PostgreSQL target table: %s\n' "${postgres_table}"
  printf -- '- MongoDB target collection: %s\n' "${mongo_collection}"
  printf '%s\n\n' '- Request pattern per strategy: seed miss -> semantic paraphrase -> exact repeat'

  printf 'Query Cache Delta\n'
  printf '%s\n' '-----------------'
  printf '| Strategy | Misses | Semantic Hits | Exact Hits | Exact Writes | Semantic Writes | Hit Rate %% | Avg Semantic Similarity | Lookup Failures | Stale Evictions |\n'
  printf '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n'
  for strategy in postgres mongodb; do
    printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' \
      "${strategy}" \
      "$(metric_delta "${report_file}" "${strategy}" cache_misses)" \
      "$(metric_delta "${report_file}" "${strategy}" semantic_hits)" \
      "$(metric_delta "${report_file}" "${strategy}" exact_hits)" \
      "$(metric_delta "${report_file}" "${strategy}" exact_writes)" \
      "$(metric_delta "${report_file}" "${strategy}" semantic_writes)" \
      "$(format_number "$(metric_after "${report_file}" "${strategy}" hit_rate_percent)")" \
      "$(format_number "$(metric_after "${report_file}" "${strategy}" avg_semantic_similarity)")" \
      "$(metric_delta "${report_file}" "${strategy}" lookup_failures)" \
      "$(metric_delta "${report_file}" "${strategy}" stale_index_evictions)"
  done
  printf '\n'

  printf 'Observed API Timings\n'
  printf '%s\n' '--------------------'
  printf '| Strategy | Seed Miss | Semantic Reuse | Exact Reuse | Avg Across 3 Requests |\n'
  printf '| --- | ---: | ---: | ---: | ---: |\n'
  for strategy in postgres mongodb; do
    printf '| %s | %s ms | %s ms | %s ms | %s ms |\n' \
      "${strategy}" \
      "$(format_number "$(request_duration "${report_file}" "${strategy}" seed)")" \
      "$(format_number "$(request_duration "${report_file}" "${strategy}" semantic)")" \
      "$(format_number "$(request_duration "${report_file}" "${strategy}" exact)")" \
      "$(format_number "$(request_avg "${report_file}" "${strategy}")")"
  done
  printf '\n'

  printf 'Leadership Summary\n'
  printf '%s\n' '------------------'
  printf -- '- Cache misses observed: postgres=%s, mongodb=%s\n' \
    "$(metric_delta "${report_file}" postgres cache_misses)" \
    "$(metric_delta "${report_file}" mongodb cache_misses)"
  printf -- '- Semantic cache reuses observed: postgres=%s, mongodb=%s\n' \
    "$(metric_delta "${report_file}" postgres semantic_hits)" \
    "$(metric_delta "${report_file}" mongodb semantic_hits)"
  printf -- '- Exact cache reuses observed: postgres=%s, mongodb=%s\n' \
    "$(metric_delta "${report_file}" postgres exact_hits)" \
    "$(metric_delta "${report_file}" mongodb exact_hits)"
  printf -- '- Approximate Gemini calls avoided after warm-up: %s\n' \
    "$(jq -r '
      (
        (((.metrics_after.strategies.postgres // {}).semantic_hits // 0) - ((.metrics_before.strategies.postgres // {}).semantic_hits // 0)) +
        (((.metrics_after.strategies.postgres // {}).exact_hits // 0) - ((.metrics_before.strategies.postgres // {}).exact_hits // 0)) +
        (((.metrics_after.strategies.mongodb // {}).semantic_hits // 0) - ((.metrics_before.strategies.mongodb // {}).semantic_hits // 0)) +
        (((.metrics_after.strategies.mongodb // {}).exact_hits // 0) - ((.metrics_before.strategies.mongodb // {}).exact_hits // 0))
      )
    ' "${report_file}")"
  printf '\n'

  printf 'Files\n'
  printf '%s\n' '-----'
  printf -- '- Raw benchmark JSON: %s\n' "${report_file}"
  if [[ -n "${output_file}" ]]; then
    printf -- '- Formatted Markdown report: %s\n' "${output_file}"
  fi
}

render_benchmark_report() {
  local report_file="$1"
  local output_file="${2:-}"
  if [[ -n "${output_file}" ]]; then
    emit_benchmark_report "${report_file}" "${output_file}" | tee "${output_file}"
  else
    emit_benchmark_report "${report_file}"
  fi
}

render_live_metrics() {
  local metrics_file
  metrics_file="$(mktemp)"
  fetch_query_cache_metrics > "${metrics_file}"

  printf 'Live Query Cache Metrics\n'
  printf '========================\n\n'
  printf 'Captured at: %s\n\n' "$(jq -r '.timestamp' "${metrics_file}")"
  printf '| Strategy | Exact Hits | Semantic Hits | Misses | Bypasses | Hit Rate %% | Avg Semantic Similarity | Lookup Failures |\n'
  printf '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n'
  for strategy in $(jq -r '.strategies | keys[]?' "${metrics_file}"); do
    printf '| %s | %s | %s | %s | %s | %s | %s | %s |\n' \
      "${strategy}" \
      "$(metric_after "${metrics_file}" "${strategy}" exact_hits)" \
      "$(metric_after "${metrics_file}" "${strategy}" semantic_hits)" \
      "$(metric_after "${metrics_file}" "${strategy}" cache_misses)" \
      "$(metric_after "${metrics_file}" "${strategy}" cache_bypasses)" \
      "$(format_number "$(metric_after "${metrics_file}" "${strategy}" hit_rate_percent)")" \
      "$(format_number "$(metric_after "${metrics_file}" "${strategy}" avg_semantic_similarity)")" \
      "$(metric_after "${metrics_file}" "${strategy}" lookup_failures)"
  done

  rm -f "${metrics_file}"
}

render_test_summary() {
  local report_dir="${API_DIR}/target/surefire-reports"
  if [[ ! -d "${report_dir}" ]]; then
    printf 'No surefire reports found at %s\n' "${report_dir}" >&2
    exit 1
  fi

  printf 'Backend Test Summary\n'
  printf '====================\n\n'
  printf '| Suite | Tests | Failures | Errors | Skipped | Time (s) |\n'
  printf '| --- | ---: | ---: | ---: | ---: | ---: |\n'

  local total_tests=0 total_failures=0 total_errors=0 total_skipped=0
  local suite tests failures errors skipped elapsed

  while IFS= read -r report; do
    suite="$(basename "${report}" .txt)"
    tests="$(sed -n 's/.*Tests run: \([0-9]*\), Failures: \([0-9]*\), Errors: \([0-9]*\), Skipped: \([0-9]*\), Time elapsed: \([0-9.]*\).*/\1/p' "${report}")"
    if [[ -z "${tests}" ]]; then
      continue
    fi
    failures="$(sed -n 's/.*Tests run: \([0-9]*\), Failures: \([0-9]*\), Errors: \([0-9]*\), Skipped: \([0-9]*\), Time elapsed: \([0-9.]*\).*/\2/p' "${report}")"
    errors="$(sed -n 's/.*Tests run: \([0-9]*\), Failures: \([0-9]*\), Errors: \([0-9]*\), Skipped: \([0-9]*\), Time elapsed: \([0-9.]*\).*/\3/p' "${report}")"
    skipped="$(sed -n 's/.*Tests run: \([0-9]*\), Failures: \([0-9]*\), Errors: \([0-9]*\), Skipped: \([0-9]*\), Time elapsed: \([0-9.]*\).*/\4/p' "${report}")"
    elapsed="$(sed -n 's/.*Tests run: \([0-9]*\), Failures: \([0-9]*\), Errors: \([0-9]*\), Skipped: \([0-9]*\), Time elapsed: \([0-9.]*\).*/\5/p' "${report}")"

    total_tests=$((total_tests + tests))
    total_failures=$((total_failures + failures))
    total_errors=$((total_errors + errors))
    total_skipped=$((total_skipped + skipped))

    printf '| %s | %s | %s | %s | %s | %s |\n' \
      "${suite}" "${tests}" "${failures}" "${errors}" "${skipped}" "${elapsed}"
  done < <(find "${report_dir}" -maxdepth 1 -name '*.txt' | sort)

  printf '| **Total** | **%s** | **%s** | **%s** | **%s** | n/a |\n' \
    "${total_tests}" "${total_failures}" "${total_errors}" "${total_skipped}"
  printf '\nRaw reports live in %s\n' "${report_dir}"
}
before_metrics_file="" 
after_metrics_file="" 
schema_metrics_file="" 
requests_file=""
run_benchmark() {
  require_cmd curl jq psql mongosh awk sed
  check_app
  ensure_report_dir
  
  local postgres_table mongo_collection timestamp report_base raw_file md_file
  
  before_metrics_file="$(mktemp)"
  after_metrics_file="$(mktemp)"
  schema_metrics_file="$(mktemp)"
  requests_file="$(mktemp)"
  trap 'rm -f "${before_metrics_file}" "${after_metrics_file}" "${schema_metrics_file}" "${requests_file}"' EXIT

  if (( THROTTLE_SECONDS * REQUESTS_PER_MINUTE_LIMIT < 60 )); then
    printf 'Warning: throttle=%ss may exceed the configured %s req/min budget.\n' \
      "${THROTTLE_SECONDS}" "${REQUESTS_PER_MINUTE_LIMIT}" >&2
  fi

  printf 'Checking application and semantic query-cache endpoints...\n\n'
  printf 'Discovering current PostgreSQL tables...\n'
  postgres_table="$(discover_postgres_table)"
  if [[ -z "${postgres_table}" ]]; then
    printf 'No PostgreSQL table discovered from %s\n' "${POSTGRES_URI}" >&2
    exit 1
  fi

  printf '\nDiscovering current MongoDB collections...\n'
  mongo_collection="$(discover_mongo_collection)"
  if [[ -z "${mongo_collection}" ]]; then
    printf 'No MongoDB collection discovered from %s\n' "${MONGO_URI}" >&2
    exit 1
  fi

  printf '\nResetting query-cache metrics for a clean semantic benchmark...\n'
  reset_query_cache >/dev/null
  fetch_query_cache_metrics > "${before_metrics_file}"
  fetch_schema_cache_metrics > "${schema_metrics_file}"

  local postgres_seed postgres_semantic mongo_seed mongo_semantic
  postgres_seed="Show me the first 5 rows from the ${postgres_table} table"
  postgres_semantic="List the first five records from the ${postgres_table} table"
  mongo_seed="Show me the first 5 documents from the ${mongo_collection} collection"
  mongo_semantic="List the first five records from the ${mongo_collection} collection"

  local request_index=1
  printf '\nRunning PostgreSQL semantic-cache sequence against table `%s`...\n' "${postgres_table}"
  api_query postgres "${postgres_seed}" seed "${request_index}" >> "${requests_file}"
  request_index=$((request_index + 1))
  sleep "${THROTTLE_SECONDS}"
  api_query postgres "${postgres_semantic}" semantic "${request_index}" >> "${requests_file}"
  request_index=$((request_index + 1))
  sleep "${THROTTLE_SECONDS}"
  api_query postgres "${postgres_semantic}" exact "${request_index}" >> "${requests_file}"
  request_index=$((request_index + 1))
  sleep "${THROTTLE_SECONDS}"

  printf '\nRunning MongoDB semantic-cache sequence against collection `%s`...\n' "${mongo_collection}"
  api_query mongodb "${mongo_seed}" seed "${request_index}" >> "${requests_file}"
  request_index=$((request_index + 1))
  sleep "${THROTTLE_SECONDS}"
  api_query mongodb "${mongo_semantic}" semantic "${request_index}" >> "${requests_file}"
  request_index=$((request_index + 1))
  sleep "${THROTTLE_SECONDS}"
  api_query mongodb "${mongo_semantic}" exact "${request_index}" >> "${requests_file}"

  printf '\nFetching query-cache metrics after workload...\n'
  fetch_query_cache_metrics > "${after_metrics_file}"

  timestamp="$(date '+%Y%m%d-%H%M%S')"
  report_base="${REPORT_DIR_DEFAULT}/semantic-query-cache-${timestamp}"
  raw_file="${report_base}.json"
  md_file="${report_base}.md"

  jq -n \
    --arg captured_at "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    --arg base_url "${BASE_URL}" \
    --arg postgres_table "${postgres_table}" \
    --arg mongo_collection "${mongo_collection}" \
    --arg postgres_seed "${postgres_seed}" \
    --arg postgres_semantic "${postgres_semantic}" \
    --arg mongo_seed "${mongo_seed}" \
    --arg mongo_semantic "${mongo_semantic}" \
    --argjson requests_per_minute_limit "${REQUESTS_PER_MINUTE_LIMIT}" \
    --argjson throttle_seconds "${THROTTLE_SECONDS}" \
    --slurpfile metrics_before "${before_metrics_file}" \
    --slurpfile metrics_after "${after_metrics_file}" \
    --slurpfile schema_metrics "${schema_metrics_file}" \
    --slurpfile requests "${requests_file}" \
    '{
      captured_at: $captured_at,
      base_url: $base_url,
      requests_per_minute_limit: $requests_per_minute_limit,
      throttle_seconds: $throttle_seconds,
      workload: {
        postgres: {
          target: $postgres_table,
          seed_question: $postgres_seed,
          semantic_question: $postgres_semantic
        },
        mongodb: {
          target: $mongo_collection,
          seed_question: $mongo_seed,
          semantic_question: $mongo_semantic
        }
      },
      requests: $requests,
      metrics_before: $metrics_before[0],
      metrics_after: $metrics_after[0],
      schema_cache_snapshot_before: $schema_metrics[0]
    }' > "${raw_file}"

  printf '\n'
  render_benchmark_report "${raw_file}" "${md_file}"
}

main() {
  local command="${1:-run}"
  case "${command}" in
    run)
      run_benchmark
      ;;
    live)
      require_cmd curl jq
      check_app
      render_live_metrics
      ;;
    report)
      require_cmd jq awk
      if [[ $# -lt 2 ]]; then
        printf 'Please provide a benchmark JSON file.\n' >&2
        exit 1
      fi
      render_benchmark_report "$2"
      ;;
    tests)
      render_test_summary
      ;;
    -h|--help|help)
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
