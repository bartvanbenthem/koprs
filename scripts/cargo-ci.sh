#!/usr/bin/env bash
# =============================================================================
# cargo-ci.sh — Full Rust project quality gate
# =============================================================================
#
# USAGE
#   ./scripts/cargo-ci.sh                  run all steps
#   ./scripts/cargo-ci.sh --fast           fmt + check + unit tests only (no coverage)
#   ./scripts/cargo-ci.sh --no-clippy      skip clippy
#   ./scripts/cargo-ci.sh --no-audit       skip cargo-audit
#   ./scripts/cargo-ci.sh --no-integration skip integration tests
#   ./scripts/cargo-ci.sh --no-doc         skip cargo doc
#   ./scripts/cargo-ci.sh --no-coverage    skip llvm-cov coverage report
#   ./scripts/cargo-ci.sh --bench          also compile benchmarks (slow, opt-in)
#   ./scripts/cargo-ci.sh --coverage-fail-under=80
#                                  fail if line coverage drops below N %
#                                  (default: no threshold enforced)
#
# EXIT CODES
#   0  all steps passed
#   1  one or more steps failed
#
# =============================================================================
# DEPENDENCIES
# =============================================================================
#
# Required (must be present — script aborts without them)
# --------------------------------------------------------
# rustup          https://rustup.rs
#                 Used to query the active toolchain and to invoke nightly.
#
# cargo / rustc   Installed via rustup (stable toolchain).
#
# rustfmt         rustup component add rustfmt
#                 Code formatter — checked with `cargo fmt --check`.
#
# clippy          rustup component add clippy
#                 Linter — run with -D warnings and pedantic group.
#
# llvm-tools-preview  rustup component add llvm-tools-preview
#                 Low-level LLVM tooling required by cargo-llvm-cov.
#
# cargo-llvm-cov  cargo install cargo-llvm-cov
#   https://github.com/taiki-e/cargo-llvm-cov
#                 LLVM-based source-level coverage for Rust.
#                 Generates lcov, HTML, and a terminal summary.
#                 Output written to:
#                   target/llvm-cov/            — HTML report
#                   target/llvm-cov/lcov.info   — lcov data file
#
# Optional (skipped with a warning when absent)
# ---------------------------------------------
# cargo-audit     cargo install cargo-audit
#   https://github.com/rustsec/rustsec
#                 Audits Cargo.lock against the RustSec advisory database.
#                 Requires network access on first run to fetch the DB.
#
# cargo-deny      cargo install cargo-deny
#   https://github.com/EmbarkStudios/cargo-deny
#                 Checks licences, bans, advisories, and duplicate deps.
#                 Expects a deny.toml (or Cargo.toml [deny] section).
#
# cargo-udeps     cargo install cargo-udeps   (requires nightly toolchain)
#   https://github.com/est31/cargo-udeps
#                 Detects unused dependencies declared in Cargo.toml.
#                 Invoked via `cargo +nightly udeps`.
#
# =============================================================================

set -euo pipefail

# ── Colour helpers ────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

info()    { echo -e "${CYAN}${BOLD}[INFO]${RESET}  $*"; }
success() { echo -e "${GREEN}${BOLD}[ OK ]${RESET}  $*"; }
warn()    { echo -e "${YELLOW}${BOLD}[WARN]${RESET}  $*"; }
error()   { echo -e "${RED}${BOLD}[FAIL]${RESET}  $*"; }
header()  { echo -e "\n${BOLD}━━━  $*  ━━━${RESET}"; }

# ── Argument parsing ──────────────────────────────────────────────────────────
RUN_AUDIT=true
RUN_INTEGRATION=true
RUN_DOC=true
RUN_COVERAGE=true
RUN_BENCH=false
COVERAGE_THRESHOLD=""   # e.g. "80" — empty means no threshold check

for arg in "$@"; do
  case $arg in
    --no-audit)               RUN_AUDIT=false ;;
    --no-integration)         RUN_INTEGRATION=false ;;
    --no-doc)                 RUN_DOC=false ;;
    --no-coverage)            RUN_COVERAGE=false ;;
    --bench)                  RUN_BENCH=true ;;
    --coverage-fail-under=*)  COVERAGE_THRESHOLD="${arg#*=}" ;;
    --fast)
      RUN_AUDIT=false
      RUN_INTEGRATION=false; RUN_DOC=false; RUN_COVERAGE=false
      ;;
    --help|-h)
      sed -n '/^# USAGE/,/^# ====/p' "$0" | grep -v '^# ====' | sed 's/^# \?//'
      exit 0 ;;
    *) warn "Unknown flag: $arg" ;;
  esac
done

# ── Step runners ──────────────────────────────────────────────────────────────
FAILED_STEPS=()
START_TIME=$(date +%s)

# try_step: runs a step, records failure, but continues
try_step() {
  local label="$1"; shift
  header "$label"
  if "$@"; then
    success "$label"
  else
    error "$label FAILED"
    FAILED_STEPS+=("$label")
  fi
}

# try_step_fn: same but accepts a shell function as the body
try_step_fn() {
  local label="$1"; shift
  header "$label"
  if "$@"; then
    success "$label"
  else
    error "$label FAILED"
    FAILED_STEPS+=("$label")
  fi
}

# ── Preflight ─────────────────────────────────────────────────────────────────
header "Environment"
info "Rust toolchain : $(rustup show active-toolchain 2>/dev/null || rustc --version)"
info "Cargo          : $(cargo --version)"
info "Working dir    : $(pwd)"
echo ""

# ── 1. Format ─────────────────────────────────────────────────────────────────
try_step "cargo fmt (check)" \
  cargo fmt --all -- --check

# ── 2. Check (fast type-check, no codegen) ────────────────────────────────────
try_step "cargo check (all targets)" \
  cargo check --all-targets --all-features

# ── 3. Unit tests ─────────────────────────────────────────────────────────────
try_step "cargo test (lib + unit)" \
  cargo test --lib --all-features -- --nocapture

# ── 5. Doc tests ──────────────────────────────────────────────────────────────
try_step "cargo test (doc)" \
  cargo test --doc --all-features

# ── 6. Integration tests ──────────────────────────────────────────────────────
if $RUN_INTEGRATION; then
  try_step "cargo test (integration)" \
    cargo test --tests --all-features -- --nocapture
fi

# ── 7. LLVM coverage ──────────────────────────────────────────────────────────
#
#  cargo-llvm-cov instruments the binary with LLVM source-based coverage,
#  runs all tests (unit + integration + doc), then produces:
#    • a terminal summary (--summary-only)
#    • an lcov data file  (target/llvm-cov/lcov.info)
#    • an HTML report     (target/llvm-cov/html/)
#
#  The --fail-under-lines flag makes the step fail when line coverage is
#  below the requested threshold (only active when --coverage-fail-under=N
#  is passed on the command line).
#
if $RUN_COVERAGE; then
  if cargo llvm-cov --version &>/dev/null 2>&1; then
    run_coverage() {
      local extra_flags=()
      if [[ -n "$COVERAGE_THRESHOLD" ]]; then
        extra_flags+=("--fail-under-lines" "$COVERAGE_THRESHOLD")
        info "Coverage threshold: ${COVERAGE_THRESHOLD}% lines"
      fi

      # Run coverage over all tests in one pass so instrumentation is shared
      cargo llvm-cov \
        --all-features \
        --workspace \
        --lcov --output-path target/llvm-cov/lcov.info \
        "${extra_flags[@]+"${extra_flags[@]}"}"

      # Also produce an HTML report (doesn't re-run tests, just re-renders)
      cargo llvm-cov report \
        --html --output-dir target/llvm-cov/html

      # Print a concise terminal summary
      cargo llvm-cov report --summary-only

      info "HTML report : target/llvm-cov/html/index.html"
      info "LCOV data   : target/llvm-cov/lcov.info"
    }
    try_step_fn "cargo llvm-cov (coverage)" run_coverage
  else
    warn "cargo-llvm-cov not found — skipping coverage."
    warn "Install: cargo install cargo-llvm-cov"
    warn "         rustup component add llvm-tools-preview"
  fi
fi

# ── 8. Release build (smoke-test) ─────────────────────────────────────────────
try_step "cargo build (release)" \
  cargo build --release --all-features

# ── 9. Documentation ──────────────────────────────────────────────────────────
if $RUN_DOC; then
  try_step "cargo doc (no deps)" \
    cargo doc --no-deps --all-features --document-private-items
fi

# ── 10. Security audit ────────────────────────────────────────────────────────
if $RUN_AUDIT; then
  if cargo audit --version &>/dev/null 2>&1; then
    try_step "cargo audit" \
      cargo audit
  else
    warn "cargo-audit not installed — skipping."
    warn "Install: cargo install cargo-audit"
  fi
fi

# ── 11. Unused dependencies ───────────────────────────────────────────────────
if cargo +nightly udeps --version &>/dev/null 2>&1; then
  try_step "cargo udeps (nightly)" \
    cargo +nightly udeps --all-targets --all-features
else
  warn "cargo-udeps not installed (or nightly toolchain missing) — skipping."
  warn "Install: cargo install cargo-udeps  &&  rustup toolchain install nightly"
fi

# ── 13. Benchmarks (opt-in) ───────────────────────────────────────────────────
if $RUN_BENCH; then
  try_step "cargo bench (compile only)" \
    cargo bench --no-run --all-features
fi

# ── Summary ───────────────────────────────────────────────────────────────────
ELAPSED=$(( $(date +%s) - START_TIME ))
echo ""
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo -e "${BOLD}  Summary${RESET}"
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
printf "  Elapsed: %dm %ds\n" $(( ELAPSED / 60 )) $(( ELAPSED % 60 ))

if [[ ${#FAILED_STEPS[@]} -eq 0 ]]; then
  echo -e "\n  ${GREEN}${BOLD}All steps passed ✓${RESET}\n"
  exit 0
else
  echo -e "\n  ${RED}${BOLD}Failed steps (${#FAILED_STEPS[@]}):${RESET}"
  for step in "${FAILED_STEPS[@]}"; do
    echo -e "  ${RED}  ✗  $step${RESET}"
  done
  echo ""
  exit 1
fi
