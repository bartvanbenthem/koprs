#!/usr/bin/env bash
# scripts/publish.sh — publish kube-genops to crates.io
#
# Usage:
#   ./scripts/publish.sh              # full pre-flight + publish
#   ./scripts/publish.sh --dry-run    # stop before `cargo publish`
#   ./scripts/publish.sh --skip-ci    # skip CI checks, publish only

set -euo pipefail

# ── colours ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

info()    { echo -e "${CYAN}${BOLD}[publish]${RESET} $*"; }
success() { echo -e "${GREEN}${BOLD}[ok]${RESET}     $*"; }
warn()    { echo -e "${YELLOW}${BOLD}[warn]${RESET}   $*"; }
die()     { echo -e "${RED}${BOLD}[error]${RESET}  $*" >&2; exit 1; }

# ── flags ──────────────────────────────────────────────────────────────────────
DRY_RUN=false
SKIP_CI=false

for arg in "$@"; do
  case $arg in
    --dry-run)  DRY_RUN=true  ;;
    --skip-ci)  SKIP_CI=true  ;;
    --help|-h)
      echo "Usage: $0 [--dry-run] [--skip-ci]"
      echo "  --dry-run   run all checks but stop before cargo publish"
      echo "  --skip-ci   skip CI checks, go straight to packaging + publish"
      exit 0
      ;;
    *) die "unknown argument: $arg" ;;
  esac
done

# ── repo root ──────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

# ── read version from Cargo.toml ───────────────────────────────────────────────
VERSION="$(grep -m1 '^version' Cargo.toml | sed 's/.*= *"\(.*\)"/\1/')"
CRATE_NAME="$(grep -m1 '^name' Cargo.toml | sed 's/.*= *"\(.*\)"/\1/')"

info "crate   : ${CRATE_NAME}"
info "version : ${VERSION}"
echo ""

# ── 1. prerequisite checks ─────────────────────────────────────────────────────
info "checking prerequisites..."

command -v cargo  &>/dev/null || die "'cargo' not found — install Rust via rustup"
command -v git    &>/dev/null || die "'git' not found"

# Logged in to crates.io?
if ! cargo owner --list "${CRATE_NAME}" &>/dev/null; then
  warn "could not verify crates.io login — make sure you ran 'cargo login <token>'"
fi

# Uncommitted changes?
if [[ -n "$(git status --porcelain)" ]]; then
  die "working tree is dirty — commit or stash all changes before publishing"
fi

# On the right git tag?
EXPECTED_TAG="v${VERSION}"
CURRENT_TAGS="$(git tag --points-at HEAD)"
if ! echo "${CURRENT_TAGS}" | grep -qx "${EXPECTED_TAG}"; then
  warn "HEAD is not tagged '${EXPECTED_TAG}' — current tags: ${CURRENT_TAGS:-<none>}"
  warn "tag with: git tag ${EXPECTED_TAG} && git push origin ${EXPECTED_TAG}"
  read -rp "continue anyway? [y/N] " confirm
  [[ "${confirm}" =~ ^[Yy]$ ]] || die "aborted"
fi

success "prerequisites ok"
echo ""

# ── 2. CI checks ──────────────────────────────────────────────────────────────
if [[ "${SKIP_CI}" == false ]]; then
  info "running CI checks (fmt + check + unit tests)..."
  if [[ -x "./cargo-ci.sh" ]]; then
    ./cargo-ci.sh --fast --no-integration --no-audit
  else
    cargo fmt --check
    cargo clippy --all-features -- -D warnings
    cargo test --lib --no-integration 2>/dev/null || cargo test --lib
  fi
  success "CI checks passed"
  echo ""
fi

# ── 3. doc check ──────────────────────────────────────────────────────────────
info "verifying docs build cleanly..."
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features --quiet
success "docs ok"
echo ""

# ── 4. package dry-run ────────────────────────────────────────────────────────
info "packaging (dry-run)..."
cargo package --no-verify --quiet
info "files that will be uploaded:"
cargo package --list 2>/dev/null | sed 's/^/    /'
echo ""

# ── 5. crates.io duplicate check ──────────────────────────────────────────────
info "checking crates.io for existing version..."
PUBLISHED="$(curl -sf "https://crates.io/api/v1/crates/${CRATE_NAME}/${VERSION}" \
             -H 'User-Agent: publish-script' 2>/dev/null || true)"
if echo "${PUBLISHED}" | grep -q '"num"'; then
  die "${CRATE_NAME} v${VERSION} is already published on crates.io"
fi
success "${VERSION} not yet published — good to go"
echo ""

# ── 6. dry-run exit point ─────────────────────────────────────────────────────
if [[ "${DRY_RUN}" == true ]]; then
  warn "dry-run mode — stopping before publish"
  info "run without --dry-run to publish ${CRATE_NAME} v${VERSION}"
  exit 0
fi

# ── 7. publish ────────────────────────────────────────────────────────────────
echo -e "${BOLD}About to publish ${CRATE_NAME} v${VERSION} to crates.io.${RESET}"
read -rp "confirm publish? [y/N] " confirm
[[ "${confirm}" =~ ^[Yy]$ ]] || die "aborted"

info "publishing..."
cargo publish

echo ""
success "${CRATE_NAME} v${VERSION} published!"
echo ""
info "links:"
echo "    crate : https://crates.io/crates/${CRATE_NAME}/${VERSION}"
echo "    docs  : https://docs.rs/${CRATE_NAME}/${VERSION}    (builds in ~5 min)"