#!/usr/bin/env bash
# scripts/publish.sh — publish koprs workspace crates to crates.io
#
# Usage:
#   ./scripts/publish.sh              # full pre-flight + publish
#   ./scripts/publish.sh --dry-run    # stop before `cargo publish`
#   ./scripts/publish.sh --skip-ci    # skip CI checks, publish only
#   ./scripts/publish.sh --crate koprs          # publish a single crate only
#   ./scripts/publish.sh --crate koprs-external # publish a single crate only

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
SINGLE_CRATE=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --dry-run)   DRY_RUN=true ;;
    --skip-ci)   SKIP_CI=true ;;
    --crate)     shift; SINGLE_CRATE="${1:-}" ;;
    --crate=*)   SINGLE_CRATE="${1#--crate=}" ;;
    --help|-h)
      echo "Usage: $0 [--dry-run] [--skip-ci] [--crate <name>]"
      echo ""
      echo "  --dry-run          run all checks but stop before cargo publish"
      echo "  --skip-ci          skip CI checks, go straight to packaging + publish"
      echo "  --crate <name>     publish a single crate (koprs, koprs-external)"
      exit 0
      ;;
    *) die "unknown argument: $1" ;;
  esac
  shift
done

# ── repo root ──────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

ALL_CRATES=("koprs" "koprs-external")

if [[ -n "${SINGLE_CRATE}" ]]; then
  valid=false
  for c in "${ALL_CRATES[@]}"; do
    [[ "${c}" == "${SINGLE_CRATE}" ]] && valid=true && break
  done
  [[ "${valid}" == true ]] || die "unknown crate '${SINGLE_CRATE}' — must be one of: ${ALL_CRATES[*]}"
  CRATES=("${SINGLE_CRATE}")
else
  CRATES=("${ALL_CRATES[@]}")
fi

# ── read workspace version ─────────────────────────────────────────────────────
VERSION="$(grep -m1 '^version' Cargo.toml | sed 's/.*= *"\(.*\)"/\1/')"

info "workspace version : ${VERSION}"
info "crates to publish : ${CRATES[*]}"
echo ""

# ── 1. prerequisite checks ─────────────────────────────────────────────────────
info "checking prerequisites..."

command -v cargo &>/dev/null || die "'cargo' not found — install Rust via rustup"
command -v git   &>/dev/null || die "'git' not found"

# Logged in to crates.io?
if ! cargo owner --list koprs &>/dev/null; then
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
  if [[ -x "./scripts/cargo-ci.sh" ]]; then
    ./scripts/cargo-ci.sh --fast --no-integration --no-audit
  else
    cargo fmt --check
    cargo check --all-features
    cargo test --lib
  fi
  success "CI checks passed"
  echo ""
fi

# ── 3. doc check ──────────────────────────────────────────────────────────────
info "verifying docs build cleanly..."
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features --quiet
success "docs ok"
echo ""

# ── 4. per-crate pre-flight ───────────────────────────────────────────────────
for CRATE in "${CRATES[@]}"; do
  CRATE_DIR="crates/${CRATE}"

  [[ -d "${CRATE_DIR}" ]] || die "crate directory not found: ${CRATE_DIR}"

  info "[${CRATE}] packaging (dry-run)..."
  cargo package -p "${CRATE}" --no-verify --quiet

  info "[${CRATE}] files that will be uploaded:"
  cargo package -p "${CRATE}" --list 2>/dev/null | sed 's/^/    /'
  echo ""

  info "[${CRATE}] checking crates.io for existing version..."
  PUBLISHED="$(curl -sf "https://crates.io/api/v1/crates/${CRATE}/${VERSION}" \
               -H 'User-Agent: publish-script' 2>/dev/null || true)"
  if echo "${PUBLISHED}" | grep -q '"num"'; then
    die "${CRATE} v${VERSION} is already published on crates.io"
  fi
  success "[${CRATE}] ${VERSION} not yet published — good to go"
  echo ""
done

# ── 5. dry-run exit point ─────────────────────────────────────────────────────
if [[ "${DRY_RUN}" == true ]]; then
  warn "dry-run mode — stopping before publish"
  info "run without --dry-run to publish: ${CRATES[*]} @ v${VERSION}"
  exit 0
fi

# ── 6. publish ────────────────────────────────────────────────────────────────
echo -e "${BOLD}About to publish ${CRATES[*]} @ v${VERSION} to crates.io.${RESET}"
read -rp "confirm publish? [y/N] " confirm
[[ "${confirm}" =~ ^[Yy]$ ]] || die "aborted"

for CRATE in "${CRATES[@]}"; do
  info "[${CRATE}] publishing..."
  cargo publish -p "${CRATE}"

  # crates.io needs a moment to index each crate before the next one
  # can resolve it as a registry dependency
  if [[ "${CRATE}" != "${CRATES[-1]}" ]]; then
    info "waiting 20s for crates.io to index ${CRATE}..."
    sleep 20
  fi

  success "[${CRATE}] v${VERSION} published!"
done

echo ""
success "all crates published @ v${VERSION}"
echo ""
info "links:"
for CRATE in "${CRATES[@]}"; do
  echo "    ${CRATE}"
  echo "      crate : https://crates.io/crates/${CRATE}/${VERSION}"
  echo "      docs  : https://docs.rs/${CRATE}/${VERSION}    (builds in ~5 min)"
done