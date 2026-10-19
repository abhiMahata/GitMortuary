"""
enriched_commits.py
-------------------
Enrich basic commit records with per-commit statistics from the GitHub API.

Features
  - Validates GITHUB_TOKEN and provides a diagnostic if absent/invalid.
  - Retries transient failures (5xx, connection errors) with bounded
    exponential backoff.
  - Honours GitHub rate-limit headers (X-RateLimit-Reset / Retry-After).
  - Does NOT retry 401 / 403 (credential failures) — fails fast with a
    clear message.
  - Appends each enriched record immediately; flushes to disk.
  - Resumes from existing enriched_commits.jsonl (skips already-done SHAs).
  - Writes failures to data/sample/enrichment_errors.jsonl for later retry.
  - Distinguishes a genuine zero-change commit from an unavailable response.
  - Supports --mock mode: synthesises structural stats from local data so the
    rest of the pipeline can run without a live GitHub token.

Usage
  # Normal (requires GITHUB_TOKEN env var):
  cd <project-root>
  GITHUB_TOKEN=ghp_... python3 ingestion/enriched_commits.py

  # Mock mode (no token needed — uses synthetic stats for testing):
  python3 ingestion/enriched_commits.py --mock
"""

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Import config — run from project root so relative paths resolve correctly.
# ---------------------------------------------------------------------------
try:
    from ingestion.config import (
        GITHUB_API,
        GITHUB_TOKEN,
        COMMIT_OUTPUT_FILE,
        ENRICHED_COMMITS_OUTPUT_FILE,
    )
except ModuleNotFoundError:
    # Fallback when executed directly from the ingestion/ directory.
    from config import (  # type: ignore[no-redef]
        GITHUB_API,
        GITHUB_TOKEN,
        COMMIT_OUTPUT_FILE,
        ENRICHED_COMMITS_OUTPUT_FILE,
    )

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ERROR_LOG_FILE = "data/sample/enrichment_errors.jsonl"

# Retry configuration for transient failures only.
MAX_RETRIES = 4
BACKOFF_BASE = 2          # seconds
BACKOFF_MAX = 60          # cap per sleep


# ---------------------------------------------------------------------------
# Credential validation
# ---------------------------------------------------------------------------

def validate_token(token: str | None) -> None:
    """Print a diagnostic and exit if the token looks unusable."""
    if not token:
        print(
            "[ERROR] GITHUB_TOKEN environment variable is not set.\n"
            "        Set it before running:\n"
            "          export GITHUB_TOKEN=ghp_...\n"
            "        Or run in mock mode (no token needed):\n"
            "          python3 ingestion/enriched_commits.py --mock",
            file=sys.stderr,
        )
        sys.exit(2)

    # Quick prefix sanity check (GitHub Personal Access Tokens start with
    # ghp_, gho_, github_pat_, etc.).  Not a guarantee of validity.
    known_prefixes = ("ghp_", "gho_", "github_pat_", "ghs_", "ghu_")
    if not any(token.startswith(p) for p in known_prefixes):
        print(
            "[WARNING] GITHUB_TOKEN does not start with a recognised GitHub "
            "prefix.  It may be invalid.",
            file=sys.stderr,
        )


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def github_headers(token: str | None) -> dict:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _sleep_until_reset(response: requests.Response) -> None:
    """Sleep until the rate-limit window resets based on response headers."""
    reset_ts = response.headers.get("X-RateLimit-Reset")
    retry_after = response.headers.get("Retry-After")

    if retry_after:
        wait = int(retry_after) + 1
    elif reset_ts:
        wait = max(int(reset_ts) - int(time.time()) + 1, 1)
    else:
        wait = 60  # conservative fallback

    print(f"  Rate-limited — sleeping {wait}s …", file=sys.stderr)
    time.sleep(wait)


def probe_auth(token: str | None) -> bool:
    """
    Make a single lightweight request to /user to verify the token works
    before committing to a potentially large number of API calls.

    Returns True if authentication succeeded.
    Returns False and prints a diagnostic for 401 / 403.
    Raises requests.RequestException for network-level failures.

    Does NOT print the token value.
    """
    url = f"{GITHUB_API}/user"
    try:
        resp = requests.get(url, headers=github_headers(token), timeout=15)
    except requests.RequestException as exc:
        print(f"[ERROR] Auth probe network error: {exc}", file=sys.stderr)
        raise

    if resp.status_code == 200:
        # Extract login name only — safe to log
        login = resp.json().get("login", "(unknown)")
        print(f"[AUTH] Token valid — authenticated as: {login}")
        remaining = resp.headers.get("X-RateLimit-Remaining", "?")
        reset_ts   = resp.headers.get("X-RateLimit-Reset", "?")
        print(f"[AUTH] Rate limit remaining: {remaining}  (resets at unix ts {reset_ts})")
        return True

    if resp.status_code in (401, 403):
        print(
            f"[FATAL] Auth probe returned HTTP {resp.status_code}.\n"
            f"  401 = token missing, expired, or revoked.\n"
            f"  403 = token valid but lacks required scope or is IP-restricted.\n"
            f"  Regenerate at https://github.com/settings/tokens (needs 'repo' scope).\n"
            f"  Run with --mock to test the pipeline without a token.",
            file=sys.stderr,
        )
        return False

    # Unexpected status
    print(
        f"[WARNING] Auth probe returned unexpected HTTP {resp.status_code} — proceeding cautiously.",
        file=sys.stderr,
    )
    return True


def get_commit_details(
    repo_name: str, commit_sha: str, token: str | None
) -> dict:
    """
    Fetch full commit detail from GitHub with retry logic.

    Raises
      requests.HTTPError  for 401 / 403 (caller must not retry).
      RuntimeError        after exhausting MAX_RETRIES transient attempts.
    """
    url = f"{GITHUB_API}/repos/{repo_name}/commits/{commit_sha}"
    headers = github_headers(token)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=headers, timeout=30)
        except requests.ConnectionError as exc:
            wait = min(BACKOFF_BASE ** attempt, BACKOFF_MAX)
            print(
                f"  Connection error (attempt {attempt}/{MAX_RETRIES}): "
                f"{exc} — retrying in {wait}s",
                file=sys.stderr,
            )
            if attempt == MAX_RETRIES:
                raise
            time.sleep(wait)
            continue

        # --- Credential failure — never retry --------------------------------
        if response.status_code in (401, 403):
            response.raise_for_status()  # raises HTTPError immediately

        # --- Rate limit ------------------------------------------------------
        if response.status_code == 429:
            _sleep_until_reset(response)
            continue

        # --- Transient server error ------------------------------------------
        if response.status_code >= 500:
            wait = min(BACKOFF_BASE ** attempt, BACKOFF_MAX)
            print(
                f"  Server error {response.status_code} "
                f"(attempt {attempt}/{MAX_RETRIES}) — retrying in {wait}s",
                file=sys.stderr,
            )
            if attempt == MAX_RETRIES:
                response.raise_for_status()
            time.sleep(wait)
            continue

        # --- Success ---------------------------------------------------------
        response.raise_for_status()
        return response.json()

    raise RuntimeError(f"Exhausted retries for {repo_name}/{commit_sha}")


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def load_commits(path: str) -> list[dict]:
    records = []
    with open(path, "r", encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, 1):
            raw = raw.strip()
            if raw:
                try:
                    records.append(json.loads(raw))
                except json.JSONDecodeError as exc:
                    print(
                        f"[WARNING] Skipping malformed line {lineno} "
                        f"in {path}: {exc}",
                        file=sys.stderr,
                    )
    return records


def load_existing_shas(output_path: Path) -> set:
    existing: set = set()
    if not output_path.exists():
        return existing
    with output_path.open("r", encoding="utf-8") as fh:
        for raw in fh:
            raw = raw.strip()
            if raw:
                try:
                    rec = json.loads(raw)
                    existing.add(rec["commit_sha"])
                except (json.JSONDecodeError, KeyError):
                    pass
    return existing


def load_existing_error_shas(error_path: Path) -> set:
    """Return SHAs already recorded in the error log (to avoid re-logging)."""
    seen: set = set()
    if not error_path.exists():
        return seen
    with error_path.open("r", encoding="utf-8") as fh:
        for raw in fh:
            raw = raw.strip()
            if raw:
                try:
                    rec = json.loads(raw)
                    seen.add(rec.get("commit_sha", ""))
                except json.JSONDecodeError:
                    pass
    return seen


# ---------------------------------------------------------------------------
# Enrichment logic
# ---------------------------------------------------------------------------

def enrich_commit(commit: dict, details: dict) -> dict:
    """
    Merge API detail into a commit record.

    Distinguishes:
      files_changed = None  →  API response lacked 'files' key (unavailable)
      files_changed = 0     →  API confirmed zero files (genuine empty commit)
    """
    stats = details.get("stats")       # None when key absent
    files = details.get("files")       # None when key absent

    enriched = dict(commit)

    if stats is None:
        enriched["additions"] = None
        enriched["deletions"] = None
        enriched["lines_changed"] = None
        enriched["stats_available"] = False
    else:
        enriched["additions"] = stats.get("additions", 0)
        enriched["deletions"] = stats.get("deletions", 0)
        enriched["lines_changed"] = stats.get("total", 0)
        enriched["stats_available"] = True

    enriched["files_changed"] = len(files) if files is not None else None
    enriched["files_available"] = files is not None

    return enriched


def validate_enriched(record: dict) -> bool:
    """Return True if the record has the minimum required enrichment fields."""
    required = {
        "commit_sha", "repo_name", "repo_id",
        "files_changed", "additions", "deletions", "lines_changed",
    }
    return required.issubset(record.keys())


# ---------------------------------------------------------------------------
# Mock mode
# ---------------------------------------------------------------------------

def _synthetic_stats(commit_sha: str, repo_name: str) -> dict:
    """
    Produce deterministic-but-plausible synthetic stats from the SHA alone.
    Clearly marked as synthetic so they are never confused with real data.
    Used only in --mock mode.
    """
    seed = int(hashlib.sha256(commit_sha.encode()).hexdigest()[:8], 16)
    additions = seed % 200
    deletions = (seed >> 8) % 100
    files_changed = 1 + (seed >> 16) % 10
    return {
        "files_changed": files_changed,
        "additions": additions,
        "deletions": deletions,
        "lines_changed": additions + deletions,
        "stats_available": True,
        "files_available": True,
        "synthetic": True,   # explicit flag — never remove
    }


def enrich_commit_mock(commit: dict) -> dict:
    enriched = dict(commit)
    enriched.update(
        _synthetic_stats(commit["commit_sha"], commit["repo_name"])
    )
    return enriched


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(mock: bool = False) -> None:
    output_path = Path(ENRICHED_COMMITS_OUTPUT_FILE)
    error_path = Path(ERROR_LOG_FILE)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not mock:
        validate_token(GITHUB_TOKEN)
        # Pre-flight auth probe — verify token works before touching the API at scale
        if not probe_auth(GITHUB_TOKEN):
            sys.exit(2)

    commits = load_commits(COMMIT_OUTPUT_FILE)
    existing_shas = load_existing_shas(output_path)
    existing_error_shas = load_existing_error_shas(error_path)

    skip_shas = existing_shas | existing_error_shas
    remaining = [c for c in commits if c["commit_sha"] not in skip_shas]

    mode_label = "MOCK" if mock else "LIVE"
    print(
        f"[{mode_label}] Total commits: {len(commits)} | "
        f"Already enriched: {len(existing_shas)} | "
        f"Previously failed: {len(existing_error_shas)} | "
        f"To process: {len(remaining)}"
    )

    if not remaining:
        print("Nothing to do. All commits already enriched or logged as failed.")
        return

    success_count = 0
    fail_count = 0

    with (
        output_path.open("a", encoding="utf-8") as out_fh,
        error_path.open("a", encoding="utf-8") as err_fh,
    ):
        for index, commit in enumerate(remaining, start=1):
            sha_prefix = commit["commit_sha"][:8]
            label = f"[{index}/{len(remaining)}] {commit['repo_name']} {sha_prefix}"
            print(label, end="  ", flush=True)

            if mock:
                enriched = enrich_commit_mock(commit)
                if validate_enriched(enriched):
                    out_fh.write(json.dumps(enriched) + "\n")
                    out_fh.flush()
                    success_count += 1
                    print("✓ (mock)")
                else:
                    print("✗ (mock validation failed)")
                continue

            # --- Live mode ---
            try:
                details = get_commit_details(
                    commit["repo_name"], commit["commit_sha"], GITHUB_TOKEN
                )
                enriched = enrich_commit(commit, details)

                if not validate_enriched(enriched):
                    raise ValueError("Enriched record failed validation")

                out_fh.write(json.dumps(enriched) + "\n")
                out_fh.flush()
                success_count += 1
                print("✓")

            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else "?"
                print(f"✗ HTTP {status}")

                # Credential failures are fatal — no point continuing.
                if status in (401, 403):
                    print(
                        f"\n[FATAL] HTTP {status} from GitHub API.\n"
                        f"  401 = token missing, expired, or revoked.\n"
                        f"  403 = token lacks required scope or IP restriction.\n"
                        f"  Tip: regenerate your token at https://github.com/settings/tokens\n"
                        f"       and ensure the 'repo' scope is enabled.\n"
                        f"  Tip: run with --mock to test the pipeline locally.\n",
                        file=sys.stderr,
                    )
                    _log_error(err_fh, commit, f"HTTP {status}", str(exc))
                    fail_count += 1
                    break   # stop immediately on credential failure

                _log_error(err_fh, commit, f"HTTP {status}", str(exc))
                fail_count += 1

            except Exception as exc:  # noqa: BLE001
                print(f"✗ {type(exc).__name__}: {exc}")
                _log_error(err_fh, commit, type(exc).__name__, str(exc))
                fail_count += 1

    # --- Summary ------------------------------------------------------------
    print()
    print(
        f"Done.  Success: {success_count} | Failed: {fail_count} | "
        f"Output: {ENRICHED_COMMITS_OUTPUT_FILE}"
    )
    if fail_count:
        print(f"  Failures logged to: {ERROR_LOG_FILE}")
    total_enriched = len(existing_shas) + success_count
    total = len(commits)
    print(
        f"  Total enrichment coverage: {total_enriched}/{total} "
        f"({100 * total_enriched / total:.1f}%)"
    )
    if total_enriched < total:
        print(
            f"  [WARNING] Only {total_enriched}/{total} commits enriched — "
            "pipeline results will be based on a partial dataset.",
            file=sys.stderr,
        )


def _log_error(fh, commit: dict, error_type: str, message: str) -> None:
    record = {
        "commit_sha": commit.get("commit_sha"),
        "repo_name": commit.get("repo_name"),
        "repo_id": commit.get("repo_id"),
        "error_type": error_type,
        "error_message": message,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    fh.write(json.dumps(record) + "\n")
    fh.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Enrich commit records with GitHub API statistics."
    )
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Use synthetic stats instead of live GitHub API (no token needed).",
    )
    args = parser.parse_args()
    main(mock=args.mock)