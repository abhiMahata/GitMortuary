"""
validate_data.py
----------------
Dataset validation script for Git Pattern Analysis.

Reports:
  1. File existence and readable record counts.
  2. Malformed JSON lines with line numbers.
  3. Required fields missing from each record.
  4. Duplicate repo IDs and commit SHAs.
  5. Commit records referencing repos in repositories.jsonl.
  6. Enriched SHAs present in the basic commit dataset.
  7. Enrichment coverage (enriched / total commits).
  8. Numerical stat quality in enriched records (None vs. zero).
  9. Commits per repository.
  10. Overall verdict: PASS, PASS WITH WARNINGS, or FAIL.

Usage:
  python3 scripts/validate_data.py
  python3 scripts/validate_data.py --verbose
"""

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

# ---------------------------------------------------------------------------
# Expected schemas
# ---------------------------------------------------------------------------

REPO_REQUIRED = {
    "repo_id", "repo_name", "language", "stars", "forks",
    "created_at", "updated_at", "archived", "default_branch",
}

COMMIT_REQUIRED = {
    "repo_id", "repo_name", "commit_sha",
    "author_identity", "author_timestamp",
    "committer_identity", "committer_timestamp",
    "commit_message", "parent_count",
}

ENRICHED_REQUIRED = COMMIT_REQUIRED | {
    "files_changed", "additions", "deletions", "lines_changed",
}

# ---------------------------------------------------------------------------
# ANSI helpers
# ---------------------------------------------------------------------------

BOLD   = "\033[1m"
GREEN  = "\033[32m"
YELLOW = "\033[33m"
RED    = "\033[31m"
RESET  = "\033[0m"


def _ok(msg):   print(f"  {GREEN}✓{RESET}  {msg}")
def _warn(msg): print(f"  {YELLOW}⚠{RESET}  {msg}")
def _fail(msg): print(f"  {RED}✗{RESET}  {msg}")


# ---------------------------------------------------------------------------
# JSONL loader — never silently discards; always reports line numbers
# ---------------------------------------------------------------------------

def load_jsonl(path: Path, verbose: bool = False):
    """Return (records, list_of_(lineno, error_str))."""
    records = []
    errors = []
    if not path.exists():
        return records, errors
    with path.open("r", encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, 1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                records.append(json.loads(raw))
            except json.JSONDecodeError as exc:
                errors.append((lineno, str(exc)))
                if verbose:
                    print(f"    [line {lineno}] JSON error: {exc}")
    return records, errors


# ---------------------------------------------------------------------------
# Per-file check
# ---------------------------------------------------------------------------

def check_file(
    label: str,
    path: Path,
    required_fields: set,
    pk_field: str,
    expected_count: int | None = None,
    verbose: bool = False,
) -> dict:
    print(f"\n{BOLD}── {label}{RESET}  ({path})")

    if not path.exists():
        _fail(f"File not found: {path}")
        return {
            "ok": False, "warnings": False, "total": 0,
            "bad": 1, "dupes": 0, "missing": {}, "records": [],
        }

    records, errors = load_jsonl(path, verbose)
    total = len(records)
    bad = len(errors)

    if bad:
        _fail(f"Malformed lines: {bad} (line numbers: {[e[0] for e in errors[:10]]})")
    else:
        _ok("All lines parse as valid JSON")

    if total > 0:
        _ok(f"Records loaded: {total}")
    else:
        _fail("No records found")

    if expected_count is not None:
        if total == expected_count:
            _ok(f"Count matches expected: {expected_count}")
        else:
            _warn(f"Expected ~{expected_count}, got {total}")

    # Duplicate primary keys
    pks = [r.get(pk_field) for r in records if pk_field in r]
    pk_counts = Counter(pks)
    dupes = sum(cnt - 1 for cnt in pk_counts.values() if cnt > 1)
    if dupes:
        _fail(f"Duplicate {pk_field} values: {dupes} extra occurrences")
        if verbose:
            for val, cnt in pk_counts.items():
                if cnt > 1:
                    print(f"    {pk_field}={val!r}  appears {cnt}x")
    else:
        _ok(f"No duplicate {pk_field} values")

    # Missing required fields
    missing_totals: dict = {}
    for rec in records:
        for field in required_fields:
            if field not in rec:
                missing_totals[field] = missing_totals.get(field, 0) + 1

    if missing_totals:
        for field, cnt in sorted(missing_totals.items()):
            _fail(f"Missing field '{field}': {cnt}/{total} records")
    else:
        _ok("All required fields present in every record")

    has_warn = (expected_count is not None and total != expected_count)
    ok = (bad == 0 and dupes == 0 and not missing_totals and total > 0)
    return {
        "ok": ok,
        "warnings": has_warn and ok,
        "total": total,
        "bad": bad,
        "dupes": dupes,
        "missing": missing_totals,
        "records": records,
    }


# ---------------------------------------------------------------------------
# Cross-file consistency
# ---------------------------------------------------------------------------

def check_consistency(repos_result: dict, commits_result: dict) -> None:
    print(f"\n{BOLD}── Cross-file Consistency{RESET}")

    repos   = repos_result.get("records", [])
    commits = commits_result.get("records", [])

    repo_names    = {r["repo_name"] for r in repos   if "repo_name" in r}
    commit_rnames = {c["repo_name"] for c in commits if "repo_name" in c}

    # 5. Commits referencing repos in repositories.jsonl
    orphan = commit_rnames - repo_names
    if orphan:
        _warn(f"Commit repo_names not in repositories.jsonl: {len(orphan)}")
        for n in sorted(orphan)[:5]:
            print(f"    {n}")
    else:
        _ok("All commit repo_names exist in repositories.jsonl")

    repos_no_commits = repo_names - commit_rnames
    if repos_no_commits:
        _warn(f"Repos with no commits in commits.jsonl: {len(repos_no_commits)}")
        for n in sorted(repos_no_commits):
            print(f"    {n}")
    else:
        _ok("All repos have at least one commit record")


# ---------------------------------------------------------------------------
# 9. Commits per repository
# ---------------------------------------------------------------------------

def check_commit_distribution(commits_result: dict) -> None:
    print(f"\n{BOLD}── Commits Per Repository{RESET}")
    commits = commits_result.get("records", [])
    if not commits:
        _warn("No commits to analyse")
        return

    counts = Counter(c.get("repo_name", "<missing>") for c in commits)
    total = sum(counts.values())
    _ok(f"Total commits: {total} across {len(counts)} repositories")
    for repo, cnt in sorted(counts.items(), key=lambda x: -x[1]):
        bar = "x" * (cnt // 5)
        print(f"  {repo:<55} {cnt:>4}  {bar}")


# ---------------------------------------------------------------------------
# 6-8. Enrichment coverage + stat quality
# ---------------------------------------------------------------------------

def check_enrichment(commits_result: dict, enriched_result: dict) -> None:
    print(f"\n{BOLD}── Enrichment Coverage{RESET}")
    commits  = commits_result.get("records", [])
    enriched = enriched_result.get("records", [])

    commit_shas   = {r["commit_sha"] for r in commits  if "commit_sha" in r}
    enriched_shas = {r["commit_sha"] for r in enriched if "commit_sha" in r}

    overlap    = commit_shas & enriched_shas
    unenriched = commit_shas - enriched_shas
    phantom    = enriched_shas - commit_shas   # 6. enriched SHAs not in commits

    pct = 100.0 * len(overlap) / len(commit_shas) if commit_shas else 0.0

    if pct == 100.0:
        _ok(f"Coverage: {len(overlap)}/{len(commit_shas)} (100.0%)")
    elif pct >= 50.0:
        _warn(f"Coverage: {len(overlap)}/{len(commit_shas)} ({pct:.1f}%) -- partial")
    else:
        _fail(f"Coverage: {len(overlap)}/{len(commit_shas)} ({pct:.1f}%) -- very low")

    # 6. Phantom enriched SHAs
    if phantom:
        _warn(f"Enriched SHAs not found in commits.jsonl (phantom): {len(phantom)}")
    else:
        _ok("No phantom enriched SHAs")

    if unenriched:
        print(f"\n  Unenriched commits: {len(unenriched)}")
        for sha in sorted(unenriched)[:5]:
            print(f"    {sha}")
        if len(unenriched) > 5:
            print(f"    ... and {len(unenriched) - 5} more")

    # 8. Numerical stat quality
    print(f"\n{BOLD}── Enrichment Stat Quality{RESET}")
    synthetic_count = sum(1 for r in enriched if r.get("synthetic") is True)
    unavail_stats   = sum(1 for r in enriched if r.get("stats_available") is False)
    none_additions  = sum(1 for r in enriched if r.get("additions") is None and not r.get("synthetic"))
    none_deletions  = sum(1 for r in enriched if r.get("deletions") is None and not r.get("synthetic"))
    none_files      = sum(1 for r in enriched if r.get("files_changed") is None and not r.get("synthetic"))
    real_count      = len(enriched) - synthetic_count

    _ok(f"Real (non-synthetic) enriched records: {real_count}")
    if synthetic_count:
        _warn(
            f"Synthetic (mock) records: {synthetic_count} -- "
            "not real GitHub data; cluster results are illustrative only"
        )

    if unavail_stats:
        _warn(
            f"Records where GitHub stats key was absent: {unavail_stats} "
            "(stored as None, not zero -- correct behaviour)"
        )
    else:
        _ok("No records with unavailable stats (or all unavailability correctly flagged)")

    if none_additions and real_count > 0:
        _warn(f"Real records with additions=None: {none_additions} (stats unavailable from API)")
    if none_deletions and real_count > 0:
        _warn(f"Real records with deletions=None: {none_deletions}")
    if none_files and real_count > 0:
        _warn(f"Real records with files_changed=None: {none_files}")

    # Suspicious all-zero real records
    suspicious_zeros = [
        r for r in enriched
        if not r.get("synthetic")
        and r.get("stats_available") is True
        and r.get("additions") == 0
        and r.get("deletions") == 0
        and r.get("files_changed") == 0
        and r.get("lines_changed") == 0
    ]
    if suspicious_zeros:
        _warn(
            f"Real records with all stats=0 and stats_available=True: "
            f"{len(suspicious_zeros)} "
            "(may be genuine empty commits -- verify manually)"
        )
    else:
        _ok("No suspicious all-zero stat records among real enriched data")


# ---------------------------------------------------------------------------
# Error log summary
# ---------------------------------------------------------------------------

def check_error_log(errors_path: Path) -> int:
    if not errors_path.exists():
        return 0
    records, _ = load_jsonl(errors_path)
    if not records:
        return 0
    print(f"\n{BOLD}-- Enrichment Error Log{RESET}  ({errors_path})")
    by_type = Counter(e.get("error_type", "unknown") for e in records)
    _warn(f"Total failed commits logged: {len(records)}")
    for etype, cnt in by_type.most_common():
        print(f"    {etype}: {cnt}")
    return len(records)


# ---------------------------------------------------------------------------
# Verdict
# ---------------------------------------------------------------------------

def verdict(failures: list, warnings: list) -> None:
    print()
    print("=" * 55)
    if failures:
        print(f"{RED}{BOLD}RESULT: FAIL{RESET}")
        print(f"  {len(failures)} failure(s):")
        for f in failures:
            print(f"    * {f}")
    elif warnings:
        print(f"{YELLOW}{BOLD}RESULT: PASS WITH WARNINGS{RESET}")
        print(f"  {len(warnings)} warning(s):")
        for w in warnings:
            print(f"    * {w}")
    else:
        print(f"{GREEN}{BOLD}RESULT: PASS{RESET}")
    print("=" * 55)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate Git Pattern Analysis datasets."
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Print per-anomaly details."
    )
    args = parser.parse_args()

    base          = Path("data/sample")
    repos_path    = base / "repositories.jsonl"
    commits_path  = base / "commits.jsonl"
    enriched_path = base / "enriched_commits.jsonl"
    errors_path   = base / "enrichment_errors.jsonl"

    print(f"\n{BOLD}Git Pattern Analysis -- Dataset Validation{RESET}")
    print("=" * 55)

    failures = []
    warnings = []

    r = check_file("repositories.jsonl",     repos_path,    REPO_REQUIRED,     "repo_id",    20,   args.verbose)
    c = check_file("commits.jsonl",          commits_path,  COMMIT_REQUIRED,   "commit_sha", 1000, args.verbose)
    e = check_file("enriched_commits.jsonl", enriched_path, ENRICHED_REQUIRED, "commit_sha", None, args.verbose)

    for label, result in [
        ("repositories.jsonl", r),
        ("commits.jsonl", c),
        ("enriched_commits.jsonl", e),
    ]:
        if not result.get("ok"):
            if result.get("total", 0) == 0:
                failures.append(f"{label}: file not found or empty")
            elif result.get("bad", 0):
                failures.append(f"{label}: {result['bad']} malformed lines")
            elif result.get("dupes", 0):
                failures.append(f"{label}: {result['dupes']} duplicate primary keys")
            elif result.get("missing"):
                failures.append(f"{label}: missing required fields {list(result['missing'])}")
        elif result.get("warnings"):
            warnings.append(f"{label}: record count differs from expected")

    check_consistency(r, c)
    check_commit_distribution(c)
    check_enrichment(c, e)

    err_count = check_error_log(errors_path)
    if err_count:
        warnings.append(f"enrichment_errors.jsonl: {err_count} failed commits logged")

    enriched_total = e.get("total", 0)
    commit_total   = c.get("total", 0)
    if enriched_total == 0:
        failures.append("enriched_commits.jsonl: no enriched records (analytics pipeline blocked)")
    elif enriched_total < commit_total:
        pct = 100.0 * enriched_total / commit_total if commit_total else 0
        warnings.append(
            f"enrichment coverage: {enriched_total}/{commit_total} ({pct:.1f}%) -- partial dataset"
        )

    synthetic = sum(1 for rx in e.get("records", []) if rx.get("synthetic"))
    if synthetic:
        warnings.append(
            f"{synthetic}/{enriched_total} enriched records are synthetic (mock mode) -- "
            "not real GitHub data"
        )

    verdict(failures, warnings)
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
