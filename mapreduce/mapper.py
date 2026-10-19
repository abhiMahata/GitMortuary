#!/usr/bin/env python3
"""
mapreduce/mapper.py
-------------------
Hadoop Streaming mapper for Git Pattern Analysis.

Input:  lines from enriched_commits.jsonl (via stdin)
Output: tab-separated  repo_name <TAB> JSON-encoded feature dict

One output line per input commit record.
"""

import json
import sys


def emit(repo_name, features):
    print("{}\t{}".format(repo_name, json.dumps(features)))


def safe_int(val):
    """Safely parse non-negative integer; return None if missing, invalid, or boolean."""
    if val is None or isinstance(val, bool):
        return None
    try:
        n = int(val)
        return n if n >= 0 else None
    except (ValueError, TypeError):
        return None


def is_merge_commit(parent_count):
    """Return 1 if parent_count > 1, else 0 without raising exceptions."""
    val = safe_int(parent_count)
    return 1 if (val is not None and val > 1) else 0


def process_line(line):
    line = line.strip()
    if not line:
        return

    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        return  # skip malformed lines silently

    if not isinstance(record, dict):
        return

    repo_name = record.get("repo_name")
    if not repo_name or not isinstance(repo_name, str):
        return

    additions = safe_int(record.get("additions"))
    deletions = safe_int(record.get("deletions"))
    lines_changed = safe_int(record.get("lines_changed"))
    if lines_changed is None and additions is not None and deletions is not None:
        lines_changed = additions + deletions
    files_changed = safe_int(record.get("files_changed"))

    emit(repo_name, {
        "commit_count":    1,
        "additions":       additions,
        "deletions":       deletions,
        "lines_changed":   lines_changed,
        "files_changed":   files_changed,
        "is_merge":        is_merge_commit(record.get("parent_count")),
    })


def main() -> None:
    for line in sys.stdin:
        process_line(line)


if __name__ == "__main__":
    main()
