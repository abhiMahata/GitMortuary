#!/usr/bin/env python3
"""
mapreduce/reducer.py
--------------------
Hadoop Streaming reducer for Git Pattern Analysis.

Input:  sorted tab-separated  repo_name <TAB> JSON-encoded feature dict
Output: one JSON line per repository with aggregated features

Repository-level features produced:
  repo_name          : str
  commit_count       : total commits in sample
  total_additions    : sum of additions
  total_deletions    : sum of deletions
  total_lines        : sum of lines changed
  total_files        : sum of files changed
  merge_count        : number of merge commits (parent_count > 1)
  avg_additions      : total_additions / commit_count
  avg_deletions      : total_deletions / commit_count
  avg_lines_changed  : total_lines / commit_count
  avg_files_changed  : total_files / commit_count
  merge_ratio        : merge_count / commit_count
"""

import json
import sys
from collections import defaultdict


def safe_int(val):
    """Safely parse non-negative integer; return None if missing, invalid, or boolean."""
    if val is None or isinstance(val, bool):
        return None
    try:
        n = int(val)
        return n if n >= 0 else None
    except (ValueError, TypeError):
        return None


def zero_features():
    return {
        "commit_count":    0,
        "total_additions": 0,
        "total_deletions": 0,
        "total_lines":     0,
        "total_files":     0,
        "merge_count":     0,
        "additions_count": 0,
        "deletions_count": 0,
        "lines_count":     0,
        "files_count":     0,
    }


def emit_repo(repo_name, agg):
    n = agg["commit_count"]
    if n == 0:
        return None

    n_add = agg.get("additions_count", 0)
    n_del = agg.get("deletions_count", 0)
    n_lines = agg.get("lines_count", 0)
    n_files = agg.get("files_count", 0)

    result = {
        "repo_name":         repo_name,
        "commit_count":      n,
        "total_additions":   agg["total_additions"],
        "total_deletions":   agg["total_deletions"],
        "total_lines":       agg["total_lines"],
        "total_files":       agg["total_files"],
        "merge_count":       agg["merge_count"],
        "avg_additions":     round(agg["total_additions"] / n_add, 4) if n_add > 0 else 0.0,
        "avg_deletions":     round(agg["total_deletions"] / n_del, 4) if n_del > 0 else 0.0,
        "avg_lines_changed": round(agg["total_lines"] / n_lines, 4) if n_lines > 0 else 0.0,
        "avg_files_changed": round(agg["total_files"] / n_files, 4) if n_files > 0 else 0.0,
        "merge_ratio":       round(agg["merge_count"] / n, 4),
    }
    print(json.dumps(result))
    return result


def process_stream(stream):
    repos = defaultdict(zero_features)

    for line in stream:
        line = line.rstrip("\n")
        if "\t" not in line:
            continue

        parts = line.split("\t", 1)
        repo_name = parts[0].strip()
        feature_json = parts[1]

        if not repo_name:
            continue

        try:
            features = json.loads(feature_json)
        except json.JSONDecodeError:
            continue

        if not isinstance(features, dict):
            continue

        agg = repos[repo_name]
        c = safe_int(features.get("commit_count")) or 1
        agg["commit_count"] += c

        add = safe_int(features.get("additions"))
        if add is not None:
            agg["total_additions"] += add
            agg["additions_count"] += c

        dele = safe_int(features.get("deletions"))
        if dele is not None:
            agg["total_deletions"] += dele
            agg["deletions_count"] += c

        lines = safe_int(features.get("lines_changed"))
        if lines is not None:
            agg["total_lines"] += lines
            agg["lines_count"] += c

        files = safe_int(features.get("files_changed"))
        if files is not None:
            agg["total_files"] += files
            agg["files_count"] += c

        merge = safe_int(features.get("is_merge"))
        if merge:
            agg["merge_count"] += merge

    results = []
    for repo_name, agg in repos.items():
        res = emit_repo(repo_name, agg)
        if res:
            results.append(res)
    return results


def main() -> None:
    process_stream(sys.stdin)


if __name__ == "__main__":
    main()
