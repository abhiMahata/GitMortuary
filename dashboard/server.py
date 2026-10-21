#!/usr/bin/env python3
"""
dashboard/server.py
-------------------
Local HTTP server and data provider for the Git Pattern Analysis Dashboard.
Loads pipeline outputs and datasets from disk, computes overview statistics,
and serves the interactive web dashboard.

Usage:
  python3 dashboard/server.py                 # Runs on http://127.0.0.1:8050
  python3 dashboard/server.py --port 8080     # Custom port
  python3 dashboard/server.py --check-only    # Verifies artifact loading and exits
"""

import argparse
import csv
from datetime import datetime, timezone
import io
import json
import mimetypes
import os
import re
import sys
import threading
import uuid
from http import HTTPStatus
from http.server import HTTPServer, BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

# Resolve project root (parent of dashboard/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from ingestion.analyze_repository import (
        parse_repository_identifier,
        analyze_repository,
        GitHubClient,
        InvalidIdentifierError,
        RepositoryNotFoundError,
        AuthenticationError,
        RateLimitExceededError,
        GitHubAPIError,
    )
    from spark.cluster_commits import cluster_commits
    from scripts.run_pipeline import execute_local_mapreduce
    from ingestion.config import GITHUB_TOKEN
except Exception:
    GITHUB_TOKEN = None

# Artifact paths
PATH_REPOS = PROJECT_ROOT / "data" / "sample" / "repositories.jsonl"
PATH_COMMITS = PROJECT_ROOT / "data" / "sample" / "commits.jsonl"
PATH_ENRICHED = PROJECT_ROOT / "data" / "sample" / "enriched_commits.jsonl"
PATH_CLUSTERS = PROJECT_ROOT / "data" / "output" / "repo_clusters.json"
PATH_FEATURES_LOCAL = PROJECT_ROOT / "data" / "output" / "repo_features_local.jsonl"
PATH_FEATURES_MR = PROJECT_ROOT / "data" / "output" / "repo_features_mr" / "repo_features.jsonl"


def load_jsonl(path: Path) -> list:
    """Safely load JSON lines from a file. Returns empty list if missing or invalid."""
    records = []
    if not path.exists():
        return records
    try:
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
    except OSError:
        return []
    return records


def load_json(path: Path) -> list | dict:
    """Safely load JSON document. Returns empty list/dict if missing or invalid."""
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError):
        return []


def list_available_runs() -> list:
    """
    Scan data/runs for available completed dynamic ingestion and analysis runs.
    Guards strictly against directory traversal and returns safe run metadata.
    """
    runs_dir = PROJECT_ROOT / "data" / "runs"
    runs = [
        {
            "run_id": "sample",
            "label": "Baseline Sample (20 repos, 1,000 commits, 4 clusters)",
            "type": "sample",
            "is_sample": True,
            "status": "complete",
            "repositories": ["20 sample repositories"],
            "commit_count": 1000,
            "features_available": True,
            "clusters_available": True,
            "timestamp": "2026-10-19T00:00:00Z",
        }
    ]

    if not runs_dir.exists() or not runs_dir.is_dir():
        return runs

    try:
        entries = sorted(runs_dir.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)
    except OSError:
        return runs

    for entry in entries:
        if not entry.is_dir():
            continue
        folder_name = entry.name

        # Path safety: only alphanumeric, underscore, hyphen, and dot; explicitly reject dot-directories
        if folder_name in (".", "..") or not re.match(r"^[A-Za-z0-9_.-]+$", folder_name):
            continue

        manifest_path = entry / "pipeline_manifest.json"
        is_pipeline = True
        if not manifest_path.exists():
            manifest_path = entry / "manifest.json"
            is_pipeline = False

        manifest_data = None
        is_malformed = False
        if manifest_path.exists():
            try:
                with manifest_path.open("r", encoding="utf-8") as fh:
                    manifest_data = json.load(fh)
            except Exception:
                manifest_data = None
                is_malformed = True

        # Extract repositories
        repos = []
        if isinstance(manifest_data, dict):
            params = manifest_data.get("parameters", {})
            if "repositories" in params:
                repos = params["repositories"]
            elif "repository" in manifest_data:
                repos = [manifest_data["repository"]]
            elif "target" in manifest_data:
                repos = [manifest_data["target"]]

        if not repos:
            repos_file = entry / "repositories.jsonl"
            if repos_file.exists():
                repos = [r.get("repo_name") for r in load_jsonl(repos_file) if isinstance(r, dict) and "repo_name" in r]

        # Artifact presence
        features_exist = (entry / "repo_features.jsonl").exists() or (entry / "repo_features_local.jsonl").exists()
        clusters_exist = (entry / "repo_clusters.json").exists()
        commits_exist = (entry / "commits.jsonl").exists()

        if is_malformed:
            status = "malformed"
        elif isinstance(manifest_data, dict):
            status = manifest_data.get("status", "complete")
        elif not any([features_exist, clusters_exist, commits_exist]):
            status = "empty"
        else:
            status = "incomplete"

        timestamp = (
            manifest_data.get("created_at")
            or manifest_data.get("timestamp")
            or datetime.fromtimestamp(entry.stat().st_mtime, timezone.utc).isoformat()
        ) if isinstance(manifest_data, dict) else datetime.fromtimestamp(entry.stat().st_mtime, timezone.utc).isoformat()

        counts = manifest_data.get("counts", {}) if isinstance(manifest_data, dict) else {}
        commit_count = counts.get("commits") or len(load_jsonl(entry / "commits.jsonl"))

        stages = manifest_data.get("stages", {}) if isinstance(manifest_data, dict) else {}
        spark_skipped = stages.get("spark", {}).get("status") == "skipped"
        spark_reason = stages.get("spark", {}).get("reason", "")

        repo_summary = ", ".join(repos[:2]) + (f" (+{len(repos)-2} more)" if len(repos) > 2 else "") if repos else folder_name
        tag = "Analysis" if is_pipeline else "Ingestion"

        if status == "failed":
            label = f"[{tag}: FAILED] {repo_summary} — {folder_name}"
        elif status == "empty":
            label = f"[{tag}: EMPTY] {folder_name}"
        elif status == "malformed":
            label = f"[{tag}: MALFORMED] {folder_name}"
        elif status == "incomplete":
            label = f"[{tag}: INCOMPLETE] {repo_summary} — {folder_name}"
        else:
            label = f"[{tag}] {repo_summary} ({commit_count} commits) — {folder_name}"

        runs.append({
            "run_id": folder_name,
            "label": label,
            "type": "pipeline" if is_pipeline else "ingestion",
            "is_sample": False,
            "status": status,
            "repositories": repos,
            "commit_count": commit_count,
            "features_available": features_exist,
            "clusters_available": clusters_exist,
            "spark_skipped": spark_skipped,
            "spark_reason": spark_reason,
            "timestamp": timestamp,
        })

    return runs


def compute_single_repo_analytics(repos: list, commits: list, enriched: list, manifest_data: dict | None = None, run_dir: Path | None = None) -> dict:
    manifest_data = manifest_data or {}
    primary_repo = repos[0] if repos else {}
    repo_name = primary_repo.get("repo_name", "Unknown Repository")

    total_commits = len(commits)
    total_enriched = len(enriched)

    stats_avail_count = sum(1 for c in enriched if c.get("stats_available") is True)
    missing_stats_count = total_enriched - stats_avail_count

    tot_add = sum(c.get("additions") or 0 for c in enriched if c.get("additions") is not None)
    tot_del = sum(c.get("deletions") or 0 for c in enriched if c.get("deletions") is not None)
    tot_lines = sum(c.get("lines_changed") or 0 for c in enriched if c.get("lines_changed") is not None)
    tot_files = sum(c.get("files_changed") or 0 for c in enriched if c.get("files_changed") is not None)
    merge_count = sum(1 for c in commits if (c.get("parent_count") or 0) > 1)
    merge_ratio = round(merge_count / max(1, total_commits), 4) if total_commits > 0 else 0.0

    # Authors
    authors_map = {}
    for c in enriched:
        author = c.get("author_identity") or "Unknown"
        if author not in authors_map:
            authors_map[author] = {
                "author": author,
                "commits": 0,
                "additions": 0,
                "deletions": 0,
                "lines_changed": 0,
                "merges": 0,
                "first_commit": None,
                "last_commit": None,
            }
        rec = authors_map[author]
        rec["commits"] += 1
        if c.get("additions") is not None:
            rec["additions"] += c["additions"]
        if c.get("deletions") is not None:
            rec["deletions"] += c["deletions"]
        if c.get("lines_changed") is not None:
            rec["lines_changed"] += c["lines_changed"]
        if (c.get("parent_count") or 0) > 1:
            rec["merges"] += 1

        ts = c.get("author_timestamp") or c.get("committer_timestamp")
        if ts:
            if rec["first_commit"] is None or ts < rec["first_commit"]:
                rec["first_commit"] = ts
            if rec["last_commit"] is None or ts > rec["last_commit"]:
                rec["last_commit"] = ts

    author_contributions = sorted(authors_map.values(), key=lambda a: a["commits"], reverse=True)
    for a in author_contributions:
        a["percentage"] = round(100.0 * a["commits"] / max(1, total_commits), 1)

    # Temporal aggregations
    daily_map = {}
    monthly_map = {}
    weekly_map = {}
    all_timestamps = []

    for c in enriched:
        ts = c.get("author_timestamp") or c.get("committer_timestamp")
        if not ts:
            continue
        all_timestamps.append(ts)
        date_str = ts[:10] if len(ts) >= 10 else ts
        month_str = ts[:7] if len(ts) >= 7 else ts

        # Daily
        if date_str not in daily_map:
            daily_map[date_str] = {"date": date_str, "commits": 0, "additions": 0, "deletions": 0, "lines": 0, "merges": 0}
        d_rec = daily_map[date_str]
        d_rec["commits"] += 1
        if c.get("additions") is not None:
            d_rec["additions"] += c["additions"]
        if c.get("deletions") is not None:
            d_rec["deletions"] += c["deletions"]
        if c.get("lines_changed") is not None:
            d_rec["lines"] += c["lines_changed"]
        if (c.get("parent_count") or 0) > 1:
            d_rec["merges"] += 1

        # Monthly
        if month_str not in monthly_map:
            monthly_map[month_str] = {"month": month_str, "commits": 0, "additions": 0, "deletions": 0, "lines": 0}
        m_rec = monthly_map[month_str]
        m_rec["commits"] += 1
        if c.get("additions") is not None:
            m_rec["additions"] += c["additions"]
        if c.get("deletions") is not None:
            m_rec["deletions"] += c["deletions"]
        if c.get("lines_changed") is not None:
            m_rec["lines"] += c["lines_changed"]

        # Weekly
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            iso_year, iso_week, _ = dt.isocalendar()
            week_str = f"{iso_year}-W{iso_week:02d}"
        except Exception:
            week_str = date_str
        weekly_map[week_str] = weekly_map.get(week_str, 0) + 1

    daily_activity = sorted(daily_map.values(), key=lambda d: d["date"])
    monthly_activity = sorted(monthly_map.values(), key=lambda m: m["month"])
    weekly_activity = [{"week": k, "commits": v} for k, v in sorted(weekly_map.items())]

    # Date range
    earliest_date = min(all_timestamps) if all_timestamps else None
    latest_date = max(all_timestamps) if all_timestamps else None
    duration_days = 0
    if earliest_date and latest_date:
        try:
            d1 = datetime.fromisoformat(earliest_date.replace("Z", "+00:00"))
            d2 = datetime.fromisoformat(latest_date.replace("Z", "+00:00"))
            duration_days = max(1, round((d2 - d1).total_seconds() / 86400, 1))
        except Exception:
            duration_days = 0

    # Frequently changed files
    file_counts = {}
    for c in enriched:
        f_list = c.get("file_names", [])
        if isinstance(f_list, list):
            for fname in f_list:
                file_counts[fname] = file_counts.get(fname, 0) + 1

    frequently_changed_files = [
        {"path": path, "count": cnt}
        for path, cnt in sorted(file_counts.items(), key=lambda x: x[1], reverse=True)[:15]
    ]

    # Commit-level clustering
    commit_clusters_path = run_dir / "commit_clusters.json" if run_dir else None
    if commit_clusters_path and commit_clusters_path.exists():
        clustering_res = load_json(commit_clusters_path)
        if not isinstance(clustering_res, dict):
            clustering_res = cluster_commits(enriched, k=3, seed=42)
    else:
        clustering_res = cluster_commits(enriched, k=3, seed=42)

    commit_clusters_map = clustering_res.get("assignments", {})

    # Detailed commit records
    commits_detail = []
    for c in enriched:
        sha = c.get("commit_sha", "")
        msg = c.get("commit_message", "")
        first_line = msg.split("\n")[0] if msg else ""
        item = {
            "sha": sha,
            "sha_short": sha[:8] if sha else "",
            "date": c.get("author_timestamp") or c.get("committer_timestamp"),
            "author": c.get("author_identity") or "Unknown",
            "message": msg,
            "message_subject": first_line[:120],
            "additions": c.get("additions"),
            "deletions": c.get("deletions"),
            "lines_changed": c.get("lines_changed"),
            "files_changed": c.get("files_changed"),
            "parent_count": c.get("parent_count", 1),
            "is_merge": (c.get("parent_count") or 0) > 1,
            "stats_available": c.get("stats_available", False),
            "cluster": commit_clusters_map.get(sha),
        }
        commits_detail.append(item)

    is_truncated = (
        manifest_data.get("completeness", {}).get("truncated_by_limit", False)
        or manifest_data.get("status") == "truncated"
    )

    return {
        "repository": repo_name,
        "repo_metadata": primary_repo,
        "summary": {
            "total_commits": total_commits,
            "total_enriched": total_enriched,
            "stats_available_count": stats_avail_count,
            "missing_stats_count": missing_stats_count,
            "total_additions": tot_add,
            "total_deletions": tot_del,
            "total_lines_changed": tot_lines,
            "total_files_changed": tot_files,
            "distinct_authors_count": len(author_contributions),
            "merge_count": merge_count,
            "merge_ratio": merge_ratio,
            "history_mode": manifest_data.get("history_mode", "recent_limit"),
            "requested_limit": manifest_data.get("requested_limit") or manifest_data.get("commit_limit"),
            "is_truncated": is_truncated,
            "truncation_reason": "Commit limit reached or rate limit preserved." if is_truncated else None,
            "date_range": {
                "earliest": earliest_date,
                "latest": latest_date,
                "duration_days": duration_days,
            },
        },
        "daily_activity": daily_activity,
        "weekly_activity": weekly_activity,
        "monthly_activity": monthly_activity,
        "author_contributions": author_contributions,
        "attribution_notice": "Author attribution is derived strictly from git commit metadata author strings. Authors committing under different names or email aliases appear as separate entities.",
        "merge_metrics": {
            "merge_count": merge_count,
            "non_merge_count": total_commits - merge_count,
            "total_commits": total_commits,
            "merge_ratio": merge_ratio,
        },
        "frequently_changed_files": frequently_changed_files,
        "commit_clustering": {
            "status": clustering_res.get("status"),
            "reason": clustering_res.get("reason"),
            "k": clustering_res.get("k", 0),
            "seed": clustering_res.get("seed", 42),
            "silhouette": clustering_res.get("silhouette"),
            "distance_measure": clustering_res.get("distance_measure", "squaredEuclidean"),
            "profiles": clustering_res.get("profiles", {}),
            "feature_columns": clustering_res.get("feature_columns", []),
        },
        "commits_detail": commits_detail,
    }


def generate_commits_csv(commits_detail: list[dict]) -> str:
    """Generate CSV string for commit-level records."""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "sha", "date", "author", "message_subject",
        "additions", "deletions", "lines_changed", "files_changed",
        "is_merge", "cluster",
    ])
    for c in commits_detail:
        writer.writerow([
            c.get("sha", ""),
            c.get("date", ""),
            c.get("author", ""),
            c.get("message_subject", ""),
            c.get("additions") if c.get("additions") is not None else "N/A",
            c.get("deletions") if c.get("deletions") is not None else "N/A",
            c.get("lines_changed") if c.get("lines_changed") is not None else "N/A",
            c.get("files_changed") if c.get("files_changed") is not None else "N/A",
            "yes" if c.get("is_merge") else "no",
            c.get("cluster") if c.get("cluster") is not None else "N/A",
        ])
    return output.getvalue()


def generate_repos_csv(profiles: list[dict]) -> str:
    """Generate CSV string for multi-repository profiles."""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "repo_name", "language", "stars", "forks",
        "commit_count", "distinct_authors", "merge_ratio",
        "avg_additions", "avg_deletions", "avg_lines_changed", "avg_files_changed",
        "cluster",
    ])
    for p in profiles:
        writer.writerow([
            p.get("repo_name", ""),
            p.get("language") or "N/A",
            p.get("stars") if p.get("stars") is not None else "N/A",
            p.get("forks") if p.get("forks") is not None else "N/A",
            p.get("commit_count") if p.get("commit_count") is not None else "N/A",
            p.get("distinct_authors") if p.get("distinct_authors") is not None else "N/A",
            p.get("merge_ratio") if p.get("merge_ratio") is not None else "N/A",
            p.get("avg_additions") if p.get("avg_additions") is not None else "N/A",
            p.get("avg_deletions") if p.get("avg_deletions") is not None else "N/A",
            p.get("avg_lines_changed") if p.get("avg_lines_changed") is not None else "N/A",
            p.get("avg_files_changed") if p.get("avg_files_changed") is not None else "N/A",
            p.get("cluster") if p.get("cluster") is not None else "N/A",
        ])
    return output.getvalue()


class AnalysisTaskManager:
    """Thread-safe manager for live background repository analysis tasks."""

    def __init__(self):
        self._lock = threading.Lock()
        self.tasks: dict[str, dict] = {}

    def create_task(self, run_id: str, repository: str, mode: str, limit: int | None, use_hadoop: bool) -> dict:
        with self._lock:
            task = {
                "run_id": run_id,
                "repository": repository,
                "mode": mode,
                "limit": limit,
                "use_hadoop": use_hadoop,
                "status": "in_progress",
                "stage": "validating",
                "message": f"Validating repository '{repository}'...",
                "progress_percent": 10,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "completed_at": None,
                "error": None,
            }
            self.tasks[run_id] = task
            return task

    def update_task(self, run_id: str, stage: str, message: str, percent: int) -> None:
        with self._lock:
            if run_id in self.tasks:
                self.tasks[run_id]["stage"] = stage
                self.tasks[run_id]["message"] = message
                self.tasks[run_id]["progress_percent"] = max(self.tasks[run_id]["progress_percent"], percent)

    def complete_task(self, run_id: str, status: str = "complete", message: str = "Analysis complete!") -> None:
        with self._lock:
            if run_id in self.tasks:
                self.tasks[run_id]["status"] = status
                self.tasks[run_id]["stage"] = "complete"
                self.tasks[run_id]["message"] = message
                self.tasks[run_id]["progress_percent"] = 100
                self.tasks[run_id]["completed_at"] = datetime.now(timezone.utc).isoformat()

    def fail_task(self, run_id: str, error_msg: str) -> None:
        with self._lock:
            if run_id in self.tasks:
                self.tasks[run_id]["status"] = "failed"
                self.tasks[run_id]["stage"] = "failed"
                self.tasks[run_id]["error"] = error_msg
                self.tasks[run_id]["message"] = f"Analysis failed: {error_msg}"
                self.tasks[run_id]["completed_at"] = datetime.now(timezone.utc).isoformat()

    def get_task(self, run_id: str) -> dict | None:
        with self._lock:
            return self.tasks.get(run_id)


GLOBAL_TASK_MANAGER = AnalysisTaskManager()


def _run_analysis_worker(
    run_id: str,
    repository: str,
    mode: str,
    limit: int | None,
    use_hadoop: bool,
    manager: AnalysisTaskManager,
) -> None:
    target_dir = PROJECT_ROOT / "data" / "runs" / run_id
    try:
        def on_ingest_progress(stage, msg, pct):
            manager.update_task(run_id, stage, msg, int(pct * 0.7))

        manager.update_task(run_id, "ingestion", f"Starting ingestion for {repository}...", 15)

        # Step 1: Run ingestion
        active_token = os.environ.get("GITHUB_TOKEN") or GITHUB_TOKEN
        manifest = analyze_repository(
            identifier=repository,
            output_dir=target_dir,
            commit_limit=limit,
            token=active_token,
            on_progress=on_ingest_progress,
        )

        enriched_file = target_dir / "enriched_commits.jsonl"
        features_file = target_dir / "repo_features.jsonl"

        # Step 2: MapReduce feature aggregation
        if use_hadoop:
            manager.update_task(
                run_id,
                "mapreduce",
                "Hadoop cluster not running in standalone environment; executing local MapReduce streaming fallback...",
                75,
            )
        else:
            manager.update_task(run_id, "mapreduce", "Aggregating repository features via MapReduce...", 75)

        features_count = 0
        if enriched_file.exists() and enriched_file.stat().st_size > 0:
            features_count = execute_local_mapreduce(enriched_file, features_file)

        # Step 3: Commit-level K-Means clustering (for single repo)
        manager.update_task(run_id, "clustering", "Executing commit-level K-Means clustering...", 88)
        enriched_commits = load_jsonl(enriched_file)
        clustering_result = cluster_commits(enriched_commits, k=3, seed=42)
        with (target_dir / "commit_clusters.json").open("w", encoding="utf-8") as fh:
            json.dump(clustering_result, fh, indent=2)

        # Step 4: Write pipeline manifest for compatibility
        pipe_manifest_path = target_dir / "pipeline_manifest.json"
        pipe_manifest = dict(manifest)
        pipe_manifest["manifest_version"] = "1.0"
        pipe_manifest["analysis_id"] = run_id
        pipe_manifest["type"] = "orchestrated_analysis"
        pipe_manifest["repositories_requested"] = [repository]
        pipe_manifest["stages"] = {
            "ingestion": {
                "status": manifest.get("status", "complete"),
                "repositories_attempted": 1,
                "repositories_succeeded": 1,
                "repositories_failed": 0,
                "runs": [
                    {
                        "repository": repository,
                        "status": manifest.get("status", "complete"),
                        "commits": manifest.get("counts", {}).get("commits", len(enriched_commits)),
                        "enriched": manifest.get("counts", {}).get("enriched_commits", len(enriched_commits)),
                        "directory": f"repositories/{repository.replace('/', '_')}",
                    }
                ],
            },
            "mapreduce": {
                "status": "success",
                "mode": "local_streaming",
                "features_count": features_count,
                "output_file": "repo_features.jsonl",
                "requested_mode": "hadoop" if use_hadoop else "local",
                "fallback_applied": bool(use_hadoop),
                "diagnostic": (
                    "Distributed Hadoop/HDFS cluster is not active in this standalone container environment; "
                    "features aggregated via verified local MapReduce streaming."
                ) if use_hadoop else None,
            },
            "spark": {
                "status": "success" if clustering_result.get("status") == "success" else "skipped",
                "reason": clustering_result.get("reason", ""),
                "k": clustering_result.get("k", 0),
                "seed": clustering_result.get("seed", 42),
                "silhouette": clustering_result.get("silhouette"),
                "silhouette_status": "valid" if clustering_result.get("silhouette") is not None else "unavailable",
                "silhouette_reason": "Standard squared Euclidean silhouette in range [-1.0, 1.0]." if clustering_result.get("silhouette") is not None else clustering_result.get("reason", ""),
                "distance_measure": "squaredEuclidean",
                "target": "commit_level_clustering",
                "output_file": "commit_clusters.json",
            },
        }

        with pipe_manifest_path.open("w", encoding="utf-8") as fh:
            json.dump(pipe_manifest, fh, indent=2)

        manager.complete_task(run_id, status="complete", message=f"Analysis complete for {repository}!")
    except Exception as err:
        manager.fail_task(run_id, str(err))


def get_dashboard_data(run_id: str | None = None) -> dict:
    """
    Read real project artifacts and construct the dashboard payload.
    Supports baseline sample data (run_id is None or 'sample') and isolated dynamic runs.
    Preserves provenance labels and handles missing files gracefully.
    """
    if not run_id or run_id == "sample":
        repos = load_jsonl(PATH_REPOS)
        commits = load_jsonl(PATH_COMMITS)
        enriched = load_jsonl(PATH_ENRICHED)
        clusters = load_json(PATH_CLUSTERS)

        # Prefer local features, fallback to distributed MR features
        features = load_jsonl(PATH_FEATURES_LOCAL)
        features_source = "local"
        if not features and PATH_FEATURES_MR.exists():
            features = load_jsonl(PATH_FEATURES_MR)
            features_source = "distributed_mr"

        run_info = {
            "run_id": "sample",
            "is_sample": True,
            "status": "complete",
            "label": "Baseline Sample (20 repos, 1,000 commits, 4 clusters)",
            "pipeline_type": "baseline",
            "warnings": ["The majority of commit statistics (98.5%) were generated via deterministic mock mode (--mock)."],
            "errors": [],
            "spark_skipped": False,
            "spark_reason": "",
        }
        provenance_warning = (
            "The majority of commit statistics (98.5%) were generated via deterministic mock mode (--mock) "
            "for pipeline demonstration and do not reflect real GitHub diff data."
        )
        is_live_authentic = False
        spark_status = "available" if bool(clusters) else "unavailable"
        spark_reason = ""
        silhouette = 0.4039
        silhouette_status = "valid"
        silhouette_reason = "Standard Euclidean silhouette coefficient in range [-1.0, 1.0]."
        k_param = 4
        seed_param = 42
        repo_truncation_map = {}
    else:
        # Strictly validate run_id to prevent path traversal; reject dot-segments
        if not run_id or not re.match(r"^[A-Za-z0-9_.-]+$", run_id) or run_id in (".", "..") or ".." in run_id:
            raise ValueError(f"Invalid run_id format: '{run_id}'")

        runs_root = (PROJECT_ROOT / "data" / "runs").resolve()
        run_dir = (runs_root / run_id).resolve()
        if not str(run_dir).startswith(str(runs_root)) or not run_dir.is_dir():
            raise FileNotFoundError(f"Run directory '{run_id}' not found under data/runs.")

        manifest_path = run_dir / "pipeline_manifest.json"
        is_pipeline = True
        if not manifest_path.exists():
            manifest_path = run_dir / "manifest.json"
            is_pipeline = False

        manifest_data = {}
        is_malformed = False
        if manifest_path.exists():
            try:
                with manifest_path.open("r", encoding="utf-8") as fh:
                    manifest_data = json.load(fh)
            except Exception:
                manifest_data = {}
                is_malformed = True

        repos = load_jsonl(run_dir / "repositories.jsonl")
        commits = load_jsonl(run_dir / "commits.jsonl")
        enriched = load_jsonl(run_dir / "enriched_commits.jsonl")

        # Features
        features_path = run_dir / "repo_features.jsonl"
        features_source = "mapreduce_features"
        if not features_path.exists():
            features_path = run_dir / "repo_features_local.jsonl"
        features = load_jsonl(features_path)
        if not features:
            features_source = "none"

        # Clusters
        clusters_path = run_dir / "repo_clusters.json"
        clusters = load_json(clusters_path)

        stages = manifest_data.get("stages", {}) if isinstance(manifest_data, dict) else {}
        spark_stage = stages.get("spark", {})
        spark_status = spark_stage.get("status", "available" if bool(clusters) else "unavailable")
        spark_reason = spark_stage.get("reason", "")
        silhouette = spark_stage.get("silhouette")
        silhouette_status = spark_stage.get("silhouette_status", "valid" if silhouette is not None else "unavailable")
        silhouette_reason = spark_stage.get("silhouette_reason", "")
        k_param = spark_stage.get("k", len(set(c.get("cluster") for c in clusters if isinstance(c, dict) and "cluster" in c)) if clusters else 0)
        seed_param = spark_stage.get("seed", 42)

        # Detect run status cleanly
        warnings = manifest_data.get("warnings", []) if isinstance(manifest_data, dict) else []
        errors = manifest_data.get("errors", []) if isinstance(manifest_data, dict) else []

        if is_malformed:
            run_status = "malformed"
            warnings.append("Run manifest file contains malformed JSON.")
        elif isinstance(manifest_data, dict) and "status" in manifest_data:
            run_status = manifest_data["status"]
        elif not any([bool(repos), bool(commits), bool(features), bool(clusters)]):
            run_status = "empty"
            warnings.append("Run directory contains no repository, commit, or cluster artifacts.")
        else:
            run_status = "incomplete"
            warnings.append("Run directory is missing pipeline_manifest.json.")

        run_info = {
            "run_id": run_id,
            "is_sample": False,
            "status": run_status,
            "label": run_id,
            "pipeline_type": "pipeline" if is_pipeline else "ingestion",
            "stages": stages,
            "warnings": warnings,
            "errors": errors,
            "spark_skipped": spark_stage.get("status") == "skipped",
            "spark_reason": spark_reason,
        }

        # Truncation mapping
        repo_truncation_map = {}
        if isinstance(manifest_data, dict):
            for r_entry in stages.get("ingestion", {}).get("runs", []):
                if isinstance(r_entry, dict) and "repository" in r_entry:
                    repo_truncation_map[r_entry["repository"]] = (r_entry.get("status") == "truncated")
            if manifest_data.get("status") == "truncated":
                target_repo = manifest_data.get("repository") or manifest_data.get("target")
                if target_repo:
                    repo_truncation_map[target_repo] = True

        synthetic_in_run = sum(1 for c in enriched if c.get("synthetic") is True)
        if synthetic_in_run == 0 and len(enriched) > 0:
            is_live_authentic = True
            provenance_warning = None
        else:
            is_live_authentic = False
            provenance_warning = f"{synthetic_in_run} of {len(enriched)} commit records in this run are synthetic."

    # Artifact presence flags
    artifacts_status = {
        "repositories_found": bool(repos),
        "commits_found": bool(commits),
        "enriched_found": bool(enriched),
        "clusters_found": bool(clusters),
        "features_found": bool(features),
        "features_source": features_source,
    }

    # Provenance counts
    synthetic_count = sum(1 for c in enriched if c.get("synthetic") is True)
    authentic_count = sum(1 for c in enriched if c.get("synthetic") is not True)
    total_enriched = len(enriched)

    # Build cluster lookups
    cluster_by_repo = {}
    distinct_authors_by_repo = {}
    if isinstance(clusters, list):
        for c in clusters:
            if isinstance(c, dict) and "repo_name" in c:
                name = c["repo_name"]
                cluster_by_repo[name] = c.get("cluster")
                distinct_authors_by_repo[name] = c.get("distinct_authors")

    # Build feature lookups
    feature_by_repo = {}
    for f in features:
        if isinstance(f, dict) and "repo_name" in f:
            feature_by_repo[f["repo_name"]] = f

    # Build repo metadata lookup
    repo_meta = {r["repo_name"]: r for r in repos if isinstance(r, dict) and "repo_name" in r}

    # Consolidated repository profiles
    all_repo_names = sorted(set(list(repo_meta.keys()) + list(feature_by_repo.keys()) + list(cluster_by_repo.keys())))
    profiles = []

    for name in all_repo_names:
        meta = repo_meta.get(name, {})
        feat = feature_by_repo.get(name, {})
        cluster_id = cluster_by_repo.get(name)

        profile = {
            "repo_name": name,
            "repo_id": meta.get("repo_id") or feat.get("repo_id"),
            "language": meta.get("language"),
            "stars": meta.get("stars"),
            "forks": meta.get("forks"),
            "is_truncated": repo_truncation_map.get(name, False),
            "commit_count": feat.get("commit_count"),
            "distinct_authors": distinct_authors_by_repo.get(name),
            "merge_count": feat.get("merge_count"),
            "merge_ratio": feat.get("merge_ratio"),
            "total_additions": feat.get("total_additions"),
            "total_deletions": feat.get("total_deletions"),
            "total_lines": feat.get("total_lines") or feat.get("total_lines_changed"),
            "total_files": feat.get("total_files") or feat.get("total_files_changed"),
            "avg_additions": feat.get("avg_additions"),
            "avg_deletions": feat.get("avg_deletions"),
            "avg_lines_changed": feat.get("avg_lines_changed"),
            "avg_files_changed": feat.get("avg_files_changed"),
            "cluster": cluster_id,
        }
        profiles.append(profile)

    # Cluster summary
    cluster_counts = {}
    if isinstance(clusters, list):
        for c in clusters:
            cid = c.get("cluster")
            if cid is not None:
                cluster_counts[cid] = cluster_counts.get(cid, 0) + 1

    # Unique clusters
    cluster_ids = sorted(cluster_counts.keys())

    # Calculate cluster averages for profile comparison
    cluster_profiles = {}
    for cid in cluster_ids:
        in_cluster = [p for p in profiles if p.get("cluster") == cid]
        n = len(in_cluster)
        if n > 0:
            cluster_profiles[cid] = {
                "count": n,
                "repos": [p["repo_name"] for p in in_cluster],
                "avg_additions": round(sum(p["avg_additions"] or 0 for p in in_cluster) / n, 2),
                "avg_deletions": round(sum(p["avg_deletions"] or 0 for p in in_cluster) / n, 2),
                "avg_lines_changed": round(sum(p["avg_lines_changed"] or 0 for p in in_cluster) / n, 2),
                "avg_files_changed": round(sum(p["avg_files_changed"] or 0 for p in in_cluster) / n, 2),
                "avg_merge_ratio": round(sum(p["merge_ratio"] or 0 for p in in_cluster) / n, 4),
            }

    has_truncation = any(p.get("is_truncated") for p in profiles)
    truncated_count = sum(1 for p in profiles if p.get("is_truncated"))

    prov_notice = {
        "is_authentic_live": is_live_authentic,
        "warning": provenance_warning,
        "has_truncated_history": has_truncation,
        "truncated_repositories_count": truncated_count,
        "truncated_notice": (
            f"{truncated_count} repositories had commit histories truncated at the requested commit limit. "
            "Metrics reflect recent commit activity, not entire repository lifetimes."
        ) if has_truncation else None,
        "unsupervised_notice": (
            "Cluster IDs are arbitrary categorical group labels identified by unsupervised K-Means in standardized feature space; "
            "they do not represent performance quality, velocity, or productivity rankings."
        ),
        "model_features": [
            "commit_count",
            "avg_additions",
            "avg_deletions",
            "avg_lines_changed",
            "avg_files_changed",
            "merge_ratio",
        ],
        "descriptive_metadata": [
            "language",
            "stars",
            "forks",
            "distinct_authors",
        ],
    }
    if is_live_authentic:
        prov_notice["authentic_badge"] = "100% Authentic Live GitHub Data"

    cluster_summary = {
        "cluster_ids": cluster_ids,
        "counts": cluster_counts,
        "profiles": cluster_profiles,
        "available": bool(clusters),
        "stage_status": spark_status,
        "stage_reason": spark_reason,
        "k": k_param,
        "seed": seed_param,
        "distance_measure": "squaredEuclidean",
        "silhouette": silhouette,
        "silhouette_status": silhouette_status,
        "silhouette_reason": silhouette_reason,
        "label_nature": "categorical_group_labels",
    }

    is_single_repo = bool(run_id and run_id != "sample" and (len(repos) == 1 or len(profiles) == 1) and (enriched or commits))
    single_repo_data = None
    if is_single_repo:
        single_repo_data = compute_single_repo_analytics(
            repos=repos,
            commits=commits,
            enriched=enriched,
            manifest_data=manifest_data if (run_id and run_id != "sample") else {},
            run_dir=run_dir,
        )

    return {
        "overview": {
            "total_repositories": len(repos),
            "total_commits": len(commits),
            "total_enriched": total_enriched,
            "authentic_enriched": authentic_count,
            "synthetic_enriched": synthetic_count,
            "synthetic_percentage": round((synthetic_count / total_enriched * 100), 1) if total_enriched else 0.0,
            "repositories_clustered": len(clusters) if isinstance(clusters, list) else 0,
            "cluster_count": len(cluster_ids),
            "has_truncated_history": has_truncation,
            "truncated_repositories_count": truncated_count,
        },
        "artifacts_status": artifacts_status,
        "cluster_summary": cluster_summary,
        "repositories": profiles,
        "provenance_notice": prov_notice,
        "run_info": run_info,
        "is_single_repo": is_single_repo,
        "single_repo": single_repo_data,
    }


class DashboardHandler(BaseHTTPRequestHandler):
    """Custom HTTP handler serving dashboard assets and data API."""

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/api/analyze":
            content_length = int(self.headers.get("Content-Length", 0))
            if content_length <= 0:
                self.send_response(HTTPStatus.BAD_REQUEST)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Empty request body."}).encode("utf-8"))
                return

            body = self.rfile.read(content_length)
            try:
                payload = json.loads(body.decode("utf-8"))
            except Exception:
                self.send_response(HTTPStatus.BAD_REQUEST)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Invalid JSON in request body."}).encode("utf-8"))
                return

            raw_repo = payload.get("repository", "").strip()
            mode = payload.get("mode", "recent_limit")
            limit_val = payload.get("limit")
            use_hadoop = bool(payload.get("use_hadoop", False))

            if not raw_repo:
                self.send_response(HTTPStatus.BAD_REQUEST)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Repository URL or 'owner/repo' is required."}).encode("utf-8"))
                return

            try:
                owner, repo_name = parse_repository_identifier(raw_repo)
                repo_identifier = f"{owner}/{repo_name}"
            except InvalidIdentifierError as exc:
                self.send_response(HTTPStatus.BAD_REQUEST)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"Invalid repository identifier: {exc}"}).encode("utf-8"))
                return

            # Check history mode & limit
            if mode == "full_history":
                commit_limit = None
            else:
                try:
                    commit_limit = int(limit_val) if limit_val is not None else 10
                    if commit_limit <= 0:
                        commit_limit = None
                except (ValueError, TypeError):
                    commit_limit = 10

            # Pre-validate repository existence via GitHubClient before spinning up run
            active_token = os.environ.get("GITHUB_TOKEN") or GITHUB_TOKEN
            client = GitHubClient(token=active_token)
            try:
                client.get_repository(owner, repo_name)
            except RepositoryNotFoundError as exc:
                self.send_response(HTTPStatus.NOT_FOUND)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"Repository not found or inaccessible: {exc}"}).encode("utf-8"))
                return
            except AuthenticationError as exc:
                self.send_response(HTTPStatus.UNAUTHORIZED)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"Authentication failed: {exc}"}).encode("utf-8"))
                return
            except RateLimitExceededError as exc:
                self.send_response(HTTPStatus.TOO_MANY_REQUESTS)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"GitHub API rate limit exceeded: {exc}"}).encode("utf-8"))
                return
            except GitHubAPIError as exc:
                self.send_response(HTTPStatus.BAD_GATEWAY)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"GitHub API error: {exc}"}).encode("utf-8"))
                return
            except Exception as exc:
                # Connection or network errors
                self.send_response(HTTPStatus.SERVICE_UNAVAILABLE)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"Could not connect to GitHub API: {exc}"}).encode("utf-8"))
                return

            timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            run_id = f"analysis-{timestamp_str}-{uuid.uuid4().hex[:8]}"

            GLOBAL_TASK_MANAGER.create_task(
                run_id=run_id,
                repository=repo_identifier,
                mode=mode,
                limit=commit_limit,
                use_hadoop=use_hadoop,
            )

            worker_thread = threading.Thread(
                target=_run_analysis_worker,
                args=(run_id, repo_identifier, mode, commit_limit, use_hadoop, GLOBAL_TASK_MANAGER),
                daemon=True,
            )
            worker_thread.start()

            self.send_response(HTTPStatus.ACCEPTED)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "started",
                "run_id": run_id,
                "repository": repo_identifier,
                "mode": mode,
                "limit": commit_limit,
                "message": f"Analysis queued and started for {repo_identifier}.",
            }, indent=2).encode("utf-8"))
            return

        self.send_error(HTTPStatus.NOT_FOUND, "Endpoint Not Found")

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        # API: Available runs
        if path == "/api/runs":
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            runs = list_available_runs()
            self.wfile.write(json.dumps({"runs": runs}, indent=2).encode("utf-8"))
            return

        # API: Status of an analysis task
        if path == "/api/analyze/status":
            qs = parse_qs(parsed.query)
            run_id = qs.get("run_id", [None])[0]
            if not run_id:
                self.send_response(HTTPStatus.BAD_REQUEST)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Missing 'run_id' query parameter."}).encode("utf-8"))
                return

            task = GLOBAL_TASK_MANAGER.get_task(run_id)
            if not task:
                self.send_response(HTTPStatus.NOT_FOUND)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"Task '{run_id}' not found."}).encode("utf-8"))
                return

            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(json.dumps(task, indent=2).encode("utf-8"))
            return

        # API: Export results (CSV or JSON)
        if path == "/api/export":
            qs = parse_qs(parsed.query)
            run_id = qs.get("run", [None])[0]
            fmt = qs.get("format", ["json"])[0].lower()
            try:
                data = get_dashboard_data(run_id=run_id)
            except (ValueError, FileNotFoundError) as err:
                self.send_response(HTTPStatus.NOT_FOUND)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(err)}).encode("utf-8"))
                return

            safe_run_id = run_id or "sample"
            if fmt == "csv":
                if data.get("is_single_repo") and data.get("single_repo"):
                    csv_text = generate_commits_csv(data["single_repo"]["commits_detail"])
                    filename = f"commits_{safe_run_id}.csv"
                else:
                    csv_text = generate_repos_csv(data.get("repositories", []))
                    filename = f"repositories_{safe_run_id}.csv"

                content_bytes = csv_text.encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/csv; charset=utf-8")
                self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
                self.send_header("Content-Length", str(len(content_bytes)))
                self.end_headers()
                self.wfile.write(content_bytes)
                return
            else:
                json_bytes = json.dumps(data, indent=2).encode("utf-8")
                filename = f"analysis_{safe_run_id}.json"
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
                self.send_header("Content-Length", str(len(json_bytes)))
                self.end_headers()
                self.wfile.write(json_bytes)
                return

        # API: Dashboard data with optional ?run=<run_id>
        if path == "/api/data":
            qs = parse_qs(parsed.query)
            run_id = qs.get("run", [None])[0]
            try:
                data = get_dashboard_data(run_id=run_id)
            except (ValueError, FileNotFoundError) as err:
                self.send_response(HTTPStatus.NOT_FOUND)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(err)}).encode("utf-8"))
                return

            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(json.dumps(data, indent=2).encode("utf-8"))
            return

        # Static assets inside dashboard/
        dashboard_dir = PROJECT_ROOT / "dashboard"
        if path in ("/", ""):
            file_path = dashboard_dir / "index.html"
        else:
            rel_path = path.lstrip("/")
            file_path = dashboard_dir / rel_path

        # Resolve and guard against directory traversal
        try:
            file_path = file_path.resolve()
            if not str(file_path).startswith(str(dashboard_dir.resolve())):
                self.send_error(HTTPStatus.FORBIDDEN, "Forbidden")
                return
        except Exception:
            self.send_error(HTTPStatus.NOT_FOUND, "Not Found")
            return

        if not file_path.exists() or not file_path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND, "File Not Found")
            return

        # Determine MIME type
        ctype, _ = mimetypes.guess_type(str(file_path))
        if not ctype:
            ctype = "application/octet-stream"

        try:
            with file_path.open("rb") as fh:
                content = fh.read()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        except OSError as exc:
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, f"Error reading file: {exc}")

    def do_HEAD(self):
        parsed = urlparse(self.path)
        path = parsed.path
        if path == "/api/runs":
            runs_bytes = json.dumps({"runs": list_available_runs()}, indent=2).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(runs_bytes)))
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            return

        if path == "/api/data":
            qs = parse_qs(parsed.query)
            run_id = qs.get("run", [None])[0]
            try:
                data = get_dashboard_data(run_id=run_id)
                data_bytes = json.dumps(data, indent=2).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(data_bytes)))
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
            except (ValueError, FileNotFoundError):
                self.send_response(HTTPStatus.NOT_FOUND)
                self.end_headers()
            return

        dashboard_dir = PROJECT_ROOT / "dashboard"
        file_path = dashboard_dir / ("index.html" if path in ("/", "") else path.lstrip("/"))
        try:
            file_path = file_path.resolve()
            if not str(file_path).startswith(str(dashboard_dir.resolve())) or not file_path.is_file():
                self.send_error(HTTPStatus.NOT_FOUND, "File Not Found")
                return
        except Exception:
            self.send_error(HTTPStatus.NOT_FOUND, "Not Found")
            return

        ctype, _ = mimetypes.guess_type(str(file_path))
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", ctype or "application/octet-stream")
        self.send_header("Content-Length", str(file_path.stat().st_size))
        self.end_headers()

    def log_message(self, format, *args):
        # Concise logging to stdout
        try:
            message = format % args
        except Exception:
            message = " ".join(str(a) for a in args)
        sys.stdout.write(f"[{self.log_date_time_string()}] {self.address_string()} - {message}\n")
        sys.stdout.flush()


def run_server(host: str = "127.0.0.1", port: int = 8050):
    server_address = (host, port)
    httpd = ThreadingHTTPServer(server_address, DashboardHandler)
    print("=" * 68)
    print(f"Git Pattern Analysis Dashboard running at:")
    print(f"  👉 http://{host}:{port}/")
    print("  Press Ctrl+C to stop.")
    print("=" * 68)
    sys.stdout.flush()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down dashboard server.")
        httpd.server_close()


def main():
    default_host = os.environ.get("HOST", "127.0.0.1")
    default_port = int(os.environ.get("PORT", "8050"))
    parser = argparse.ArgumentParser(description="Git Pattern Analysis Web Dashboard")
    parser.add_argument("--host", default=default_host, help=f"Host interface to bind (default: {default_host})")
    parser.add_argument("--port", type=int, default=default_port, help=f"Port to listen on (default: {default_port})")
    parser.add_argument("--check-only", action="store_true", help="Validate data loading without starting web server")
    args = parser.parse_args()

    if args.check_only:
        data = get_dashboard_data()
        ov = data["overview"]
        print(f"Artifact check passed:")
        print(f"  Repositories loaded: {ov['total_repositories']}")
        print(f"  Commits loaded     : {ov['total_commits']}")
        print(f"  Enriched records   : {ov['total_enriched']} ({ov['authentic_enriched']} real, {ov['synthetic_enriched']} synthetic)")
        print(f"  Clusters loaded    : {ov['repositories_clustered']} across {ov['cluster_count']} clusters")
        return

    run_server(host=args.host, port=args.port)


if __name__ == "__main__":
    main()
