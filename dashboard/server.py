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
from datetime import datetime, timezone
import json
import mimetypes
import os
import re
import sys
from http import HTTPStatus
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse, parse_qs

# Resolve project root (parent of dashboard/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

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

        # Path safety: only alphanumeric, underscore, hyphen, and dot
        if not re.match(r"^[A-Za-z0-9_.-]+$", folder_name):
            continue

        manifest_path = entry / "pipeline_manifest.json"
        is_pipeline = True
        if not manifest_path.exists():
            manifest_path = entry / "manifest.json"
            is_pipeline = False

        manifest_data = {}
        if manifest_path.exists():
            try:
                with manifest_path.open("r", encoding="utf-8") as fh:
                    manifest_data = json.load(fh)
            except Exception:
                manifest_data = {}

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

        status = manifest_data.get("status", "complete") if isinstance(manifest_data, dict) else "complete"
        timestamp = (
            manifest_data.get("created_at")
            or manifest_data.get("timestamp")
            or datetime.fromtimestamp(entry.stat().st_mtime, timezone.utc).isoformat()
        ) if isinstance(manifest_data, dict) else datetime.fromtimestamp(entry.stat().st_mtime, timezone.utc).isoformat()

        counts = manifest_data.get("counts", {}) if isinstance(manifest_data, dict) else {}
        commit_count = counts.get("commits") or len(load_jsonl(entry / "commits.jsonl"))

        features_exist = (entry / "repo_features.jsonl").exists() or (entry / "repo_features_local.jsonl").exists()
        clusters_exist = (entry / "repo_clusters.json").exists()

        stages = manifest_data.get("stages", {}) if isinstance(manifest_data, dict) else {}
        spark_skipped = stages.get("spark", {}).get("status") == "skipped"
        spark_reason = stages.get("spark", {}).get("reason", "")

        repo_summary = ", ".join(repos[:2]) + (f" (+{len(repos)-2} more)" if len(repos) > 2 else "") if repos else folder_name
        tag = "Analysis" if is_pipeline else "Ingestion"
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
        }
        provenance_warning = (
            "The majority of commit statistics (98.5%) were generated via deterministic mock mode (--mock) "
            "for pipeline demonstration and do not reflect real GitHub diff data."
        )
        is_live_authentic = False
        spark_status = "available" if bool(clusters) else "unavailable"
        spark_reason = ""
    else:
        # Strictly validate run_id to prevent path traversal
        if not re.match(r"^[A-Za-z0-9_.-]+$", run_id):
            raise ValueError(f"Invalid run_id format: '{run_id}'")

        runs_root = (PROJECT_ROOT / "data" / "runs").resolve()
        run_dir = (runs_root / run_id).resolve()
        if not str(run_dir).startswith(str(runs_root)) or not run_dir.is_dir():
            raise FileNotFoundError(f"Run directory '{run_id}' not found under data/runs/")

        manifest_path = run_dir / "pipeline_manifest.json"
        is_pipeline = True
        if not manifest_path.exists():
            manifest_path = run_dir / "manifest.json"
            is_pipeline = False

        manifest_data = load_json(manifest_path) if manifest_path.exists() else {}

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

        run_info = {
            "run_id": run_id,
            "is_sample": False,
            "status": manifest_data.get("status", "complete") if isinstance(manifest_data, dict) else "complete",
            "label": run_id,
            "pipeline_type": "pipeline" if is_pipeline else "ingestion",
            "stages": stages,
            "warnings": manifest_data.get("warnings", []) if isinstance(manifest_data, dict) else [],
            "errors": manifest_data.get("errors", []) if isinstance(manifest_data, dict) else [],
            "spark_skipped": spark_stage.get("status") == "skipped",
            "spark_reason": spark_reason,
        }

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

    prov_notice = {
        "is_authentic_live": is_live_authentic,
        "warning": provenance_warning,
        "unsupervised_notice": "K-Means cluster assignments reflect geometric feature grouping, not quality rankings, health indicators, or productivity ratings.",
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
    }

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
        },
        "artifacts_status": artifacts_status,
        "cluster_summary": cluster_summary,
        "repositories": profiles,
        "provenance_notice": prov_notice,
        "run_info": run_info,
    }


class DashboardHandler(BaseHTTPRequestHandler):
    """Custom HTTP handler serving dashboard assets and data API."""

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
    httpd = HTTPServer(server_address, DashboardHandler)
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
    parser = argparse.ArgumentParser(description="Git Pattern Analysis Web Dashboard")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8050, help="Port to listen on (default: 8050)")
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
