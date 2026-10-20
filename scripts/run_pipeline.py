#!/usr/bin/env python3
"""
scripts/run_pipeline.py
-----------------------
End-to-end pipeline orchestrator for Git Pattern Analysis.

Ingests one or more GitHub repositories, combines their records,
aggregates features using Hadoop MapReduce (local or HDFS),
and optionally clusters repositories using Apache Spark MLlib.

Usage:
  python3 scripts/run_pipeline.py octocat/Hello-World
  python3 scripts/run_pipeline.py psf/requests pallets/flask --max-commits 25 --clusters 2
  python3 scripts/run_pipeline.py --output-dir data/runs/my_analysis psf/requests
"""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import uuid

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.analyze_repository import (
    analyze_repository,
    parse_repository_identifier,
    InvalidIdentifierError,
    GitHubAPIError,
)

DEFAULT_RUNS_DIR = PROJECT_ROOT / "data" / "runs"
PROTECTED_SAMPLE_DIR = PROJECT_ROOT / "data" / "sample"
PROTECTED_OUTPUT_DIR = PROJECT_ROOT / "data" / "output"


def generate_analysis_directory(
    base_dir: Path | str = DEFAULT_RUNS_DIR,
) -> Path:
    """Generate a unique analysis directory: data/runs/analysis-<timestamp>-<short_id>/."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    unique_suffix = uuid.uuid4().hex[:8]
    dir_name = f"analysis-{timestamp}-{unique_suffix}"
    return Path(base_dir) / dir_name


def validate_analysis_directory(target_dir: Path) -> None:
    """Ensure target directory does not point to protected sample/output directories."""
    resolved = target_dir.resolve()
    for protected in (PROTECTED_SAMPLE_DIR.resolve(), PROTECTED_OUTPUT_DIR.resolve()):
        if resolved == protected or protected in resolved.parents:
            raise ValueError(
                f"Cannot use protected path '{target_dir}'. "
                "Analysis must be written to an isolated directory under 'data/runs/'."
            )


def execute_local_mapreduce(
    enriched_commits_file: Path,
    output_features_file: Path,
) -> int:
    """
    Execute mapper.py | sort | reducer.py locally using safe subprocess argument lists.
    Returns the count of repository feature records generated.
    """
    mapper_path = PROJECT_ROOT / "mapreduce" / "mapper.py"
    reducer_path = PROJECT_ROOT / "mapreduce" / "reducer.py"

    if not enriched_commits_file.exists() or enriched_commits_file.stat().st_size == 0:
        raise ValueError(f"Input file for MapReduce does not exist or is empty: {enriched_commits_file}")

    with open(enriched_commits_file, "r", encoding="utf-8") as fh_in, \
         open(output_features_file, "w", encoding="utf-8") as fh_out:

        p_mapper = subprocess.Popen(
            [sys.executable, str(mapper_path)],
            stdin=fh_in,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        p_sort = subprocess.Popen(
            ["sort"],
            stdin=p_mapper.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        p_reducer = subprocess.Popen(
            [sys.executable, str(reducer_path)],
            stdin=p_sort.stdout,
            stdout=fh_out,
            stderr=subprocess.PIPE,
        )

        # Allow p_mapper and p_sort to receive SIGPIPE if downstream exits
        if p_mapper.stdout:
            p_mapper.stdout.close()
        if p_sort.stdout:
            p_sort.stdout.close()

        _, mapper_err = p_mapper.communicate()
        _, sort_err = p_sort.communicate()
        _, reducer_err = p_reducer.communicate()

        if p_mapper.returncode != 0:
            raise RuntimeError(f"Mapper failed with return code {p_mapper.returncode}: {mapper_err.decode('utf-8')}")
        if p_sort.returncode != 0:
            raise RuntimeError(f"Sort failed with return code {p_sort.returncode}: {sort_err.decode('utf-8')}")
        if p_reducer.returncode != 0:
            raise RuntimeError(f"Reducer failed with return code {p_reducer.returncode}: {reducer_err.decode('utf-8')}")

    # Count output feature lines
    count = 0
    with open(output_features_file, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                count += 1
    return count


def execute_spark_clustering(
    enriched_commits_file: Path,
    output_clusters_file: Path,
    k: int = 4,
    seed: int = 42,
) -> tuple[int, float | None]:
    """
    Execute Spark MLlib clustering using safe subprocess argument lists.
    Returns (clusters_count, repos_clustered, silhouette_score).
    """
    spark_script = PROJECT_ROOT / "spark" / "cluster_repos.py"

    cmd = [
        sys.executable,
        str(spark_script),
        "--input", str(enriched_commits_file),
        "--output", str(output_clusters_file),
        "--k", str(k),
        "--seed", str(seed),
    ]

    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"Spark clustering failed (exit {proc.returncode}):\n{proc.stderr}\n{proc.stdout}")

    if not output_clusters_file.exists():
        raise RuntimeError(f"Spark clustering succeeded but output file not found: {output_clusters_file}")

    with open(output_clusters_file, "r", encoding="utf-8") as f:
        clusters_data = json.load(f)

    # Parse silhouette score from output if available
    silhouette = None
    for line in proc.stdout.splitlines():
        if "Silhouette score" in line:
            m = re.search(r"Silhouette score\s*=\s*([-+]?\d*\.?\d+)", line)
            if m:
                try:
                    val = float(m.group(1))
                    if -1.0 <= val <= 1.0:
                        silhouette = round(val, 4)
                except ValueError:
                    pass

    distinct_clusters = len(set(r.get("cluster") for r in clusters_data if "cluster" in r))
    repos_clustered = len(clusters_data)
    return distinct_clusters, repos_clustered, silhouette


def run_pipeline(
    repositories: list[str],
    commit_limit: int = 50,
    output_dir: Path | str | None = None,
    clusters: int = 4,
    seed: int = 42,
    token: str | None = None,
    timeout: float = 30.0,
    use_hadoop: bool = False,
    skip_spark: bool = False,
    verbose: bool = True,
) -> dict:
    """
    Orchestrate full end-to-end ingestion, aggregation, and clustering pipeline.

    Returns:
      pipeline_manifest dictionary.
    """
    start_time = datetime.now(timezone.utc)

    if not repositories:
        raise ValueError("At least one repository identifier must be provided.")

    # Validate all repository identifiers first before starting work
    canonical_repos = []
    for r in repositories:
        owner, repo = parse_repository_identifier(r)
        canonical_repos.append(f"{owner}/{repo}")

    if output_dir is None:
        analysis_dir = generate_analysis_directory()
    else:
        analysis_dir = Path(output_dir)

    validate_analysis_directory(analysis_dir)

    if analysis_dir.exists():
        # Disallow reusing an output directory that already has a pipeline manifest or non-repo contents
        if (analysis_dir / "pipeline_manifest.json").exists():
            raise ValueError(f"Output directory '{analysis_dir}' already contains a completed pipeline manifest.")
        non_repo_items = [p.name for p in analysis_dir.iterdir() if p.name != "repositories"]
        if non_repo_items:
            raise ValueError(f"Output directory '{analysis_dir}' already exists and contains unexpected items: {non_repo_items}")

    analysis_dir.mkdir(parents=True, exist_ok=True)
    repos_sub_dir = analysis_dir / "repositories"
    repos_sub_dir.mkdir(parents=True, exist_ok=True)

    manifest: dict = {
        "manifest_version": "1.0",
        "analysis_id": analysis_dir.name,
        "type": "orchestrated_analysis",
        "started_at": start_time.isoformat(),
        "completed_at": None,
        "duration_seconds": None,
        "status": "in_progress",
        "repositories_requested": canonical_repos,
        "commit_limit": commit_limit,
        "stages": {
            "ingestion": {
                "status": "pending",
                "repositories_attempted": len(canonical_repos),
                "repositories_succeeded": 0,
                "repositories_failed": 0,
                "runs": [],
            },
            "mapreduce": {
                "status": "pending",
                "mode": "hadoop" if use_hadoop else "local",
                "features_count": 0,
                "output_file": "repo_features.jsonl",
            },
            "spark": {
                "status": "pending",
                "k": clusters,
                "seed": seed,
                "clusters_count": 0,
                "repositories_clustered": 0,
                "silhouette": None,
                "silhouette_status": "pending",
                "silhouette_reason": None,
                "output_file": "repo_clusters.json",
            },
        },
        "counts": {
            "repositories": 0,
            "commits": 0,
            "enriched_commits": 0,
            "authentic_records": 0,
            "synthetic_records": 0,
        },
        "files": {
            "repositories": "repositories.jsonl",
            "commits": "commits.jsonl",
            "enriched_commits": "enriched_commits.jsonl",
            "features": "repo_features.jsonl",
            "clusters": None,
            "manifest": "pipeline_manifest.json",
        },
        "warnings": [],
        "errors": [],
    }

    path_combined_repos = analysis_dir / "repositories.jsonl"
    path_combined_commits = analysis_dir / "commits.jsonl"
    path_combined_enriched = analysis_dir / "enriched_commits.jsonl"
    path_features = analysis_dir / "repo_features.jsonl"
    path_clusters = analysis_dir / "repo_clusters.json"
    path_manifest = analysis_dir / "pipeline_manifest.json"
    path_compat_manifest = analysis_dir / "manifest.json"

    def _log(msg: str) -> None:
        if verbose:
            print(msg)

    def _write_manifest(status: str) -> None:
        manifest["status"] = status
        end_time = datetime.now(timezone.utc)
        if status in ("complete", "partial"):
            manifest["completed_at"] = end_time.isoformat()
        manifest["duration_seconds"] = round((end_time - start_time).total_seconds(), 2)

        with path_manifest.open("w", encoding="utf-8") as fh:
            json.dump(manifest, fh, indent=2)
        with path_compat_manifest.open("w", encoding="utf-8") as fh:
            json.dump(manifest, fh, indent=2)

    _log("=" * 70)
    _log(f"Git Pattern Analysis — Pipeline Orchestration: {len(canonical_repos)} Repositories")
    _log(f"  Analysis Directory : {analysis_dir}")
    _log(f"  Repositories       : {', '.join(canonical_repos)}")
    _log(f"  Commit Limit       : {commit_limit} per repository")
    _log("=" * 70)

    try:
        # ===================================================================
        # STAGE 1: INGESTION
        # ===================================================================
        _log("\n[STAGE 1/4] Ingesting repository metadata and commit history …")
        ingestion_runs = []
        successful_repo_dirs = []

        for repo_identifier in canonical_repos:
            owner, repo_name = parse_repository_identifier(repo_identifier)
            repo_slug = f"{owner}_{repo_name}"
            repo_run_dir = repos_sub_dir / repo_slug

            try:
                # Reuse pre-existing authentic repository data if already present in target directory
                if (
                    repo_run_dir.exists()
                    and (repo_run_dir / "enriched_commits.jsonl").exists()
                    and (repo_run_dir / "repositories.jsonl").exists()
                ):
                    _log(f"  ✓ Reusing existing authentic data for {repo_identifier} (avoids API quota consumption)")
                    manifest_file = repo_run_dir / "manifest.json"
                    run_manifest = {}
                    if manifest_file.exists():
                        try:
                            with manifest_file.open("r", encoding="utf-8") as fh:
                                run_manifest = json.load(fh)
                        except Exception:
                            run_manifest = {}

                    c_file = repo_run_dir / "commits.jsonl"
                    e_file = repo_run_dir / "enriched_commits.jsonl"
                    c_count = sum(1 for l in c_file.open("r", encoding="utf-8") if l.strip()) if c_file.exists() else 0
                    e_count = sum(1 for l in e_file.open("r", encoding="utf-8") if l.strip()) if e_file.exists() else 0

                    run_manifest.setdefault("status", "truncated" if c_count >= commit_limit else "complete")
                    run_manifest.setdefault("counts", {"commits": c_count, "enriched_commits": e_count})
                else:
                    run_manifest = analyze_repository(
                        identifier=repo_identifier,
                        output_dir=repo_run_dir,
                        commit_limit=commit_limit,
                        token=token,
                        timeout=timeout,
                        verbose=verbose,
                    )
                ingestion_runs.append({
                    "repository": repo_identifier,
                    "status": run_manifest["status"],
                    "commits": run_manifest["counts"]["commits"],
                    "enriched": run_manifest["counts"]["enriched_commits"],
                    "directory": str(repo_run_dir.relative_to(analysis_dir)),
                })
                successful_repo_dirs.append(repo_run_dir)
                manifest["stages"]["ingestion"]["repositories_succeeded"] += 1
            except Exception as err:
                _log(f"  ✗ Ingestion failed for {repo_identifier}: {err}")
                ingestion_runs.append({
                    "repository": repo_identifier,
                    "status": "failed",
                    "error": str(err),
                })
                manifest["stages"]["ingestion"]["repositories_failed"] += 1
                manifest["warnings"].append(f"Ingestion failed for {repo_identifier}: {err}")

        manifest["stages"]["ingestion"]["runs"] = ingestion_runs

        if not successful_repo_dirs:
            manifest["stages"]["ingestion"]["status"] = "failed"
            manifest["errors"].append("All requested repositories failed ingestion. Pipeline halted.")
            _write_manifest("failed")
            _log("\n[FATAL] No repositories were successfully ingested. Halting pipeline.")
            return manifest

        manifest["stages"]["ingestion"]["status"] = "success"
        _log(f"  ✓ Stage 1 complete: {len(successful_repo_dirs)}/{len(canonical_repos)} repositories ready.")

        # ===================================================================
        # STAGE 2: DATASET COMBINATION
        # ===================================================================
        _log("\n[STAGE 2/4] Aggregating multi-repository datasets …")
        total_repos_count = 0
        total_commits_count = 0
        total_enriched_count = 0

        with path_combined_repos.open("w", encoding="utf-8") as f_repos, \
             path_combined_commits.open("w", encoding="utf-8") as f_commits, \
             path_combined_enriched.open("w", encoding="utf-8") as f_enriched:

            for r_dir in successful_repo_dirs:
                p_r = r_dir / "repositories.jsonl"
                p_c = r_dir / "commits.jsonl"
                p_e = r_dir / "enriched_commits.jsonl"

                if p_r.exists():
                    with p_r.open("r", encoding="utf-8") as fh:
                        for line in fh:
                            if line.strip():
                                f_repos.write(line)
                                total_repos_count += 1

                if p_c.exists():
                    with p_c.open("r", encoding="utf-8") as fh:
                        for line in fh:
                            if line.strip():
                                f_commits.write(line)
                                total_commits_count += 1

                if p_e.exists():
                    with p_e.open("r", encoding="utf-8") as fh:
                        for line in fh:
                            if line.strip():
                                f_enriched.write(line)
                                total_enriched_count += 1

        manifest["counts"]["repositories"] = total_repos_count
        manifest["counts"]["commits"] = total_commits_count
        manifest["counts"]["enriched_commits"] = total_enriched_count
        manifest["counts"]["authentic_records"] = total_enriched_count
        manifest["counts"]["synthetic_records"] = 0

        _log(f"  ✓ Combined records: {total_repos_count} repos, {total_commits_count} commits, {total_enriched_count} enriched.")

        if total_enriched_count == 0:
            manifest["stages"]["mapreduce"]["status"] = "skipped"
            manifest["stages"]["mapreduce"]["reason"] = "Zero enriched commits available for aggregation."
            manifest["stages"]["spark"]["status"] = "skipped"
            manifest["stages"]["spark"]["reason"] = "Zero commits available."
            _write_manifest("complete" if manifest["stages"]["ingestion"]["repositories_failed"] == 0 else "partial")
            return manifest

        # ===================================================================
        # STAGE 3: MAPREDUCE FEATURE AGGREGATION
        # ===================================================================
        _log("\n[STAGE 3/4] Running MapReduce feature aggregation …")
        try:
            if use_hadoop:
                _log("  Attempting distributed execution on Hadoop/HDFS …")
                # Parameterized invocation of run_mapreduce.sh
                hdfs_base = f"/user/hadoop/git-pattern/runs/{analysis_dir.name}"
                up_cmd = [
                    "bash", str(PROJECT_ROOT / "scripts" / "upload_to_hdfs.sh"),
                    "--hdfs-base", hdfs_base,
                    "--local-data", str(analysis_dir),
                ]
                mr_cmd = [
                    "bash", str(PROJECT_ROOT / "scripts" / "run_mapreduce.sh"),
                    "--hdfs-base", hdfs_base,
                    "--local-output", str(analysis_dir),
                ]
                up_res = subprocess.run(up_cmd, capture_output=True, text=True)
                if up_res.returncode == 0:
                    mr_res = subprocess.run(mr_cmd, capture_output=True, text=True)
                    if mr_res.returncode == 0:
                        manifest["stages"]["mapreduce"]["mode"] = "hadoop_streaming"
                        _log("  ✓ Distributed Hadoop Streaming MapReduce completed successfully.")
                    else:
                        _log("  ⚠ Hadoop Streaming job failed, falling back to local MapReduce.")
                        execute_local_mapreduce(path_combined_enriched, path_features)
                else:
                    _log("  ⚠ HDFS upload failed (cluster likely not running), falling back to local MapReduce.")
                    execute_local_mapreduce(path_combined_enriched, path_features)
            else:
                features_count = execute_local_mapreduce(path_combined_enriched, path_features)
                manifest["stages"]["mapreduce"]["mode"] = "local_streaming"
                manifest["stages"]["mapreduce"]["features_count"] = features_count
                _log(f"  ✓ Local MapReduce completed: {features_count} repository features generated.")

            manifest["stages"]["mapreduce"]["status"] = "success"
        except Exception as mr_err:
            manifest["stages"]["mapreduce"]["status"] = "failed"
            manifest["stages"]["mapreduce"]["error"] = str(mr_err)
            manifest["errors"].append(f"MapReduce stage failed: {mr_err}")
            _write_manifest("failed")
            _log(f"  ✗ MapReduce failed: {mr_err}")
            raise

        # ===================================================================
        # STAGE 4: SPARK MLLIB CLUSTERING (SAFEGUARDED)
        # ===================================================================
        _log("\n[STAGE 4/4] Evaluating Spark MLlib clustering prerequisites …")
        features_count = manifest["stages"]["mapreduce"].get("features_count", 0)

        if skip_spark:
            manifest["stages"]["spark"]["status"] = "skipped"
            manifest["stages"]["spark"]["reason"] = "User explicitly requested --skip-spark."
            _log("  ℹ Spark clustering skipped (--skip-spark flag passed).")
        elif features_count < 2:
            manifest["stages"]["spark"]["status"] = "skipped"
            manifest["stages"]["spark"]["reason"] = (
                f"Clustering requires at least 2 repositories (found {features_count}). "
                "Single-repository clustering is mathematically degenerate and not presented as meaningful analysis."
            )
            _log(f"  ℹ Spark clustering skipped: requires ≥ 2 repositories (found {features_count}).")
        else:
            effective_k = min(clusters, features_count)
            if effective_k < clusters:
                _log(f"  ℹ Adjusted k from {clusters} to {effective_k} based on available repositories.")

            _log(f"  Running PySpark K-Means clustering (k={effective_k}, seed={seed}) …")
            try:
                clusters_count, repos_clustered, silhouette = execute_spark_clustering(
                    enriched_commits_file=path_combined_enriched,
                    output_clusters_file=path_clusters,
                    k=effective_k,
                    seed=seed,
                )
                manifest["stages"]["spark"]["status"] = "success"
                manifest["stages"]["spark"]["k"] = effective_k
                manifest["stages"]["spark"]["clusters_count"] = clusters_count
                manifest["stages"]["spark"]["repositories_clustered"] = repos_clustered
                manifest["stages"]["spark"]["silhouette"] = silhouette
                if silhouette is not None:
                    manifest["stages"]["spark"]["silhouette_status"] = "valid"
                    manifest["stages"]["spark"]["silhouette_reason"] = "Standard Euclidean silhouette coefficient in range [-1.0, 1.0]."
                else:
                    manifest["stages"]["spark"]["silhouette_status"] = "unavailable"
                    manifest["stages"]["spark"]["silhouette_reason"] = (
                        "Silhouette score is mathematically undefined or not meaningful for degenerate "
                        "configurations (e.g. number of repositories N equals cluster count K, resulting in all singleton clusters)."
                    )
                manifest["files"]["clusters"] = "repo_clusters.json"
                _log(f"  ✓ Spark clustering complete: {repos_clustered} repos assigned to {clusters_count} clusters (silhouette: {silhouette}).")
            except Exception as spark_err:
                manifest["stages"]["spark"]["status"] = "failed"
                manifest["stages"]["spark"]["error"] = str(spark_err)
                manifest["warnings"].append(f"Spark clustering failed: {spark_err}")
                _log(f"  ⚠ Spark clustering stage failed: {spark_err}")

        # Finalize
        overall_status = "complete" if manifest["stages"]["ingestion"]["repositories_failed"] == 0 else "partial"
        _write_manifest(overall_status)

        _log("\n" + "=" * 70)
        _log(f"✓ Pipeline Finished: {overall_status.upper()}")
        _log(f"  Run Artifacts : {analysis_dir}")
        _log(f"  Manifest      : {path_manifest}")
        _log("=" * 70)

        return manifest

    except Exception as exc:
        manifest["errors"].append(str(exc))
        _write_manifest("failed")
        _log(f"\n[FATAL] Pipeline execution halted: {exc}")
        raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python3 scripts/run_pipeline.py",
        description="Run end-to-end Git Pattern Analysis (Ingestion → MapReduce → Spark) on arbitrary GitHub repositories.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single repository analysis (MapReduce features produced; Spark clustering skipped safely):
  python3 scripts/run_pipeline.py octocat/Hello-World

  # Multi-repository analysis with clustering (k=2):
  python3 scripts/run_pipeline.py psf/requests pallets/flask --max-commits 20 --clusters 2

  # Custom output directory:
  python3 scripts/run_pipeline.py --output-dir data/runs/my_analysis psf/requests
        """,
    )
    parser.add_argument(
        "repositories",
        nargs="+",
        help="One or more GitHub repository URLs or 'owner/repo' slugs to analyze.",
    )
    parser.add_argument(
        "-n", "--max-commits", "--limit",
        type=int,
        default=50,
        help="Maximum commits to retrieve per repository (default: 50).",
    )
    parser.add_argument(
        "-o", "--output-dir",
        default=None,
        help="Isolated directory path for analysis artifacts (default: data/runs/analysis-<timestamp>-<id>/).",
    )
    parser.add_argument(
        "-k", "--clusters",
        type=int,
        default=4,
        help="Number of clusters for Spark MLlib K-Means (default: 4).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for Spark clustering (default: 42).",
    )
    parser.add_argument(
        "-t", "--token",
        default=None,
        help="GitHub personal access token (optional; reads GITHUB_TOKEN environment variable by default).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP request timeout in seconds (default: 30.0).",
    )
    parser.add_argument(
        "--hadoop",
        action="store_true",
        help="Run MapReduce in Docker Hadoop cluster if running (falls back to local MapReduce).",
    )
    parser.add_argument(
        "--skip-spark",
        action="store_true",
        help="Skip the Spark clustering stage entirely.",
    )
    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Suppress console progress output.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.max_commits <= 0:
        parser.error("--max-commits must be a positive integer.")
    if args.clusters <= 1:
        parser.error("--clusters must be an integer >= 2.")

    try:
        manifest = run_pipeline(
            repositories=args.repositories,
            commit_limit=args.max_commits,
            output_dir=args.output_dir,
            clusters=args.clusters,
            seed=args.seed,
            token=args.token,
            timeout=args.timeout,
            use_hadoop=args.hadoop,
            skip_spark=args.skip_spark,
            verbose=not args.quiet,
        )
        status = manifest.get("status", "unknown")
        if status in ("complete", "partial"):
            sys.exit(0)
        else:
            sys.exit(1)
    except InvalidIdentifierError as exc:
        print(f"[ERROR] Invalid repository: {exc}", file=sys.stderr)
        sys.exit(2)
    except Exception as exc:
        print(f"[FATAL] Pipeline error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
