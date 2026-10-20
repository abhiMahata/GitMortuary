"""
tests/test_pipeline.py
----------------------
Automated unit and integration test suite for the end-to-end analysis pipeline orchestrator.
Tests path isolation, stage failure handling, single-repo vs multi-repo Spark safeguards,
local MapReduce streaming, manifest consistency, and dashboard run integration.
"""

import json
from pathlib import Path
import sys
import pytest

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.run_pipeline import (
    generate_analysis_directory,
    validate_analysis_directory,
    execute_local_mapreduce,
    run_pipeline,
)
from dashboard.server import (
    list_available_runs,
    get_dashboard_data,
)


# ===========================================================================
# 1. PATH SAFETY & ISOLATION TESTS
# ===========================================================================

class TestPathSafetyAndIsolation:
    def test_generate_analysis_directory_creates_unique_subfolder(self, tmp_path):
        d1 = generate_analysis_directory(base_dir=tmp_path)
        d2 = generate_analysis_directory(base_dir=tmp_path)
        assert d1 != d2
        assert "analysis-" in d1.name
        assert d1.parent == tmp_path

    def test_validate_analysis_directory_rejects_sample_and_output(self):
        sample_dir = PROJECT_ROOT / "data" / "sample"
        output_dir = PROJECT_ROOT / "data" / "output"

        with pytest.raises(ValueError, match="Cannot use protected path"):
            validate_analysis_directory(sample_dir)

        with pytest.raises(ValueError, match="Cannot use protected path"):
            validate_analysis_directory(sample_dir / "nested")

        with pytest.raises(ValueError, match="Cannot use protected path"):
            validate_analysis_directory(output_dir)

    def test_validate_analysis_directory_accepts_runs_folder(self, tmp_path):
        safe_dir = tmp_path / "analysis-20261020T120000Z-12345678"
        # Must not raise
        validate_analysis_directory(safe_dir)


# ===========================================================================
# 2. LOCAL MAPREDUCE STREAMING EXECUTION
# ===========================================================================

class TestLocalMapReduceExecution:
    def test_execute_local_mapreduce_on_test_fixture(self, tmp_path):
        input_file = tmp_path / "enriched_commits.jsonl"
        output_file = tmp_path / "repo_features.jsonl"

        commits = [
            {
                "repo_name": "octocat/Hello-World",
                "commit_sha": "sha1",
                "parent_count": 1,
                "additions": 10,
                "deletions": 2,
                "lines_changed": 12,
                "files_changed": 1,
                "stats_available": True,
            },
            {
                "repo_name": "octocat/Hello-World",
                "commit_sha": "sha2",
                "parent_count": 2,
                "additions": 20,
                "deletions": 5,
                "lines_changed": 25,
                "files_changed": 2,
                "stats_available": True,
            },
        ]
        with input_file.open("w", encoding="utf-8") as fh:
            for c in commits:
                fh.write(json.dumps(c) + "\n")

        count = execute_local_mapreduce(input_file, output_file)
        assert count == 1
        assert output_file.exists()

        with output_file.open("r", encoding="utf-8") as fh:
            features = json.loads(fh.readline())

        assert features["repo_name"] == "octocat/Hello-World"
        assert features["commit_count"] == 2
        assert features["total_additions"] == 30
        assert features["total_deletions"] == 7
        assert features["merge_count"] == 1
        assert features["merge_ratio"] == 0.5

    def test_execute_local_mapreduce_missing_input_raises_error(self, tmp_path):
        missing = tmp_path / "does_not_exist.jsonl"
        out = tmp_path / "out.jsonl"
        with pytest.raises(ValueError, match="does not exist or is empty"):
            execute_local_mapreduce(missing, out)


# ===========================================================================
# 3. ORCHESTRATION & SPARK SAFEGUARDS
# ===========================================================================

class TestOrchestratorSafeguards:
    def test_single_repository_skips_spark_clustering_safely(self, monkeypatch, tmp_path):
        """Single repo should produce MapReduce features but skip Spark clustering."""
        run_dir = tmp_path / "analysis-single"

        # Mock analyze_repository to produce an isolated repo without network
        def mock_analyze(identifier, output_dir, commit_limit, token, timeout, verbose):
            p = Path(output_dir)
            p.mkdir(parents=True, exist_ok=True)
            (p / "repositories.jsonl").write_text(
                json.dumps({"repo_name": "test/solo", "repo_id": 1, "language": "Python"}) + "\n"
            )
            (p / "commits.jsonl").write_text(
                json.dumps({"repo_name": "test/solo", "commit_sha": "c1"}) + "\n"
            )
            (p / "enriched_commits.jsonl").write_text(
                json.dumps({
                    "repo_name": "test/solo",
                    "commit_sha": "c1",
                    "parent_count": 1,
                    "additions": 10,
                    "deletions": 2,
                    "lines_changed": 12,
                    "files_changed": 1,
                    "stats_available": True,
                    "synthetic": False,
                }) + "\n"
            )
            return {
                "status": "complete",
                "counts": {"commits": 1, "enriched_commits": 1},
            }

        monkeypatch.setattr("scripts.run_pipeline.analyze_repository", mock_analyze)

        manifest = run_pipeline(
            repositories=["test/solo"],
            output_dir=run_dir,
            commit_limit=5,
            use_hadoop=False,
            verbose=False,
        )

        assert manifest["status"] == "complete"
        assert manifest["stages"]["ingestion"]["status"] == "success"
        assert manifest["stages"]["mapreduce"]["status"] == "success"
        # Spark clustering must be skipped
        assert manifest["stages"]["spark"]["status"] == "skipped"
        assert "requires at least 2 repositories" in manifest["stages"]["spark"]["reason"]
        assert (run_dir / "repo_features.jsonl").exists()
        assert not (run_dir / "repo_clusters.json").exists()

    def test_multi_repository_runs_spark_clustering(self, monkeypatch, tmp_path):
        """Two repositories should produce MapReduce features and run Spark clustering (k=2)."""
        run_dir = tmp_path / "analysis-multi"

        def mock_analyze(identifier, output_dir, commit_limit, token, timeout, verbose):
            p = Path(output_dir)
            p.mkdir(parents=True, exist_ok=True)
            repo_name = identifier
            (p / "repositories.jsonl").write_text(
                json.dumps({"repo_name": repo_name, "repo_id": hash(repo_name), "language": "Python"}) + "\n"
            )
            (p / "commits.jsonl").write_text(
                json.dumps({"repo_name": repo_name, "commit_sha": f"c_{repo_name}"}) + "\n"
            )
            (p / "enriched_commits.jsonl").write_text(
                json.dumps({
                    "repo_name": repo_name,
                    "commit_sha": f"c_{repo_name}",
                    "parent_count": 1,
                    "additions": 10 if "1" in repo_name else 100,
                    "deletions": 2,
                    "lines_changed": 12 if "1" in repo_name else 102,
                    "files_changed": 1,
                    "stats_available": True,
                    "synthetic": False,
                }) + "\n"
            )
            return {
                "status": "complete",
                "counts": {"commits": 1, "enriched_commits": 1},
            }

        monkeypatch.setattr("scripts.run_pipeline.analyze_repository", mock_analyze)

        manifest = run_pipeline(
            repositories=["test/repo1", "test/repo2"],
            output_dir=run_dir,
            commit_limit=5,
            clusters=2,
            use_hadoop=False,
            verbose=False,
        )

        assert manifest["status"] == "complete"
        assert manifest["stages"]["ingestion"]["status"] == "success"
        assert manifest["stages"]["mapreduce"]["status"] == "success"
        assert manifest["stages"]["spark"]["status"] == "success"
        assert manifest["stages"]["spark"]["clusters_count"] == 2
        assert manifest["stages"]["spark"]["repositories_clustered"] == 2
        # N=2, k=2 is degenerate (all singletons), so silhouette is unavailable
        assert manifest["stages"]["spark"]["silhouette"] is None
        assert manifest["stages"]["spark"]["silhouette_status"] == "unavailable"
        assert (run_dir / "repo_features.jsonl").exists()
        assert (run_dir / "repo_clusters.json").exists()

    def test_multi_repository_runs_spark_clustering_with_valid_silhouette(self, monkeypatch, tmp_path):
        """Three repositories with k=2 (N > k) should produce a valid, mathematically sound silhouette score."""
        run_dir = tmp_path / "analysis-multi-valid-sil"

        def mock_analyze(identifier, output_dir, commit_limit, token, timeout, verbose):
            p = Path(output_dir)
            p.mkdir(parents=True, exist_ok=True)
            repo_name = identifier
            add_val = 10 if "1" in repo_name else (12 if "2" in repo_name else 500)
            (p / "repositories.jsonl").write_text(
                json.dumps({"repo_name": repo_name, "repo_id": hash(repo_name), "language": "Python"}) + "\n"
            )
            (p / "commits.jsonl").write_text(
                json.dumps({"repo_name": repo_name, "commit_sha": f"c_{repo_name}"}) + "\n"
            )
            (p / "enriched_commits.jsonl").write_text(
                json.dumps({
                    "repo_name": repo_name,
                    "commit_sha": f"c_{repo_name}",
                    "parent_count": 1,
                    "additions": add_val,
                    "deletions": 2,
                    "lines_changed": add_val + 2,
                    "files_changed": 1,
                    "stats_available": True,
                    "synthetic": False,
                }) + "\n"
            )
            return {"status": "complete", "counts": {"commits": 1, "enriched_commits": 1}}

        monkeypatch.setattr("scripts.run_pipeline.analyze_repository", mock_analyze)

        manifest = run_pipeline(
            repositories=["test/repo1", "test/repo2", "test/repo3"],
            output_dir=run_dir,
            commit_limit=5,
            clusters=2,
            use_hadoop=False,
            verbose=False,
        )

        assert manifest["status"] == "complete"
        assert manifest["stages"]["spark"]["status"] == "success"
        assert manifest["stages"]["spark"]["clusters_count"] == 2
        assert manifest["stages"]["spark"]["repositories_clustered"] == 3
        # N=3, k=2 is non-degenerate; silhouette is valid
        assert manifest["stages"]["spark"]["silhouette"] is not None
        assert manifest["stages"]["spark"]["silhouette_status"] == "valid"
        assert -1.0 <= manifest["stages"]["spark"]["silhouette"] <= 1.0

    def test_skip_spark_flag_skips_clustering_explicitly(self, monkeypatch, tmp_path):
        run_dir = tmp_path / "analysis-skip-spark"

        def mock_analyze(identifier, output_dir, commit_limit, token, timeout, verbose):
            p = Path(output_dir)
            p.mkdir(parents=True, exist_ok=True)
            (p / "repositories.jsonl").write_text(json.dumps({"repo_name": identifier}) + "\n")
            (p / "commits.jsonl").write_text(json.dumps({"repo_name": identifier, "commit_sha": "c1"}) + "\n")
            (p / "enriched_commits.jsonl").write_text(
                json.dumps({"repo_name": identifier, "commit_sha": "c1", "parent_count": 1, "additions": 5, "deletions": 1, "lines_changed": 6, "files_changed": 1, "stats_available": True}) + "\n"
            )
            return {"status": "complete", "counts": {"commits": 1, "enriched_commits": 1}}

        monkeypatch.setattr("scripts.run_pipeline.analyze_repository", mock_analyze)

        manifest = run_pipeline(
            repositories=["test/repo1", "test/repo2"],
            output_dir=run_dir,
            skip_spark=True,
            use_hadoop=False,
            verbose=False,
        )

        assert manifest["stages"]["spark"]["status"] == "skipped"
        assert "explicitly requested --skip-spark" in manifest["stages"]["spark"]["reason"]

    def test_all_repositories_failing_halts_pipeline(self, monkeypatch, tmp_path):
        run_dir = tmp_path / "analysis-fail"

        def mock_failing_analyze(identifier, output_dir, commit_limit, token, timeout, verbose):
            raise RuntimeError("Network timeout simulation")

        monkeypatch.setattr("scripts.run_pipeline.analyze_repository", mock_failing_analyze)

        manifest = run_pipeline(
            repositories=["invalid/nonexistent"],
            output_dir=run_dir,
            use_hadoop=False,
            verbose=False,
        )

        assert manifest["status"] == "failed"
        assert manifest["stages"]["ingestion"]["status"] == "failed"
        assert manifest["stages"]["mapreduce"]["status"] == "pending"


# ===========================================================================
# 4. DASHBOARD RUN INTEGRATION & PATH TRAVERSAL GUARDS
# ===========================================================================

class TestDashboardRunIntegration:
    def test_list_available_runs_contains_sample(self):
        runs = list_available_runs()
        assert len(runs) >= 1
        sample_run = next((r for r in runs if r["run_id"] == "sample"), None)
        assert sample_run is not None
        assert sample_run["is_sample"] is True
        assert sample_run["features_available"] is True
        assert sample_run["clusters_available"] is True

    def test_get_dashboard_data_sample_default(self):
        data = get_dashboard_data(run_id="sample")
        assert data["run_info"]["is_sample"] is True
        assert data["overview"]["total_repositories"] == 20
        assert data["overview"]["total_commits"] == 1000

    def test_get_dashboard_data_invalid_path_traversal_rejected(self):
        with pytest.raises(ValueError, match="Invalid run_id format"):
            get_dashboard_data(run_id="../../etc/passwd")

        with pytest.raises(ValueError, match="Invalid run_id format"):
            get_dashboard_data(run_id="foo/bar")

        with pytest.raises(ValueError, match="Invalid run_id format"):
            get_dashboard_data(run_id="..")

    def test_get_dashboard_data_nonexistent_run_raises_404(self):
        with pytest.raises(FileNotFoundError, match="not found under data/runs"):
            get_dashboard_data(run_id="nonexistent-run-12345678")

    def test_get_dashboard_data_live_run_agrees_with_artifacts(self):
        """Verify that selecting live run loads actual artifacts matching manifest and outputs."""
        live_run_id = "analysis-20261020T064812Z-04c4400f"
        data = get_dashboard_data(run_id=live_run_id)

        assert data["run_info"]["run_id"] == live_run_id
        assert data["run_info"]["is_sample"] is False
        assert data["run_info"]["status"] == "complete"

        # Overview counts
        assert data["overview"]["total_repositories"] == 4
        assert data["overview"]["total_commits"] == 40
        assert data["overview"]["total_enriched"] == 40
        assert data["overview"]["authentic_enriched"] == 40
        assert data["overview"]["synthetic_enriched"] == 0
        assert data["overview"]["has_truncated_history"] is True

        # Provenance notice
        assert data["provenance_notice"]["is_authentic_live"] is True
        assert "100% Authentic" in data["provenance_notice"]["authentic_badge"]
        assert data["provenance_notice"]["has_truncated_history"] is True

        # Cluster and silhouette agreement
        assert data["cluster_summary"]["k"] == 2
        assert data["cluster_summary"]["distance_measure"] == "squaredEuclidean"
        assert data["cluster_summary"]["silhouette"] == 0.569
        assert data["cluster_summary"]["silhouette_status"] == "valid"

        # Specific cluster assignments
        repo_clusters = {r["repo_name"]: r["cluster"] for r in data["repositories"]}
        assert repo_clusters["pallets/click"] == 0
        assert repo_clusters["encode/httpx"] == 1
        assert repo_clusters["pallets/flask"] == 1
        assert repo_clusters["psf/requests"] == 1

        # Truncation flags per repo
        for r in data["repositories"]:
            assert r["is_truncated"] is True

    def test_get_dashboard_data_empty_run_produces_clear_status(self):
        """An empty run directory must produce an explicit empty status and warning message."""
        from dashboard.server import PROJECT_ROOT
        actual_runs = PROJECT_ROOT / "data" / "runs"
        test_empty_dir = actual_runs / "test-empty-run-tmp"
        try:
            test_empty_dir.mkdir(parents=True, exist_ok=True)
            data = get_dashboard_data(run_id="test-empty-run-tmp")
            assert data["run_info"]["status"] == "empty"
            assert any("contains no repository" in w for w in data["run_info"]["warnings"])
            assert data["overview"]["total_repositories"] == 0
        finally:
            if test_empty_dir.exists():
                test_empty_dir.rmdir()

    def test_get_dashboard_data_malformed_manifest_handled_gracefully(self):
        """Malformed JSON manifest must result in status='malformed' rather than crashing."""
        from dashboard.server import PROJECT_ROOT
        actual_runs = PROJECT_ROOT / "data" / "runs"
        test_malformed_dir = actual_runs / "test-malformed-run-tmp"
        try:
            test_malformed_dir.mkdir(parents=True, exist_ok=True)
            (test_malformed_dir / "pipeline_manifest.json").write_text("{ corrupt json: [")
            data = get_dashboard_data(run_id="test-malformed-run-tmp")
            assert data["run_info"]["status"] == "malformed"
            assert any("malformed JSON" in w for w in data["run_info"]["warnings"])
        finally:
            if (test_malformed_dir / "pipeline_manifest.json").exists():
                (test_malformed_dir / "pipeline_manifest.json").unlink()
            if test_malformed_dir.exists():
                test_malformed_dir.rmdir()

    def test_unavailable_silhouette_score_not_returned_as_number(self):
        """Single-repository run with skipped clustering must report silhouette as None, not 0.0 or a number."""
        single_repo_run = "analysis-20261019T201621Z-a569d49e"
        data = get_dashboard_data(run_id=single_repo_run)
        assert data["cluster_summary"]["silhouette"] is None
        assert data["cluster_summary"]["silhouette_status"] == "unavailable"
        assert not isinstance(data["cluster_summary"]["silhouette"], (int, float))

