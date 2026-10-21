"""
tests/test_dashboard.py
-----------------------
Automated tests for the dashboard server, artifact loading, and metric calculations.
Tests missing-file behavior, malformed data handling, and HTTP endpoints.
"""

import json
import sys
import threading
import time
from pathlib import Path
import pytest
import requests

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dashboard.server import (
    load_json,
    load_jsonl,
    get_dashboard_data,
    compute_single_repo_analytics,
    generate_commits_csv,
    generate_repos_csv,
    DashboardHandler,
    HTTPServer,
)


# ===========================================================================
# 1. ARTIFACT LOADER & TOLERANCE TESTS
# ===========================================================================

class TestArtifactLoader:
    def test_load_jsonl_valid(self, tmp_path):
        p = tmp_path / "valid.jsonl"
        p.write_text('{"a": 1}\n{"b": 2}\n', encoding="utf-8")
        loaded = load_jsonl(p)
        assert loaded == [{"a": 1}, {"b": 2}]

    def test_load_jsonl_missing_file_returns_empty_list(self, tmp_path):
        p = tmp_path / "does_not_exist.jsonl"
        assert load_jsonl(p) == []

    def test_load_jsonl_skips_malformed_lines(self, tmp_path):
        p = tmp_path / "corrupt.jsonl"
        p.write_text('{"valid": 1}\nnot json\n{"valid": 2}\n', encoding="utf-8")
        loaded = load_jsonl(p)
        assert loaded == [{"valid": 1}, {"valid": 2}]

    def test_load_json_valid(self, tmp_path):
        p = tmp_path / "valid.json"
        p.write_text('[{"cluster": 0, "name": "repo1"}]', encoding="utf-8")
        loaded = load_json(p)
        assert loaded == [{"cluster": 0, "name": "repo1"}]

    def test_load_json_missing_file_returns_empty_list(self, tmp_path):
        p = tmp_path / "missing.json"
        assert load_json(p) == []

    def test_load_json_malformed_returns_empty_list(self, tmp_path):
        p = tmp_path / "broken.json"
        p.write_text("{broken json", encoding="utf-8")
        assert load_json(p) == []


# ===========================================================================
# 2. DASHBOARD DATA AGGREGATION & PROVENANCE
# ===========================================================================

class TestDashboardDataAggregation:
    def test_real_dataset_aggregation(self):
        data = get_dashboard_data()

        # Check overview counts against ground truth
        ov = data["overview"]
        assert ov["total_repositories"] == 20
        assert ov["total_commits"] == 1000
        assert ov["total_enriched"] == 1000
        assert ov["authentic_enriched"] == 15
        assert ov["synthetic_enriched"] == 985
        assert math_close(ov["synthetic_percentage"], 98.5)
        assert ov["repositories_clustered"] == 20
        assert ov["cluster_count"] == 4

        # Provenance notice must be present
        assert "provenance_notice" in data
        assert "98.5%" in data["provenance_notice"]["warning"]

        # Check repository profiles
        repos = data["repositories"]
        assert len(repos) == 20
        names = {r["repo_name"] for r in repos}
        assert "torvalds/linux" in names
        assert "freeCodeCamp/freeCodeCamp" in names

        # Invariants on profiles
        for r in repos:
            assert r["commit_count"] == 50
            assert r["cluster"] in (0, 1, 2, 3)
            assert 0.0 <= r["merge_ratio"] <= 1.0

        # Check cluster summary
        c_sum = data["cluster_summary"]
        assert sorted(c_sum["cluster_ids"]) == [0, 1, 2, 3]
        total_in_clusters = sum(c_sum["counts"].values())
        assert total_in_clusters == 20


# ===========================================================================
# 3. HTTP SERVER ENDPOINTS
# ===========================================================================

@pytest.fixture(scope="module")
def dashboard_server():
    """Start dashboard HTTP server on an ephemeral high port for integration tests."""
    test_port = 8099
    server = HTTPServer(("127.0.0.1", test_port), DashboardHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    time.sleep(0.3)
    yield f"http://127.0.0.1:{test_port}"
    server.shutdown()
    server.server_close()


class TestDashboardHttpServer:
    def test_get_index_html(self, dashboard_server):
        resp = requests.get(f"{dashboard_server}/")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("Content-Type", "")
        assert "Git Pattern Analysis" in resp.text

    def test_get_style_css(self, dashboard_server):
        resp = requests.get(f"{dashboard_server}/style.css")
        assert resp.status_code == 200
        assert "text/css" in resp.headers.get("Content-Type", "")

    def test_get_app_js(self, dashboard_server):
        resp = requests.get(f"{dashboard_server}/app.js")
        assert resp.status_code == 200
        assert "javascript" in resp.headers.get("Content-Type", "") or "text/plain" in resp.headers.get("Content-Type", "")

    def test_get_api_data_endpoint(self, dashboard_server):
        resp = requests.get(f"{dashboard_server}/api/data")
        assert resp.status_code == 200
        assert "application/json" in resp.headers.get("Content-Type", "")
        data = resp.json()
        assert data["overview"]["total_repositories"] == 20
        assert len(data["repositories"]) == 20

    def test_get_nonexistent_file_returns_404(self, dashboard_server):
        resp = requests.get(f"{dashboard_server}/nonexistent_file.png")
        assert resp.status_code == 404

    def test_head_index_returns_200(self, dashboard_server):
        resp = requests.head(f"{dashboard_server}/")
        assert resp.status_code == 200
        assert int(resp.headers.get("Content-Length", 0)) > 0

    def test_get_export_csv_and_json(self, dashboard_server):
        # CSV Export
        resp_csv = requests.get(f"{dashboard_server}/api/export?run=sample&format=csv")
        assert resp_csv.status_code == 200
        assert "text/csv" in resp_csv.headers.get("Content-Type", "")
        assert "repo_name" in resp_csv.text
        assert "torvalds/linux" in resp_csv.text

        # JSON Export
        resp_json = requests.get(f"{dashboard_server}/api/export?run=sample&format=json")
        assert resp_json.status_code == 200
        assert "application/json" in resp_json.headers.get("Content-Type", "")
        data = resp_json.json()
        assert "overview" in data
        assert data["overview"]["total_repositories"] == 20

    def test_post_analyze_invalid_url(self, dashboard_server):
        resp = requests.post(
            f"{dashboard_server}/api/analyze",
            json={"repository": "https://gitlab.com/invalid/repo", "mode": "recent_limit", "limit": 5},
        )
        assert resp.status_code == 400
        data = resp.json()
        assert "error" in data
        assert "Unsupported host" in data["error"] or "invalid" in data["error"].lower()

    def test_post_analyze_empty_body(self, dashboard_server):
        resp = requests.post(f"{dashboard_server}/api/analyze", json={})
        assert resp.status_code == 400
        data = resp.json()
        assert "error" in data

    def test_get_analyze_status_validation(self, dashboard_server):
        # Missing run_id
        resp1 = requests.get(f"{dashboard_server}/api/analyze/status")
        assert resp1.status_code == 400

        # Nonexistent run_id
        resp2 = requests.get(f"{dashboard_server}/api/analyze/status?run_id=analysis-does-not-exist")
        assert resp2.status_code == 404


class TestSingleRepoAnalyticsLogic:
    def test_compute_single_repo_analytics(self):
        repos = [{"repo_name": "test/repo", "language": "Python", "stars": 100}]
        commits = [
            {"commit_sha": "c1", "author_timestamp": "2026-01-01T10:00:00Z", "parent_count": 1},
            {"commit_sha": "c2", "author_timestamp": "2026-01-02T12:00:00Z", "parent_count": 2},
        ]
        enriched = [
            {
                "commit_sha": "c1",
                "author_identity": "Alice",
                "author_timestamp": "2026-01-01T10:00:00Z",
                "additions": 10,
                "deletions": 2,
                "lines_changed": 12,
                "files_changed": 2,
                "parent_count": 1,
                "stats_available": True,
                "file_names": ["README.md", "main.py"],
            },
            {
                "commit_sha": "c2",
                "author_identity": "Bob",
                "author_timestamp": "2026-01-02T12:00:00Z",
                "additions": None,
                "deletions": None,
                "lines_changed": None,
                "files_changed": None,
                "parent_count": 2,
                "stats_available": False,
                "file_names": [],
            },
        ]

        res = compute_single_repo_analytics(repos, commits, enriched, {"history_mode": "recent_limit", "requested_limit": 2})
        assert res["repository"] == "test/repo"
        s = res["summary"]
        assert s["total_commits"] == 2
        assert s["total_enriched"] == 2
        assert s["stats_available_count"] == 1
        assert s["missing_stats_count"] == 1
        assert s["total_additions"] == 10
        assert s["total_deletions"] == 2
        assert s["merge_count"] == 1
        assert s["merge_ratio"] == 0.5
        assert s["date_range"]["earliest"] == "2026-01-01T10:00:00Z"
        assert s["date_range"]["latest"] == "2026-01-02T12:00:00Z"

        # Authors
        authors = res["author_contributions"]
        assert len(authors) == 2
        author_names = {a["author"] for a in authors}
        assert author_names == {"Alice", "Bob"}

        # Frequently changed files
        f_paths = [f["path"] for f in res["frequently_changed_files"]]
        assert "README.md" in f_paths

        # Detailed commits: c2 missing stats must remain None
        details = res["commits_detail"]
        assert len(details) == 2
        c2_detail = next(d for d in details if d["sha"] == "c2")
        assert c2_detail["additions"] is None
        assert c2_detail["stats_available"] is False

    def test_csv_generators(self):
        commit_details = [
            {
                "sha": "1234567890abcdef",
                "date": "2026-01-01T10:00:00Z",
                "author": "Alice",
                "message_subject": "Initial commit",
                "additions": 10,
                "deletions": 2,
                "lines_changed": 12,
                "files_changed": 2,
                "is_merge": False,
                "cluster": 0,
            },
            {
                "sha": "abcdef1234567890",
                "date": "2026-01-02T10:00:00Z",
                "author": "Bob",
                "message_subject": "Merge PR",
                "additions": None,
                "deletions": None,
                "lines_changed": None,
                "files_changed": None,
                "is_merge": True,
                "cluster": None,
            },
        ]
        csv_str = generate_commits_csv(commit_details)
        assert "sha,date,author,message_subject,additions,deletions" in csv_str
        assert "1234567890abcdef" in csv_str
        assert "N/A" in csv_str  # Missing stats output as N/A



# ===========================================================================
# 5. DASHBOARD DEFAULT STATE & REDESIGN TESTS
# ===========================================================================

class TestDashboardDefaultStateAndRedesign:
    def test_default_html_landing_state(self):
        html_path = PROJECT_ROOT / "dashboard" / "index.html"
        assert html_path.exists()
        content = html_path.read_text(encoding="utf-8")

        # Landing view is default visible, progress and results views are hidden
        assert 'id="landing-view"' in content
        assert 'id="landing-view" class="view-panel" style="display: block;"' in content
        assert 'id="progress-view" class="view-panel" style="display: none;"' in content
        assert 'id="results-view" class="view-panel" style="display: none;"' in content

        # Required landing controls exist
        assert 'id="input-repo-url"' in content
        assert 'id="btn-start-analysis"' in content
        assert 'id="radio-mode-limit"' in content
        assert 'id="radio-mode-full"' in content
        assert 'id="input-commit-limit"' in content

        # Capabilities grid exists
        assert 'class="capabilities-grid"' in content
        assert "Commit Velocity" in content
        assert "Line Churn" in content
        assert "Author Contributions" in content
        assert "Modification Hotspots" in content
        assert "MapReduce Feature Extraction" in content
        assert "Spark MLlib K-Means" in content

        # Previous Runs container & distinct Demonstration Sample button exist
        assert 'id="previous-runs-container"' in content
        assert 'id="previous-runs-empty-state"' in content
        assert 'id="btn-load-demo-sample"' in content

    def test_app_js_default_state_not_preloaded(self):
        js_path = PROJECT_ROOT / "dashboard" / "app.js"
        assert js_path.exists()
        content = js_path.read_text(encoding="utf-8")

        # activeRunId is initially null, NOT hardcoded to 'sample'
        assert "let activeRunId = null;" in content
        # Initial load invokes showView('landing') and loadRuns()
        assert 'showView("landing");' in content
        assert "loadRuns();" in content
        # Sample data is filtered out of real previous runs
        assert "runsList.filter(r => !r.is_sample)" in content
        # Demonstration sample button explicitly loads sample
        assert 'loadRunAndDisplay("sample");' in content

    def test_style_css_graphite_and_signal_palette(self):
        css_path = PROJECT_ROOT / "dashboard" / "style.css"
        assert css_path.exists()
        content = css_path.read_text(encoding="utf-8")

        # Palette tokens
        assert "--canvas-bg: #0b0f19;" in content
        assert "--panel-bg: #111827;" in content
        assert "--cobalt-primary: #2563eb;" in content
        assert "--signal-green: #10b981;" in content
        assert "--signal-amber: #f59e0b;" in content
        assert "--signal-red: #ef4444;" in content

        # No radial-gradient or glassmorphism on body
        assert "radial-gradient" not in content
        assert "backdrop-filter" not in content


def math_close(a, b, tol=0.1):
    return abs(a - b) <= tol

