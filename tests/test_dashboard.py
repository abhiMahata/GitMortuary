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


def math_close(a, b, tol=0.1):
    return abs(a - b) <= tol
