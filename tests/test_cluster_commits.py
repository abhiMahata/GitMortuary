"""
tests/test_cluster_commits.py
-----------------------------
Unit tests for commit-level K-Means clustering in spark/cluster_commits.py.
Tests feature extraction, standardization, silhouette scoring, and edge cases.
"""

import pytest
from spark.cluster_commits import (
    _extract_commit_features,
    cluster_commits,
    COMMIT_FEATURE_COLS,
)


class TestCommitClustering:
    def test_extract_commit_features_valid(self):
        records = [
            {"commit_sha": "abc1", "additions": 10, "deletions": 5, "lines_changed": 15, "files_changed": 2, "parent_count": 1},
            {"commit_sha": "abc2", "additions": 0, "deletions": 0, "lines_changed": 0, "files_changed": 1, "parent_count": 2},
        ]
        valid_commits, feats = _extract_commit_features(records)
        assert [c["commit_sha"] for c in valid_commits] == ["abc1", "abc2"]
        assert len(feats) == 2
        assert feats[0] == [10.0, 5.0, 15.0, 2.0, 1.0]
        assert feats[1] == [0.0, 0.0, 0.0, 1.0, 2.0]

    def test_extract_commit_features_skips_missing_stats(self):
        records = [
            {"commit_sha": "c1", "additions": 10, "deletions": 5, "lines_changed": 15, "files_changed": 2, "parent_count": 1},
            {"commit_sha": "c2", "additions": None, "deletions": None, "lines_changed": None, "files_changed": None, "parent_count": 1},
            {"commit_sha": "c3", "additions": "invalid", "deletions": 2, "lines_changed": 2, "files_changed": 1, "parent_count": 1},
        ]
        valid_commits, feats = _extract_commit_features(records)
        assert [c["commit_sha"] for c in valid_commits] == ["c1"]
        assert len(feats) == 1

    def test_cluster_commits_too_few_observations(self):
        records = [
            {"commit_sha": "c1", "additions": 10, "deletions": 5, "lines_changed": 15, "files_changed": 2, "parent_count": 1},
            {"commit_sha": "c2", "additions": 20, "deletions": 1, "lines_changed": 21, "files_changed": 1, "parent_count": 1},
        ]
        res = cluster_commits(records, k=3)
        assert res["status"] == "skipped"
        assert "at least 3 commits" in res["reason"]
        assert res["k"] == 0

    def test_cluster_commits_zero_variance(self):
        records = [
            {"commit_sha": "c1", "additions": 5, "deletions": 2, "lines_changed": 7, "files_changed": 1, "parent_count": 1},
            {"commit_sha": "c2", "additions": 5, "deletions": 2, "lines_changed": 7, "files_changed": 1, "parent_count": 1},
            {"commit_sha": "c3", "additions": 5, "deletions": 2, "lines_changed": 7, "files_changed": 1, "parent_count": 1},
        ]
        res = cluster_commits(records, k=3)
        assert res["status"] == "skipped"
        assert "identical" in res["reason"] or "zero variance" in res["reason"]

    def test_cluster_commits_successful(self):
        records = [
            {"commit_sha": "c1", "additions": 2, "deletions": 1, "lines_changed": 3, "files_changed": 1, "parent_count": 1},
            {"commit_sha": "c2", "additions": 5, "deletions": 0, "lines_changed": 5, "files_changed": 1, "parent_count": 1},
            {"commit_sha": "c3", "additions": 500, "deletions": 200, "lines_changed": 700, "files_changed": 15, "parent_count": 1},
            {"commit_sha": "c4", "additions": 600, "deletions": 150, "lines_changed": 750, "files_changed": 20, "parent_count": 2},
            {"commit_sha": "c5", "additions": 10, "deletions": 10, "lines_changed": 20, "files_changed": 2, "parent_count": 1},
        ]
        res = cluster_commits(records, k=2, seed=42)
        assert res["status"] == "success"
        assert res["k"] == 2
        assert res["seed"] == 42
        assert -1.0 <= res["silhouette"] <= 1.0
        assert len(res["assignments"]) == 5
        assert len(res["profiles"]) == 2
        for cid, profile in res["profiles"].items():
            assert profile["count"] > 0
            assert "avg_additions" in profile
            assert "avg_deletions" in profile
            assert "avg_lines_changed" in profile
            assert "avg_files_changed" in profile
            assert "merge_ratio" in profile
