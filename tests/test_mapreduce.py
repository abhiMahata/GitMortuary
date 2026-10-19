"""
tests/test_mapreduce.py
-----------------------
Automated test suite for MapReduce mapper and reducer components.
Tests input validation, error handling, numeric safety, missing statistic preservation,
interleaved stream aggregation, and schema invariants.
"""

import json
import sys
from io import StringIO
from pathlib import Path
import pytest

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mapreduce.mapper import (
    process_line as mapper_process_line,
    safe_int,
    is_merge_commit,
)
from mapreduce.reducer import (
    process_stream as reducer_process_stream,
    emit_repo,
    zero_features,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_ENRICHED_COMMIT = {
    "repo_id": 100,
    "repo_name": "octocat/Hello-World",
    "commit_sha": "6dcb09b5b57875f334f61aebed695e2e4193db5e",
    "author_identity": "Mona Lisa",
    "author_timestamp": "2024-01-01T00:00:00Z",
    "committer_identity": "Mona Lisa",
    "committer_timestamp": "2024-01-01T00:00:00Z",
    "commit_message": "Fix all bugs",
    "parent_count": 1,
    "files_changed": 3,
    "additions": 45,
    "deletions": 15,
    "lines_changed": 60,
    "stats_available": True,
    "files_available": True,
    "synthetic": False,
}

EXPECTED_OUTPUT_FIELDS = {
    "repo_name",
    "commit_count",
    "total_additions",
    "total_deletions",
    "total_lines",
    "total_files",
    "merge_count",
    "avg_additions",
    "avg_deletions",
    "avg_lines_changed",
    "avg_files_changed",
    "merge_ratio",
}


# ===========================================================================
# 1. MAPPER: VALID INPUT & FIELD EXTRACTION
# ===========================================================================

class TestMapperValidInput:
    def test_valid_commit_output_format(self, capsys):
        mapper_process_line(json.dumps(SAMPLE_ENRICHED_COMMIT))
        captured = capsys.readouterr()
        assert captured.err == ""
        lines = captured.out.strip().split("\n")
        assert len(lines) == 1

        repo_name, payload_str = lines[0].split("\t", 1)
        assert repo_name == "octocat/Hello-World"

        payload = json.loads(payload_str)
        assert payload["commit_count"] == 1
        assert payload["additions"] == 45
        assert payload["deletions"] == 15
        assert payload["lines_changed"] == 60
        assert payload["files_changed"] == 3
        assert payload["is_merge"] == 0

    def test_merge_commit_multiple_parents(self, capsys):
        commit = dict(SAMPLE_ENRICHED_COMMIT, parent_count=2)
        mapper_process_line(json.dumps(commit))
        captured = capsys.readouterr()
        _, payload_str = captured.out.strip().split("\t", 1)
        payload = json.loads(payload_str)
        assert payload["is_merge"] == 1

    def test_octopus_merge_three_parents(self, capsys):
        commit = dict(SAMPLE_ENRICHED_COMMIT, parent_count=3)
        mapper_process_line(json.dumps(commit))
        captured = capsys.readouterr()
        _, payload_str = captured.out.strip().split("\t", 1)
        payload = json.loads(payload_str)
        assert payload["is_merge"] == 1

    def test_root_commit_zero_parents(self, capsys):
        commit = dict(SAMPLE_ENRICHED_COMMIT, parent_count=0)
        mapper_process_line(json.dumps(commit))
        captured = capsys.readouterr()
        _, payload_str = captured.out.strip().split("\t", 1)
        payload = json.loads(payload_str)
        assert payload["is_merge"] == 0


# ===========================================================================
# 2. MAPPER: MALFORMED JSON & MISSING FIELDS
# ===========================================================================

class TestMapperMalformedAndMissing:
    def test_empty_line_produces_no_output(self, capsys):
        mapper_process_line("")
        mapper_process_line("   \n")
        assert capsys.readouterr().out == ""

    def test_malformed_json_skipped_silently(self, capsys):
        mapper_process_line("{bad json: true,")
        mapper_process_line("not even close to json")
        assert capsys.readouterr().out == ""

    def test_non_dict_json_skipped(self, capsys):
        mapper_process_line("12345")
        mapper_process_line('"just a string"')
        mapper_process_line("[1, 2, 3]")
        mapper_process_line("true")
        assert capsys.readouterr().out == ""

    def test_missing_repo_name_skipped(self, capsys):
        commit = dict(SAMPLE_ENRICHED_COMMIT)
        del commit["repo_name"]
        mapper_process_line(json.dumps(commit))
        assert capsys.readouterr().out == ""

    def test_empty_or_non_string_repo_name_skipped(self, capsys):
        mapper_process_line(json.dumps(dict(SAMPLE_ENRICHED_COMMIT, repo_name="")))
        mapper_process_line(json.dumps(dict(SAMPLE_ENRICHED_COMMIT, repo_name=None)))
        mapper_process_line(json.dumps(dict(SAMPLE_ENRICHED_COMMIT, repo_name=123)))
        assert capsys.readouterr().out == ""


# ===========================================================================
# 3. MAPPER: NUMERIC SAFETY & MISSING STATS PRESERVATION
# ===========================================================================

class TestMapperStatsHandling:
    def test_genuine_zero_stats_preserved(self, capsys):
        commit = dict(
            SAMPLE_ENRICHED_COMMIT,
            additions=0,
            deletions=0,
            lines_changed=0,
            files_changed=0,
        )
        mapper_process_line(json.dumps(commit))
        captured = capsys.readouterr()
        _, payload_str = captured.out.strip().split("\t", 1)
        payload = json.loads(payload_str)
        assert payload["additions"] == 0
        assert payload["deletions"] == 0
        assert payload["lines_changed"] == 0
        assert payload["files_changed"] == 0

    def test_unavailable_stats_preserved_as_none_not_zero(self, capsys):
        commit = dict(
            SAMPLE_ENRICHED_COMMIT,
            additions=None,
            deletions=None,
            lines_changed=None,
            files_changed=None,
            stats_available=False,
        )
        mapper_process_line(json.dumps(commit))
        captured = capsys.readouterr()
        _, payload_str = captured.out.strip().split("\t", 1)
        payload = json.loads(payload_str)
        assert payload["additions"] is None
        assert payload["deletions"] is None
        assert payload["lines_changed"] is None
        assert payload["files_changed"] is None

    def test_lines_changed_computed_when_omitted(self, capsys):
        commit = dict(SAMPLE_ENRICHED_COMMIT, additions=30, deletions=10, lines_changed=None)
        mapper_process_line(json.dumps(commit))
        captured = capsys.readouterr()
        _, payload_str = captured.out.strip().split("\t", 1)
        payload = json.loads(payload_str)
        assert payload["lines_changed"] == 40

    def test_unexpected_types_safely_sanitized(self, capsys):
        commit = dict(
            SAMPLE_ENRICHED_COMMIT,
            additions="not-a-number",
            deletions=-10,
            files_changed=True,  # boolean should not be treated as int 1
            parent_count=None,
        )
        mapper_process_line(json.dumps(commit))
        captured = capsys.readouterr()
        _, payload_str = captured.out.strip().split("\t", 1)
        payload = json.loads(payload_str)
        assert payload["additions"] is None
        assert payload["deletions"] is None
        assert payload["files_changed"] is None
        assert payload["is_merge"] == 0


# ===========================================================================
# 4. REDUCER: AGGREGATION & NUMERICAL CONSISTENCY
# ===========================================================================

class TestReducerAggregation:
    def test_single_repo_multiple_commits(self):
        stream = StringIO(
            "repo/one\t" + json.dumps({"commit_count": 1, "additions": 100, "deletions": 20, "lines_changed": 120, "files_changed": 3, "is_merge": 0}) + "\n"
            "repo/one\t" + json.dumps({"commit_count": 1, "additions": 50, "deletions": 10, "lines_changed": 60, "files_changed": 1, "is_merge": 1}) + "\n"
            "repo/one\t" + json.dumps({"commit_count": 1, "additions": 0, "deletions": 0, "lines_changed": 0, "files_changed": 0, "is_merge": 0}) + "\n"
        )
        results = reducer_process_stream(stream)
        assert len(results) == 1
        r = results[0]

        assert r["repo_name"] == "repo/one"
        assert r["commit_count"] == 3
        assert r["total_additions"] == 150
        assert r["total_deletions"] == 30
        assert r["total_lines"] == 180
        assert r["total_files"] == 4
        assert r["merge_count"] == 1
        assert r["avg_additions"] == 50.0
        assert r["avg_deletions"] == 10.0
        assert r["avg_lines_changed"] == 60.0
        assert r["avg_files_changed"] == 1.3333
        assert r["merge_ratio"] == 0.3333

    def test_missing_stats_not_silently_deflating_averages(self):
        # 2 commits: 1 with 100 additions, 1 with None (unavailable)
        # Average additions should be 100.0 (over available commits), not 50.0!
        stream = StringIO(
            "repo/partial\t" + json.dumps({"commit_count": 1, "additions": 100, "deletions": 50, "lines_changed": 150, "files_changed": 2, "is_merge": 0}) + "\n"
            "repo/partial\t" + json.dumps({"commit_count": 1, "additions": None, "deletions": None, "lines_changed": None, "files_changed": None, "is_merge": 0}) + "\n"
        )
        results = reducer_process_stream(stream)
        assert len(results) == 1
        r = results[0]
        assert r["commit_count"] == 2
        assert r["total_additions"] == 100
        assert r["avg_additions"] == 100.0
        assert r["avg_deletions"] == 50.0

    def test_all_missing_stats_defaults_gracefully(self):
        stream = StringIO(
            "repo/none\t" + json.dumps({"commit_count": 1, "additions": None, "deletions": None, "lines_changed": None, "files_changed": None, "is_merge": 0}) + "\n"
        )
        results = reducer_process_stream(stream)
        assert len(results) == 1
        r = results[0]
        assert r["commit_count"] == 1
        assert r["total_additions"] == 0
        assert r["avg_additions"] == 0.0
        assert r["avg_deletions"] == 0.0
        assert r["avg_lines_changed"] == 0.0
        assert r["avg_files_changed"] == 0.0
        assert r["merge_ratio"] == 0.0


# ===========================================================================
# 5. REDUCER: INTERLEAVED STREAMING
# ===========================================================================

class TestReducerInterleavedRepos:
    def test_interleaved_repositories_aggregated_per_repo(self):
        # Multiple repos arriving interleaved (unsorted or distributed streaming)
        stream = StringIO(
            "repo/A\t" + json.dumps({"commit_count": 1, "additions": 10, "deletions": 2, "lines_changed": 12, "files_changed": 1, "is_merge": 0}) + "\n"
            "repo/B\t" + json.dumps({"commit_count": 1, "additions": 100, "deletions": 20, "lines_changed": 120, "files_changed": 2, "is_merge": 1}) + "\n"
            "repo/A\t" + json.dumps({"commit_count": 1, "additions": 30, "deletions": 8, "lines_changed": 38, "files_changed": 3, "is_merge": 0}) + "\n"
            "repo/B\t" + json.dumps({"commit_count": 1, "additions": 200, "deletions": 40, "lines_changed": 240, "files_changed": 4, "is_merge": 0}) + "\n"
        )
        results = reducer_process_stream(stream)
        assert len(results) == 2

        by_name = {r["repo_name"]: r for r in results}
        assert "repo/A" in by_name
        assert "repo/B" in by_name

        repo_a = by_name["repo/A"]
        assert repo_a["commit_count"] == 2
        assert repo_a["total_additions"] == 40
        assert repo_a["avg_additions"] == 20.0
        assert repo_a["merge_count"] == 0
        assert repo_a["merge_ratio"] == 0.0

        repo_b = by_name["repo/B"]
        assert repo_b["commit_count"] == 2
        assert repo_b["total_additions"] == 300
        assert repo_b["avg_additions"] == 150.0
        assert repo_b["merge_count"] == 1
        assert repo_b["merge_ratio"] == 0.5


# ===========================================================================
# 6. REDUCER: MALFORMED AND EMPTY INPUTS
# ===========================================================================

class TestReducerMalformedInputs:
    def test_empty_stream_returns_empty_list(self):
        assert reducer_process_stream(StringIO("")) == []
        assert reducer_process_stream(StringIO("   \n\n")) == []

    def test_lines_without_tab_skipped(self):
        stream = StringIO("no_tab_line\nanother line without tab\n")
        assert reducer_process_stream(stream) == []

    def test_malformed_json_payload_skipped(self):
        stream = StringIO("repo/X\t{malformed json}\nrepo/X\tnot json at all\n")
        assert reducer_process_stream(stream) == []

    def test_non_dict_payload_skipped(self):
        stream = StringIO('repo/X\t"scalar string"\nrepo/X\t12345\nrepo/X\t[1, 2]\n')
        assert reducer_process_stream(stream) == []

    def test_empty_repo_name_skipped(self):
        stream = StringIO('\t{"commit_count": 1, "additions": 10}\n')
        assert reducer_process_stream(stream) == []


# ===========================================================================
# 7. MAPREDUCE END-TO-END SCHEMA & INVARIANTS
# ===========================================================================

class TestMapReduceEndToEnd:
    def test_mapper_to_reducer_pipeline(self, capsys):
        commits = [
            dict(SAMPLE_ENRICHED_COMMIT, commit_sha="c1", additions=10, deletions=5, parent_count=1),
            dict(SAMPLE_ENRICHED_COMMIT, commit_sha="c2", additions=20, deletions=15, parent_count=2),
            dict(SAMPLE_ENRICHED_COMMIT, commit_sha="c3", additions=0, deletions=0, parent_count=1),
        ]

        # 1. Mapper phase
        for c in commits:
            mapper_process_line(json.dumps(c))

        mapper_output = capsys.readouterr().out
        assert mapper_output != ""

        # 2. Reducer phase
        results = reducer_process_stream(StringIO(mapper_output))
        assert len(results) == 1
        record = results[0]

        # Verify all expected schema fields exist
        assert set(record.keys()) == EXPECTED_OUTPUT_FIELDS

        # Invariant checks
        assert record["commit_count"] == 3
        assert record["total_additions"] == 30
        assert record["total_deletions"] == 20
        assert record["merge_count"] == 1
        assert 0.0 <= record["merge_ratio"] <= 1.0
        assert record["avg_additions"] == round(30 / 3, 4)
        assert record["avg_deletions"] == round(20 / 3, 4)
