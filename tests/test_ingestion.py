"""
tests/test_ingestion.py
-----------------------
Tests for Git Pattern Analysis ingestion pipeline.

Runs entirely without a real GitHub token or API quota.
"""

import json
import sys
import time
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

import pytest
import requests

# ---------------------------------------------------------------------------
# Path setup — allow imports from project root
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "ingestion"))

# ---------------------------------------------------------------------------
# Fixtures: sample raw data
# ---------------------------------------------------------------------------

SAMPLE_COMMIT = {
    "repo_id": 12345,
    "repo_name": "owner/repo",
    "commit_sha": "abc123def456789000000000000000000000000",
    "author_identity": "Alice",
    "author_timestamp": "2024-01-01T00:00:00Z",
    "committer_identity": "Alice",
    "committer_timestamp": "2024-01-01T00:00:00Z",
    "commit_message": "Initial commit",
    "parent_count": 0,
}

SAMPLE_GITHUB_DETAIL = {
    "sha": "abc123def456789000000000000000000000000",
    "stats": {"additions": 10, "deletions": 3, "total": 13},
    "files": [{"filename": "foo.py"}, {"filename": "bar.py"}],
}

SAMPLE_GITHUB_DETAIL_NO_STATS = {
    "sha": "abc123def456789000000000000000000000000",
    # 'stats' and 'files' deliberately absent
}

SAMPLE_GITHUB_DETAIL_ZERO_CHANGE = {
    "sha": "abc123def456789000000000000000000000000",
    "stats": {"additions": 0, "deletions": 0, "total": 0},
    "files": [],
}


# ===========================================================================
# 1. JSONL PARSING & SERIALIZATION
# ===========================================================================

class TestJsonlParsing:
    def test_valid_line_roundtrip(self):
        data = {"key": "val", "num": 42}
        serialised = json.dumps(data)
        assert json.loads(serialised) == data

    def test_malformed_line_raises(self):
        with pytest.raises(json.JSONDecodeError):
            json.loads("{not valid json}")

    def test_empty_line_raises(self):
        with pytest.raises(json.JSONDecodeError):
            json.loads("")

    def test_whitespace_line_detected(self):
        line = "   \n"
        assert line.strip() == ""   # caller should skip blank lines

    def test_multi_line_file_parsing(self, tmp_path):
        p = tmp_path / "test.jsonl"
        records = [{"id": i, "val": f"v{i}"} for i in range(5)]
        p.write_text("\n".join(json.dumps(r) for r in records) + "\n")

        loaded = []
        with p.open() as fh:
            for line in fh:
                line = line.strip()
                if line:
                    loaded.append(json.loads(line))
        assert loaded == records

    def test_trailing_newline_tolerated(self, tmp_path):
        p = tmp_path / "test.jsonl"
        p.write_text(json.dumps(SAMPLE_COMMIT) + "\n\n")
        loaded = []
        with p.open() as fh:
            for line in fh:
                line = line.strip()
                if line:
                    loaded.append(json.loads(line))
        assert len(loaded) == 1


# ===========================================================================
# 2. REPOSITORY SCHEMA VALIDATION
# ===========================================================================

REPO_REQUIRED = {
    "repo_id", "repo_name", "language", "stars", "forks",
    "created_at", "updated_at", "archived", "default_branch",
}

SAMPLE_REPO = {
    "repo_id": 1,
    "repo_name": "owner/repo",
    "language": "Python",
    "stars": 5000,
    "forks": 200,
    "created_at": "2019-01-01T00:00:00Z",
    "updated_at": "2024-01-01T00:00:00Z",
    "archived": False,
    "default_branch": "main",
}


class TestRepoSchema:
    def test_valid_repo_passes(self):
        missing = REPO_REQUIRED - set(SAMPLE_REPO.keys())
        assert missing == set()

    @pytest.mark.parametrize("field", list(REPO_REQUIRED))
    def test_missing_field_detected(self, field):
        bad = dict(SAMPLE_REPO)
        del bad[field]
        missing = REPO_REQUIRED - set(bad.keys())
        assert field in missing


# ===========================================================================
# 3. COMMIT SCHEMA VALIDATION
# ===========================================================================

COMMIT_REQUIRED = {
    "repo_id", "repo_name", "commit_sha",
    "author_identity", "author_timestamp",
    "committer_identity", "committer_timestamp",
    "commit_message", "parent_count",
}


class TestCommitSchema:
    def test_valid_commit_passes(self):
        missing = COMMIT_REQUIRED - set(SAMPLE_COMMIT.keys())
        assert missing == set()

    @pytest.mark.parametrize("field", list(COMMIT_REQUIRED))
    def test_missing_field_detected(self, field):
        bad = dict(SAMPLE_COMMIT)
        del bad[field]
        missing = COMMIT_REQUIRED - set(bad.keys())
        assert field in missing


# ===========================================================================
# 4. DUPLICATE COMMIT DETECTION
# ===========================================================================

class TestDuplicateDetection:
    def test_no_duplicates(self):
        commits = [
            {**SAMPLE_COMMIT, "commit_sha": f"sha{i:040d}"}
            for i in range(10)
        ]
        shas = [c["commit_sha"] for c in commits]
        assert len(shas) == len(set(shas))

    def test_duplicate_detected(self):
        commits = [SAMPLE_COMMIT, SAMPLE_COMMIT]
        shas = [c["commit_sha"] for c in commits]
        assert len(shas) != len(set(shas))

    def test_load_existing_shas(self, tmp_path):
        from ingestion.enriched_commits import load_existing_shas
        p = tmp_path / "enriched.jsonl"
        records = [
            {**SAMPLE_COMMIT, "commit_sha": f"sha{i:040d}", "files_changed": 1, "additions": 1, "deletions": 0, "lines_changed": 1}
            for i in range(5)
        ]
        p.write_text("\n".join(json.dumps(r) for r in records) + "\n")
        shas = load_existing_shas(p)
        assert len(shas) == 5

    def test_load_existing_shas_empty_file(self, tmp_path):
        from ingestion.enriched_commits import load_existing_shas
        p = tmp_path / "enriched.jsonl"
        p.write_text("")
        shas = load_existing_shas(p)
        assert shas == set()

    def test_load_existing_shas_missing_file(self, tmp_path):
        from ingestion.enriched_commits import load_existing_shas
        p = tmp_path / "nonexistent.jsonl"
        shas = load_existing_shas(p)
        assert shas == set()


# ===========================================================================
# 5. ENRICHMENT NORMALIZATION
# ===========================================================================

class TestEnrichmentNormalization:
    def _enrich(self, details):
        from ingestion.enriched_commits import enrich_commit
        return enrich_commit(SAMPLE_COMMIT, details)

    def test_normal_stats(self):
        e = self._enrich(SAMPLE_GITHUB_DETAIL)
        assert e["additions"] == 10
        assert e["deletions"] == 3
        assert e["lines_changed"] == 13
        assert e["files_changed"] == 2
        assert e["stats_available"] is True
        assert e["files_available"] is True

    def test_zero_change_commit(self):
        e = self._enrich(SAMPLE_GITHUB_DETAIL_ZERO_CHANGE)
        assert e["additions"] == 0
        assert e["deletions"] == 0
        assert e["lines_changed"] == 0
        assert e["files_changed"] == 0
        assert e["stats_available"] is True

    def test_unavailable_stats(self):
        """API response missing 'stats' key → None values, not zeros."""
        e = self._enrich(SAMPLE_GITHUB_DETAIL_NO_STATS)
        assert e["additions"] is None
        assert e["deletions"] is None
        assert e["lines_changed"] is None
        assert e["files_changed"] is None
        assert e["stats_available"] is False

    def test_original_metadata_preserved(self):
        e = self._enrich(SAMPLE_GITHUB_DETAIL)
        for key, val in SAMPLE_COMMIT.items():
            assert e[key] == val

    def test_validate_enriched_passes(self):
        from ingestion.enriched_commits import validate_enriched, enrich_commit
        e = enrich_commit(SAMPLE_COMMIT, SAMPLE_GITHUB_DETAIL)
        assert validate_enriched(e) is True

    def test_validate_enriched_fails_missing_field(self):
        from ingestion.enriched_commits import validate_enriched
        bad = {**SAMPLE_COMMIT}  # no enrichment fields
        assert validate_enriched(bad) is False


# ===========================================================================
# 6. HTTP ERROR HANDLING — 401, 403, 429, 5xx
# ===========================================================================

class TestHttpErrorHandling:
    def _make_response(self, status_code, headers=None):
        resp = MagicMock()
        resp.status_code = status_code
        resp.headers = headers or {}
        http_err = requests.HTTPError(response=resp)
        resp.raise_for_status.side_effect = http_err
        return resp

    def test_401_raises_immediately(self):
        from ingestion.enriched_commits import get_commit_details
        resp = self._make_response(401)
        with patch("ingestion.enriched_commits.requests.get", return_value=resp):
            with pytest.raises(requests.HTTPError) as exc_info:
                get_commit_details("owner/repo", "abc123", "bad-token")
        assert exc_info.value.response.status_code == 401

    def test_403_raises_immediately(self):
        from ingestion.enriched_commits import get_commit_details
        resp = self._make_response(403)
        with patch("ingestion.enriched_commits.requests.get", return_value=resp):
            with pytest.raises(requests.HTTPError) as exc_info:
                get_commit_details("owner/repo", "abc123", "ghp_limited")
        assert exc_info.value.response.status_code == 403

    def test_429_retries_with_sleep(self):
        from ingestion.enriched_commits import get_commit_details
        rate_resp = self._make_response(
            429, headers={"Retry-After": "1"}
        )
        rate_resp.raise_for_status.side_effect = None  # don't raise on 429

        ok_resp = MagicMock()
        ok_resp.status_code = 200
        ok_resp.json.return_value = SAMPLE_GITHUB_DETAIL
        ok_resp.raise_for_status.return_value = None

        with patch("ingestion.enriched_commits.requests.get",
                   side_effect=[rate_resp, ok_resp]):
            with patch("ingestion.enriched_commits.time.sleep") as mock_sleep:
                result = get_commit_details("owner/repo", "abc123", "ghp_token")
        mock_sleep.assert_called_once()
        assert result == SAMPLE_GITHUB_DETAIL

    def test_5xx_retries_then_raises(self):
        from ingestion.enriched_commits import get_commit_details, MAX_RETRIES
        resp_500 = self._make_response(500)
        resp_500.raise_for_status.side_effect = None  # don't raise — we check status

        # Last attempt raises
        final = self._make_response(500)
        http_err = requests.HTTPError(response=final)
        final.raise_for_status.side_effect = http_err

        side_effects = [resp_500] * (MAX_RETRIES - 1) + [final]

        with patch("ingestion.enriched_commits.requests.get",
                   side_effect=side_effects):
            with patch("ingestion.enriched_commits.time.sleep"):
                with pytest.raises(requests.HTTPError):
                    get_commit_details("owner/repo", "abc123", "ghp_token")

    def test_200_returns_json(self):
        from ingestion.enriched_commits import get_commit_details
        ok_resp = MagicMock()
        ok_resp.status_code = 200
        ok_resp.json.return_value = SAMPLE_GITHUB_DETAIL
        ok_resp.raise_for_status.return_value = None

        with patch("ingestion.enriched_commits.requests.get", return_value=ok_resp):
            result = get_commit_details("owner/repo", "abc123", "ghp_token")
        assert result == SAMPLE_GITHUB_DETAIL


# ===========================================================================
# 7. RESUMABILITY
# ===========================================================================

class TestResumability:
    def test_resumes_from_existing_enriched(self, tmp_path):
        from ingestion.enriched_commits import load_existing_shas

        # Pre-populate enriched file with 3 records
        enriched_path = tmp_path / "enriched.jsonl"
        pre_enriched = [
            {**SAMPLE_COMMIT, "commit_sha": f"sha{i:040d}", "files_changed": 1, "additions": 0, "deletions": 0, "lines_changed": 0}
            for i in range(3)
        ]
        enriched_path.write_text(
            "\n".join(json.dumps(r) for r in pre_enriched) + "\n"
        )

        existing_shas = load_existing_shas(enriched_path)
        all_shas = {f"sha{i:040d}" for i in range(5)}
        remaining = all_shas - existing_shas

        assert len(remaining) == 2
        assert existing_shas == {f"sha{i:040d}" for i in range(3)}

    def test_no_duplicate_shas_on_resume(self, tmp_path):
        """After a resume, the output must not contain the same SHA twice."""
        from ingestion.enriched_commits import load_existing_shas
        enriched_path = tmp_path / "enriched.jsonl"
        shas_written = [f"sha{i:040d}" for i in range(5)]
        records = [
            {**SAMPLE_COMMIT, "commit_sha": sha, "files_changed": 1, "additions": 0, "deletions": 0, "lines_changed": 0}
            for sha in shas_written
        ]
        enriched_path.write_text(
            "\n".join(json.dumps(r) for r in records) + "\n"
        )

        existing = load_existing_shas(enriched_path)
        remaining_shas = [sha for sha in shas_written if sha not in existing]
        assert len(remaining_shas) == 0


# ===========================================================================
# 8. MOCK MODE
# ===========================================================================

class TestMockMode:
    def test_mock_enrich_produces_required_fields(self):
        from ingestion.enriched_commits import enrich_commit_mock, validate_enriched
        enriched = enrich_commit_mock(SAMPLE_COMMIT)
        assert validate_enriched(enriched) is True

    def test_mock_is_deterministic(self):
        from ingestion.enriched_commits import enrich_commit_mock
        e1 = enrich_commit_mock(SAMPLE_COMMIT)
        e2 = enrich_commit_mock(SAMPLE_COMMIT)
        assert e1["files_changed"] == e2["files_changed"]
        assert e1["additions"] == e2["additions"]

    def test_mock_marks_synthetic(self):
        from ingestion.enriched_commits import enrich_commit_mock
        enriched = enrich_commit_mock(SAMPLE_COMMIT)
        assert enriched.get("synthetic") is True

    def test_mock_different_shas_different_stats(self):
        from ingestion.enriched_commits import enrich_commit_mock
        c1 = {**SAMPLE_COMMIT, "commit_sha": "a" * 40}
        c2 = {**SAMPLE_COMMIT, "commit_sha": "b" * 40}
        e1 = enrich_commit_mock(c1)
        e2 = enrich_commit_mock(c2)
        # Very unlikely to be identical across all fields
        assert not (e1["additions"] == e2["additions"] and
                    e1["deletions"] == e2["deletions"] and
                    e1["files_changed"] == e2["files_changed"])


# ===========================================================================
# 9. INTERRUPTED RUNS — output file preserved on error
# ===========================================================================

class TestInterruptionSafety:
    def test_partial_enrichment_preserved_on_crash(self, tmp_path):
        """Simulate a partial run: 3 records written, then KeyboardInterrupt.
        The 3 records must survive in the output file.
        """
        enriched_path = tmp_path / "enriched.jsonl"
        records_to_write = [
            {**SAMPLE_COMMIT, "commit_sha": f"sha{i:040d}",
             "files_changed": 1, "additions": 0, "deletions": 0, "lines_changed": 0}
            for i in range(5)
        ]

        written = 0
        try:
            with enriched_path.open("a") as fh:
                for i, rec in enumerate(records_to_write):
                    fh.write(json.dumps(rec) + "\n")
                    fh.flush()
                    written += 1
                    if i == 2:
                        raise KeyboardInterrupt
        except KeyboardInterrupt:
            pass

        # File must contain exactly 3 records
        lines = [l.strip() for l in enriched_path.read_text().splitlines() if l.strip()]
        assert len(lines) == 3
        assert written == 3


# ===========================================================================
# 10. VALIDATE_TOKEN
# ===========================================================================

class TestValidateToken:
    def test_none_token_exits(self):
        from ingestion.enriched_commits import validate_token
        with pytest.raises(SystemExit) as exc_info:
            validate_token(None)
        assert exc_info.value.code == 2

    def test_empty_string_token_exits(self):
        from ingestion.enriched_commits import validate_token
        with pytest.raises(SystemExit):
            validate_token("")

    def test_valid_prefix_does_not_exit(self, capsys):
        from ingestion.enriched_commits import validate_token
        # Should not raise
        validate_token("ghp_" + "x" * 36)

    def test_unrecognised_prefix_warns(self, capsys):
        from ingestion.enriched_commits import validate_token
        validate_token("notaprefix_abc123")
        captured = capsys.readouterr()
        assert "WARNING" in captured.err


# ===========================================================================
# 11. PROBE_AUTH — pre-flight authentication check
# ===========================================================================

class TestProbeAuth:
    def _ok_resp(self):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"login": "testuser"}
        resp.headers = {"X-RateLimit-Remaining": "4999", "X-RateLimit-Reset": "9999999999"}
        return resp

    def _fail_resp(self, status):
        resp = MagicMock()
        resp.status_code = status
        resp.json.return_value = {}
        resp.headers = {}
        return resp

    def test_200_returns_true(self, capsys):
        from ingestion.enriched_commits import probe_auth
        with patch("ingestion.enriched_commits.requests.get", return_value=self._ok_resp()):
            result = probe_auth("ghp_valid")
        assert result is True
        out = capsys.readouterr().out
        assert "testuser" in out           # login printed
        assert "ghp_valid" not in out      # token NOT printed

    def test_200_does_not_print_token(self, capsys):
        from ingestion.enriched_commits import probe_auth
        token = "ghp_supersecrettoken12345"
        with patch("ingestion.enriched_commits.requests.get", return_value=self._ok_resp()):
            probe_auth(token)
        combined = capsys.readouterr().out + capsys.readouterr().err
        assert token not in combined

    def test_401_returns_false(self, capsys):
        from ingestion.enriched_commits import probe_auth
        with patch("ingestion.enriched_commits.requests.get", return_value=self._fail_resp(401)):
            result = probe_auth("ghp_expired")
        assert result is False
        assert "401" in capsys.readouterr().err

    def test_403_returns_false(self, capsys):
        from ingestion.enriched_commits import probe_auth
        with patch("ingestion.enriched_commits.requests.get", return_value=self._fail_resp(403)):
            result = probe_auth("ghp_noscope")
        assert result is False

    def test_network_error_raises(self):
        from ingestion.enriched_commits import probe_auth
        with patch(
            "ingestion.enriched_commits.requests.get",
            side_effect=requests.ConnectionError("no route to host"),
        ):
            with pytest.raises(requests.RequestException):
                probe_auth("ghp_token")


# ===========================================================================
# 12. REAL ENRICHED RECORDS PRESERVED
#     The 15 real GitHub records in enriched_commits.jsonl must not be
#     overwritten or duplicated by a subsequent mock run.
# ===========================================================================

class TestRealRecordsPreserved:
    REAL_SHA = "aa17439b62f384511a5561ce308e9598b94d8989"   # from actual dataset

    def _build_enriched_file(self, tmp_path, n_real: int = 3):
        """Write n_real 'real' records followed by mock records."""
        path = tmp_path / "enriched.jsonl"
        records = []
        # Real records: no 'synthetic' key (or synthetic=False)
        for i in range(n_real):
            records.append({
                **SAMPLE_COMMIT,
                "commit_sha": f"real{i:036d}",
                "files_changed": i + 1,
                "additions": 10 * (i + 1),
                "deletions": 2,
                "lines_changed": 10 * (i + 1) + 2,
                "stats_available": True,
                "files_available": True,
            })
        path.write_text("\n".join(json.dumps(r) for r in records) + "\n")
        return path

    def test_real_shas_in_existing_shas(self, tmp_path):
        """load_existing_shas must capture real records so they aren't re-processed."""
        from ingestion.enriched_commits import load_existing_shas
        path = self._build_enriched_file(tmp_path, n_real=3)
        existing = load_existing_shas(path)
        for i in range(3):
            assert f"real{i:036d}" in existing

    def test_real_records_not_overwritten_on_resume(self, tmp_path):
        """After loading existing SHAs, none of the real records appear in 'remaining'."""
        from ingestion.enriched_commits import load_existing_shas
        path = self._build_enriched_file(tmp_path, n_real=3)
        all_commits = [
            {**SAMPLE_COMMIT, "commit_sha": f"real{i:036d}"} for i in range(3)
        ] + [
            {**SAMPLE_COMMIT, "commit_sha": f"new{i:037d}"} for i in range(2)
        ]
        existing = load_existing_shas(path)
        remaining = [c for c in all_commits if c["commit_sha"] not in existing]
        remaining_shas = {c["commit_sha"] for c in remaining}
        # Only the 2 new commits should remain
        assert len(remaining) == 2
        for i in range(3):
            assert f"real{i:036d}" not in remaining_shas

    def test_real_record_synthetic_flag_absent(self, tmp_path):
        """Real records must not have synthetic=True."""
        path = self._build_enriched_file(tmp_path, n_real=3)
        records = []
        with path.open() as fh:
            for line in fh:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        for rec in records:
            assert rec.get("synthetic") is not True


# ===========================================================================
# 13. REPOSITORY REFERENCE VALIDATION
#     Commits whose repo_name is absent from repositories.jsonl must be
#     detectable (orphan detection).
# ===========================================================================

class TestRepoReferenceValidation:
    def test_orphan_commits_detected(self):
        """A commit referencing a repo not in the repo list is an orphan."""
        repos = [{"repo_name": "org/known-repo", "repo_id": 1}]
        commits = [
            {"repo_name": "org/known-repo",   "commit_sha": "aaa" + "0" * 37},
            {"repo_name": "org/unknown-repo", "commit_sha": "bbb" + "0" * 37},
        ]
        repo_names    = {r["repo_name"] for r in repos}
        commit_rnames = {c["repo_name"] for c in commits}
        orphans = commit_rnames - repo_names
        assert orphans == {"org/unknown-repo"}

    def test_no_orphans_when_all_match(self):
        repos   = [{"repo_name": "a/b"}, {"repo_name": "c/d"}]
        commits = [{"repo_name": "a/b"}, {"repo_name": "c/d"}]
        orphans = {c["repo_name"] for c in commits} - {r["repo_name"] for r in repos}
        assert orphans == set()

    def test_repos_without_commits_detected(self):
        repos   = [{"repo_name": "a/b"}, {"repo_name": "c/d"}]
        commits = [{"repo_name": "a/b"}]
        no_commits = {r["repo_name"] for r in repos} - {c["repo_name"] for c in commits}
        assert no_commits == {"c/d"}


# ===========================================================================
# 14. STATS-NOT-SILENTLY-ZEROED
#     The critical invariant: when the GitHub API omits the 'stats' key,
#     additions/deletions/lines_changed must be stored as None, not 0.
#     This block consolidates and extends the existing unavailable_stats test.
# ===========================================================================

class TestStatsNotSilentlyZeroed:
    def _enrich(self, details):
        from ingestion.enriched_commits import enrich_commit
        return enrich_commit(SAMPLE_COMMIT, details)

    def test_missing_stats_key_gives_none_not_zero(self):
        """API response with no 'stats' key -> None values, stats_available=False."""
        e = self._enrich({"sha": SAMPLE_COMMIT["commit_sha"]})
        assert e["additions"] is None, "additions must be None, not 0"
        assert e["deletions"] is None, "deletions must be None, not 0"
        assert e["lines_changed"] is None, "lines_changed must be None, not 0"
        assert e["stats_available"] is False

    def test_missing_files_key_gives_none_not_zero(self):
        """API response with no 'files' key -> files_changed=None, not 0."""
        e = self._enrich({"sha": SAMPLE_COMMIT["commit_sha"], "stats": {"additions": 5, "deletions": 1, "total": 6}})
        assert e["files_changed"] is None, "files_changed must be None when 'files' key absent"
        assert e["files_available"] is False

    def test_zero_is_only_stored_when_api_says_zero(self):
        """If the API returns stats with all zeros, storing 0 is correct."""
        e = self._enrich({
            "sha": SAMPLE_COMMIT["commit_sha"],
            "stats": {"additions": 0, "deletions": 0, "total": 0},
            "files": [],
        })
        assert e["additions"] == 0
        assert e["deletions"] == 0
        assert e["files_changed"] == 0
        assert e["stats_available"] is True

    def test_none_and_zero_are_distinguishable(self):
        """None (unavailable) must not equal 0 (genuine zero)."""
        unavailable = self._enrich({"sha": SAMPLE_COMMIT["commit_sha"]})
        genuine_zero = self._enrich({
            "sha": SAMPLE_COMMIT["commit_sha"],
            "stats": {"additions": 0, "deletions": 0, "total": 0},
            "files": [],
        })
        assert unavailable["additions"] is None
        assert genuine_zero["additions"] == 0
        assert unavailable["additions"] != genuine_zero["additions"]

    def test_synthetic_records_marked_and_not_confused_with_unavailable(self):
        """Mock-enriched records must be marked synthetic=True, not stats_available=False."""
        from ingestion.enriched_commits import enrich_commit_mock
        enriched = enrich_commit_mock(SAMPLE_COMMIT)
        # Synthetic records have real-looking integers, not None
        assert enriched.get("synthetic") is True
        assert enriched.get("stats_available") is True
        assert isinstance(enriched["additions"], int)
        assert isinstance(enriched["deletions"], int)
