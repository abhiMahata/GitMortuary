"""
tests/test_analyze_repository.py
--------------------------------
Isolated unit tests for dynamic GitHub repository ingestion.
Uses mocked HTTP requests; requires no live token or network access.
"""

from datetime import datetime, timezone
import json
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest
import requests

from ingestion.analyze_repository import (
    parse_repository_identifier,
    GitHubClient,
    InvalidIdentifierError,
    GitHubAPIError,
    RepositoryNotFoundError,
    AuthenticationError,
    RateLimitExceededError,
    EmptyRepositoryError,
    normalize_repository,
    normalize_commit,
    enrich_commit_authentic,
    generate_run_directory,
    validate_output_directory,
    analyze_repository,
)


# ===========================================================================
# 1. IDENTIFIER PARSING TESTS
# ===========================================================================

class TestIdentifierParsing:
    def test_valid_owner_repo_slug(self):
        owner, repo = parse_repository_identifier("psf/requests")
        assert owner == "psf"
        assert repo == "requests"

    def test_valid_https_url(self):
        owner, repo = parse_repository_identifier("https://github.com/psf/requests")
        assert owner == "psf"
        assert repo == "requests"

    def test_valid_http_url_with_trailing_slash(self):
        owner, repo = parse_repository_identifier("http://github.com/torvalds/linux/")
        assert owner == "torvalds"
        assert repo == "linux"

    def test_valid_url_with_git_suffix(self):
        owner, repo = parse_repository_identifier("https://github.com/pallets/flask.git")
        assert owner == "pallets"
        assert repo == "flask"

    def test_valid_domain_shorthand(self):
        owner, repo = parse_repository_identifier("github.com/numpy/numpy")
        assert owner == "numpy"
        assert repo == "numpy"

    def test_valid_ssh_url(self):
        owner, repo = parse_repository_identifier("git@github.com:scikit-learn/scikit-learn.git")
        assert owner == "scikit-learn"
        assert repo == "scikit-learn"

    def test_invalid_empty_or_none(self):
        with pytest.raises(InvalidIdentifierError, match="cannot be empty"):
            parse_repository_identifier("")
        with pytest.raises(InvalidIdentifierError, match="cannot be empty"):
            parse_repository_identifier(None)  # type: ignore

    def test_invalid_single_segment(self):
        with pytest.raises(InvalidIdentifierError, match="Invalid repository identifier"):
            parse_repository_identifier("requests")

    def test_invalid_too_many_segments(self):
        with pytest.raises(InvalidIdentifierError, match="Invalid repository identifier"):
            parse_repository_identifier("psf/requests/extra/path")

    def test_invalid_host(self):
        with pytest.raises(InvalidIdentifierError, match="Unsupported host 'gitlab.com'"):
            parse_repository_identifier("https://gitlab.com/psf/requests")

    def test_invalid_owner_characters(self):
        with pytest.raises(InvalidIdentifierError, match="Invalid GitHub owner"):
            parse_repository_identifier("-badowner/repo")
        with pytest.raises(InvalidIdentifierError, match="Invalid GitHub owner"):
            parse_repository_identifier("owner--bad/repo")

    def test_invalid_repo_characters(self):
        with pytest.raises(InvalidIdentifierError, match="Invalid GitHub repository"):
            parse_repository_identifier("owner/..")


# ===========================================================================
# 2. CLIENT ERROR HANDLING & RETRY TESTS
# ===========================================================================

class TestClientErrorHandling:
    def test_401_raises_authentication_error_immediately(self):
        client = GitHubClient(token="bad_token", max_retries=3)
        mock_resp = MagicMock(spec=requests.Response)
        mock_resp.status_code = 401
        mock_resp.text = "Bad credentials"
        mock_resp.headers = {}

        with patch.object(client.session, "get", return_value=mock_resp) as mock_get:
            with pytest.raises(AuthenticationError, match="HTTP 401"):
                client.get_repository("owner", "repo")
            assert mock_get.call_count == 1  # Never retry 401

    def test_404_raises_not_found_error_immediately(self):
        client = GitHubClient(token=None, max_retries=3)
        mock_resp = MagicMock(spec=requests.Response)
        mock_resp.status_code = 404
        mock_resp.text = "Not Found"
        mock_resp.headers = {}

        with patch.object(client.session, "get", return_value=mock_resp) as mock_get:
            with pytest.raises(RepositoryNotFoundError, match="HTTP 404"):
                client.get_repository("owner", "nonexistent")
            assert mock_get.call_count == 1  # Never retry 404

    def test_429_raises_rate_limit_exceeded(self):
        client = GitHubClient(token=None, max_retries=2)
        mock_resp = MagicMock(spec=requests.Response)
        mock_resp.status_code = 429
        mock_resp.text = "Too Many Requests"
        mock_resp.headers = {"Retry-After": "60"}

        with patch.object(client.session, "get", return_value=mock_resp):
            with pytest.raises(RateLimitExceededError, match="rate limit exceeded"):
                client.get_repository("owner", "repo")

    def test_403_rate_limit_zero_remaining_raises(self):
        client = GitHubClient(token=None, max_retries=2)
        mock_resp = MagicMock(spec=requests.Response)
        mock_resp.status_code = 403
        mock_resp.text = "API rate limit exceeded for 127.0.0.1"
        mock_resp.headers = {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1999999999"}

        with patch.object(client.session, "get", return_value=mock_resp):
            with pytest.raises(RateLimitExceededError, match="rate limit exceeded"):
                client.get_repository("owner", "repo")

    def test_5xx_retries_with_backoff_and_raises(self):
        client = GitHubClient(token=None, max_retries=3, backoff_base=1.0)
        mock_resp = MagicMock(spec=requests.Response)
        mock_resp.status_code = 503
        mock_resp.text = "Service Unavailable"
        mock_resp.headers = {}

        with patch.object(client.session, "get", return_value=mock_resp) as mock_get:
            with patch("time.sleep"):  # avoid actual sleeping in tests
                with pytest.raises(GitHubAPIError, match="HTTP 503"):
                    client.get_repository("owner", "repo")
            assert mock_get.call_count == 3  # Exhausted 3 retries

    def test_connection_error_retries_and_raises(self):
        client = GitHubClient(token=None, max_retries=2, backoff_base=1.0)

        with patch.object(client.session, "get", side_effect=requests.ConnectionError("DNS failure")) as mock_get:
            with patch("time.sleep"):
                with pytest.raises(GitHubAPIError, match="Network error"):
                    client.get_repository("owner", "repo")
            assert mock_get.call_count == 2

    def test_malformed_json_response_raises(self):
        client = GitHubClient(token=None, max_retries=1)
        mock_resp = MagicMock(spec=requests.Response)
        mock_resp.status_code = 200
        mock_resp.json.side_effect = ValueError("Invalid JSON string")
        mock_resp.text = "Not JSON"
        mock_resp.headers = {}

        with patch.object(client.session, "get", return_value=mock_resp):
            with pytest.raises(GitHubAPIError, match="Malformed JSON response"):
                client.get_repository("owner", "repo")


# ===========================================================================
# 3. EMPTY REPOSITORY & PAGINATION TESTS
# ===========================================================================

class TestEmptyRepositoryAndPagination:
    def test_empty_repository_409_handled(self):
        client = GitHubClient(token=None)
        mock_resp = MagicMock(spec=requests.Response)
        mock_resp.status_code = 409
        mock_resp.json.return_value = {"message": "Git Repository is empty."}
        mock_resp.headers = {}

        with patch.object(client.session, "get", return_value=mock_resp):
            commits, has_more = client.get_commits("owner", "empty_repo", limit=50)
            assert commits == []
            assert has_more is False

    def test_empty_repository_200_empty_list(self):
        client = GitHubClient(token=None)
        mock_resp = MagicMock(spec=requests.Response)
        mock_resp.status_code = 200
        mock_resp.json.return_value = []
        mock_resp.headers = {}

        with patch.object(client.session, "get", return_value=mock_resp):
            commits, has_more = client.get_commits("owner", "empty_repo", limit=50)
            assert commits == []
            assert has_more is False

    def test_pagination_fetches_multiple_pages(self):
        client = GitHubClient(token=None)

        page1_resp = MagicMock(spec=requests.Response)
        page1_resp.status_code = 200
        page1_resp.json.return_value = [{"sha": f"sha_{i}"} for i in range(1, 3)]
        page1_resp.headers = {"Link": '<https://api.github.com/commits?page=2>; rel="next"'}

        page2_resp = MagicMock(spec=requests.Response)
        page2_resp.status_code = 200
        page2_resp.json.return_value = [{"sha": f"sha_{i}"} for i in range(3, 5)]
        page2_resp.headers = {}  # no next link

        with patch.object(client.session, "get", side_effect=[page1_resp, page2_resp]):
            commits, has_more = client.get_commits("owner", "repo", limit=10, per_page=2)
            assert len(commits) == 4
            assert [c["sha"] for c in commits] == ["sha_1", "sha_2", "sha_3", "sha_4"]
            assert has_more is False

    def test_commit_limit_stops_pagination_and_marks_has_more(self):
        client = GitHubClient(token=None)

        page1_resp = MagicMock(spec=requests.Response)
        page1_resp.status_code = 200
        page1_resp.json.return_value = [{"sha": f"sha_{i}"} for i in range(1, 4)]
        page1_resp.headers = {"Link": '<https://api.github.com/commits?page=2>; rel="next"'}

        with patch.object(client.session, "get", return_value=page1_resp):
            commits, has_more = client.get_commits("owner", "repo", limit=2, per_page=2)
            assert len(commits) == 2
            assert has_more is True


# ===========================================================================
# 4. NORMALIZATION & MISSING AUTHOR METADATA TESTS
# ===========================================================================

class TestNormalizationAndMissingMetadata:
    def test_normalize_repository_complete(self):
        raw = {
            "id": 999,
            "full_name": "psf/requests",
            "language": "Python",
            "stargazers_count": 50000,
            "forks_count": 9000,
            "created_at": "2011-02-13T18:38:17Z",
            "updated_at": "2026-10-19T10:00:00Z",
            "archived": False,
            "default_branch": "main",
        }
        norm = normalize_repository(raw)
        assert norm["repo_id"] == 999
        assert norm["repo_name"] == "psf/requests"
        assert norm["language"] == "Python"
        assert norm["stars"] == 50000
        assert norm["forks"] == 9000
        assert norm["archived"] is False
        assert norm["default_branch"] == "main"

    def test_missing_author_and_committer_preserves_none(self):
        raw_commit = {
            "sha": "a1b2c3d4e5f60000000000000000000000000000",
            "commit": {
                "message": "Orphan commit with no author info",
                "author": None,
                "committer": None,
            },
            "parents": [],
        }
        norm = normalize_commit(123, "owner/repo", raw_commit)
        assert norm["author_identity"] is None
        assert norm["author_timestamp"] is None
        assert norm["committer_identity"] is None
        assert norm["committer_timestamp"] is None
        assert norm["commit_message"] == "Orphan commit with no author info"
        assert norm["parent_count"] == 0

    def test_merge_commit_parent_count(self):
        raw_commit = {
            "sha": "merge_sha",
            "commit": {
                "message": "Merge pull request #1",
                "author": {"name": "Bob", "date": "2026-01-01T00:00:00Z"},
                "committer": {"name": "Bob", "date": "2026-01-01T00:00:00Z"},
            },
            "parents": [{"sha": "p1"}, {"sha": "p2"}],
        }
        norm = normalize_commit(123, "owner/repo", raw_commit)
        assert norm["parent_count"] == 2


# ===========================================================================
# 5. ENRICHMENT & AUTHENTIC PROVENANCE TESTS
# ===========================================================================

class TestEnrichmentAndProvenance:
    def test_enrich_with_stats_marks_authentic_not_synthetic(self):
        base_commit = {
            "repo_id": 1,
            "repo_name": "owner/repo",
            "commit_sha": "sha123",
            "author_identity": "Dev",
            "author_timestamp": "2026-01-01T00:00:00Z",
            "committer_identity": "Dev",
            "committer_timestamp": "2026-01-01T00:00:00Z",
            "commit_message": "Add feature",
            "parent_count": 1,
        }
        details = {
            "stats": {"additions": 45, "deletions": 12, "total": 57},
            "files": [{"filename": "main.py"}, {"filename": "utils.py"}],
        }
        enriched = enrich_commit_authentic(base_commit, details)
        assert enriched["additions"] == 45
        assert enriched["deletions"] == 12
        assert enriched["lines_changed"] == 57
        assert enriched["files_changed"] == 2
        assert enriched["stats_available"] is True
        assert enriched["files_available"] is True
        assert enriched["synthetic"] is False

    def test_enrich_with_genuine_zero_change_commit(self):
        base_commit = {
            "repo_id": 1,
            "repo_name": "owner/repo",
            "commit_sha": "sha000",
            "author_identity": "Dev",
            "author_timestamp": "2026-01-01T00:00:00Z",
            "committer_identity": "Dev",
            "committer_timestamp": "2026-01-01T00:00:00Z",
            "commit_message": "Empty commit",
            "parent_count": 1,
        }
        details = {
            "stats": {"additions": 0, "deletions": 0, "total": 0},
            "files": [],
        }
        enriched = enrich_commit_authentic(base_commit, details)
        assert enriched["additions"] == 0
        assert enriched["deletions"] == 0
        assert enriched["lines_changed"] == 0
        assert enriched["files_changed"] == 0
        assert enriched["stats_available"] is True
        assert enriched["files_available"] is True
        assert enriched["synthetic"] is False

    def test_enrich_with_unavailable_stats_preserves_none(self):
        base_commit = {
            "repo_id": 1,
            "repo_name": "owner/repo",
            "commit_sha": "sha_nostats",
            "author_identity": "Dev",
            "author_timestamp": "2026-01-01T00:00:00Z",
            "committer_identity": "Dev",
            "committer_timestamp": "2026-01-01T00:00:00Z",
            "commit_message": "Commit without detail",
            "parent_count": 1,
        }
        enriched = enrich_commit_authentic(base_commit, None)
        assert enriched["additions"] is None
        assert enriched["deletions"] is None
        assert enriched["lines_changed"] is None
        assert enriched["files_changed"] is None
        assert enriched["stats_available"] is False
        assert enriched["files_available"] is False
        assert enriched["synthetic"] is False


# ===========================================================================
# 6. END-TO-END WORKFLOW, MANIFEST, & RUN DIRECTORY TESTS
# ===========================================================================

class TestAnalyzeRepositoryWorkflow:
    def test_full_workflow_with_mocked_client(self, tmp_path):
        run_dir = tmp_path / "runs" / "test_run"

        mock_client = MagicMock(spec=GitHubClient)
        mock_client.token = None
        mock_client.last_rate_limit = {"limit": 60, "remaining": 50, "reset": 1999999999, "used": 10}

        mock_client.get_repository.return_value = {
            "id": 101,
            "full_name": "testowner/testrepo",
            "language": "Python",
            "stargazers_count": 150,
            "forks_count": 25,
            "created_at": "2020-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
            "archived": False,
            "default_branch": "main",
        }

        mock_client.get_commits.return_value = (
            [
                {
                    "sha": "commit_1",
                    "commit": {
                        "message": "Initial commit",
                        "author": {"name": "Alice", "date": "2026-01-01T00:00:00Z"},
                        "committer": {"name": "Alice", "date": "2026-01-01T00:00:00Z"},
                    },
                    "parents": [],
                },
                {
                    "sha": "commit_2",
                    "commit": {
                        "message": "Update README",
                        "author": {"name": "Bob", "date": "2026-01-02T00:00:00Z"},
                        "committer": {"name": "Bob", "date": "2026-01-02T00:00:00Z"},
                    },
                    "parents": [{"sha": "commit_1"}],
                },
            ],
            False,  # no more commits
        )

        mock_client.get_commit_details.side_effect = [
            {"stats": {"additions": 100, "deletions": 0, "total": 100}, "files": [{"filename": "main.py"}]},
            {"stats": {"additions": 5, "deletions": 2, "total": 7}, "files": [{"filename": "README.md"}]},
        ]

        manifest = analyze_repository(
            identifier="testowner/testrepo",
            output_dir=run_dir,
            commit_limit=10,
            client=mock_client,
            verbose=False,
        )

        assert manifest["status"] == "complete"
        assert manifest["completeness"]["is_complete"] is True
        assert manifest["counts"]["repositories"] == 1
        assert manifest["counts"]["commits"] == 2
        assert manifest["counts"]["enriched_commits"] == 2
        assert manifest["counts"]["authentic_records"] == 2
        assert manifest["counts"]["synthetic_records"] == 0

        # Verify output files exist
        assert (run_dir / "repositories.jsonl").exists()
        assert (run_dir / "commits.jsonl").exists()
        assert (run_dir / "enriched_commits.jsonl").exists()
        assert (run_dir / "manifest.json").exists()

        # Verify JSONL lines
        with open(run_dir / "repositories.jsonl") as f:
            repos = [json.loads(line) for line in f if line.strip()]
        assert len(repos) == 1
        assert repos[0]["repo_name"] == "testowner/testrepo"

        with open(run_dir / "enriched_commits.jsonl") as f:
            enriched = [json.loads(line) for line in f if line.strip()]
        assert len(enriched) == 2
        assert enriched[0]["additions"] == 100
        assert enriched[1]["lines_changed"] == 7
        assert all(r["synthetic"] is False for r in enriched)

    def test_truncated_run_marked_properly(self, tmp_path):
        run_dir = tmp_path / "runs" / "trunc_run"

        mock_client = MagicMock(spec=GitHubClient)
        mock_client.token = None
        mock_client.last_rate_limit = {"limit": 60, "remaining": 40, "reset": 1999999999, "used": 20}
        mock_client.get_repository.return_value = {
            "id": 102, "full_name": "test/large_repo", "stargazers_count": 10,
            "forks_count": 1, "created_at": "2020-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z",
            "archived": False, "default_branch": "main",
        }
        mock_client.get_commits.return_value = (
            [{"sha": f"sha_{i}", "commit": {"message": f"c{i}"}, "parents": []} for i in range(2)],
            True,  # has_more == True
        )
        mock_client.get_commit_details.return_value = {"stats": {"additions": 1, "deletions": 1, "total": 2}, "files": []}

        manifest = analyze_repository(
            identifier="test/large_repo",
            output_dir=run_dir,
            commit_limit=2,
            client=mock_client,
            verbose=False,
        )

        assert manifest["status"] == "truncated"
        assert manifest["completeness"]["is_complete"] is False
        assert manifest["completeness"]["truncated_by_limit"] is True
        assert len(manifest["warnings"]) > 0


# ===========================================================================
# 7. SAMPLE DATA PROTECTION TESTS
# ===========================================================================

class TestSampleDataProtection:
    def test_protects_against_writing_to_data_sample(self):
        with pytest.raises(ValueError, match="protected sample path"):
            validate_output_directory(Path("data/sample"))

        with pytest.raises(ValueError, match="protected sample path"):
            validate_output_directory(Path("data/sample/subfolder"))

    def test_existing_sample_files_not_modified(self):
        sample_repo_file = Path("data/sample/repositories.jsonl")
        sample_commit_file = Path("data/sample/commits.jsonl")
        sample_enriched_file = Path("data/sample/enriched_commits.jsonl")

        assert sample_repo_file.exists()
        assert sample_commit_file.exists()
        assert sample_enriched_file.exists()

        mtime_repos = sample_repo_file.stat().st_mtime
        mtime_commits = sample_commit_file.stat().st_mtime
        mtime_enriched = sample_enriched_file.stat().st_mtime

        # Run identifier parser and generator to ensure no side effects
        parse_repository_identifier("psf/requests")
        generate_run_directory("psf/requests")

        # Confirm timestamps untouched
        assert sample_repo_file.stat().st_mtime == mtime_repos
        assert sample_commit_file.stat().st_mtime == mtime_commits
        assert sample_enriched_file.stat().st_mtime == mtime_enriched
