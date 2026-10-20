#!/usr/bin/env python3
"""
ingestion/analyze_repository.py
-------------------------------
Dynamic GitHub repository ingestion CLI and library for Git Pattern Analysis.

Retrieves authentic repository metadata and commit history from the GitHub REST API,
enriches each commit with file and line diff statistics, and saves schema-compatible
artifacts into an isolated run directory.

Usage:
  python3 -m ingestion.analyze_repository psf/requests
  python3 -m ingestion.analyze_repository https://github.com/psf/requests
  python3 -m ingestion.analyze_repository --max-commits 20 --output-dir data/runs/my_run psf/requests
"""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
import time
import urllib.parse
import uuid

import requests

try:
    from ingestion.config import GITHUB_API, GITHUB_TOKEN
except ModuleNotFoundError:
    from config import GITHUB_API, GITHUB_TOKEN  # type: ignore[no-redef]

# ---------------------------------------------------------------------------
# Constants & Defaults
# ---------------------------------------------------------------------------

DEFAULT_COMMIT_LIMIT = 50
DEFAULT_TIMEOUT = 30.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_BASE = 1.5
DEFAULT_BACKOFF_MAX = 30.0

DEFAULT_RUNS_DIR = Path("data/runs")
PROTECTED_SAMPLE_DIR = Path("data/sample")

GITHUB_OWNER_PATTERN = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9]|-(?=[A-Za-z0-9])){0,38}$")
GITHUB_REPO_PATTERN = re.compile(r"^[A-Za-z0-9_.-]{1,100}$")


# ---------------------------------------------------------------------------
# Custom Exceptions
# ---------------------------------------------------------------------------

class IngestionError(Exception):
    """Base exception for repository ingestion failures."""


class InvalidIdentifierError(IngestionError):
    """Raised when repository URL or owner/name format is invalid."""


class GitHubAPIError(IngestionError):
    """Raised when GitHub API returns an unhandled HTTP error."""

    def __init__(self, message: str, status_code: int | None = None, response_body: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


class RepositoryNotFoundError(GitHubAPIError):
    """Raised when repository does not exist or is inaccessible (404)."""


class AuthenticationError(GitHubAPIError):
    """Raised when token is invalid or lacks access (401)."""


class PermissionDeniedError(GitHubAPIError):
    """Raised when access is forbidden (403 without rate limit)."""


class RateLimitExceededError(GitHubAPIError):
    """Raised when GitHub API rate limits are exhausted (403/429)."""


class EmptyRepositoryError(GitHubAPIError):
    """Raised when repository exists but contains no commits (409 Conflict)."""


# ---------------------------------------------------------------------------
# Identifier Parsing
# ---------------------------------------------------------------------------

def parse_repository_identifier(identifier: str) -> tuple[str, str]:
    """
    Parse a GitHub repository identifier or URL into (owner, repo).

    Accepts:
      - 'psf/requests'
      - 'https://github.com/psf/requests'
      - 'http://github.com/psf/requests'
      - 'github.com/psf/requests'
      - 'https://github.com/psf/requests.git'
      - 'git@github.com:psf/requests.git'
      - Trailing slashes

    Returns:
      (owner, repo) as clean lowercase strings.

    Raises:
      InvalidIdentifierError: If identifier cannot be parsed or violates GitHub naming conventions.
    """
    if not identifier or not isinstance(identifier, str):
        raise InvalidIdentifierError("Repository identifier cannot be empty.")

    raw = identifier.strip()

    # Handle SSH git@github.com:owner/repo(.git)
    if raw.startswith("git@github.com:"):
        path = raw[len("git@github.com:"):]
    elif "://" in raw or raw.startswith("github.com/"):
        if not raw.startswith("http://") and not raw.startswith("https://"):
            raw = "https://" + raw
        parsed = urllib.parse.urlparse(raw)
        host = (parsed.netloc or "").lower()
        if host not in ("github.com", "www.github.com"):
            raise InvalidIdentifierError(
                f"Unsupported host '{host}'. Only 'github.com' repositories are supported."
            )
        path = parsed.path.lstrip("/")
    else:
        path = raw.lstrip("/")

    # Remove trailing slashes and optional .git suffix
    path = path.rstrip("/")
    if path.endswith(".git"):
        path = path[:-4]

    parts = [p for p in path.split("/") if p]
    if len(parts) != 2:
        raise InvalidIdentifierError(
            f"Invalid repository identifier '{identifier}'. "
            "Expected format 'owner/repo' or 'https://github.com/owner/repo'."
        )

    owner, repo = parts[0].strip(), parts[1].strip()

    if not GITHUB_OWNER_PATTERN.match(owner):
        raise InvalidIdentifierError(
            f"Invalid GitHub owner/organization name '{owner}'. "
            "Must be 1-39 alphanumeric characters or non-consecutive hyphens."
        )

    if not GITHUB_REPO_PATTERN.match(repo) or repo in (".", ".."):
        raise InvalidIdentifierError(
            f"Invalid GitHub repository name '{repo}'. "
            "Must be 1-100 alphanumeric characters, hyphens, underscores, or periods."
        )

    return owner, repo


# ---------------------------------------------------------------------------
# GitHub Client with Rate Limiting & Retry
# ---------------------------------------------------------------------------

class GitHubClient:
    """
    HTTP client for the GitHub REST API.
    Handles authentication, bounded retries, rate-limit headers, and status mapping.
    """

    def __init__(
        self,
        token: str | None = None,
        base_url: str = GITHUB_API,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_base: float = DEFAULT_BACKOFF_BASE,
        backoff_max: float = DEFAULT_BACKOFF_MAX,
        session: requests.Session | None = None,
    ):
        self.token = token or os.getenv("GITHUB_TOKEN") or GITHUB_TOKEN
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self.session = session or requests.Session()

        self.last_rate_limit: dict[str, int | None] = {
            "limit": None,
            "remaining": None,
            "reset": None,
            "used": None,
        }

    def _headers(self) -> dict[str, str]:
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "GitPatternAnalysis-DynamicIngestion/1.0",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def _update_rate_limit(self, response: requests.Response) -> None:
        def _safe_int(val: str | None) -> int | None:
            if val is None:
                return None
            try:
                return int(val)
            except ValueError:
                return None

        self.last_rate_limit = {
            "limit": _safe_int(response.headers.get("X-RateLimit-Limit")),
            "remaining": _safe_int(response.headers.get("X-RateLimit-Remaining")),
            "reset": _safe_int(response.headers.get("X-RateLimit-Reset")),
            "used": _safe_int(response.headers.get("X-RateLimit-Used")),
        }

    def request(
        self,
        endpoint_or_url: str,
        params: dict | None = None,
    ) -> tuple[int, dict | list, requests.Response]:
        """
        Execute an HTTP GET request against the GitHub REST API with bounded exponential backoff.
        """
        if endpoint_or_url.startswith("http://") or endpoint_or_url.startswith("https://"):
            url = endpoint_or_url
        else:
            url = f"{self.base_url}/{endpoint_or_url.lstrip('/')}"

        headers = self._headers()

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=self.timeout,
                )
            except (requests.ConnectionError, requests.Timeout) as exc:
                if attempt == self.max_retries:
                    raise GitHubAPIError(f"Network error after {self.max_retries} attempts: {exc}") from exc
                wait_time = min(self.backoff_base ** attempt, self.backoff_max)
                time.sleep(wait_time)
                continue

            self._update_rate_limit(response)
            status = response.status_code

            # 401 Unauthorized — Fatal credential failure
            if status == 401:
                raise AuthenticationError(
                    "GitHub API authentication failed (HTTP 401). Token is missing, expired, or revoked.",
                    status_code=401,
                    response_body=response.text,
                )

            # 404 Not Found — Repository or commit does not exist
            if status == 404:
                raise RepositoryNotFoundError(
                    f"Resource not found at {url} (HTTP 404).",
                    status_code=404,
                    response_body=response.text,
                )

            # 409 Conflict — Empty repository indicator
            if status == 409:
                body_json = {}
                try:
                    body_json = response.json()
                except Exception:
                    pass
                msg = body_json.get("message", response.text)
                if "empty" in msg.lower():
                    raise EmptyRepositoryError(
                        f"Git Repository is empty (HTTP 409): {msg}",
                        status_code=409,
                        response_body=response.text,
                    )
                raise GitHubAPIError(f"Conflict error (HTTP 409): {msg}", status_code=409)

            # Rate Limit responses (403 with rate limit message / remaining=0, or 429)
            is_rate_limited = (
                status == 429
                or (status == 403 and self.last_rate_limit.get("remaining") == 0)
                or (status == 403 and "rate limit" in response.text.lower())
            )
            if is_rate_limited:
                reset_ts = self.last_rate_limit.get("reset")
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_sec = int(retry_after) + 1
                elif reset_ts:
                    wait_sec = max(reset_ts - int(time.time()) + 1, 1)
                else:
                    wait_sec = 60

                # If wait time is reasonable (<= 5s) and we have retries left, wait and retry once
                if wait_sec <= 5 and attempt < self.max_retries:
                    time.sleep(wait_sec)
                    continue

                raise RateLimitExceededError(
                    f"GitHub API rate limit exceeded (HTTP {status}). "
                    f"Remaining: {self.last_rate_limit.get('remaining')}. Resets in {wait_sec}s.",
                    status_code=status,
                    response_body=response.text,
                )

            # 403 Forbidden (other permission issues, e.g. IP block or scope)
            if status == 403:
                raise PermissionDeniedError(
                    f"Access forbidden (HTTP 403): {response.text}",
                    status_code=403,
                    response_body=response.text,
                )

            # 5xx Server Error — Transient, retry with backoff
            if status >= 500:
                if attempt == self.max_retries:
                    raise GitHubAPIError(
                        f"GitHub server error (HTTP {status}) after {self.max_retries} attempts.",
                        status_code=status,
                        response_body=response.text,
                    )
                wait_time = min(self.backoff_base ** attempt, self.backoff_max)
                time.sleep(wait_time)
                continue

            # Check for non-2xx status
            if status < 200 or status >= 300:
                raise GitHubAPIError(
                    f"Unexpected HTTP status {status} from GitHub API.",
                    status_code=status,
                    response_body=response.text,
                )

            # Parse JSON
            try:
                data = response.json()
            except ValueError as exc:
                raise GitHubAPIError(
                    f"Malformed JSON response from GitHub API: {exc}",
                    status_code=status,
                    response_body=response.text,
                ) from exc

            return status, data, response

        raise GitHubAPIError(f"Exhausted retries for request to {url}")

    def get_repository(self, owner: str, repo: str) -> dict:
        """Fetch repository metadata from /repos/{owner}/{repo}."""
        _, data, _ = self.request(f"/repos/{owner}/{repo}")
        if not isinstance(data, dict):
            raise GitHubAPIError("Repository metadata response is not a JSON object.")
        return data

    def get_commits(
        self,
        owner: str,
        repo: str,
        limit: int = DEFAULT_COMMIT_LIMIT,
        per_page: int = 100,
    ) -> tuple[list[dict], bool]:
        """
        Fetch commits for a repository with pagination up to `limit`.

        Returns:
          (commits_list, has_more_commits)
        """
        commits: list[dict] = []
        page = 1
        has_more = False

        effective_per_page = min(max(1, per_page), 100)
        is_unlimited = (limit is None or limit <= 0)

        while True:
            if not is_unlimited and len(commits) >= limit:
                has_more = True
                break

            remaining_needed = (limit - len(commits)) if not is_unlimited else effective_per_page
            current_per_page = min(effective_per_page, remaining_needed)

            # Check rate limit headroom
            remaining_quota = self.last_rate_limit.get("remaining")
            if remaining_quota is not None and remaining_quota <= 1:
                has_more = True
                break

            try:
                _, page_data, response = self.request(
                    f"/repos/{owner}/{repo}/commits",
                    params={"per_page": current_per_page, "page": page},
                )
            except EmptyRepositoryError:
                # 409 Conflict: Git Repository is empty
                return [], False

            if not isinstance(page_data, list):
                raise GitHubAPIError("Commits endpoint returned unexpected response format (expected array).")

            if not page_data:
                # No more commits in history
                has_more = False
                break

            commits.extend(page_data)

            # Check Link header for 'rel="next"'
            link_header = response.headers.get("Link", "")
            next_available = 'rel="next"' in link_header

            if not next_available or len(page_data) < current_per_page:
                has_more = False
                break

            page += 1

        if not is_unlimited and len(commits) >= limit:
            has_more = True
            return commits[:limit], has_more

        return commits, has_more

    def get_commit_details(self, owner: str, repo: str, commit_sha: str) -> dict:
        """Fetch detailed commit diff statistics and file counts from /repos/{owner}/{repo}/commits/{sha}."""
        _, data, _ = self.request(f"/repos/{owner}/{repo}/commits/{commit_sha}")
        if not isinstance(data, dict):
            raise GitHubAPIError(f"Commit details for {commit_sha} is not a JSON object.")
        return data


# ---------------------------------------------------------------------------
# Normalization Helpers (Strict Schema Alignment)
# ---------------------------------------------------------------------------

def normalize_repository(raw_repo: dict) -> dict:
    """Normalize raw GitHub repository response to schema in repositories.jsonl."""
    return {
        "repo_id": int(raw_repo["id"]),
        "repo_name": str(raw_repo["full_name"]),
        "language": raw_repo.get("language"),
        "stars": int(raw_repo.get("stargazers_count", 0)),
        "forks": int(raw_repo.get("forks_count", 0)),
        "created_at": raw_repo.get("created_at"),
        "updated_at": raw_repo.get("updated_at"),
        "archived": bool(raw_repo.get("archived", False)),
        "default_branch": str(raw_repo.get("default_branch") or "main"),
    }


def normalize_commit(repo_id: int, repo_name: str, raw_commit: dict) -> dict:
    """Normalize raw commit object from /commits list to schema in commits.jsonl."""
    commit_data = raw_commit.get("commit") or {}
    author = commit_data.get("author") or {}
    committer = commit_data.get("committer") or {}
    parents = raw_commit.get("parents") or []

    return {
        "repo_id": repo_id,
        "repo_name": repo_name,
        "commit_sha": str(raw_commit.get("sha", "")),
        "author_identity": author.get("name") if author else None,
        "author_timestamp": author.get("date") if author else None,
        "committer_identity": committer.get("name") if committer else None,
        "committer_timestamp": committer.get("date") if committer else None,
        "commit_message": commit_data.get("message"),
        "parent_count": len(parents),
    }


def enrich_commit_authentic(commit: dict, details: dict | None) -> dict:
    """
    Enrich normalized commit with authentic stats from GitHub commit detail response.
    Never fabricates metrics. If unavailable, represents as None.
    Marks record explicitly with synthetic: False.
    """
    enriched = dict(commit)

    if details is None:
        enriched["files_changed"] = None
        enriched["additions"] = None
        enriched["deletions"] = None
        enriched["lines_changed"] = None
        enriched["stats_available"] = False
        enriched["files_available"] = False
        enriched["file_names"] = []
    else:
        stats = details.get("stats")
        files = details.get("files")

        if stats is not None:
            enriched["additions"] = stats.get("additions")
            enriched["deletions"] = stats.get("deletions")
            enriched["lines_changed"] = stats.get("total")
            enriched["stats_available"] = True
        else:
            enriched["additions"] = None
            enriched["deletions"] = None
            enriched["lines_changed"] = None
            enriched["stats_available"] = False

        if files is not None:
            enriched["files_changed"] = len(files)
            enriched["files_available"] = True
            enriched["file_names"] = [f.get("filename") for f in files if isinstance(f, dict) and "filename" in f][:20]
        else:
            enriched["files_changed"] = None
            enriched["files_available"] = False
            enriched["file_names"] = []

    enriched["synthetic"] = False
    return enriched


# ---------------------------------------------------------------------------
# Path & Directory Protection
# ---------------------------------------------------------------------------

def generate_run_directory(
    repo_name: str,
    base_dir: Path | str = DEFAULT_RUNS_DIR,
) -> Path:
    """Generate an isolated, unique run directory: data/runs/<repo>-<timestamp>-<short_id>/."""
    _, repo = parse_repository_identifier(repo_name)
    slug = re.sub(r"[^A-Za-z0-9_-]", "_", repo).lower()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    unique_suffix = uuid.uuid4().hex[:8]
    folder_name = f"{slug}-{timestamp}-{unique_suffix}"
    return Path(base_dir) / folder_name


def validate_output_directory(output_path: Path) -> None:
    """Ensure output directory does not target protected sample or baseline paths."""
    resolved = output_path.resolve()
    protected_sample = PROTECTED_SAMPLE_DIR.resolve()

    if resolved == protected_sample or protected_sample in resolved.parents:
        raise ValueError(
            f"Cannot write run outputs to protected sample path '{output_path}'. "
            "Please use an isolated directory such as 'data/runs/<run-id>'."
        )


# ---------------------------------------------------------------------------
# Core Ingestion Workflow
# ---------------------------------------------------------------------------

def analyze_repository(
    identifier: str,
    output_dir: Path | str | None = None,
    commit_limit: int = DEFAULT_COMMIT_LIMIT,
    token: str | None = None,
    timeout: float = DEFAULT_TIMEOUT,
    client: GitHubClient | None = None,
    verbose: bool = True,
    on_progress=None,
) -> dict:
    """
    Main entry point for authentic repository ingestion.

    Returns:
      manifest dictionary summarizing run outcomes, record counts, and status.
    """
    start_time = datetime.now(timezone.utc)
    owner, repo = parse_repository_identifier(identifier)
    canonical_repo_name = f"{owner}/{repo}"

    if output_dir is None:
        target_dir = generate_run_directory(canonical_repo_name)
    else:
        target_dir = Path(output_dir)

    validate_output_directory(target_dir)

    # Protect against accidental overwrite of existing files
    if target_dir.exists() and any(target_dir.iterdir()):
        raise ValueError(
            f"Target output directory '{target_dir}' already exists and is not empty. "
            "Choose a new directory or remove existing files to proceed."
        )

    target_dir.mkdir(parents=True, exist_ok=True)

    if client is None:
        client = GitHubClient(token=token, timeout=timeout)

    history_mode = "full_history" if (commit_limit is None or commit_limit <= 0) else "recent_limit"

    manifest: dict = {
        "manifest_version": "1.0",
        "run_id": target_dir.name,
        "repository": canonical_repo_name,
        "repository_url": f"https://github.com/{canonical_repo_name}",
        "started_at": start_time.isoformat(),
        "completed_at": None,
        "duration_seconds": None,
        "status": "in_progress",
        "history_mode": history_mode,
        "commit_limit": commit_limit if (commit_limit and commit_limit > 0) else None,
        "requested_limit": commit_limit if (commit_limit and commit_limit > 0) else "all",
        "date_range": {
            "earliest": None,
            "latest": None,
        },
        "completeness": {
            "is_complete": False,
            "truncated_by_limit": False,
            "has_more_commits": False,
            "total_commits_fetched": 0,
        },
        "counts": {
            "repositories": 0,
            "commits": 0,
            "enriched_commits": 0,
            "authentic_records": 0,
            "synthetic_records": 0,
        },
        "rate_limit": {
            "authenticated": bool(client.token),
            "limit": None,
            "remaining": None,
            "reset_timestamp": None,
        },
        "files": {
            "repositories": "repositories.jsonl",
            "commits": "commits.jsonl",
            "enriched_commits": "enriched_commits.jsonl",
            "manifest": "manifest.json",
        },
        "warnings": [],
        "errors": [],
    }

    path_repos = target_dir / "repositories.jsonl"
    path_commits = target_dir / "commits.jsonl"
    path_enriched = target_dir / "enriched_commits.jsonl"
    path_manifest = target_dir / "manifest.json"

    def _write_manifest(status: str) -> None:
        manifest["status"] = status
        end_time = datetime.now(timezone.utc)
        if status in ("complete", "truncated", "empty"):
            manifest["completed_at"] = end_time.isoformat()
        manifest["duration_seconds"] = round((end_time - start_time).total_seconds(), 2)
        manifest["rate_limit"]["limit"] = client.last_rate_limit.get("limit")
        manifest["rate_limit"]["remaining"] = client.last_rate_limit.get("remaining")
        manifest["rate_limit"]["reset_timestamp"] = client.last_rate_limit.get("reset")

        with path_manifest.open("w", encoding="utf-8") as fh:
            json.dump(manifest, fh, indent=2)

    def _log(msg: str) -> None:
        if verbose:
            print(msg)

    _log(f"=== Starting Ingestion: {canonical_repo_name} ===")
    _log(f"  Target Run Directory : {target_dir}")
    _log(f"  Commit Limit         : {commit_limit}")
    _log(f"  Authentication       : {'Token provided' if client.token else 'Unauthenticated (rate limits apply)'}")

    try:
        # Step 1: Retrieve Repository Metadata
        _log("Fetching repository metadata …")
        raw_repo = client.get_repository(owner, repo)
        norm_repo = normalize_repository(raw_repo)

        with path_repos.open("w", encoding="utf-8") as fh:
            fh.write(json.dumps(norm_repo) + "\n")
        manifest["counts"]["repositories"] = 1
        _log(f"  ✓ Repository '{norm_repo['repo_name']}' loaded (ID: {norm_repo['repo_id']}, Stars: {norm_repo['stars']})")
        if on_progress:
            on_progress("metadata", f"Loaded repository '{norm_repo['repo_name']}' ({norm_repo['stars']} stars)", 20)

        # Step 2: Retrieve Commits
        _log(f"Fetching commits (limit: {commit_limit}) …")
        raw_commits, has_more = client.get_commits(owner, repo, limit=commit_limit)

        if not raw_commits:
            manifest["warnings"].append("Repository has no commits (empty repository).")
            manifest["completeness"]["is_complete"] = True
            manifest["completeness"]["has_more_commits"] = False
            _write_manifest("empty")
            _log("  ⚠ Repository is empty (0 commits found).")
            if on_progress:
                on_progress("complete", "Repository has 0 commits.", 100)
            return manifest

        manifest["counts"]["commits"] = len(raw_commits)
        manifest["completeness"]["total_commits_fetched"] = len(raw_commits)
        manifest["completeness"]["has_more_commits"] = has_more
        manifest["completeness"]["truncated_by_limit"] = has_more
        manifest["completeness"]["is_complete"] = not has_more

        normalized_commits = []
        with path_commits.open("w", encoding="utf-8") as fh:
            for c in raw_commits:
                norm_c = normalize_commit(norm_repo["repo_id"], norm_repo["repo_name"], c)
                normalized_commits.append(norm_c)
                fh.write(json.dumps(norm_c) + "\n")

        dates = [c["author_timestamp"] for c in normalized_commits if c.get("author_timestamp")]
        if dates:
            manifest["date_range"]["earliest"] = min(dates)
            manifest["date_range"]["latest"] = max(dates)

        _log(f"  ✓ {len(normalized_commits)} commit records saved.")
        if on_progress:
            on_progress("commits", f"Retrieved {len(normalized_commits)} commit records", 45)

        if has_more:
            manifest["warnings"].append(
                f"History truncated by commit limit ({commit_limit}). Repository contains additional commits."
            )
            _log(f"  ℹ Run truncated at limit of {commit_limit} commits.")

        # Step 3: Retrieve Commit Details & Enrich
        _log(f"Enriching {len(normalized_commits)} commits with authentic stats …")
        enriched_count = 0

        with path_enriched.open("w", encoding="utf-8") as fh:
            for idx, c in enumerate(normalized_commits, 1):
                sha = c["commit_sha"]
                sha_short = sha[:8]

                # Check rate limit headroom before making commit detail calls
                remaining = client.last_rate_limit.get("remaining")
                if remaining is not None and remaining <= 1:
                    warning_msg = (
                        f"Rate limit nearly exhausted ({remaining} requests remaining). "
                        "Halting per-commit enrichment to protect quota."
                    )
                    manifest["warnings"].append(warning_msg)
                    manifest["completeness"]["truncated_by_limit"] = True
                    manifest["completeness"]["is_complete"] = False
                    _log(f"  ⚠ {warning_msg}")
                    # Write remaining commits without stats
                    for unproc in normalized_commits[idx - 1:]:
                        rec = enrich_commit_authentic(unproc, None)
                        fh.write(json.dumps(rec) + "\n")
                        enriched_count += 1
                    break

                try:
                    details = client.get_commit_details(owner, repo, sha)
                except GitHubAPIError as err:
                    manifest["warnings"].append(f"Failed to fetch detail for commit {sha_short}: {err}")
                    details = None

                enriched_rec = enrich_commit_authentic(c, details)
                fh.write(json.dumps(enriched_rec) + "\n")
                fh.flush()
                enriched_count += 1

                if on_progress and (idx % 5 == 0 or idx == len(normalized_commits)):
                    pct = 45 + int(50 * idx / max(1, len(normalized_commits)))
                    on_progress("enrichment", f"Enriching commits with authentic stats ({idx}/{len(normalized_commits)})...", pct)

                if verbose and idx % 10 == 0:
                    _log(f"    [{idx}/{len(normalized_commits)}] Enriched {sha_short}")

        manifest["counts"]["enriched_commits"] = enriched_count
        manifest["counts"]["authentic_records"] = enriched_count
        manifest["counts"]["synthetic_records"] = 0

        status = "truncated" if (has_more or not manifest["completeness"]["is_complete"]) else "complete"
        _write_manifest(status)
        _log(f"✓ Ingestion {status.upper()}. Manifest saved to {path_manifest}")
        if on_progress:
            on_progress("complete", f"Ingestion {status.upper()} ({enriched_count} commits ready)", 100)
        return manifest

    except Exception as exc:
        manifest["errors"].append(str(exc))
        _write_manifest("failed")
        _log(f"✗ Ingestion FAILED: {exc}")
        raise


# ---------------------------------------------------------------------------
# CLI Command
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python3 -m ingestion.analyze_repository",
        description="Retrieve authentic GitHub repository metadata and commits into an isolated run directory.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 -m ingestion.analyze_repository psf/requests
  python3 -m ingestion.analyze_repository https://github.com/psf/requests
  python3 -m ingestion.analyze_repository --max-commits 25 psf/requests
  python3 -m ingestion.analyze_repository --output-dir data/runs/requests_test psf/requests
        """,
    )
    parser.add_argument(
        "repository",
        help="GitHub repository URL (https://github.com/owner/repo) or 'owner/repo' slug.",
    )
    parser.add_argument(
        "-o", "--output-dir",
        default=None,
        help="Custom output directory for run artifacts (default: data/runs/<repo>-<timestamp>-<run-id>/).",
    )
    parser.add_argument(
        "-n", "--max-commits", "--limit",
        type=int,
        default=DEFAULT_COMMIT_LIMIT,
        help=f"Maximum number of commits to retrieve (default: {DEFAULT_COMMIT_LIMIT}).",
    )
    parser.add_argument(
        "-t", "--token",
        default=None,
        help="GitHub personal access token (optional; defaults to GITHUB_TOKEN environment variable).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT,
        help=f"HTTP request timeout in seconds (default: {DEFAULT_TIMEOUT}).",
    )
    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Suppress progress logging.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.max_commits <= 0:
        parser.error("--max-commits must be a positive integer.")

    try:
        manifest = analyze_repository(
            identifier=args.repository,
            output_dir=args.output_dir,
            commit_limit=args.max_commits,
            token=args.token,
            timeout=args.timeout,
            verbose=not args.quiet,
        )
        status = manifest.get("status", "unknown")
        if status in ("complete", "truncated", "empty"):
            sys.exit(0)
        else:
            sys.exit(1)
    except InvalidIdentifierError as exc:
        print(f"[ERROR] Invalid repository: {exc}", file=sys.stderr)
        sys.exit(2)
    except RateLimitExceededError as exc:
        print(f"[ERROR] Rate limit exceeded: {exc}", file=sys.stderr)
        sys.exit(3)
    except RepositoryNotFoundError as exc:
        print(f"[ERROR] Repository not found: {exc}", file=sys.stderr)
        sys.exit(4)
    except AuthenticationError as exc:
        print(f"[ERROR] Authentication failed: {exc}", file=sys.stderr)
        sys.exit(5)
    except IngestionError as exc:
        print(f"[ERROR] Ingestion error: {exc}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"[FATAL] Unexpected error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
