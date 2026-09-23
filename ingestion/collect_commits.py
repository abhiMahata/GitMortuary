import json
import sys
from pathlib import Path

import requests

from config import (
    GITHUB_API,
    GITHUB_TOKEN,
    COMMIT_OUTPUT_FILE,
    MAX_COMMITS_PER_REPO,
    OUTPUT_FILE,
)


def github_headers():
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    return headers


def load_repositories():
    repositories = []

    with open(OUTPUT_FILE, "r", encoding="utf-8") as file:
        for line in file:
            repositories.append(json.loads(line))

    return repositories


def collect_repository_commits(repo):
    owner_repo = repo["repo_name"]

    url = f"{GITHUB_API}/repos/{owner_repo}/commits"

    params = {
        "per_page": min(MAX_COMMITS_PER_REPO, 100),
    }

    response = requests.get(
        url,
        headers=github_headers(),
        params=params,
        timeout=30,
    )

    response.raise_for_status()

    return response.json()


def normalize_commit(repo, commit):
    commit_data = commit["commit"]

    author = commit_data.get("author") or {}
    committer = commit_data.get("committer") or {}

    return {
        "repo_id": repo["repo_id"],
        "repo_name": repo["repo_name"],

        "commit_sha": commit["sha"],

        "author_identity": author.get("name"),
        "author_timestamp": author.get("date"),

        "committer_identity": committer.get("name"),
        "committer_timestamp": committer.get("date"),

        "commit_message": commit_data.get("message"),

        "parent_count": len(commit.get("parents", [])),
    }


def save_commits(commits):
    output_path = Path(COMMIT_OUTPUT_FILE)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as file:
        for commit in commits:
            file.write(json.dumps(commit) + "\n")


def main():
    try:
        repositories = load_repositories()

        all_commits = []

        for index, repo in enumerate(repositories, start=1):
            print(
                f"[{index}/{len(repositories)}] "
                f"Collecting {repo['repo_name']}"
            )

            commits = collect_repository_commits(repo)

            for commit in commits:
                normalized = normalize_commit(repo, commit)
                all_commits.append(normalized)

        save_commits(all_commits)

        print()
        print(f"Collected {len(all_commits)} commits.")
        print(f"Saved to: {COMMIT_OUTPUT_FILE}")

    except requests.HTTPError as error:
        print(f"GitHub API error: {error}")
        sys.exit(1)

    except requests.RequestException as error:
        print(f"Network error: {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()