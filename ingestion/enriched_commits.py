import json
import sys
from pathlib import Path

import requests

from config import (
    GITHUB_API,
    GITHUB_TOKEN,
    COMMIT_OUTPUT_FILE,
    ENRICHED_COMMITS_OUTPUT_FILE,
)


def github_headers():
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    return headers


def load_commits():
    commits = []

    with open(COMMIT_OUTPUT_FILE, "r", encoding="utf-8") as file:
        for line in file:
            commits.append(json.loads(line))

    return commits


def get_commit_details(repo_name, commit_sha):
    url = (
        f"{GITHUB_API}/repos/"
        f"{repo_name}/commits/{commit_sha}"
    )

    response = requests.get(
        url,
        headers=github_headers(),
        timeout=30,
    )

    response.raise_for_status()

    return response.json()


def enrich_commit(commit, details):
    stats = details.get("stats") or {}
    files = details.get("files") or []

    enriched = dict(commit)

    enriched["files_changed"] = len(files)
    enriched["additions"] = stats.get("additions", 0)
    enriched["deletions"] = stats.get("deletions", 0)
    enriched["lines_changed"] = stats.get("total", 0)

    return enriched


def save_enriched_commits(commits):
    output_path = Path(ENRICHED_COMMITS_OUTPUT_FILE)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as file:
        for commit in commits:
            file.write(json.dumps(commit) + "\n")


def main():
    try:
        commits = load_commits()

        enriched_commits = []

        for index, commit in enumerate(commits, start=1):
            print(
                f"[{index}/{len(commits)}] "
                f"{commit['repo_name']} "
                f"{commit['commit_sha'][:8]}"
            )

            details = get_commit_details(
                commit["repo_name"],
                commit["commit_sha"],
            )

            enriched = enrich_commit(
                commit,
                details,
            )

            enriched_commits.append(enriched)

        save_enriched_commits(enriched_commits)

        print()
        print(
            f"Enriched {len(enriched_commits)} commits."
        )
        print(
            f"Saved to: "
            f"{ENRICHED_COMMITS_OUTPUT_FILE}"
        )

    except requests.HTTPError as error:
        print(f"GitHub API error: {error}")
        sys.exit(1)

    except requests.RequestException as error:
        print(f"Network error: {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()