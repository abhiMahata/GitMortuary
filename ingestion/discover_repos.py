import json
import sys
from pathlib import Path
import requests
from config import(
    GITHUB_API,
    GITHUB_TOKEN,
    MAX_REPOSITORIES,
    OUTPUT_FILE,
)

def github_headers():
    headers = {
        "Accept": "applications/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    return headers

def discover_repositories():
    params = {
        "q":  "stars:>1000",
        "sort": "stars",
        "order": "desc",
        "per_page": MAX_REPOSITORIES,
    }

    response = requests.get(
        f"{GITHUB_API}/search/repositories",
        headers = github_headers(),
        params = params,
        timeout = 30,
    )

    response.raise_for_status()

    return response.json()["items"]

def normalize_repository(repo):
    return{
        "repo_id": repo["id"],
        "repo_name": repo["full_name"],
        "language": repo["language"],
        "stars": repo["stargazers_count"],
        "forks": repo["forks_count"],
        "created_at": repo["created_at"],
        "updated_at": repo["updated_at"],
        "archived": repo["archived"],
        "default_branch": repo["default_branch"],
    }

def save_repositories(repositories):
    output_path = Path(OUTPUT_FILE)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as file:
        for repository in repositories:
            file.write(json.dumps(repository)+"\n")

def main():
    try:
        repositories = discover_repositories()
        normalized = [
            normalize_repository(repo)
            for repo in repositories
        ]
    
        save_repositories(normalized)
    
        print(f"Discovered {len(normalized)} repositories.")
        print(f"Saved to: {OUTPUT_FILE}")
    
    except requests.HTTPError as error:
        print(f"GitHub API error: {error}")
        sys.exit(1)
    except requests.RequestException as error:
        print(f"Network Error: {error}")
        sys.exit(1)

if __name__ == "__main__":
    main()