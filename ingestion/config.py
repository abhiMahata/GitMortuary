import os
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_API = "https://api.github.com"
OUTPUT_FILE = "data/sample/repositories.jsonl"
COMMIT_OUTPUT_FILE = "data/sample/commits.jsonl"
MAX_REPOSITORIES = 20
MAX_COMMITS_PER_REPO = 50