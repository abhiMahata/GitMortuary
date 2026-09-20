import os
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_API = "https://api.github.com"
OUTPUT_FILE = "data/sample/repositories.jsonl"
MAX_REPOSITORIES = 20