# Git Pattern Analysis

Analyzes GitHub commit history to extract repository-level development patterns using a data engineering pipeline: **GitHub API → HDFS → Hadoop MapReduce → Hive → Apache Spark MLlib → Web Dashboard**.

---

## Architecture

```
GitHub API
    │
    ├─ ingestion/discover_repos.py      → data/sample/repositories.jsonl
    ├─ ingestion/collect_commits.py     → data/sample/commits.jsonl
    └─ ingestion/enriched_commits.py   → data/sample/enriched_commits.jsonl
                                                    │
                                          scripts/upload_to_hdfs.sh
                                                    │
                                               HDFS (Docker)
                                                    │
                                     ┌──────────────┴──────────────┐
                               mapreduce/              spark/
                             mapper.py               cluster_repos.py
                             reducer.py                  │
                                │                   data/output/
                          repo_features               repo_clusters.json
                                │                        │
                           hive/*.hql (ref)              │
                                │                        │
                                └──────────────┬─────────┘
                                               │
                                        dashboard/server.py
                                               │
                                    Interactive Web Dashboard
                                    (http://127.0.0.1:8050/)
```

---

## Prerequisites

- Python 3.10+
- Java 11 (for PySpark 3.x) — Java 17+ required for PySpark 4.x
- Docker + Docker Compose (for HDFS/YARN)
- `GITHUB_TOKEN` env var with `repo` scope (for live enrichment only)

```bash
pip3 install -r requirements.txt --break-system-packages
```

---

## Quick Start

### 1. Repository Discovery

```bash
cd git-commit-pattern
export GITHUB_TOKEN=ghp_your_token_here
python3 ingestion/discover_repos.py
# → data/sample/repositories.jsonl  (20 repos)
```

### 2. Commit Collection

```bash
python3 ingestion/collect_commits.py
# → data/sample/commits.jsonl  (up to 1000 commits)
```

### 3. Commit Enrichment

**Live mode** (requires valid `GITHUB_TOKEN`):
```bash
python3 ingestion/enriched_commits.py
# → data/sample/enriched_commits.jsonl
# Resumes from existing records. Errors logged to data/sample/enrichment_errors.jsonl
```

**Mock mode** (no token needed — uses synthetic stats):
```bash
python3 ingestion/enriched_commits.py --mock
# → data/sample/enriched_commits.jsonl  (100% coverage, marked synthetic=True)
```

### 4. Dataset Validation

```bash
python3 scripts/validate_data.py
python3 scripts/validate_data.py --verbose
```

### 5. MapReduce Feature Aggregation

**Local simulation (no Docker needed):**
```bash
cat data/sample/enriched_commits.jsonl \
  | python3 mapreduce/mapper.py \
  | sort \
  | python3 mapreduce/reducer.py \
  > data/output/repo_features_local.jsonl
```

**With HDFS (Docker cluster required):**
```bash
docker compose up -d
bash scripts/upload_to_hdfs.sh
bash scripts/run_mapreduce.sh
```

### 6. Spark Clustering

**Local (reads JSONL directly):**
```bash
python3 spark/cluster_repos.py
# → data/output/repo_clusters.json
```

**With HDFS:**
```bash
python3 spark/cluster_repos.py --hdfs hdfs://namenode:9000/user/hadoop/git-pattern
```

Options:
```
--k 4        Number of clusters (default: 4)
--seed 42    Random seed for reproducibility
```

### 7. Hive Tables (Reference / Unverified)

> [!NOTE]
> Hive is not installed in the local environment or included in the default Docker Compose cluster. The HQL scripts in `hive/` are reference schema definitions and queries; they have not been executed or verified in this environment.

```bash
# Reference execution (requires an external Hive / HiveServer2 instance):
# hive -f hive/create_tables.hql
# hive -f hive/query_features.hql
```

### 8. Web Dashboard (Interactive Visualization)

Launch the locally runnable dashboard to visualize repository features, data provenance, and Spark MLlib clustering:

```bash
# Start server on default port 8050
python3 dashboard/server.py

# Or specify a custom port / host
python3 dashboard/server.py --host 127.0.0.1 --port 8080

# Verify data files without launching the web server
python3 dashboard/server.py --check-only
```

Once running, navigate in any modern browser to:
👉 **`http://127.0.0.1:8050/`**

**Dashboard Features:**
- **Pipeline Overview:** Displays live repository counts, total commits, enrichment breakdown (authentic vs synthetic), and cluster assignments.
- **Prominent Provenance Banner:** Transparently flags that 985 of 1,000 commits are synthetic mock data, qualifying clustering results.
- **Spark MLlib Cluster Explorer:** Neutral geometric cluster breakdown (Clusters 0–3) with interactive metric comparison bar chart (Additions, Deletions, Merge Ratio, Unique Authors).
- **Interactive Repository Profiles Table:** Real-time search by repository name, filtering by Cluster and Language, multi-column sorting, and strict missing-data handling (displaying `N/A` rather than misleading zeroes).
- **Offline & Standalone:** Powered by Python standard library `http.server`, vanilla HTML5, CSS3, and JavaScript — zero external web dependencies or GitHub tokens required.

### 9. Tests

```bash
python3 -m pytest tests/ -v
```

### 10. Dynamic Repository Ingestion (Authentic Data)

To ingest and analyze any public GitHub repository with 100% authentic data (saving to an isolated run directory without touching sample data):

```bash
# Analyze a repository using owner/name slug:
python3 -m ingestion.analyze_repository psf/requests

# Or provide a full GitHub URL:
python3 -m ingestion.analyze_repository https://github.com/psf/requests

# Custom commit limit and isolated output directory:
python3 -m ingestion.analyze_repository --max-commits 25 --output-dir data/runs/requests_run psf/requests
```

**Environment & Rate-Limit Configuration:**
- **Unauthenticated mode:** Operates without credentials for public repositories (GitHub rate limit: 60 requests/hour).
- **Authenticated mode:** Set `GITHUB_TOKEN` to increase your rate limit to 5,000 requests/hour:
  ```bash
  export GITHUB_TOKEN=ghp_your_token_here
  python3 -m ingestion.analyze_repository psf/requests
  ```
  *(Tokens are read securely from the environment and are never printed, logged, or stored in artifacts.)*

**Run Directory Structure:**
Each run outputs to an isolated directory (e.g. `data/runs/<repo>-<timestamp>-<run-id>/`):
- `repositories.jsonl`: Repository metadata matching `REPO_REQUIRED` schema.
- `commits.jsonl`: Basic commit list matching `COMMIT_REQUIRED` schema.
- `enriched_commits.jsonl`: Authentic line additions, deletions, and file counts (`synthetic: false`).
- `manifest.json`: Run manifest recording commit limits, record counts, completeness status, and rate-limit headroom.

**Completeness Semantics:**
- `--max-commits` / `-n`: Sets the maximum commit fetch threshold.
- `status: "complete"` / `is_complete: true`: Entire repository history was retrieved.
- `status: "truncated"` / `is_complete: false`: Ingestion stopped at the configured commit limit.
- `status: "empty"`: Repository contains 0 commits.

**Provenance Distinction:**
- `data/sample/`: Baseline 20-repo demonstration dataset (985 synthetic, 15 authentic). Strictly protected from overwrite.
- `data/runs/`: Isolated dynamic runs containing 100% authentic, verified GitHub API data.

---

## Running Commands (HDFS cluster)

```bash
# Start Hadoop cluster
docker compose up -d

# Check status
docker compose ps
curl http://localhost:9870   # HDFS NameNode UI
curl http://localhost:8088   # YARN ResourceManager UI

# Stop cluster
docker compose down
```

---

## Data Schemas

### `data/sample/repositories.jsonl`
| Field | Type | Description |
|-------|------|-------------|
| `repo_id` | int | GitHub repository ID |
| `repo_name` | str | `owner/repo` |
| `language` | str\|null | Primary language |
| `stars` | int | Stargazer count |
| `forks` | int | Fork count |
| `created_at` | str | ISO 8601 creation timestamp |
| `updated_at` | str | ISO 8601 last-updated timestamp |
| `archived` | bool | Whether repo is archived |
| `default_branch` | str | Default branch name |

### `data/sample/commits.jsonl`
| Field | Type | Description |
|-------|------|-------------|
| `repo_id` | int | Repository ID |
| `repo_name` | str | `owner/repo` |
| `commit_sha` | str | Full 40-char SHA |
| `author_identity` | str\|null | Author name |
| `author_timestamp` | str\|null | ISO 8601 author date |
| `committer_identity` | str\|null | Committer name |
| `committer_timestamp` | str\|null | ISO 8601 committer date |
| `commit_message` | str\|null | Full commit message |
| `parent_count` | int | Number of parents (>1 = merge) |

### `data/sample/enriched_commits.jsonl`
All commit fields plus:
| Field | Type | Description |
|-------|------|-------------|
| `files_changed` | int\|null | Files touched (null = unavailable) |
| `additions` | int\|null | Lines added (null = unavailable) |
| `deletions` | int\|null | Lines removed (null = unavailable) |
| `lines_changed` | int\|null | `additions + deletions` |
| `stats_available` | bool | Whether API returned stats |
| `files_available` | bool | Whether API returned file list |
| `synthetic` | bool\|null | True only for mock-mode records |

### `data/output/repo_features_local.jsonl` (MapReduce output)
| Field | Type | Description |
|-------|------|-------------|
| `repo_name` | str | Repository name |
| `commit_count` | int | Commits in sample |
| `total_additions` | int | Sum of additions |
| `total_deletions` | int | Sum of deletions |
| `avg_additions` | float | Mean additions per commit |
| `avg_deletions` | float | Mean deletions per commit |
| `avg_lines_changed` | float | Mean lines changed per commit |
| `avg_files_changed` | float | Mean files changed per commit |
| `merge_ratio` | float | Fraction of merge commits |

### `data/output/repo_clusters.json` (Spark output)
All repo_features fields plus:
| Field | Type | Description |
|-------|------|-------------|
| `cluster` | int | K-Means cluster assignment (0-indexed) |
| `distinct_authors` | int | Unique author names in sample |

---

## Verification Results

| Check | Result |
|-------|--------|
| `repositories.jsonl` | ✅ 20 records, 20 unique repo_ids, valid schema |
| `commits.jsonl` | ✅ 1000 records, 1000 unique SHAs, 0 duplicates |
| `enriched_commits.jsonl` (mock) | ✅ 1000/1000 records (100%), marked `synthetic=True` |
| `enriched_commits.jsonl` (live 15) | ✅ 15 real GitHub records preserved |
| Dataset validation script | ✅ All core checks pass (PASS WITH WARNINGS: mock data noted) |
| Automated Tests (145 tests) | ✅ 145 passed, 0 failures (Ingestion: 69, MapReduce: 23, Spark: 7, Dashboard: 13, Dynamic Ingestion: 33) |
| MapReduce local simulation | ✅ 20 repos aggregated correctly into `repo_features_local.jsonl` |
| MapReduce distributed (YARN/HDFS) | ✅ Hadoop Streaming job verified in Docker cluster (`repo_features_mr/`) |
| Spark MLlib clustering | ✅ 20 repos clustered (k=4, silhouette score ~0.404) |
| Web Dashboard (`dashboard/`) | ✅ HTTP server, API endpoints, and data contracts verified (13 tests); browser E2E unverified |

---

## Known Limitations

1. **Dataset scope**: The sample contains at most 50 *recent* commits per repository, not complete histories. Cluster assignments reflect patterns within this narrow window, not overall repository characteristics.

2. **Synthetic enrichment**: Mock mode produces structurally correct but statistically fabricated data (deterministic from SHA, not from actual GitHub diff data). All mock records are labelled `synthetic=True`. Do not interpret mock-mode clusters as real commit statistics.

3. **Author identity**: Commits are attributed by name string only. The same developer using different names/emails across repositories or machines will appear as distinct authors.

4. **Sample selection**: The 20 repositories were selected by star count (GitHub search `stars>1000`). This heavily biases toward popular open-source projects and is not representative of typical repository behaviour.

5. **No target variable**: Clustering is unsupervised; clusters are groupings by feature proximity, not a validated quality signal. A silhouette score near 0 means clusters overlap significantly.

6. **Java compatibility**: PySpark 4.x requires Java 17+. If your system only has Java 11, install `pyspark<4.0`.

7. **Hive not verified**: Hive is not installed in the current local environment. The HQL files in `hive/` are provided for reference. They require a running HiveServer2 (available inside a suitable Docker image) and have not been executed or tested in this project.

8. **Live GitHub API enrichment not verified end-to-end**: The `ingestion/enriched_commits.py --mock` path is fully working (985 synthetic + 15 authentic records preserved). Live mode (`--mock` omitted) requires a valid `GITHUB_TOKEN` environment variable with `repo` scope. Live enrichment was not re-executed; the 15 authentic records were collected in a prior session and preserved by the resumability mechanism.

9. **Browser-based JavaScript execution unverified**: While the dashboard backend server, routing, and data payloads are rigorously verified via unit/integration tests (`tests/test_dashboard.py`) and CLI validation, automated end-to-end browser execution (e.g., via Playwright or headless Chrome) was not performed due to local headless browser dependencies. Interactive UI features (DOM rendering, SVG charting, sorting, filtering) are designed using vanilla JavaScript and require visual verification in a standard browser.

---

## Project Structure

```
git-commit-pattern/
├── dashboard/
│   ├── index.html                # Responsive semantic interface
│   ├── style.css                 # Dark glassmorphic design system
│   ├── app.js                    # Client-side filtering, sorting, and charts
│   └── server.py                 # Standalone HTTP server & data API (zero deps)
├── data/
│   ├── sample/
│   │   ├── repositories.jsonl
│   │   ├── commits.jsonl
│   │   ├── enriched_commits.jsonl
│   │   └── enrichment_errors.jsonl   # failure log
│   └── output/
│       ├── repo_features_local.jsonl # MapReduce local output
│       ├── repo_features_mr/         # Hadoop Streaming HDFS output
│       └── repo_clusters.json        # Spark MLlib clustering output
├── ingestion/
│   ├── config.py
│   ├── discover_repos.py
│   ├── collect_commits.py
│   ├── enriched_commits.py
│   └── analyze_repository.py     # Dynamic repository ingestion CLI
├── mapreduce/
│   ├── mapper.py
│   └── reducer.py
├── spark/
│   └── cluster_repos.py
├── hive/
│   ├── create_tables.hql
│   └── query_features.hql
├── scripts/
│   ├── validate_data.py
│   ├── upload_to_hdfs.sh
│   └── run_mapreduce.sh
├── tests/
│   ├── test_ingestion.py         # 69 tests
│   ├── test_mapreduce.py         # 23 tests
│   ├── test_spark.py             # 7 tests
│   ├── test_dashboard.py         # 13 tests
│   └── test_analyze_repository.py # 33 tests
├── docker-compose.yml
└── requirements.txt
```
