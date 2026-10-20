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

### 8. Web Dashboard (Interactive Repository Analyzer & Cluster Explorer)

Launch the locally runnable dashboard to analyze arbitrary GitHub repositories, explore single-repository temporal trends and author contributions, or inspect multi-repository MapReduce feature aggregations and Spark MLlib clustering:

```bash
# Start server on default port 8050
python3 dashboard/server.py

# Or specify a custom port / host
python3 dashboard/server.py --host 127.0.0.1 --port 8050

# Verify data files without launching the web server
python3 dashboard/server.py --check-only
```

Once running, navigate in any modern browser to:
👉 **`http://127.0.0.1:8050/`**

**Primary User Workflow — Analyze Any GitHub Repository:**
1. **Repository Input:** Enter any public GitHub repository URL (e.g. `https://github.com/psf/requests`) or identifier (`psf/requests`). Full URLs, SSH paths (`git@github.com:...`), and plain slugs are supported.
2. **History Coverage Modes:**
   - **Recent Commits Limit:** Configurable recent commit fetch threshold (default 10 commits) to preserve rate limits.
   - **All Available History:** Paginates through all accessible commits until repository history is exhausted or rate limits are reached. Truncated runs are explicitly flagged (`status: "truncated"`); truncated history is never mislabeled as complete.
3. **Live Progress Tracking:** Asynchronous background execution displays live processing stages: validation, commit pagination, authentic diff enrichment, MapReduce aggregation, and commit-level K-Means clustering.
4. **Single-Repository Analytics & Visualizations:**
   - **Overview & Coverage Status:** Total commits analyzed, date range (earliest to latest commit dates, duration in days), code churn (+additions, -deletions, net lines changed), files touched, and merge ratios.
   - **Temporal Activity & Code Churn:** Dynamic SVG time-series chart with Daily, Weekly, and Monthly aggregations showing commit frequency and line additions/deletions churn.
   - **Author Contributions & Attribution Notice:** Tabular breakdown of author commit share, additions, deletions, merges, and active timeframes. Prominently displays the git identity attribution notice (aliases appear as distinct authors).
   - **Commit-Level K-Means Clustering:** Standardizes 5 commit-level features (`[additions, deletions, lines_changed, files_changed, parent_count]`), computes the standard squared Euclidean silhouette score in $[-1.0, 1.0]$, and groups commits into descriptive behavioral profiles. Gracefully skips clustering with an explicit mathematical notice when observations are insufficient ($< 3$ commits or zero feature variance).
   - **Frequently Changed Files:** Identifies top touched files across analyzed commits.
   - **Commit Records Explorer:** Client-side filterable table with real-time text search, author filter, cluster filter, merge filter, and sorting. Missing GitHub statistics are preserved as `N/A` and never silently zeroed.
   - **Data Exports:** One-click downloads for commit records (`commits_<run_id>.csv`) and complete run payloads (`analysis_<run_id>.json`).
5. **Multi-Repository View Preservation:**
   - Switch between dynamic live runs and baseline demonstration data using the run selector dropdown.
   - Multi-repository runs automatically activate the multi-repository view: Spark MLlib K-Means cluster cards, normalized feature comparison chart, and repository profile tables.
   - Previous runs and baseline datasets remain intact and isolated in `data/runs/` and `data/sample/`.

### 9. Tests

```bash
python3 -m pytest tests/ -v
```

### 10. Dynamic Repository Ingestion (Single Repository)

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
- **Unauthenticated mode:** Operates without credentials for public repositories (GitHub rate limit: 60 requests/hour). When rate limits fall below 2 requests, per-commit enrichment pauses safely to protect the remaining quota.
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

### 11. Multi-Repository Analysis Pipeline (End-to-End Orchestration)

To ingest multiple repositories, aggregate features via MapReduce, and cluster with Spark MLlib in a single isolated run:

```bash
# End-to-end multi-repository run:
python3 scripts/run_pipeline.py psf/requests pallets/flask pallets/click encode/httpx \
  --output-dir data/runs/my_analysis \
  --max-commits 10 \
  --clusters 2 \
  --seed 42
```

**Pipeline Safeguards & Guarantees:**
- **Directory Isolation:** Requires a fresh output directory inside `data/runs/` (or auto-generates one). Never touches `data/sample/` or `data/output/`.
- **Clustering Threshold:** Single-repository runs execute ingestion and MapReduce feature extraction, but automatically skip Spark K-Means with an explanatory diagnostic manifest (clustering $N=1$ is mathematically degenerate). Multi-repository runs require $N \ge 2$ and automatically cap $k \le N$.
- **Quota Preservation:** Reuses pre-staged authentic repository data if already present in the target directory to avoid re-requesting existing commits.

### 12. Model Features vs. Descriptive Metadata

The pipeline strictly separates features used in unsupervised clustering from descriptive repository metadata:

| Field Name | Category | Used in Spark MLlib K-Means? | Notes |
|------------|----------|------------------------------|-------|
| `commit_count` | **Model Feature** | Yes (Standardized) | Sampled commits in the analysis window |
| `avg_additions` | **Model Feature** | Yes (Standardized) | Mean lines added per commit |
| `avg_deletions` | **Model Feature** | Yes (Standardized) | Mean lines removed per commit |
| `avg_lines_changed` | **Model Feature** | Yes (Standardized) | Mean total lines changed (`additions + deletions`) |
| `avg_files_changed` | **Model Feature** | Yes (Standardized) | Mean files touched per commit |
| `merge_ratio` | **Model Feature** | Yes (Standardized) | Fraction of merge commits (`parent_count > 1`) |
| `language` | *Descriptive Metadata* | No | Informational context only |
| `stars` / `forks` | *Descriptive Metadata* | No | Popularity metrics, not clustering inputs |
| `distinct_authors` | *Descriptive Metadata* | No | Contributor count in window |

**Provenance Distinction:**
- `data/sample/`: Baseline 20-repo demonstration dataset (985 synthetic, 15 authentic). Strictly protected from overwrite. Mock records have `synthetic: true`.
- `data/runs/`: Isolated dynamic runs containing 100% authentic, verified GitHub API data (`synthetic: false`).

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

## Clustering Methodology & Metric Interpretation

1. **Distance Metric:** Spark MLlib's `ClusteringEvaluator` is configured with `distanceMeasure="squaredEuclidean"`. The silhouette score is bounded in $[-1.0, 1.0]$. Positive values indicate that repositories are closer to their assigned cluster center than to neighboring cluster centers.
2. **Label Nature:** Cluster identifiers (`0`, `1`, `2`, ...) are arbitrary categorical group labels determined by unsupervised K-Means in standardized feature space.
3. **No Quality or Productivity Ranking:** Cluster assignments describe geometric proximity across commit size and merge frequency features. They must **never** be interpreted as rankings of code quality, developer productivity, or project health.
4. **Sampling Horizon Caveat:** Dynamic analyses operate over a recent-history sampling window (e.g. 10–50 recent commits). Commit statistics represent recent repository churn rather than longitudinal project lifecycle behavior.

---

## Phase 8 Bounded Experimental Validation

An authentic, multi-repository experimental cohort was analyzed using the end-to-end pipeline:

### Cohort Composition (8 Repositories, 80 Commits)
- `psf/requests` (HTTP client library)
- `pallets/flask` (WSGI web framework)
- `pallets/click` (CLI composition toolkit)
- `encode/httpx` (Async/sync HTTP client)
- `pydantic/pydantic` (Data validation and parsing)
- `tiangolo/fastapi` (Async web framework)
- `urllib3/urllib3` (Low-level HTTP library)
- `tornadoweb/tornado` (Async networking framework)

### Execution Command
```bash
python3 scripts/run_pipeline.py \
  psf/requests pallets/flask pallets/click encode/httpx \
  pydantic/pydantic tiangolo/fastapi urllib3/urllib3 tornadoweb/tornado \
  --output-dir data/runs/analysis-cohort8-20261020T073500Z-8repos \
  --max-commits 10 \
  --clusters 3 \
  --seed 42
```

### Measured Outcomes
- **Record Provenance:** 8 repositories, 80 commits, 80 authentic enriched records, **0 synthetic records**.
- **K-Means Configuration:** $k=3$, Seed = 42, Features = 6 standardized metrics.
- **Silhouette Score:** **0.574** (distance measure: `squaredEuclidean`).

### Clustering Stability Assessment (Adjusted Rand Index)
To test algorithmic stability against random centroid initialization, the clustering was re-run on the identical standardized feature vectors across four distinct random seeds: `[42, 100, 2026, 777]`:

| Repository | Seed 42 | Seed 100 | Seed 2026 | Seed 777 | Profile Summary |
|------------|---------|----------|-----------|----------|-----------------|
| `encode/httpx` | Cluster 0 | Cluster 0 | Cluster 0 | Cluster 0 | Low/Moderate churn (90 lines changed, merge ratio 0.0) |
| `fastapi/fastapi` | Cluster 0 | Cluster 0 | Cluster 0 | Cluster 0 | Moderate churn (920 lines changed, merge ratio 0.0) |
| `pallets/flask` | Cluster 0 | Cluster 0 | Cluster 0 | Cluster 0 | Moderate churn (1,120 lines changed, merge ratio 0.2) |
| `psf/requests` | Cluster 0 | Cluster 0 | Cluster 0 | Cluster 0 | Low churn (40 lines changed, merge ratio 0.0) |
| `pydantic/pydantic` | Cluster 0 | Cluster 0 | Cluster 0 | Cluster 0 | Moderate churn (1,174 lines changed, merge ratio 0.0) |
| `urllib3/urllib3` | Cluster 0 | Cluster 0 | Cluster 0 | Cluster 0 | Moderate churn (723 lines changed, merge ratio 0.0) |
| `tornadoweb/tornado` | Cluster 1 | Cluster 1 | Cluster 1 | Cluster 1 | High churn outlier (3,293 lines changed, merge ratio 0.3) |
| `pallets/click` | Cluster 2 | Cluster 2 | Cluster 2 | Cluster 2 | High churn & high merge outlier (4,592 lines, merge ratio 0.5) |

- **Silhouette (all seeds):** `0.5740`
- **Pairwise Adjusted Rand Index (ARI):** `1.0000` for all seed pairs.
- **Mean Pairwise ARI:** `1.0000`
- **Methodological Takeaway:** The clustering assignment is completely stable across random initializations for this cohort. This stability is driven by distinct behavioral separation between the 6 low/moderate-churn repositories and the 2 high-churn/high-merge repositories in the recent 10-commit window.

---

## Verification Results

| Check | Result |
|-------|--------|
| `repositories.jsonl` | ✅ 20 records, 20 unique repo_ids, valid schema |
| `commits.jsonl` | ✅ 1000 records, 1000 unique SHAs, 0 duplicates |
| `enriched_commits.jsonl` (mock) | ✅ 1000/1000 records (100%), marked `synthetic=True` |
| `enriched_commits.jsonl` (live 15) | ✅ 15 real GitHub records preserved |
| Dataset validation script | ✅ All core checks pass (PASS WITH WARNINGS: mock data noted) |
| Automated Tests (178 tests) | ✅ 178 passed, 0 failures (Ingestion: 69, MapReduce: 23, Spark: 16, Dashboard: 19, Dynamic Ingestion: 33, Commit Clustering: 5, Pipeline: 13) |
| MapReduce local simulation | ✅ 20 repos aggregated correctly into `repo_features_local.jsonl` |
| MapReduce distributed (YARN/HDFS) | ✅ Hadoop Streaming job verified in Docker cluster (`repo_features_mr/`) |
| Spark MLlib clustering (Sample) | ✅ 20 repos clustered (k=4, silhouette score ~0.404) |
| Spark MLlib clustering (Authentic Cohort) | ✅ 8 repos clustered (k=3, silhouette 0.574, mean pairwise ARI 1.0000) |
| Web Dashboard (`dashboard/`) | ✅ HTTP server, live repository analysis API (`/api/analyze`), export API (`/api/export`), path traversal protection, single-repo views, and multi-repo views verified |

---

## Known Limitations

1. **Dataset scope**: Sample datasets and dynamic runs contain 10–50 *recent* commits per repository, not complete histories. Cluster assignments reflect patterns within this narrow window, not overall repository characteristics.

2. **Synthetic enrichment in baseline sample**: The baseline sample dataset in `data/sample/` contains 985 mock-generated commit records (labelled `synthetic=True`). In contrast, runs in `data/runs/` contain 100% authentic GitHub diff data.

3. **Author identity**: Commits are attributed by name string only. The same developer using different names/emails across repositories or machines will appear as distinct authors.

4. **Clustering distance metric**: Spark's `ClusteringEvaluator` uses squared Euclidean distance. Silhouette scores must not be compared directly against evaluations using standard Euclidean or cosine distance.

5. **No target variable or ranking**: Clustering is unsupervised; clusters are groupings by feature proximity, not a validated quality signal. Cluster IDs are categorical, not ordinal rankings.

6. **Java compatibility**: PySpark 4.x requires Java 17+. If your system only has Java 11, install `pyspark<4.0`.

7. **Hive not verified**: Hive is not installed in the current local environment. The HQL files in `hive/` are provided for reference. They require a running HiveServer2 (available inside a suitable Docker image) and have not been executed or tested in this project.

8. **GitHub API rate limits**: Unauthenticated requests are limited to 60/hour; authenticated requests allow 5,000/hour. Ingestion automatically pauses per-commit enrichment when headroom drops below 2 requests.

9. **Browser-based JavaScript visual execution unverified**: While the dashboard backend server, routing, and data payloads are rigorously verified via unit/integration tests and HTTP requests, automated headless browser execution (Playwright) could not run due to an upstream driver CDN packaging issue. The dashboard is verified via HTTP API tests and can be visually inspected in any standard web browser.

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
│   ├── output/
│   │   ├── repo_features_local.jsonl # MapReduce local output
│   │   ├── repo_features_mr/         # Hadoop Streaming HDFS output
│   │   └── repo_clusters.json        # Spark MLlib clustering output
│   └── runs/                         # Isolated dynamic analysis runs
│       ├── analysis-cohort8-20261020T073500Z-8repos/
│       ├── analysis-20261020T064812Z-04c4400f/
│       └── ...
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
│   └── cluster_repos.py          # PySpark K-Means clustering & evaluation
├── hive/
│   ├── create_tables.hql
│   └── query_features.hql
├── scripts/
│   ├── validate_data.py          # Dataset schema and integrity validator
│   ├── run_pipeline.py           # Multi-repository pipeline orchestrator
│   ├── upload_to_hdfs.sh
│   └── run_mapreduce.sh
├── tests/
│   ├── test_ingestion.py         # 69 tests
│   ├── test_mapreduce.py         # 23 tests
│   ├── test_spark.py             # 16 tests
│   ├── test_dashboard.py         # 13 tests
│   ├── test_analyze_repository.py # 33 tests
│   └── test_pipeline.py          # 13 tests
├── docker-compose.yml
└── requirements.txt
```
