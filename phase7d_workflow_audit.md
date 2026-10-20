# Phase 7D — End-to-End User Workflow and Dashboard Verification Report

**Date:** 2026-10-20  
**Status:** Complete  
**Environment:** Linux (Ubuntu 24.04), Python 3.12, PySpark 3.5.x  
**Regression Test Suite:** 167 passing automated tests  
**Data Validation:** `PASS WITH WARNINGS` (0 errors, 1 expected sample warning)  
**Verified Run ID:** `analysis-20261020T064812Z-04c4400f`

---

## Executive Summary

Phase 7D audited and verified Git Pattern Analysis as an end-to-end, locally runnable application. The objective was to guarantee that users can dynamically analyze arbitrary public GitHub repositories, inspect resulting runs through the interactive web dashboard, and seamlessly toggle back to the original 20-repository baseline sample dataset without loss of fidelity or cross-run data contamination.

We verified the complete pipeline workflow, identified and fixed five critical usability and provenance defects in the dashboard server and frontend, added comprehensive regression tests (expanding the test suite to 167 passing tests), and verified the dashboard API and rendered state against both the baseline sample dataset and our authentic four-repository live run (`analysis-20261020T064812Z-04c4400f`).

---

## 1. Full Workflow Audit

We traced the complete user journey across all pipeline stages:

```
┌─────────────────────────────────┐
│ User Input / CLI Invocation     │  python3 scripts/run_pipeline.py psf/requests pallets/flask ...
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│ Identifier Parsing & Validation │  Accepts owner/repo slugs or full URLs; rejects malicious strings
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│ Ingestion & Authentication      │  REST API with exponential backoff; handles 401/403/404/409 safely
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│ Isolated Run Directory Creation │  data/runs/analysis-<timestamp>-<id>/ (preserves data/sample/ intact)
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│ MapReduce Feature Aggregation   │  Produces repo_features.jsonl (commit stats & merge ratios)
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│ PySpark MLlib Clustering        │  StandardScaler normalization + K-Means + Silhouette evaluation
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│ Manifest & Artifact Generation  │  pipeline_manifest.json with full provenance & execution metrics
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│ Dashboard Run Discovery & UI    │  /api/runs and /api/data?run=<id> with live switching
└─────────────────────────────────┘
```

### Key Workflow Findings & Gaps Identified:
1. **Omission of Silhouette Metric from Dashboard**: Although Spark MLlib computed a silhouette score, the dashboard UI lacked any element to display it or communicate its validity status.
2. **Conflation of Model Inputs and Descriptive Metadata**: The dashboard presented metadata columns (`Language`, `Stars`, `Authors`) alongside model inputs (`Commits`, `Merge Ratio`, `Avg Additions`, `Avg Deletions`, `Avg Lines`, `Avg Files`) without indicating which features entered the K-Means distance calculations.
3. **Unflagged Truncated Histories**: When a repository's history was truncated at the requested commit limit (e.g., 10 commits), the UI displayed the commit count without indicating that it represented a partial sample rather than the repository's lifetime history.
4. **Hardcoded Metadata Labels in Static Template**: The HTML template had static text referring to `(K=4, Seed=42)` and `15 Real / 985 Mock`, which remained unchanged even when viewing runs with different cluster counts ($K=2$) or authentic data.
5. **Handling of Abnormal Runs**: Malformed manifests, failed runs, or empty run directories lacked distinct UI indicators, leading to confusing blank views instead of clear diagnostic status messages.

---

## 2. Defects Found and Fixes Implemented

### Defect 1: Missing Silhouette Scorecard and Metric Identification
- **Issue**: Downstream dashboard users could not inspect the mathematical quality or cohesion of cluster assignments.
- **Fix**: Added a dedicated glassmorphic **Clustering Evaluation Scorecard** to Section B of the dashboard:
  - Displays the silhouette coefficient (e.g., `0.5690 Valid` for live run; `0.4039 Valid` for baseline).
  - Explicitly states the distance measure: `Measure: Squared Euclidean Distance • Normalized Range: [-1.0, +1.0]`.
  - When the silhouette score is undefined or clustering was skipped (e.g., $N \le K$ or single-repo runs), displays `N/A (Unavailable)` or `Skipped` with an explanatory tooltip—**never displaying an unavailable score as a numerical value**.

### Defect 2: Conflation of Clustering Features with Descriptive Statistics
- **Issue**: Users could mistake descriptive statistics (`distinct_authors`, `stars`, `language`) for model inputs.
- **Fix**:
  - Added subtle color-coded column tags to table headers:
    - `<span class="col-tag tag-model">Model</span>` for the 6 standardized K-Means features (`Commits`, `Merge Ratio`, `Avg Additions`, `Avg Deletions`, `Avg Lines`, `Avg Files`).
    - `<span class="col-tag tag-meta">Meta</span>` for repository metadata (`Language`, `Stars`).
    - `<span class="col-tag tag-desc">Desc</span>` for exploratory metrics (`Authors`).
    - `<span class="col-tag tag-group">Group</span>` for cluster assignments.
  - Updated the cluster comparison chart in `dashboard/app.js` to visualize all 6 model features across clusters, with an explicit subtitle clarifying that only standardized model features participate in distance calculations.

### Defect 3: Unsurfaced Commit History Truncation
- **Issue**: A 10-commit sample could be misinterpreted as complete repository activity.
- **Fix**:
  - Added per-repository truncation tracking in `dashboard/server.py` (`profile["is_truncated"] = True`).
  - Added a visual amber badge `<span class="badge-truncated">Partial</span>` next to commit counts for truncated repositories in the data table.
  - Updated the Overview KPI card and Provenance Banner to display:
    `Partial Sample Notice: Commit histories were truncated at the configured limit (10 commits). Metrics reflect recent commit dynamics rather than complete repository lifetimes.`

### Defect 4: Robust Handling of Empty, Failed, Incomplete, and Malformed Runs
- **Issue**: If an analysis run failed, was interrupted, or contained corrupt JSON, the dashboard returned generic or blank states.
- **Fix**:
  - In `dashboard/server.py`:
    - `list_available_runs()` classifies run status as `complete`, `failed`, `empty`, `malformed`, or `incomplete`.
    - Labels in the run dropdown explicitly flag non-complete states (e.g., `[Analysis: FAILED] ...`, `[Analysis: EMPTY] ...`).
    - `get_dashboard_data()` captures errors and warnings from failed or malformed runs safely.
  - In `dashboard/app.js` and `dashboard/index.html`:
    - Added `#run-status-alert` container that dynamically renders prominent diagnostic banners for abnormal runs.

### Defect 5: Strict Path Traversal Safety and Information Leakage Prevention
- **Issue**: Nonexistent run requests returned error messages referencing internal filesystem structure (`under data/runs/`).
- **Fix**:
  - Hardened `run_id` validation: explicitly rejected dot-directories (`.`, `..`, `../`).
  - Sanitized error messages to avoid leaking absolute or relative directory paths.

### Defect 6: Dynamic Section Subtitles
- **Issue**: Subtitle text `(K=4, Seed=42)` was hardcoded in `index.html`.
- **Fix**: Updated `app.js` to dynamically render `(K=${k}, Seed=${seed})` from active run parameters, or `Clustering skipped: Requires ≥ 2 repositories` when clustering was bypassed.

---

## 3. Regression Tests & Verification Results

### 3.1 Automated Pytest Suite
Ran `python3 -m pytest tests/ -v`:
- **Total Tests:** 167
- **Passed:** 167 (100%)
- **Failed:** 0
- **Duration:** 60.87 seconds

#### New Regression Tests Added in `tests/test_pipeline.py`:
1. `test_get_dashboard_data_live_run_agrees_with_artifacts`:
   - Validates that selecting `analysis-20261020T064812Z-04c4400f` loads all 4 repositories, 40 commits, 0 synthetic records, `is_authentic_live=True`, `k=2`, `silhouette=0.569`, `distance_measure="squaredEuclidean"`, and exact cluster assignments (`pallets/click` $\to 0$, others $\to 1$).
2. `test_get_dashboard_data_empty_run_produces_clear_status`:
   - Validates that an empty directory produces `status="empty"` with clear diagnostic warnings.
3. `test_get_dashboard_data_malformed_manifest_handled_gracefully`:
   - Validates that corrupt JSON manifest produces `status="malformed"` without crashing the server.
4. `test_unavailable_silhouette_score_not_returned_as_number`:
   - Validates that single-repository runs report `silhouette=None` (not `0.0` or any manufactured number).
5. `test_get_dashboard_data_invalid_path_traversal_rejected`:
   - Validates rejection of `..`, `../../etc/passwd`, and slashes.

### 3.2 Dataset Validation
Ran `python3 scripts/validate_data.py`:
- **Result:** `PASS WITH WARNINGS`
- **Warnings (1):** Expected baseline notice: 985/1000 enriched records in `data/sample/` are synthetic.
- **Errors:** 0 errors.

---

## 4. End-to-End Verification Against Live Run

### 4.1 Verified Run Artifacts
- **Run ID:** `analysis-20261020T064812Z-04c4400f`
- **Run Directory:** `data/runs/analysis-20261020T064812Z-04c4400f/`
- **Manifest:** `pipeline_manifest.json`

### 4.2 Cross-Verification: Output Artifacts vs Dashboard API

| Metric / Dimension | Disk Output (`data/runs/...`) | Dashboard API (`/api/data?run=...`) | Status |
| :--- | :---: | :---: | :---: |
| **Total Repositories** | 4 | 4 | **Exact Match** |
| **Total Commits** | 40 | 40 | **Exact Match** |
| **Authentic Records** | 40 (100%) | 40 (100%) | **Exact Match** |
| **Synthetic Records** | 0 (0%) | 0 (0%) | **Exact Match** |
| **Cluster Count ($K$)** | 2 | 2 | **Exact Match** |
| **Silhouette Score** | 0.569 | 0.569 | **Exact Match** |
| **Distance Measure** | squaredEuclidean | squaredEuclidean | **Exact Match** |
| **pallets/click Assignment** | Cluster 0 | Cluster 0 | **Exact Match** |
| **pallets/flask Assignment** | Cluster 1 | Cluster 1 | **Exact Match** |
| **psf/requests Assignment** | Cluster 1 | Cluster 1 | **Exact Match** |
| **encode/httpx Assignment** | Cluster 1 | Cluster 1 | **Exact Match** |
| **History Truncation** | 4 truncated | 4 flagged `is_truncated=True` | **Exact Match** |

### 4.3 Baseline Sample Dataset Persistence
When toggling back to `sample`:
- Repositories: 20
- Commits: 1,000
- Clusters: 4
- Silhouette Score: 0.4039
- Provenance Warning: 98.5% synthetic enrichment warning displayed.
- All sample baseline outputs remain intact and unmodified.

---

## 5. Instructions for Running the Application

### 5.1 Launching the Interactive Web Dashboard
```bash
python3 dashboard/server.py --port 8050
```
Open **`http://127.0.0.1:8050/`** in any standard web browser (Chrome, Firefox, Safari, Edge).

### 5.2 Analyzing Arbitrary GitHub Repositories
To analyze new public repositories and generate a new isolated analysis run:

```bash
# Example: Analyze 3 repositories with a conservative 15-commit limit and 2 clusters
python3 scripts/run_pipeline.py psf/requests pallets/flask encode/httpx --max-commits 15 --clusters 2
```

Optional arguments:
- `-t, --token <TOKEN>`: GitHub personal access token (increases rate limit from 60 to 5,000 req/hr).
- `-k, --clusters <INT>`: Number of K-Means clusters (must be $\ge 2$ and $\le$ repository count).
- `--seed <INT>`: Random seed for reproducible clustering (default: 42).
- `--skip-spark`: Skip clustering and only produce feature profiles.

Once completed, the new run immediately appears in the dashboard's **Run selector** dropdown.

---

## 6. Analytical Limitations

1. **Short Commit Sampling Horizon**: Live analysis with 10–50 commits per repository reflects recent commit dynamics rather than complete project histories.
2. **Rate Limits on Unauthenticated Requests**: The GitHub API limits unauthenticated IP addresses to 60 requests/hour. Analyzing large cohorts requires passing a `GITHUB_TOKEN`.
3. **Categorical Nature of Clusters**: Cluster numbers (`0`, `1`, etc.) are arbitrary group identifiers generated by geometric distance in standardized feature space; they do not imply ranking, code quality, or team productivity.
4. **Driver Dependency for Headless Subagent**: Automated Playwright browser tests depend on external browser drivers; the web application itself is pure HTML/CSS/Vanilla JS and runs in any browser without external dependencies.
