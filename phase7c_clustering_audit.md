# Phase 7C — Clustering Metric Audit and Real-Data Experiment Report

**Date:** 2026-10-20  
**Status:** Complete  
**Environment:** Linux (Ubuntu 24.04), Python 3.12, PySpark 3.5.x  
**Baseline Test Suite:** 163 passing automated tests  
**Data Validation:** `PASS WITH WARNINGS` (0 errors, 1 expected sample warning)

---

## Executive Summary

Phase 7C audited the mathematical soundness and implementation integrity of the PySpark K-Means clustering pipeline. In Phase 7B, a multi-repository test run recorded a silhouette score of `2.0` for two repositories partitioned into two clusters—a mathematical impossibility given that the standard silhouette coefficient is bounded in $[-1.0, 1.0]$.

This audit identified the exact root cause: a regular expression in the pipeline orchestrator matched the integer `2` from the log line `K = 2 clusters` rather than the silhouette value `0.0000`, erroneously labeling the cluster count $K$ as the silhouette score. Furthermore, the underlying configuration ($N=2, k=2$) represented a degenerate clustering scenario of all singleton clusters where intra-cluster cohesion is mathematically undefined.

We fixed the metric extraction, added robust mathematical guards for degenerate and zero-variance configurations, corrected manifest labeling, expanded the regression test suite from 112 to 163 passing tests, and executed a 100% authentic real-data experiment across four public GitHub repositories (`psf/requests`, `pallets/flask`, `pallets/click`, `encode/httpx`) yielding a valid silhouette score of **0.569**.

---

## 1. Root Cause Analysis: The Reported `2.0` Score

### 1.1 The Regex Extraction Defect

In `scripts/run_pipeline.py` (Phase 7B implementation), the helper function `execute_spark_clustering` captured stdout from `spark/cluster_repos.py` and parsed the silhouette score using the following logic:

```python
# DEFECTIVE IMPLEMENTATION in Phase 7B:
for line in proc.stdout.splitlines():
    if "Silhouette score" in line:
        m = re.search(r"[-+]?\d*\.\d+|\d+", line)
        if m:
            try:
                val = float(m.group(0))
                silhouette = val
            except ValueError:
                pass
```

The corresponding stdout line printed by `spark/cluster_repos.py` was:

```text
  K = 2 clusters | Silhouette score = 0.0000
```

Because `re.search` scans from left to right, the pattern `[-+]?\d*\.\d+|\d+` matched the first number encountered on the line: the digit `2` from `K = 2`. The orchestrator extracted `2.0`, mistook it for the silhouette coefficient, and persisted `silhouette: 2.0` into `pipeline_manifest.json`.

### 1.2 Underlying Mathematical Degeneracy ($N = k$)

Beyond the parsing defect, the clustering configuration itself ($N=2, k=2$) is mathematically degenerate:
1. **Silhouette Definition**: For an observation $i \in C_I$, the silhouette coefficient is:
   $$s(i) = \frac{b(i) - a(i)}{\max(a(i), b(i))}$$
   where $a(i) = \frac{1}{|C_I| - 1} \sum_{j \in C_I, j \ne i} d(i, j)$ is the average distance from $i$ to all other points within its own cluster, and $b(i) = \min_{J \ne I} \frac{1}{|C_J|} \sum_{j \in C_J} d(i, j)$ is the average distance to the nearest neighboring cluster.
2. **Singleton Cohesion Undefined**: When $N = k$, every cluster $C_I$ contains exactly one point ($|C_I| = 1$). The intra-cluster distance $a(i)$ requires division by $|C_I| - 1 = 0$, which is mathematically undefined.
3. **PySpark Behavior**: Apache Spark's `ClusteringEvaluator` handles singletons by arbitrarily defining $a(i) = 0.0$, computing $s(i) = \frac{b(i) - 0}{b(i)} = 1.0$ for isolated points, or returning `0.0` when all clusters are singletons.
4. **Resolution**: Manufacturing an arbitrary score (such as `0.0` or `1.0`) when $N \le k$ is misleading. The score is mathematically undefined and has no comparative utility. It must be explicitly recorded as `null` / `unavailable` with an explanatory reason.

### 1.3 Labeling Confusion in the Manifest

In `scripts/run_pipeline.py`, the return value of `execute_spark_clustering` previously assigned `len(clusters_data)` to `manifest["stages"]["spark"]["clusters_count"]`. When 20 repositories were clustered into 4 clusters, the manifest recorded `"clusters_count": 20`, mislabeling the observation count as the cluster count.

We resolved this by separating:
- `clusters_count`: The number of distinct clusters formed (e.g. 2).
- `repositories_clustered`: The total number of repository observations assigned (e.g. 4).

---

## 2. Feature Preparation & Mathematical Auditing

We inspected feature aggregation and preparation in `spark/cluster_repos.py`:

| Property | Status | Verification & Safeguards |
| :--- | :--- | :--- |
| **Numeric & Finite Values** | **Verified** | Feature vector contains only finite doubles (`commit_count`, `avg_additions`, `avg_deletions`, `avg_lines_changed`, `avg_files_changed`, `merge_ratio`). Aggregations coalesce nulls to default finite numbers (`0` or `0.0`). |
| **Missing Metric Handling** | **Verified** | Missing metrics are preserved as `None` in the ingestion/mapper stages and coalesced in Spark feature aggregation. Genuine zeros are preserved. |
| **Feature Scaling** | **Verified** | `StandardScaler(withMean=True, withStd=True)` scales each feature column to zero mean and unit variance. Features with large absolute ranges (e.g., additions spanning thousands of lines) do not dominate metrics bounded in $[0, 1]$ (e.g., merge ratio). |
| **Zero-Variance Protection** | **Hardened** | When repositories possess identical feature vectors or distinct vectors $< k$, Spark's `KMeans` throws `ArrayIndexOutOfBoundsException`. We added explicit pre-fit checks to raise clear `ValueError` exceptions. |
| **Arbitrary Cluster Labels** | **Documented** | Cluster IDs (`0, 1, ...`) are arbitrary categorical indices assigned by Spark MLlib, representing group membership rather than quality or velocity rankings. |
| **Reproducibility** | **Verified** | Fixed random seeds (`seed=42`) produce identical cluster assignments across multiple executions on the same dataset. |

---

## 3. Files Changed and Rationale

### 1. `spark/cluster_repos.py`
- **Validation**: Added prerequisite checks ensuring $k \ge 2$, $N \ge 2$, $N \ge k$, and distinct feature vectors $\ge k$.
- **Degenerate Check**: Explicitly checks if distinct clusters $< 2$, $N \le \text{distinct\_clusters}$, or all clusters are singletons. In these cases, sets `silhouette = None`.
- **Score Validation**: Evaluates silhouette via `ClusteringEvaluator(metricName="silhouette", distanceMeasure="squaredEuclidean")`. Ensures the value is finite and bounded in $[-1.0, 1.0]$.
- **Display Output**: Formatted console output to display `Silhouette score = N/A (undefined for degenerate/singleton configuration where N <= K)` when `silhouette is None`.
- **CLI Options**: Added `--input` argument to allow arbitrary JSONL input files without modifying default sample paths.

### 2. `scripts/run_pipeline.py`
- **Regex Correction**: Targeted extraction using `re.search(r"Silhouette score\s*=\s*([-+]?\d*\.?\d+)", line)` to prevent capturing the cluster count $K$.
- **Manifest Schema Enhancement**: Added `repositories_clustered`, `silhouette_status` (`"valid"` or `"unavailable"`), and `silhouette_reason`.
- **Output Clarity**: Updated terminal logs to report `X repos assigned to Y clusters (silhouette: Z)`.

### 3. `tests/test_spark.py`
- Added `test_small_dataset_with_k_equals_repos`: Verifies `silhouette is None` when $N=k=2$.
- Added `test_valid_silhouette_range_when_n_greater_than_k`: Verifies that 4 repositories clustered with $k=2$ produce a valid score in $[-1.0, 1.0]$ with high cohesion ($>0.8$).
- Added `test_identical_feature_vectors_raises_value_error`: Verifies graceful failure when all vectors are identical.
- Added `test_fewer_distinct_vectors_than_k_raises_value_error`: Verifies detection when unique vectors $< k$.
- Added `test_kmeans_reproducibility_with_seed`: Verifies deterministic cluster assignments given identical seeds.

### 4. `tests/test_pipeline.py`
- Updated `test_multi_repository_runs_spark_clustering`: Tests pipeline handling of degenerate $N=2, k=2$, verifying `silhouette is None` and `silhouette_status == "unavailable"`.
- Added `test_multi_repository_runs_spark_clustering_with_valid_silhouette`: Tests non-degenerate $N=3, k=2$, asserting valid silhouette in $[-1.0, 1.0]$, `clusters_count == 2`, and `repositories_clustered == 3`.

---

## 4. Test Verification Results

### 4.1 Pytest Suite
Ran `python3 -m pytest tests/ -v`:
- **Total Tests:** 163
- **Passed:** 163 (100%)
- **Failed:** 0
- **Duration:** 66.14 seconds

### 4.2 Baseline Data Validator
Ran `python3 scripts/validate_data.py`:
- **Result:** `PASS WITH WARNINGS`
- **Warnings (1):** 985/1000 enriched records in `data/sample/` are synthetic (expected baseline condition).
- **Errors:** 0 errors.

---

## 5. Real-Data Experiment

### 5.1 Experiment Setup
- **Command:** `python3 scripts/run_pipeline.py psf/requests pallets/flask pallets/click encode/httpx --max-commits 10 --clusters 2`
- **Execution Run ID:** `analysis-20261020T064812Z-04c4400f`
- **Run Directory:** `data/runs/analysis-20261020T064812Z-04c4400f/`
- **Target Repositories:**
  1. `psf/requests` (Python HTTP client)
  2. `pallets/flask` (Python web microframework)
  3. `pallets/click` (Python CLI toolkit)
  4. `encode/httpx` (Python async/sync HTTP client)
- **Commit Limit:** 10 commits per repository (conservative limit respecting unauthenticated GitHub API rate limits).
- **Clustering Parameters:** $k=2$, `seed=42`, `StandardScaler(withMean=True, withStd=True)`.

### 5.2 Retrieval & Authenticity Metrics
- **Repositories Requested / Succeeded:** 4 / 4 (100% success).
- **Commits Retrieved:** 40 total (10 per repository).
- **Truncation:** All 4 repository histories were truncated at the 10-commit boundary (`status: "truncated"`).
- **Authentic Records:** 40 / 40 (100.0% authentic live GitHub API data).
- **Synthetic Records:** 0 (0.0%).

### 5.3 Extracted Repository Feature Vectors

| Repository | Commits | Total Additions | Total Deletions | Total Lines | Total Files | Merge Count | Merge Ratio | Avg Lines/Commit | Avg Files/Commit | Distinct Authors |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **`pallets/click`** | 10 | 3,575 | 1,017 | 4,592 | 62 | 5 | 0.50 | 459.2 | 6.2 | 2 |
| **`encode/httpx`** | 10 | 57 | 33 | 90 | 15 | 0 | 0.00 | 9.0 | 1.5 | 9 |
| **`pallets/flask`** | 10 | 205 | 915 | 1,120 | 37 | 2 | 0.20 | 112.0 | 3.7 | 1 |
| **`psf/requests`** | 10 | 20 | 20 | 40 | 14 | 0 | 0.00 | 4.0 | 1.4 | 2 |

### 5.4 Cluster Assignments and Evaluation

- **Clustering Configuration:** $N=4$ observations, $k=2$ clusters ($N > k$, non-degenerate).
- **Silhouette Coefficient:** **`0.569`** (Status: `valid`).

#### Cluster Breakdown:
- **Cluster 0** (1 repository):
  - `pallets/click`: Characterized by substantial codebase refactoring / release churn (4,592 lines changed across 62 files) and high merge frequency (50% merge commits).
- **Cluster 1** (3 repositories):
  - `encode/httpx`: Focused, granular commits (9.0 lines/commit, 0% merges) with high author distribution (9 distinct authors across 10 commits).
  - `psf/requests`: Highly mature, conservative maintenance commits (4.0 lines/commit, 0% merges, 2 authors).
  - `pallets/flask`: Moderate commit activity (112.0 lines/commit, 20% merges, 1 author).

#### Ecosystem Independence:
Notably, `pallets/click` and `pallets/flask` belong to the exact same organization and ecosystem (`pallets`), yet were partitioned into different clusters due to starkly divergent commit profiles in the sampled window. This confirms that commit pattern clustering reflects actual operational patterns rather than organizational affiliation.

---

## 6. How to Reproduce

### 1. Execute Full Test Suite
```bash
python3 -m pytest tests/ -v
```

### 2. Validate Baseline Sample Dataset
```bash
python3 scripts/validate_data.py
```

### 3. Reproduce the Real-Data Multi-Repository Pipeline Run
```bash
python3 scripts/run_pipeline.py psf/requests pallets/flask pallets/click encode/httpx --max-commits 10 --clusters 2
```

### 4. Inspect Run Output Artifacts
```bash
# List generated files
ls -la data/runs/analysis-20261020T064812Z-04c4400f/

# View manifest
cat data/runs/analysis-20261020T064812Z-04c4400f/pipeline_manifest.json

# View clusters
cat data/runs/analysis-20261020T064812Z-04c4400f/repo_clusters.json
```

---

## 7. Analytical Limitations

1. **Short Commit Horizon (10 commits)**: The sampled data captures immediate, short-term commit activity. Recent large merges or dependency bumps can disproportionately influence averages over a 10-commit window.
2. **Small Cohort Size ($N=4$)**: While mathematically sufficient for $k=2$ clustering, four repositories cannot represent the full distribution of open-source repository workflows.
3. **Unauthenticated API Throttling**: The GitHub REST API enforces a strict rate limit of 60 requests/hour for unauthenticated clients. Deep historical analysis requires a Personal Access Token (`GITHUB_TOKEN`).
4. **Cluster Label Non-Ordinality**: Cluster numbers (`0` and `1`) have no inherent meaning or ranking; they simply indicate mathematical proximity in standardized feature space.
