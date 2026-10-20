"""
spark/cluster_commits.py
------------------------
Commit-level clustering for single-repository Git pattern analysis.
Standardizes commit change metrics (additions, deletions, lines changed, files changed, parent count)
and partitions commits into descriptive behavioral clusters using K-Means.

Features:
- Validates data prerequisites (>= 3 valid commits, >= 2 distinct feature vectors).
- Computes squared Euclidean silhouette score in [-1.0, 1.0].
- Generates descriptive cluster profiles.
- Provides graceful mathematical skip explanations when clustering is not meaningful.
"""

import json
import math
import random
from pathlib import Path


COMMIT_FEATURE_COLS = [
    "additions",
    "deletions",
    "lines_changed",
    "files_changed",
    "parent_count",
]


def _extract_commit_features(commits: list[dict]) -> tuple[list[dict], list[list[float]]]:
    """
    Extract valid numeric feature vectors from commit records.
    Filters out commits with missing diff stats.
    """
    valid_commits = []
    vectors = []

    for c in commits:
        if not isinstance(c, dict):
            continue
        # Verify stats availability and presence of numeric fields
        stats_ok = c.get("stats_available") is not False
        additions = c.get("additions")
        deletions = c.get("deletions")
        lines = c.get("lines_changed")
        files = c.get("files_changed")
        parent_count = c.get("parent_count", 1)

        if additions is None or deletions is None:
            continue

        lines = lines if lines is not None else (additions + deletions)
        files = files if files is not None else 1
        parent_count = parent_count if parent_count is not None else 1

        try:
            vec = [
                float(additions),
                float(deletions),
                float(lines),
                float(files),
                float(parent_count),
            ]
            valid_commits.append(c)
            vectors.append(vec)
        except (ValueError, TypeError):
            continue

    return valid_commits, vectors


def _standardize_vectors(vectors: list[list[float]]) -> tuple[list[list[float]], list[float], list[float]]:
    """Standardize feature vectors to zero mean and unit variance (StandardScaler)."""
    n = len(vectors)
    if n == 0:
        return [], [], []

    num_dims = len(vectors[0])
    means = [0.0] * num_dims
    stds = [0.0] * num_dims

    for dim in range(num_dims):
        vals = [vec[dim] for vec in vectors]
        means[dim] = sum(vals) / n
        variance = sum((x - means[dim]) ** 2 for x in vals) / n
        stds[dim] = math.sqrt(variance) if variance > 1e-9 else 1.0

    scaled = []
    for vec in vectors:
        scaled_vec = [
            (vec[dim] - means[dim]) / stds[dim]
            for dim in range(num_dims)
        ]
        scaled.append(scaled_vec)

    return scaled, means, stds


def _squared_euclidean_distance(a: list[float], b: list[float]) -> float:
    """Calculate squared Euclidean distance between two vectors."""
    return sum((x - y) ** 2 for x, y in zip(a, b))


def _compute_silhouette(scaled_vectors: list[list[float]], labels: list[int], k: int) -> float | None:
    """
    Compute silhouette score using squared Euclidean distance, bounded in [-1.0, 1.0].
    Returns None if degenerate or undefined.
    """
    n = len(scaled_vectors)
    if n <= k or k < 2:
        return None

    # Group point indices by cluster
    clusters = {}
    for idx, label in enumerate(labels):
        clusters.setdefault(label, []).append(idx)

    # If any cluster has size 0 or all are singletons, silhouette is undefined
    if any(len(members) == 0 for members in clusters.values()):
        return None

    silhouettes = []
    for i in range(n):
        point = scaled_vectors[i]
        curr_label = labels[i]
        curr_cluster = clusters[curr_label]

        # Intra-cluster average distance a(i)
        if len(curr_cluster) > 1:
            a_i = sum(_squared_euclidean_distance(point, scaled_vectors[j]) for j in curr_cluster if j != i) / (len(curr_cluster) - 1)
        else:
            a_i = 0.0

        # Nearest other cluster average distance b(i)
        b_candidates = []
        for other_label, other_members in clusters.items():
            if other_label == curr_label or len(other_members) == 0:
                continue
            avg_dist = sum(_squared_euclidean_distance(point, scaled_vectors[j]) for j in other_members) / len(other_members)
            b_candidates.append(avg_dist)

        if not b_candidates:
            continue

        b_i = min(b_candidates)
        denom = max(a_i, b_i)
        if denom > 1e-9:
            s_i = (b_i - a_i) / denom
            silhouettes.append(s_i)
        else:
            silhouettes.append(0.0)

    if not silhouettes:
        return None

    score = sum(silhouettes) / len(silhouettes)
    return round(max(-1.0, min(1.0, score)), 4)


def cluster_commits(
    commits: list[dict],
    k: int = 3,
    seed: int = 42,
) -> dict:
    """
    Cluster commit records using K-Means with standardized features.
    
    Returns a dictionary containing:
      - status: "success" | "skipped"
      - reason: Explanation if skipped
      - k: Effective cluster count
      - silhouette: Squared Euclidean silhouette coefficient or None
      - distance_measure: "squaredEuclidean"
      - profiles: Descriptive profile per cluster
      - assignments: dict of commit_sha -> cluster_id
    """
    valid_commits, vectors = _extract_commit_features(commits)
    n = len(valid_commits)

    if n < 3:
        return {
            "status": "skipped",
            "reason": f"Commit clustering requires at least 3 commits with authentic diff statistics (found {n}).",
            "k": 0,
            "seed": seed,
            "silhouette": None,
            "distance_measure": "squaredEuclidean",
            "profiles": {},
            "assignments": {},
            "valid_commits_count": n,
        }

    # Check distinct feature vectors
    distinct_tuples = set(tuple(v) for v in vectors)
    if len(distinct_tuples) < 2:
        return {
            "status": "skipped",
            "reason": "Cannot cluster commits: all commit records have identical change metrics (zero variance across features).",
            "k": 0,
            "seed": seed,
            "silhouette": None,
            "distance_measure": "squaredEuclidean",
            "profiles": {},
            "assignments": {},
            "valid_commits_count": n,
        }

    effective_k = min(k, len(distinct_tuples), n - 1)
    if effective_k < 2:
        effective_k = 2

    scaled_vectors, means, stds = _standardize_vectors(vectors)

    # Initialize centroids deterministically with seed
    rng = random.Random(seed)
    # Pick distinct initial vectors if possible
    initial_indices = rng.sample(range(n), effective_k)
    centroids = [list(scaled_vectors[idx]) for idx in initial_indices]

    labels = [0] * n
    max_iter = 50

    for iteration in range(max_iter):
        # Assign points to nearest centroid
        new_labels = []
        for vec in scaled_vectors:
            dists = [_squared_euclidean_distance(vec, c) for c in centroids]
            nearest = dists.index(min(dists))
            new_labels.append(nearest)

        if new_labels == labels and iteration > 0:
            break
        labels = new_labels

        # Recompute centroids
        for c_idx in range(effective_k):
            members = [scaled_vectors[p_idx] for p_idx, l in enumerate(labels) if l == c_idx]
            if members:
                num_dims = len(members[0])
                centroids[c_idx] = [
                    sum(m[dim] for m in members) / len(members)
                    for dim in range(num_dims)
                ]

    # Calculate silhouette score
    silhouette = _compute_silhouette(scaled_vectors, labels, effective_k)

    # Map commit SHAs to assigned clusters
    assignments = {}
    cluster_member_commits = {c_idx: [] for c_idx in range(effective_k)}
    for c, l in zip(valid_commits, labels):
        sha = c.get("commit_sha", "")
        assignments[sha] = l
        cluster_member_commits[l].append(c)

    # Build descriptive profiles for each cluster
    profiles = {}
    for c_idx, members in cluster_member_commits.items():
        count = len(members)
        if count == 0:
            continue
        tot_add = sum(c.get("additions") or 0 for c in members)
        tot_del = sum(c.get("deletions") or 0 for c in members)
        tot_lines = sum(c.get("lines_changed") or 0 for c in members)
        tot_files = sum(c.get("files_changed") or 0 for c in members)
        merges = sum(1 for c in members if (c.get("parent_count") or 0) > 1)

        avg_add = round(tot_add / count, 1)
        avg_del = round(tot_del / count, 1)
        avg_lines = round(tot_lines / count, 1)
        avg_files = round(tot_files / count, 1)
        merge_ratio = round(merges / count, 3)

        # Label characterization based on line churn and merge ratio
        if merge_ratio >= 0.5:
            descriptive_label = "Merge & Integration Commits"
        elif avg_lines <= 25 and avg_files <= 2:
            descriptive_label = "Minor Edits & Documentation"
        elif avg_lines > 500 or avg_files > 15:
            descriptive_label = "Major Refactorings & Large Churn"
        else:
            descriptive_label = "Routine Feature & Bug Fix Commits"

        profiles[str(c_idx)] = {
            "cluster_id": c_idx,
            "label": descriptive_label,
            "count": count,
            "percentage": round(100.0 * count / n, 1),
            "avg_additions": avg_add,
            "avg_deletions": avg_del,
            "avg_lines_changed": avg_lines,
            "avg_files_changed": avg_files,
            "merge_ratio": merge_ratio,
        }

    return {
        "status": "success",
        "reason": "",
        "k": effective_k,
        "seed": seed,
        "silhouette": silhouette,
        "distance_measure": "squaredEuclidean",
        "profiles": profiles,
        "assignments": assignments,
        "valid_commits_count": n,
        "feature_columns": COMMIT_FEATURE_COLS,
    }
