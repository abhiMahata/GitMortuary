"""
tests/test_spark.py
-------------------
Automated test suite for Spark MLlib commit pattern feature aggregation
and K-Means clustering.
Tests aggregation logic, missing/null value handling, vector assembly,
clustering invariants, output schema, and small-dataset resilience.
"""

import json
import math
import sys
from pathlib import Path
import pytest

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Attempt PySpark imports; skip module if unavailable
try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType,
        DoubleType, BooleanType, LongType,
    )
    from spark.cluster_repos import (
        ENRICHED_SCHEMA,
        FEATURE_COLS,
        aggregate_features,
        run_kmeans,
        save_results,
    )
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

pytestmark = pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark is not installed")


# ---------------------------------------------------------------------------
# Fixture: Local Spark Session (shared across tests in module)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark():
    """Module-scoped SparkSession fixture configured for fast local unit testing."""
    session = (
        SparkSession.builder
        .appName("GitPatternSparkUnitTests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Helper: Create test commit records
# ---------------------------------------------------------------------------

def make_commit(
    repo_id=1,
    repo_name="org/repo-a",
    sha="sha001",
    author="Alice",
    parent_count=1,
    files_changed=3,
    additions=50,
    deletions=10,
    lines_changed=60,
    stats_available=True,
    files_available=True,
    synthetic=False,
):
    return (
        repo_id,
        repo_name,
        sha,
        author,
        "2024-01-01T00:00:00Z",
        author,
        "2024-01-01T00:00:00Z",
        "Commit msg",
        parent_count,
        files_changed,
        additions,
        deletions,
        lines_changed,
        stats_available,
        files_available,
        synthetic,
    )


# ===========================================================================
# 1. FEATURE AGGREGATION
# ===========================================================================

class TestSparkFeatureAggregation:
    def test_aggregate_features_controlled_input(self, spark):
        data = [
            # repo-a: 2 commits, 2 distinct authors (Alice, Bob), 1 merge commit
            make_commit(repo_id=10, repo_name="org/repo-a", sha="s1", author="Alice", parent_count=1, additions=100, deletions=20, lines_changed=120, files_changed=2),
            make_commit(repo_id=10, repo_name="org/repo-a", sha="s2", author="Bob", parent_count=2, additions=50, deletions=10, lines_changed=60, files_changed=1),
            # repo-b: 1 commit, 1 author (Charlie), 0 merge commits
            make_commit(repo_id=20, repo_name="org/repo-b", sha="s3", author="Charlie", parent_count=1, additions=10, deletions=5, lines_changed=15, files_changed=1),
        ]
        df = spark.createDataFrame(data, schema=ENRICHED_SCHEMA)
        agg_df = aggregate_features(df)
        rows = {r.repo_name: r for r in agg_df.collect()}

        assert len(rows) == 2
        assert "org/repo-a" in rows
        assert "org/repo-b" in rows

        # Verify repo-a metrics
        ra = rows["org/repo-a"]
        assert ra.commit_count == 2
        assert ra.total_additions == 150
        assert ra.total_deletions == 30
        assert ra.total_lines_changed == 180
        assert ra.total_files_changed == 3
        assert ra.merge_count == 1
        assert ra.distinct_authors == 2
        assert math.isclose(ra.avg_additions, 75.0)
        assert math.isclose(ra.avg_deletions, 15.0)
        assert math.isclose(ra.avg_lines_changed, 90.0)
        assert math.isclose(ra.avg_files_changed, 1.5)
        assert math.isclose(ra.merge_ratio, 0.5)

        # Verify repo-b metrics
        rb = rows["org/repo-b"]
        assert rb.commit_count == 1
        assert rb.distinct_authors == 1
        assert rb.merge_count == 0
        assert math.isclose(rb.merge_ratio, 0.0)


# ===========================================================================
# 2. MISSING & NULL STATISTIC HANDLING
# ===========================================================================

class TestSparkMissingAndNullHandling:
    def test_missing_and_null_metrics_produce_finite_values(self, spark):
        data = [
            # Commit with null additions/deletions/lines/files and null parent_count
            make_commit(
                repo_id=30,
                repo_name="org/repo-nulls",
                sha="snull",
                parent_count=None,
                files_changed=None,
                additions=None,
                deletions=None,
                lines_changed=None,
                stats_available=False,
            ),
        ]
        df = spark.createDataFrame(data, schema=ENRICHED_SCHEMA)
        agg_df = aggregate_features(df)
        row = agg_df.collect()[0]

        # Verify all feature columns are finite non-null numbers
        assert row.commit_count == 1
        assert row.total_additions == 0
        assert row.total_deletions == 0
        assert row.merge_count == 0
        assert math.isclose(row.avg_additions, 0.0)
        assert math.isclose(row.avg_deletions, 0.0)
        assert math.isclose(row.avg_lines_changed, 0.0)
        assert math.isclose(row.avg_files_changed, 0.0)
        assert math.isclose(row.merge_ratio, 0.0)

        for col_name in FEATURE_COLS:
            val = getattr(row, col_name)
            assert val is not None, f"Feature column {col_name} should not be None"
            assert math.isfinite(val), f"Feature column {col_name} must be finite"

    def test_genuine_zeros_not_treated_as_missing(self, spark):
        data = [
            make_commit(repo_id=40, repo_name="org/repo-zero", sha="sz1", additions=0, deletions=0, lines_changed=0, files_changed=0),
            make_commit(repo_id=40, repo_name="org/repo-zero", sha="sz2", additions=0, deletions=0, lines_changed=0, files_changed=0),
        ]
        df = spark.createDataFrame(data, schema=ENRICHED_SCHEMA)
        agg_df = aggregate_features(df)
        row = agg_df.collect()[0]

        assert row.commit_count == 2
        assert row.total_additions == 0
        assert math.isclose(row.avg_additions, 0.0)
        assert math.isclose(row.avg_deletions, 0.0)


# ===========================================================================
# 3. K-MEANS CLUSTERING & INVARIANTS
# ===========================================================================

class TestSparkKMeansClustering:
    def test_clustering_produces_valid_assignments(self, spark):
        # 4 repositories with distinct feature distributions
        data = [
            # Repo 1: High additions, low deletions
            make_commit(repo_id=1, repo_name="repo1", sha="r1_1", additions=500, deletions=10, lines_changed=510, files_changed=15),
            # Repo 2: High additions, low deletions
            make_commit(repo_id=2, repo_name="repo2", sha="r2_1", additions=480, deletions=12, lines_changed=492, files_changed=14),
            # Repo 3: Low additions, high deletions
            make_commit(repo_id=3, repo_name="repo3", sha="r3_1", additions=10, deletions=300, lines_changed=310, files_changed=2),
            # Repo 4: Low additions, high deletions
            make_commit(repo_id=4, repo_name="repo4", sha="r4_1", additions=12, deletions=320, lines_changed=332, files_changed=2),
        ]
        df = spark.createDataFrame(data, schema=ENRICHED_SCHEMA)
        features_df = aggregate_features(df)

        k = 2
        seed = 42
        predictions, model, silhouette = run_kmeans(features_df, k=k, seed=seed)

        rows = predictions.collect()
        assert len(rows) == 4, "All 4 repositories must receive cluster assignments"

        # Invariants
        assigned_clusters = {r.cluster for r in rows}
        for r in rows:
            assert isinstance(r.cluster, int)
            assert 0 <= r.cluster < k, f"Cluster {r.cluster} out of expected range [0, {k-1}]"

        # Silhouette invariant
        assert math.isfinite(silhouette)
        assert -1.0 <= silhouette <= 1.0

    def test_save_results_output_schema_and_integrity(self, spark, tmp_path):
        data = [
            make_commit(repo_id=1, repo_name="r1", sha="s1", additions=100, deletions=10),
            make_commit(repo_id=2, repo_name="r2", sha="s2", additions=200, deletions=20),
        ]
        df = spark.createDataFrame(data, schema=ENRICHED_SCHEMA)
        features_df = aggregate_features(df)
        predictions, _, silhouette = run_kmeans(features_df, k=2, seed=42)

        out_file = str(tmp_path / "clusters_test.json")
        save_results(predictions, out_file)

        # Verify written JSON file
        out_path = Path(out_file)
        assert out_path.exists()
        with out_path.open() as fh:
            saved = json.load(fh)

        assert isinstance(saved, list)
        assert len(saved) == 2

        expected_keys = {
            "cluster", "repo_name", "repo_id", "commit_count",
            "total_additions", "total_deletions", "total_lines_changed",
            "total_files_changed", "merge_count", "merge_ratio",
            "avg_additions", "avg_deletions", "avg_lines_changed",
            "avg_files_changed", "distinct_authors",
        }
        for item in saved:
            assert expected_keys.issubset(set(item.keys()))
            assert isinstance(item["cluster"], int)
            assert item["commit_count"] >= 1


# ===========================================================================
# 4. SMALL DATASET RESILIENCE
# ===========================================================================

class TestSparkSmallDatasetResilience:
    def test_small_dataset_with_k_equals_repos(self, spark):
        # Exactly 2 repos with k=2: Degenerate singleton clustering, silhouette is undefined
        data = [
            make_commit(repo_id=1, repo_name="tiny1", sha="t1", additions=10, deletions=2),
            make_commit(repo_id=2, repo_name="tiny2", sha="t2", additions=100, deletions=20),
        ]
        df = spark.createDataFrame(data, schema=ENRICHED_SCHEMA)
        features_df = aggregate_features(df)

        predictions, model, silhouette = run_kmeans(features_df, k=2, seed=42)
        rows = predictions.collect()
        assert len(rows) == 2
        # Mathematically undefined: N=k results in all singleton clusters
        assert silhouette is None

    def test_valid_silhouette_range_when_n_greater_than_k(self, spark):
        # 4 repos with k=2: Proper clustering where silhouette score is valid and in [-1, 1]
        data = [
            make_commit(repo_id=1, repo_name="repo1", sha="r1", additions=10, deletions=2),
            make_commit(repo_id=2, repo_name="repo2", sha="r2", additions=12, deletions=3),
            make_commit(repo_id=3, repo_name="repo3", sha="r3", additions=500, deletions=100),
            make_commit(repo_id=4, repo_name="repo4", sha="r4", additions=520, deletions=110),
        ]
        df = spark.createDataFrame(data, schema=ENRICHED_SCHEMA)
        features_df = aggregate_features(df)

        predictions, model, silhouette = run_kmeans(features_df, k=2, seed=42)
        assert silhouette is not None
        assert math.isfinite(silhouette)
        assert -1.0 <= silhouette <= 1.0
        assert silhouette > 0.8  # Well-separated clusters should yield high positive silhouette

    def test_identical_feature_vectors_raises_value_error(self, spark):
        # Repositories with identical metrics cannot form k >= 2 clusters
        data = [
            make_commit(repo_id=1, repo_name="r1", sha="s1", additions=10, deletions=2),
            make_commit(repo_id=2, repo_name="r2", sha="s2", additions=10, deletions=2),
            make_commit(repo_id=3, repo_name="r3", sha="s3", additions=10, deletions=2),
        ]
        df = spark.createDataFrame(data, schema=ENRICHED_SCHEMA)
        features_df = aggregate_features(df)

        with pytest.raises(ValueError, match="identical feature vectors"):
            run_kmeans(features_df, k=2, seed=42)

    def test_fewer_distinct_vectors_than_k_raises_value_error(self, spark):
        # 3 repos, but only 2 distinct vectors, with k=3 requested
        data = [
            make_commit(repo_id=1, repo_name="r1", sha="s1", additions=10, deletions=2),
            make_commit(repo_id=2, repo_name="r2", sha="s2", additions=10, deletions=2),
            make_commit(repo_id=3, repo_name="r3", sha="s3", additions=50, deletions=10),
        ]
        df = spark.createDataFrame(data, schema=ENRICHED_SCHEMA)
        features_df = aggregate_features(df)

        with pytest.raises(ValueError, match="Cannot form 3 clusters: only 2 distinct feature vectors exist"):
            run_kmeans(features_df, k=3, seed=42)

    def test_kmeans_reproducibility_with_seed(self, spark):
        # Same dataset and seed must yield identical cluster assignments
        data = [
            make_commit(repo_id=1, repo_name="r1", sha="s1", additions=10, deletions=2),
            make_commit(repo_id=2, repo_name="r2", sha="s2", additions=15, deletions=3),
            make_commit(repo_id=3, repo_name="r3", sha="s3", additions=100, deletions=20),
            make_commit(repo_id=4, repo_name="r4", sha="s4", additions=120, deletions=25),
        ]
        df = spark.createDataFrame(data, schema=ENRICHED_SCHEMA)
        features_df = aggregate_features(df)

        preds1, _, _ = run_kmeans(features_df, k=2, seed=42)
        preds2, _, _ = run_kmeans(features_df, k=2, seed=42)

        c1 = [r.cluster for r in preds1.orderBy("repo_name").collect()]
        c2 = [r.cluster for r in preds2.orderBy("repo_name").collect()]
        assert c1 == c2

    def test_invalid_k_less_than_two_raises_value_error(self, spark):
        # KMeans requires at least k >= 2
        data = [
            make_commit(repo_id=1, repo_name="solo", sha="solo1", additions=25, deletions=5),
        ]
        df = spark.createDataFrame(data, schema=ENRICHED_SCHEMA)
        features_df = aggregate_features(df)

        with pytest.raises(ValueError) as excinfo:
            run_kmeans(features_df, k=1, seed=42)
        assert "k >= 2" in str(excinfo.value)
