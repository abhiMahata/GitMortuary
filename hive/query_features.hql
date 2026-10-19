-- hive/query_features.hql
-- -----------------------
-- Analytical queries on the git_repo_features table.
-- Run after create_tables.hql.

-- 1. Repos ranked by average additions per commit (most active code production)
SELECT repo_name, commit_count, avg_additions, avg_deletions, avg_lines_changed
FROM   git_repo_features
ORDER  BY avg_additions DESC
LIMIT  10;

-- 2. Merge-heavy repos (likely release-flow or frequent PR mergers)
SELECT repo_name, commit_count, merge_ratio, merge_count
FROM   git_repo_features
WHERE  merge_ratio > 0.3
ORDER  BY merge_ratio DESC;

-- 3. High-churn repos (many deletions relative to additions)
SELECT
    repo_name,
    avg_additions,
    avg_deletions,
    ROUND(avg_deletions / NULLIF(avg_additions, 0), 3) AS deletion_ratio
FROM  git_repo_features
ORDER BY deletion_ratio DESC
LIMIT 10;

-- 4. Summary statistics
SELECT
    COUNT(*)                              AS total_repos,
    SUM(commit_count)                     AS total_commits,
    ROUND(AVG(avg_additions), 2)          AS mean_avg_additions,
    ROUND(AVG(merge_ratio), 4)            AS mean_merge_ratio,
    ROUND(AVG(avg_files_changed), 2)      AS mean_avg_files_changed
FROM  git_repo_features;
