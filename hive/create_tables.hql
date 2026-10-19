-- hive/create_tables.hql
-- ----------------------
-- Create Hive external tables for Git Pattern Analysis data.
-- Run via HiveServer2 inside the Docker cluster, or locally with
-- an embedded metastore (e.g. via spark-sql).
--
-- Prerequisites:
--   1. docker compose up -d
--   2. bash scripts/upload_to_hdfs.sh
--
-- Usage (inside container):
--   docker exec -it <hive-server-container> hive -f /tmp/create_tables.hql

-- Repository inventory
CREATE EXTERNAL TABLE IF NOT EXISTS git_repositories (
    repo_id        BIGINT,
    repo_name      STRING,
    language       STRING,
    stars          INT,
    forks          INT,
    created_at     STRING,
    updated_at     STRING,
    archived       BOOLEAN,
    default_branch STRING
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/user/hadoop/git-pattern/repositories'
TBLPROPERTIES ('serialization.null.format'='');

-- Basic commits
CREATE EXTERNAL TABLE IF NOT EXISTS git_commits (
    repo_id             BIGINT,
    repo_name           STRING,
    commit_sha          STRING,
    author_identity     STRING,
    author_timestamp    STRING,
    committer_identity  STRING,
    committer_timestamp STRING,
    commit_message      STRING,
    parent_count        INT
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/user/hadoop/git-pattern/commits'
TBLPROPERTIES ('serialization.null.format'='');

-- Enriched commits
CREATE EXTERNAL TABLE IF NOT EXISTS git_enriched_commits (
    repo_id             BIGINT,
    repo_name           STRING,
    commit_sha          STRING,
    author_identity     STRING,
    author_timestamp    STRING,
    committer_identity  STRING,
    committer_timestamp STRING,
    commit_message      STRING,
    parent_count        INT,
    files_changed       INT,
    additions           INT,
    deletions           INT,
    lines_changed       INT,
    stats_available     BOOLEAN,
    files_available     BOOLEAN,
    synthetic           BOOLEAN
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/user/hadoop/git-pattern/enriched'
TBLPROPERTIES ('serialization.null.format'='');

-- MapReduce-aggregated repo features
CREATE EXTERNAL TABLE IF NOT EXISTS git_repo_features (
    repo_name           STRING,
    commit_count        INT,
    total_additions     BIGINT,
    total_deletions     BIGINT,
    total_lines         BIGINT,
    total_files         BIGINT,
    merge_count         INT,
    avg_additions       DOUBLE,
    avg_deletions       DOUBLE,
    avg_lines_changed   DOUBLE,
    avg_files_changed   DOUBLE,
    merge_ratio         DOUBLE
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/user/hadoop/git-pattern/repo_features'
TBLPROPERTIES ('serialization.null.format'='');
