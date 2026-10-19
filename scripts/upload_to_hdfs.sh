#!/usr/bin/env bash
# scripts/upload_to_hdfs.sh
# -------------------------
# Upload local JSONL datasets into HDFS.
# Requires the Docker Hadoop cluster to be running:
#   docker compose up -d
#
# Usage:
#   bash scripts/upload_to_hdfs.sh
#   bash scripts/upload_to_hdfs.sh --hdfs-base /user/hadoop/git-pattern

set -euo pipefail

HDFS_BASE="/user/hadoop/git-pattern"
if [ "${1:-}" = "--hdfs-base" ] && [ -n "${2:-}" ]; then
    HDFS_BASE="$2"
elif [ -n "${1:-}" ] && [[ "${1:-}" != --* ]]; then
    HDFS_BASE="$1"
fi

NAMENODE_CONTAINER="git-commit-pattern-namenode-1"
LOCAL_DATA="data/sample"
FILES=(
    "repositories.jsonl"
    "commits.jsonl"
    "enriched_commits.jsonl"
)

echo "=== Git Pattern Analysis — HDFS Upload ==="
echo "  HDFS base  : $HDFS_BASE"
echo "  Container  : $NAMENODE_CONTAINER"
echo ""

# --- Check container is running ---
if ! docker inspect --format '{{.State.Running}}' "$NAMENODE_CONTAINER" 2>/dev/null | grep -q "true"; then
    echo "[ERROR] Namenode container '$NAMENODE_CONTAINER' is not running."
    echo "  Start the cluster: docker compose up -d"
    exit 1
fi

# --- Create HDFS directory ---
echo "Creating HDFS directory: $HDFS_BASE"
docker exec "$NAMENODE_CONTAINER" \
    hdfs dfs -mkdir -p "$HDFS_BASE"

# --- Upload each file ---
for filename in "${FILES[@]}"; do
    local_path="$LOCAL_DATA/$filename"
    hdfs_path="$HDFS_BASE/$filename"

    if [ ! -f "$local_path" ]; then
        echo "  [SKIP] $local_path not found"
        continue
    fi

    echo "  Uploading $filename …"
    # Copy to container tmp, then put to HDFS
    docker cp "$local_path" "$NAMENODE_CONTAINER:/tmp/$filename"
    docker exec "$NAMENODE_CONTAINER" \
        hdfs dfs -put -f "/tmp/$filename" "$hdfs_path"

    # Count lines directly in HDFS to verify
    line_count=$(docker exec "$NAMENODE_CONTAINER" bash -c "hdfs dfs -cat '$hdfs_path' | wc -l")
    echo "  ✓  $hdfs_path ($line_count records verified in HDFS)"
done

echo ""
echo "Listing HDFS directory:"
docker exec "$NAMENODE_CONTAINER" hdfs dfs -ls "$HDFS_BASE"
echo ""
echo "Upload complete."
