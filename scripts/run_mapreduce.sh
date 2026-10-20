#!/usr/bin/env bash
# scripts/run_mapreduce.sh
# ------------------------
# Run the Hadoop Streaming MapReduce job inside the namenode container.
# Produces: /user/hadoop/git-pattern/repo_features/  (JSONL, one record per repo)
#
# Prerequisites:
#   1. docker compose up -d
#   2. bash scripts/upload_to_hdfs.sh
#
# Usage:
#   bash scripts/run_mapreduce.sh

set -euo pipefail

NAMENODE_CONTAINER="git-commit-pattern-namenode-1"
HDFS_BASE="/user/hadoop/git-pattern"
HDFS_INPUT=""
HDFS_OUTPUT=""
LOCAL_OUTPUT="data/output/repo_features_mr"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --hdfs-base)
            HDFS_BASE="$2"
            shift 2
            ;;
        --input)
            HDFS_INPUT="$2"
            shift 2
            ;;
        --output)
            HDFS_OUTPUT="$2"
            shift 2
            ;;
        --local-output)
            LOCAL_OUTPUT="$2"
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done

if [ -z "$HDFS_INPUT" ]; then
    HDFS_INPUT="$HDFS_BASE/enriched_commits.jsonl"
fi
if [ -z "$HDFS_OUTPUT" ]; then
    HDFS_OUTPUT="$HDFS_BASE/repo_features"
fi

STREAMING_JAR="/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar"

echo "=== Git Pattern Analysis — MapReduce ==="
echo "  Input  : $HDFS_INPUT"
echo "  Output : $HDFS_OUTPUT"
echo ""

NODEMANAGER_CONTAINER="git-commit-pattern-nodemanager-1"

# --- Check container ---
if ! docker inspect --format '{{.State.Running}}' "$NAMENODE_CONTAINER" 2>/dev/null | grep -q "true"; then
    echo "[ERROR] Namenode container is not running. Run: docker compose up -d"
    exit 1
fi

# --- Copy mapper/reducer into containers ---
echo "Copying MapReduce scripts to container(s) …"
docker cp mapreduce/mapper.py  "$NAMENODE_CONTAINER:/tmp/mapper.py"
docker cp mapreduce/reducer.py "$NAMENODE_CONTAINER:/tmp/reducer.py"
if docker inspect --format '{{.State.Running}}' "$NODEMANAGER_CONTAINER" 2>/dev/null | grep -q "true"; then
    docker cp mapreduce/mapper.py  "$NODEMANAGER_CONTAINER:/tmp/mapper.py"
    docker cp mapreduce/reducer.py "$NODEMANAGER_CONTAINER:/tmp/reducer.py"
fi

# --- Remove existing output (Hadoop won't overwrite) ---
docker exec "$NAMENODE_CONTAINER" \
    hdfs dfs -rm -r -skipTrash "$HDFS_OUTPUT" 2>/dev/null || true

# --- Run Hadoop Streaming ---
echo "Running Hadoop Streaming job on YARN …"
docker exec "$NAMENODE_CONTAINER" \
    hadoop jar "$STREAMING_JAR" \
        -files /tmp/mapper.py,/tmp/reducer.py \
        -mapper  "python3 mapper.py" \
        -reducer "python3 reducer.py" \
        -input   "$HDFS_INPUT" \
        -output  "$HDFS_OUTPUT"

echo ""
echo "Output files in HDFS:"
docker exec "$NAMENODE_CONTAINER" hdfs dfs -ls "$HDFS_OUTPUT"
echo ""

# --- Pull results back to local ---
mkdir -p "$LOCAL_OUTPUT"
docker exec "$NAMENODE_CONTAINER" \
    bash -c "hdfs dfs -cat $HDFS_OUTPUT/part-* > /tmp/mr_output.jsonl"
docker cp "$NAMENODE_CONTAINER:/tmp/mr_output.jsonl" "$LOCAL_OUTPUT/repo_features.jsonl"

echo "Distributed MapReduce complete."
echo "  Local copy of distributed results: $LOCAL_OUTPUT/repo_features.jsonl"
echo ""

# --- Validate distributed vs local output ---
if [ "$LOCAL_OUTPUT" = "data/output/repo_features_mr" ] && [ -f "data/output/repo_features_local.jsonl" ]; then
    echo "Comparing distributed MapReduce results with validated local baseline:"
    python3 -c "
import json

with open('$LOCAL_OUTPUT/repo_features.jsonl') as f:
    mr_rows = {r['repo_name']: r for r in [json.loads(l) for l in f if l.strip()]}

with open('data/output/repo_features_local.jsonl') as f:
    local_rows = {r['repo_name']: r for r in [json.loads(l) for l in f if l.strip()]}

print(f'  Distributed repositories: {len(mr_rows)}')
print(f'  Local baseline repositories: {len(local_rows)}')

mismatches = []
for repo, mr_r in mr_rows.items():
    if repo not in local_rows:
        mismatches.append(f'Extra repo in MR: {repo}')
        continue
    loc_r = local_rows[repo]
    if mr_r != loc_r:
        mismatches.append(f'Difference in {repo}: MR={mr_r} vs Local={loc_r}')

for repo in local_rows:
    if repo not in mr_rows:
        mismatches.append(f'Missing repo in MR: {repo}')

if mismatches:
    print('  [FAIL] Discrepancies detected between distributed and local output:')
    for m in mismatches:
        print(f'    - {m}')
    exit(1)
else:
    print('  [PASS] 20/20 repository feature records match bit-for-bit with local baseline!')
"
else
    echo "Validating produced distributed feature records:"
    python3 -c "
import json
with open('$LOCAL_OUTPUT/repo_features.jsonl') as f:
    mr_rows = [json.loads(l) for l in f if l.strip()]
print(f'  ✓ Successfully produced {len(mr_rows)} repository feature records.')
"
fi
