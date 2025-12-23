#!/usr/bin/env bash
set -euo pipefail

# ====== 基本配置 ======
REPO_NAME="narwhal"
REPO_URL="https://github.com/Michael112233/narwhal.git"
BRANCH="experiment1"

# CloudLab 所有节点（和 cloudlab_settings.json / remote_setup_config.json 保持一致）
NODES=(
  "er076.utah.cloudlab.us"
  "er127.utah.cloudlab.us"
  "er093.utah.cloudlab.us"
  "er086.utah.cloudlab.us"
  "er116.utah.cloudlab.us"
  "er124.utah.cloudlab.us"
  "er108.utah.cloudlab.us"
  "er095.utah.cloudlab.us"
  "er092.utah.cloudlab.us"
  "er107.utah.cloudlab.us"
)

USER="wucy"
SSH_KEY="$HOME/.ssh/cloudlab_key"

# ====== 1. 清理本地配置/数据库 ======
echo "[INFO] Cleaning local db and config files ..."
rm -rf .db-* .*.json
mkdir -p narwhal/benchmark/results

# ====== 2. 本地编译（带 benchmark 特性） ======
echo "[INFO] Building node with benchmark feature ..."
source "$HOME/.cargo/env" 2>/dev/null || export PATH="$HOME/.cargo/bin:$PATH"

cd narwhal/benchmark && cargo build --release --features benchmark

# 创建方便调用的软链接 ./node 和 ./benchmark_client
rm -rf narwhal/benchmark/node narwhal/benchmark/benchmark_client 2>/dev/null || true
ln -s ./target/release/node narwhal/benchmark
ln -s ./target/release/benchmark_client narwhal/benchmark

# ====== 3. 生成 keys 和 committee / parameters ======
echo "[INFO] Generating keys on controller node ..."
KEY_FILES=()
NODE_COUNT=${#NODES[@]}

for ((i=0; i<NODE_COUNT; i++)); do
  KEY_FILE="narwhal/benchmark/.node-${i}.json"
  ./narwhal/benchmark/node generate_keys --filename "$KEY_FILE"
  KEY_FILES+=("$KEY_FILE")
done

echo "[INFO] Building committee.json and parameters.json via Rust node ..."
./target/release/node \
  --keys "${KEY_FILES[0]}" \
  --committee .committee.json \
  --store .db-0 \
  --parameters .parameters.json \
  --help >/dev/null 2>&1 || true

# ====== 4. 上传配置到所有节点 ======
echo "[INFO] Uploading keys & configs to all nodes ..."

for i in "${!NODES[@]}"; do
  HOST="${NODES[$i]}"
  echo "[INFO] Uploading to $HOST ..."

  scp -i "$SSH_KEY" narwhal/benchmark/.committee.json   "${USER}@${HOST}:${REPO_NAME}/.committee.json"
  scp -i "$SSH_KEY" narwhal/benchmark/.parameters.json  "${USER}@${HOST}:${REPO_NAME}/.parameters.json"
  scp -i "$SSH_KEY" narwhal/benchmark/.node-${i}.json "${USER}@${HOST}:${REPO_NAME}/.node-${i}.json"
done

echo "[INFO] Full benchmark setup (clean → pull → build → keys → upload) finished."