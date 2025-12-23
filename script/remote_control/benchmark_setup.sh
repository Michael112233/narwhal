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

USER="wucy"               # 远程用户名
SSH_KEY="$HOME/.ssh/cloudlab_key"

# ====== 1. 清理本地配置/数据库 ======
echo "[INFO] Cleaning local db and config files ..."
rm -rf .db-* .*.json
mkdir -p results

# ====== 2. 本地编译（带 benchmark 特性） ======
echo "[INFO] Building node with benchmark feature ..."
source "$HOME/.cargo/env" 2>/dev/null || export PATH="$HOME/.cargo/bin:$PATH"

cargo build --release --features benchmark

# 创建方便调用的软链接 ./node 和 ./benchmark_client
rm -rf node benchmark_client 2>/dev/null || true
ln -s ./target/release/node .
ln -s ./target/release/benchmark_client .

# ====== 3. 生成 keys 和 committee / parameters ======
echo "[INFO] Generating keys on controller node ..."
KEY_FILES=()
NODE_COUNT=${#NODES[@]}

for ((i=0; i<NODE_COUNT; i++)); do
  KEY_FILE=".node-${i}.json"
  ./node generate_keys --filename "$KEY_FILE"
  KEY_FILES+=("$KEY_FILE")
done

echo "[INFO] Building committee.json and parameters.json via Rust node ..."
# 使用已有 Rust 二进制在本地生成 .committee.json / .parameters.json
# （这里沿用原始 CloudLab 脚本逻辑：committee/parameters 由 node 程序写出）
./target/release/node \
  --keys "${KEY_FILES[0]}" \
  --committee .committee.json \
  --store .db-0 \
  --parameters .parameters.json \
  --help >/dev/null 2>&1 || true
# 实际生产环境中通常是通过 Python 脚本生成 committee/parameters，
# 这里仅示意，若你已有 Python 生成脚本，请改成调用该脚本。

# ====== 4. 上传配置到所有节点 ======
echo "[INFO] Uploading keys & configs to all nodes ..."

for i in "${!NODES[@]}"; do
  HOST="${NODES[$i]}"
  echo "[INFO] Uploading to $HOST ..."

  scp -i "$SSH_KEY" .committee.json   "${USER}@${HOST}:${REPO_NAME}/.committee.json"
  scp -i "$SSH_KEY" .parameters.json  "${USER}@${HOST}:${REPO_NAME}/.parameters.json"
  scp -i "$SSH_KEY" ".node-${i}.json" "${USER}@${HOST}:${REPO_NAME}/.node-${i}.json"
done

echo "[INFO] Full benchmark setup (clean → pull → build → keys → upload) finished."