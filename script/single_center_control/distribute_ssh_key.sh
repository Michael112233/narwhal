#!/usr/bin/env bash
set -euo pipefail

# ====== 配置：需要分发的 SSH 私钥路径（本机） ======
LOCAL_KEY="$HOME/.ssh/cloudlab_key"
REMOTE_KEY_NAME="cloudlab_key"

# CloudLab 所有节点（与 remote_setup_config.json 中保持一致）
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

if [ ! -f "$LOCAL_KEY" ]; then
  echo "[ERROR] Local SSH key not found: $LOCAL_KEY"
  exit 1
fi

echo "[INFO] Distributing SSH key $LOCAL_KEY to all nodes..."

for HOST in "${NODES[@]}"; do
  echo "[INFO] -> $HOST"

  # 确保远程 ~/.ssh 目录存在
  ssh -i "$LOCAL_KEY" -o StrictHostKeyChecking=accept-new "${USER}@${HOST}" "mkdir -p ~/.ssh" >/dev/null 2>&1

  # 拷贝私钥到远程 ~/.ssh/cloudlab_key
  scp -i "$LOCAL_KEY" -o StrictHostKeyChecking=accept-new \
    "$LOCAL_KEY" "${USER}@${HOST}:~/.ssh/${REMOTE_KEY_NAME}"

  # 设置权限
  ssh -i "$LOCAL_KEY" -o StrictHostKeyChecking=accept-new "${USER}@${HOST}" \
    "chmod 600 ~/.ssh/${REMOTE_KEY_NAME}"
done

echo "[INFO] SSH key distribution finished."


