## 环境配置

sudo apt-get update
sudo apt-get install tmux

curl https://sh.rustup.rs -sSf | sh -s -- -y
sudo apt-get install libclang-dev

sudo apt-get update
sudo apt-get install iproute2

sudo apt-get install python3-pip

cd benchmark && pip install -r requirements.txt

## 运行

cd ../benchmark

fab local

## 参数说明

- `ATTACK_START_TIME_SEC`: 攻击开始的时间（秒），通常以实验启动为 0 秒的相对时间计算。
- `ATTACK_DURATION_SEC`: 攻击持续时间（秒），攻击开始后持续的时长。
- `NETWORK_DELAY`: 网络延迟设置（单位通常为毫秒或秒，具体取决于脚本/配置文件），用于模拟节点间通信的额外延时。