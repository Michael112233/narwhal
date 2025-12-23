sudo apt-get update
sudo apt-get install tmux

curl --proto '=https' --tlsv1.2 -sSf [https://sh.rustup.rs](https://sh.rustup.rs/) | sh
. "$HOME/.cargo/env"
sudo apt-get install libclang-dev

sudo apt-get update
sudo apt-get install iproute2

cd narwhal/benchmark
pip install -r requirements.txt
