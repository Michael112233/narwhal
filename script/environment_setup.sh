sudo apt-get update
sudo apt-get install tmux

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
. $HOME/.cargo/env

sudo apt-get install libclang-dev

apt-get update
apt-get install iproute2

cd benchmark
pip install -r requirements.txt

fab local

