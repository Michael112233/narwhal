sudo apt-get update
sudo apt-get install tmux

curl https://sh.rustup.rs -sSf | sh -s -- -y
sudo apt-get install libclang-dev

sudo apt-get update
sudo apt-get install iproute2

cd narwhal/benchmark
pip install -r requirements.txt
