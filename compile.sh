#!/usr/bin/env bash
set -euo pipefail

# Script to manually compile the narwhal project
# This compiles both 'node' and 'benchmark_client' binaries

echo "=========================================="
echo "Compiling Narwhal Project"
echo "=========================================="

# Check if cargo is available
if ! command -v cargo &> /dev/null; then
    echo "[ERROR] cargo is not installed or not in PATH"
    echo "Please install Rust: https://www.rust-lang.org/tools/install"
    exit 1
fi

echo "[INFO] Cargo version: $(cargo --version)"
echo ""

# Navigate to project root
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

echo "[INFO] Project root: $PROJECT_ROOT"
echo ""

# Compile with benchmark features
echo "[INFO] Compiling with benchmark features..."
echo "[INFO] Command: cargo build --release --features benchmark"
echo ""

cd node
cargo build --release --features benchmark

if [ $? -eq 0 ]; then
    echo ""
    echo "[OK] Compilation successful!"
    echo ""
    
    # Check if binaries exist
    if [ -f "../target/release/node" ]; then
        echo "[OK] Binary created: target/release/node"
        ls -lh ../target/release/node
    fi
    
    if [ -f "../target/release/benchmark_client" ]; then
        echo "[OK] Binary created: target/release/benchmark_client"
        ls -lh ../target/release/benchmark_client
    fi
    
    echo ""
    echo "[INFO] Creating symlinks in benchmark directory..."
    cd ../benchmark
    
    # Remove old symlinks if they exist
    rm -f node benchmark_client
    
    # Create symlinks
    ln -s ../target/release/node node
    ln -s ../target/release/benchmark_client benchmark_client
    
    if [ -L node ] && [ -L benchmark_client ]; then
        echo "[OK] Symlinks created:"
        ls -lh node benchmark_client
    fi
    
    echo ""
    echo "=========================================="
    echo "Compilation completed successfully!"
    echo "=========================================="
else
    echo ""
    echo "[ERROR] Compilation failed!"
    exit 1
fi




