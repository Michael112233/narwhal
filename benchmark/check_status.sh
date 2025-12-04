#!/bin/bash
# 检查基准测试运行状态的脚本
# 可以直接在 CloudLab 节点上执行

echo "=========================================="
echo "基准测试运行状态检查"
echo "=========================================="
echo ""

# 1. 检查 tmux 会话
echo "1. Tmux 会话状态:"
if command -v tmux &> /dev/null; then
    tmux ls 2>/dev/null || echo "  没有活动的 tmux 会话"
else
    echo "  tmux 未安装"
fi
echo ""

# 2. 检查 Primary 进程
echo "2. Primary 进程:"
PRIMARY_COUNT=$(pgrep -f "node.*primary" | wc -l)
if [ "$PRIMARY_COUNT" -gt 0 ]; then
    echo "  ✓ 运行中 (数量: $PRIMARY_COUNT)"
    echo "  进程详情:"
    pgrep -f "node.*primary" | xargs ps -p | tail -n +2
else
    echo "  ✗ 未运行"
fi
echo ""

# 3. 检查 Worker 进程
echo "3. Worker 进程:"
WORKER_COUNT=$(pgrep -f "node.*worker" | wc -l)
if [ "$WORKER_COUNT" -gt 0 ]; then
    echo "  ✓ 运行中 (数量: $WORKER_COUNT)"
    echo "  进程详情:"
    pgrep -f "node.*worker" | xargs ps -p | tail -n +2
else
    echo "  ✗ 未运行"
fi
echo ""

# 4. 检查 Client 进程
echo "4. Client 进程:"
CLIENT_COUNT=$(pgrep -f "benchmark_client" | wc -l)
if [ "$CLIENT_COUNT" -gt 0 ]; then
    echo "  ✓ 运行中 (数量: $CLIENT_COUNT)"
    echo "  进程详情:"
    pgrep -f "benchmark_client" | xargs ps -p | tail -n +2
else
    echo "  ✗ 未运行"
fi
echo ""

# 5. 检查网络连接（监听端口）
echo "5. 网络端口监听状态:"
if command -v netstat &> /dev/null; then
    netstat -tlnp 2>/dev/null | grep -E ":(3000|3001|3002|3003|5000|5001|5002|5003)" || echo "  没有相关端口在监听"
elif command -v ss &> /dev/null; then
    ss -tlnp 2>/dev/null | grep -E ":(3000|3001|3002|3003|5000|5001|5002|5003)" || echo "  没有相关端口在监听"
else
    echo "  无法检查端口（netstat/ss 未安装）"
fi
echo ""

# 6. 检查日志文件
echo "6. 最近的日志文件:"
if [ -d "logs" ]; then
    echo "  日志目录存在，最新文件:"
    ls -lht logs/ 2>/dev/null | head -5 || echo "  日志目录为空"
else
    echo "  日志目录不存在"
fi
echo ""

# 7. 总结
echo "=========================================="
echo "总结:"
TOTAL=$((PRIMARY_COUNT + WORKER_COUNT + CLIENT_COUNT))
if [ "$TOTAL" -gt 0 ]; then
    echo "  ✓ 基准测试正在运行"
    echo "  - Primary: $PRIMARY_COUNT"
    echo "  - Worker: $WORKER_COUNT"
    echo "  - Client: $CLIENT_COUNT"
    echo "  - 总计: $TOTAL 个进程"
else
    echo "  ✗ 基准测试未运行"
fi
echo "=========================================="

