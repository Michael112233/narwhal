# CloudLab Benchmark 脚本使用说明

## run_cloudlab_benchmark.py

这个脚本用于运行 CloudLab benchmark 并处理日志。

### 基本用法

```bash
# 使用 anaconda python（推荐）
/Users/michael/opt/anaconda3/bin/python run_cloudlab_benchmark.py

# 或者如果 fabric 已安装在系统 python 中
python3 run_cloudlab_benchmark.py
```

### 主要功能

1. **运行 benchmark**：执行 `fab cloudlab_remote`
2. **下载日志**：如果本地没有日志，自动从远程节点下载
3. **解析日志**：使用 LogParser 解析日志并显示结果
4. **保存结果**：将结果保存到 `results/` 目录

### 常用选项

```bash
# 只处理现有日志（不运行 benchmark）
python3 run_cloudlab_benchmark.py --no-run

# 只下载日志（不运行 benchmark 也不解析）
python3 run_cloudlab_benchmark.py --download-only

# 以 debug 模式运行
python3 run_cloudlab_benchmark.py --debug

# 不保存结果到文件
python3 run_cloudlab_benchmark.py --no-save

# 指定 faulty nodes 数量
python3 run_cloudlab_benchmark.py --faults 1
```

### 相关脚本

- **download_logs.py**：仅下载日志文件
  ```bash
  python3 download_logs.py
  ```

- **fab cloudlab_remote**：运行 benchmark（会自动下载日志）
  ```bash
  fab cloudlab_remote
  ```

- **fab logs**：解析并显示日志摘要
  ```bash
  fab logs
  ```

### 输出文件

- **日志文件**：`logs/primary-*.log`, `logs/worker-*.log`, `logs/client-*.log`
- **结果文件**：`results/benchmark_result_YYYYMMDD_HHMMSS.txt`
