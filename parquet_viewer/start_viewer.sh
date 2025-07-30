#!/bin/bash

# Parquet 文件查看器启动脚本

echo "正在启动 Parquet 文件查看器..."

# 检查 Python 是否安装
if ! command -v python3 &> /dev/null; then
    echo "错误: 未找到 Python3，请先安装 Python3"
    exit 1
fi

# 检查是否在正确的目录
if [ ! -f "app.py" ]; then
    echo "错误: 请在 parquet_viewer 目录下运行此脚本"
    exit 1
fi

# 安装依赖
echo "正在安装依赖包..."
pip3 install -r requirements.txt

# 启动应用
echo "启动 Web 服务器..."
echo "访问地址: http://localhost:5001"
echo "按 Ctrl+C 停止服务器"
python3 app.py 