#!/bin/bash

# JSON格式化工具服务器启动脚本

echo "🚀 启动JSON格式化工具服务器..."

# 检查Python是否安装
if ! command -v python3 &> /dev/null; then
    echo "❌ 错误: 未找到Python3，请先安装Python3"
    exit 1
fi

# 检查index.html是否存在
if [ ! -f "index.html" ]; then
    echo "❌ 错误: index.html 文件不存在"
    echo "请确保在包含index.html的目录中运行此脚本"
    exit 1
fi

# 检查server.py是否存在
if [ ! -f "server.py" ]; then
    echo "❌ 错误: server.py 文件不存在"
    exit 1
fi

# 设置默认参数
PORT=${1:-7777}
HOST=${2:-localhost}

echo "📁 当前目录: $(pwd)"
echo "🌐 服务器地址: http://$HOST:$PORT"
echo "📄 服务文件: index.html"
echo ""

# 启动服务器
python3 server.py --port $PORT --host $HOST 