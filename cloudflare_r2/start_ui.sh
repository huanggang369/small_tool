#!/bin/bash

# 安装依赖
# echo "正在安装依赖..."
# pip3 install -r requirements.txt

# # 启动Flask应用
# echo "启动Cloudflare R2 UI界面..."
# echo "访问地址: http://localhost:5000"
# python3 r2_ui.py 

sudo su
nohup python3 r2_ui.py &