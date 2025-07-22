#!/usr/bin/env python3
"""
简单的HTTP服务器，用于提供JSON格式化工具的网页服务
"""

import http.server
import socketserver
import os
import sys
from pathlib import Path

class CustomHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    """自定义HTTP请求处理器"""
    
    def end_headers(self):
        """添加CORS头，允许跨域访问"""
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        super().end_headers()
    
    def do_GET(self):
        """处理GET请求"""
        # 如果请求根路径，返回index.html
        if self.path == '/':
            self.path = '/index.html'
        
        # 如果请求的文件不存在，返回index.html
        file_path = Path(self.path.lstrip('/'))
        if not file_path.exists():
            self.path = '/index.html'
        
        return super().do_GET()

def run_server(port=7766, host='localhost'):
    """运行HTTP服务器"""
    
    # 确保在正确的目录中运行
    script_dir = Path(__file__).parent
    os.chdir(script_dir)
    
    # 检查index.html是否存在
    if not Path('index.html').exists():
        print("❌ 错误: index.html 文件不存在")
        print(f"请确保 index.html 文件在 {script_dir} 目录中")
        sys.exit(1)
    
    # 创建服务器
    with socketserver.TCPServer((host, port), CustomHTTPRequestHandler) as httpd:
        print(f"🚀 服务器启动成功!")
        print(f"📁 服务目录: {script_dir}")
        print(f"🌐 访问地址: http://{host}:{port}")
        print(f"📄 主页文件: index.html")
        print("\n按 Ctrl+C 停止服务器")
        print("-" * 50)
        
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n🛑 服务器已停止")
            httpd.shutdown()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='JSON格式化工具HTTP服务器')
    parser.add_argument('--port', '-p', type=int, default=7766, 
                       help='服务器端口 (默认: 7766)')
    parser.add_argument('--host', '-H', default='0.0.0.0', 
                       help='服务器主机 (默认: localhost)')
    parser.add_argument('--host-all', action='store_true',
                       help='允许所有主机访问 (使用 0.0.0.0)')
    
    args = parser.parse_args()
    
    # 如果指定了--host-all，使用0.0.0.0
    host = '0.0.0.0' if args.host_all else args.host
    
    run_server(args.port, host) 