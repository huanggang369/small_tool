from flask import Flask, render_template, request, jsonify, send_from_directory
import os
import json
from file_reader import FileReader
import traceback
from werkzeug.utils import secure_filename
import uuid

app = Flask(__name__)

# 配置上传文件夹
UPLOAD_FOLDER = 'uploads'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # 500MB 限制

# 允许的文件扩展名
ALLOWED_EXTENSIONS = {'parquet', 'json', 'jsonl', 'ndjson', 'txt', 'csv', 'log'}

def allowed_file(filename):
    """检查文件扩展名是否允许"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def index():
    """主页"""
    return render_template('index.html')

@app.route('/api/file_info', methods=['POST'])
def get_file_info():
    """获取文件信息"""
    try:
        data = request.get_json()
        file_path = data.get('file_path')
        
        if not file_path or not os.path.exists(file_path):
            return jsonify({'error': '文件不存在'}), 400
        
        reader = FileReader(file_path)
        info = reader.get_file_info()
        
        return jsonify({
            'success': True,
            'data': info
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/read_data', methods=['POST'])
def read_data():
    """读取文件数据"""
    try:
        data = request.get_json()
        file_path = data.get('file_path')
        num_rows = data.get('num_rows', 10)
        start_row = data.get('start_row', 0)
        
        if not file_path or not os.path.exists(file_path):
            return jsonify({'error': '文件不存在'}), 400
        
        reader = FileReader(file_path)
        
        if start_row == 0:
            # 读取前 N 行
            result = reader.read_top_rows(num_rows)
        else:
            # 读取指定范围的行
            result = reader.read_slice(start_row, num_rows)
        
        return jsonify({
            'success': True,
            'data': result
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/column_stats', methods=['POST'])
def get_column_stats():
    """获取列统计信息（仅支持parquet文件）"""
    try:
        data = request.get_json()
        file_path = data.get('file_path')
        
        if not file_path or not os.path.exists(file_path):
            return jsonify({'error': '文件不存在'}), 400
        
        reader = FileReader(file_path)
        stats = reader.get_column_stats()
        
        return jsonify({
            'success': True,
            'data': stats
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500







@app.route('/api/list_files', methods=['GET'])
def list_files():
    """列出指定目录下的 parquet 文件"""
    try:
        # 获取应用目录
        app_dir = os.path.dirname(os.path.abspath(__file__))
        
        # 获取请求参数，默认使用应用目录
        directory = request.args.get('directory', app_dir)
        
        # 安全检查：确保目录在允许的范围内
        if not os.path.exists(directory):
            directory = app_dir
        
        parquet_files = []
        directories = []
        
        try:
            for item in os.listdir(directory):
                # 过滤掉以.开头的隐藏文件和文件夹
                if item.startswith('.'):
                    continue
                    
                item_path = os.path.join(directory, item)
                
                if os.path.isdir(item_path):
                    # 添加目录
                    directories.append({
                        'name': item,
                        'path': item_path,
                        'type': 'directory'
                    })
                else:
                    # 添加所有文件（不支持的格式当作txt文件处理）
                    file_size = os.path.getsize(item_path)
                    parquet_files.append({
                        'name': item,
                        'path': item_path,
                        'size_mb': round(file_size / (1024 * 1024), 2),
                        'type': 'file'
                    })
        except PermissionError:
            # 如果没有权限访问目录，返回空列表
            pass
        
        # 排序：文件夹按文件夹名排序，文件按文件名排序
        directories.sort(key=lambda x: x['name'].lower())  # 文件夹按名称排序（忽略大小写）
        parquet_files.sort(key=lambda x: x['name'].lower())  # 文件按名称排序（忽略大小写）
        
        return jsonify({
            'success': True,
            'data': {
                'files': parquet_files,
                'directories': directories,
                'current_directory': directory
            }
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/upload_file', methods=['POST'])
def upload_file():
    """上传 parquet 文件"""
    try:
        # 检查是否有文件
        if 'file' not in request.files:
            return jsonify({
                'success': False,
                'error': '没有选择文件'
            }), 400
        
        file = request.files['file']
        
        # 检查文件名
        if file.filename == '':
            return jsonify({
                'success': False,
                'error': '没有选择文件'
            }), 400
        
        # 检查文件类型
        if not allowed_file(file.filename):
            return jsonify({
                'success': False,
                'error': '只允许上传 .parquet 文件'
            }), 400
        
        # 生成安全的文件名
        filename = secure_filename(file.filename)
        # 添加唯一标识符避免文件名冲突
        unique_filename = f"{uuid.uuid4().hex}_{filename}"
        
        # 保存文件
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], unique_filename)
        file.save(file_path)
        
        return jsonify({
            'success': True,
            'data': {
                'file_path': file_path,
                'original_name': filename,
                'size_mb': round(os.path.getsize(file_path) / (1024 * 1024), 2)
            }
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'文件上传失败: {str(e)}'
        }), 500

@app.route('/uploads/<filename>')
def uploaded_file(filename):
    """提供上传文件的访问"""
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001) 