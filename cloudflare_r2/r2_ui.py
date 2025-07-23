from flask import Flask, render_template, request, jsonify, session
import boto3
import json
from datetime import datetime
import os

app = Flask(__name__)
app.secret_key = 'r2_ui_secret_key_2024'  # 用于session

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/save_config', methods=['POST'])
def save_config():
    try:
        config = {
            'endpoint_url': request.form.get('endpoint_url'),
            'aws_access_key_id': request.form.get('aws_access_key_id'),
            'aws_secret_access_key': request.form.get('aws_secret_access_key'),
            'bucket': request.form.get('bucket')
        }
        
        # 保存到session
        session['r2_config'] = config
        
        return jsonify({
            'success': True,
            'message': '配置已保存'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/get_config', methods=['GET'])
def get_config():
    config = session.get('r2_config', {})
    return jsonify(config)

@app.route('/list_objects', methods=['POST'])
def list_objects():
    try:
        # 获取表单数据
        endpoint_url = request.form.get('endpoint_url')
        aws_access_key_id = request.form.get('aws_access_key_id')
        aws_secret_access_key = request.form.get('aws_secret_access_key')
        bucket = request.form.get('bucket')
        prefix = request.form.get('prefix', '')
        
        # 验证必需参数
        if not all([endpoint_url, aws_access_key_id, aws_secret_access_key, bucket]):
            return jsonify({
                'success': False,
                'error': '请填写所有必需参数'
            })
        
        # 创建S3客户端 - 为Cloudflare R2配置SigV4
        s3_client = boto3.client('s3',
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name='auto',
            config=boto3.session.Config(
                signature_version='s3v4'
            )
        )
        
        # 列出对象
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        # 检查是否超过1000个对象
        is_truncated = response.get('IsTruncated', False)
        total_count = len(response.get('Contents', []))
        
        # 处理结果
        if 'Contents' in response:
            objects = []
            folders = set()
            
            for obj in response['Contents']:
                key = obj['Key']
                
                # 跳过当前目录本身
                if key == prefix:
                    continue
                
                # 检查是否是文件夹（包含子路径）
                if '/' in key[len(prefix):]:
                    # 提取文件夹路径
                    folder_path = prefix + key[len(prefix):].split('/')[0] + '/'
                    
                    if folder_path not in folders:
                        folders.add(folder_path)
                        objects.append({
                            'key': folder_path,
                            'name': folder_path.rstrip('/').split('/')[-1] + '/',
                            'type': 'folder',
                            'size': 0,
                            'size_mb': 0,
                            'last_modified': None
                        })
                else:
                    # 文件
                    objects.append({
                        'key': obj['Key'],
                        'name': obj['Key'].split('/')[-1],
                        'type': 'file',
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'].isoformat() if obj['LastModified'] else None,
                        'size_mb': round(obj['Size'] / (1024 * 1024), 2)
                    })
            
            return jsonify({
                'success': True,
                'count': len(objects),
                'objects': objects,
                'is_truncated': is_truncated,
                'total_count': total_count
            })
        else:
            return jsonify({
                'success': True,
                'count': 0,
                'objects': [],
                'message': '未找到对象或存储桶为空',
                'is_truncated': False,
                'total_count': 0
            })
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/download_file', methods=['POST', 'GET'])
def download_file():
    try:
        # 获取参数 - 支持GET和POST
        if request.method == 'POST':
            endpoint_url = request.form.get('endpoint_url')
            aws_access_key_id = request.form.get('aws_access_key_id')
            aws_secret_access_key = request.form.get('aws_secret_access_key')
            bucket = request.form.get('bucket')
            key = request.form.get('key')
            download_type = request.form.get('type', 'file')
        else:  # GET
            endpoint_url = request.args.get('endpoint_url')
            aws_access_key_id = request.args.get('aws_access_key_id')
            aws_secret_access_key = request.args.get('aws_secret_access_key')
            bucket = request.args.get('bucket')
            key = request.args.get('key')
            download_type = request.args.get('type', 'file')
        
        # 验证必需参数
        if not all([endpoint_url, aws_access_key_id, aws_secret_access_key, bucket, key]):
            return jsonify({
                'success': False,
                'error': '缺少必需参数'
            })
        
        # 创建S3客户端 - 为Cloudflare R2配置SigV4
        s3_client = boto3.client('s3',
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name='auto',
            config=boto3.session.Config(
                signature_version='s3v4'
            )
        )
        
        # 下载单个文件
        try:
            # 获取文件名
            filename = key.split('/')[-1] if '/' in key else key
            
            # 处理文件名编码问题
            try:
                safe_filename = filename.encode('latin-1').decode('latin-1')
            except UnicodeEncodeError:
                # 如果文件名包含非ASCII字符，使用安全的文件名
                import hashlib
                file_hash = hashlib.md5(filename.encode('utf-8')).hexdigest()[:8]
                file_ext = filename.split('.')[-1] if '.' in filename else 'txt'
                safe_filename = f"{file_hash}.{file_ext}"
            
            # 从S3获取对象
            response = s3_client.get_object(Bucket=bucket, Key=key)
            
            # 创建流式响应，让浏览器后台下载
            from flask import Response, stream_with_context
            
            def generate():
                # 分块读取和传输文件内容
                chunk_size = 8192  # 8KB chunks
                body = response['Body']
                
                while True:
                    chunk = body.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk
            
            return Response(
                stream_with_context(generate()),
                mimetype=response.get('ContentType', 'application/octet-stream'),
                headers={
                    'Content-Disposition': f'attachment; filename="{safe_filename}"',
                    'Content-Length': str(response.get('ContentLength', 0)),
                    'Cache-Control': 'no-cache',
                    'X-Accel-Buffering': 'no'  # 禁用nginx缓冲
                }
            )
        except Exception as e:
            return jsonify({
                'success': False,
                'error': f'下载文件失败: {str(e)}'
            })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000) 