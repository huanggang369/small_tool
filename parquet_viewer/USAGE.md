# Parquet 文件查看器使用说明

## 快速开始

### 1. 启动应用

```bash
cd parquet_viewer
./start_viewer.sh
```

或者手动启动：

```bash
cd parquet_viewer
pip3 install -r requirements.txt
python3 app.py
```

### 2. 访问界面

打开浏览器访问：http://localhost:5001

## 功能详解

### 文件选择方式

#### 方式一：直接输入路径
在文件路径输入框中输入 parquet 文件的完整路径，然后点击"加载"按钮。

#### 方式二：文件上传
1. 点击"浏览"按钮
2. 在弹出的文件选择对话框中选择 .parquet 文件
3. 文件会自动上传并加载

#### 方式三：目录浏览
1. 在文件列表区域可以看到当前目录下的所有文件夹和 parquet 文件
2. 点击文件夹卡片上的"打开"按钮进入子目录
3. 点击"上级目录"按钮返回父目录
4. 找到目标文件后，点击文件卡片上的"选择"按钮

### 数据查看

#### 表格视图
- 以表格形式显示数据
- 支持滚动查看大量数据
- 自动处理 null 值显示

#### JSON 视图
- 以 JSON 格式显示数据
- 便于查看数据结构
- 支持复制 JSON 数据

#### 列统计
- 显示每列的数据类型
- 统计空值数量
- 显示唯一值数量
- 提供示例值

### 文件信息

加载文件后会显示详细的文件信息：

- **文件大小**: 显示文件的实际大小
- **总行数**: 文件包含的数据行数
- **列数**: 数据表的列数
- **Row Groups**: parquet 文件的 row group 数量
- **压缩格式**: 自动检测并显示压缩类型（SNAPPY、GZIP、UNCOMPRESSED 等）
- **压缩详情**: 显示每列使用的压缩方式

### 性能优化

#### 切片读取
- 只读取需要的行数，避免加载整个大文件
- 支持指定起始行和行数
- 优先读取前几个 row group，减少 I/O 操作

#### 内存管理
- 自动处理大文件，避免内存溢出
- 支持最大 500MB 文件上传
- 智能数据类型转换，确保 JSON 序列化

## 常见问题

### Q: 为什么看不到文件？
A: 检查以下几点：
1. 确保文件是 .parquet 格式
2. 检查文件路径是否正确
3. 确保有文件读取权限

### Q: 大文件加载很慢？
A: 
1. 减少显示行数（如从 500 行改为 10 行）
2. 检查文件是否使用了合适的压缩格式
3. 确保网络连接稳定

### Q: 文件上传失败？
A:
1. 检查文件大小是否超过 500MB 限制
2. 确保文件是 .parquet 格式
3. 检查磁盘空间是否充足

### Q: 压缩信息显示不正确？
A:
1. 确保文件是标准的 parquet 格式
2. 检查文件是否损坏
3. 尝试使用其他 parquet 文件测试

## 高级功能

### 命令行使用

除了 Web 界面，还可以直接使用命令行：

```bash
python3 parquet_reader.py /path/to/file.parquet
```

### API 接口

应用提供了完整的 REST API：

- `GET /api/list_files` - 获取文件列表
- `POST /api/upload_file` - 上传文件
- `POST /api/file_info` - 获取文件信息
- `POST /api/read_data` - 读取数据
- `POST /api/column_stats` - 获取列统计

### 自定义配置

可以修改 `app.py` 中的配置：

```python
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # 修改最大文件大小
app.config['UPLOAD_FOLDER'] = 'uploads'  # 修改上传目录
```

## 技术支持

如果遇到问题，可以：

1. 查看浏览器控制台的错误信息
2. 检查应用日志输出
3. 运行测试脚本验证功能：
   ```bash
   python3 test_reader.py      # 基础功能测试
   python3 test_snappy.py      # 压缩文件测试
   python3 test_browse.py      # 文件浏览测试
   ``` 