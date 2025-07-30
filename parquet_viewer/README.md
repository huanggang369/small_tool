# Parquet 文件查看器

一个高效的 Parquet 文件查看工具，支持切片读取大文件，避免内存溢出。

## 功能特性

- 🚀 **高效读取**: 使用切片读取技术，避免读取整个大文件
- 📊 **多种视图**: 支持表格视图、JSON 视图和列统计
- 🎨 **现代化界面**: 美观的 Web 界面，支持响应式设计
- 📈 **文件信息**: 显示文件大小、行数、列数等详细信息
- 🔍 **列统计**: 提供每列的数据类型、空值数量、唯一值等统计信息
- 📁 **文件列表**: 自动扫描当前目录下的 parquet 文件
- 🗜️ **压缩支持**: 完全支持 snappy、gzip 等压缩格式，自动检测并显示压缩信息
- 📁 **文件系统浏览**: 类似文件系统的界面，支持路径导航、目录浏览和文件选择，紧凑布局设计

## 安装依赖

```bash
pip install -r requirements.txt
```

## 快速启动

### 方法一：使用启动脚本（推荐）

```bash
cd parquet_viewer
./start_viewer.sh
```

### 方法二：手动启动

```bash
cd parquet_viewer
pip install -r requirements.txt
python app.py
```

启动后访问: http://localhost:5001

## 使用方法

### Web 界面使用

1. **选择文件**: 
   - 在输入框中输入 parquet 文件路径
   - 点击"浏览"按钮选择本地文件
   - 从下方文件列表中选择文件
   - 使用目录浏览功能导航到文件位置

2. **文件系统浏览**:
   - **路径导航栏**: 紧凑的面包屑导航，显示当前路径，可点击快速跳转
   - **文件系统视图**: 类似文件管理器的界面，紧凑布局设计，每个项目高度32px
   - **目录浏览**: 点击文件夹图标进入子目录
   - **文件选择**: 点击文件图标选择并加载文件
   - **智能浏览按钮**: 根据输入路径自动浏览对应目录或文件所在目录

3. **设置行数**: 选择要显示的行数（10、50、100、500）

4. **查看数据**: 
   - **表格视图**: 以表格形式显示数据
   - **JSON 视图**: 以 JSON 格式显示数据
   - **列统计**: 查看每列的统计信息

## 技术实现

### 切片读取技术

使用 `pyarrow.parquet` 库实现高效的切片读取：

```python
# 读取前 N 行，避免读取整个文件
parquet_file = pq.ParquetFile(file_path)
table = parquet_file.read_row_group(0).slice(0, num_rows)
```

### 压缩支持

自动检测和显示 parquet 文件的压缩信息：

```python
# 获取压缩信息
metadata = parquet_file.metadata
row_group = metadata.row_group(0)
for i in range(row_group.num_columns):
    column = row_group.column(i)
    compression = column.compression  # SNAPPY, GZIP, UNCOMPRESSED 等
```

### 主要组件

- `parquet_reader.py`: 核心读取器，实现切片读取逻辑
- `app.py`: Flask Web 应用，提供 API 接口
- `templates/index.html`: 现代化 Web 界面

## API 接口

### 获取文件信息
```
POST /api/file_info
{
    "file_path": "/path/to/file.parquet"
}
```

### 读取数据
```
POST /api/read_data
{
    "file_path": "/path/to/file.parquet",
    "num_rows": 10,
    "start_row": 0
}
```

### 获取列统计
```
POST /api/column_stats
{
    "file_path": "/path/to/file.parquet"
}
```

### 列出文件
```
GET /api/list_files
```

## 命令行使用

也可以直接使用命令行工具：

```bash
python parquet_reader.py /path/to/file.parquet
```

## 性能优化

1. **内存优化**: 只读取需要的行数，避免加载整个文件
2. **Row Group 优化**: 优先读取前几个 row group，减少 I/O 操作
3. **数据类型处理**: 自动处理各种数据类型，确保 JSON 序列化
4. **错误处理**: 完善的错误处理和用户提示

## 系统要求

- Python 3.7+
- 依赖包：pandas, pyarrow, flask

## 注意事项

- 支持的文件格式：`.parquet`
- 最大文件大小：500MB（可在 app.py 中调整）
- 建议在本地网络环境下使用
- 大文件处理时请耐心等待

## 故障排除

1. **文件不存在**: 检查文件路径是否正确
2. **内存不足**: 减少显示行数
3. **端口占用**: 修改 app.py 中的端口号
4. **依赖问题**: 重新安装依赖包

## 开发说明

如需修改或扩展功能：

1. 修改 `parquet_reader.py` 添加新的读取方法
2. 在 `app.py` 中添加对应的 API 接口
3. 更新 `templates/index.html` 添加新的界面功能 