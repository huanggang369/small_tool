import pandas as pd
import pyarrow.parquet as pq
import json
from typing import Dict, List, Any, Optional
import os


class ParquetReader:
    """Parquet 文件读取器，支持切片读取避免读取整个大文件"""
    
    def __init__(self, file_path: str):
        """
        初始化 ParquetReader
        
        Args:
            file_path: parquet 文件路径
        """
        self.file_path = file_path
        self._validate_file()
    
    def _validate_file(self):
        """验证文件是否存在且为 parquet 格式"""
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"文件不存在: {self.file_path}")
        
        if not self.file_path.endswith('.parquet'):
            raise ValueError(f"文件不是 parquet 格式: {self.file_path}")
    
    def get_file_info(self) -> Dict[str, Any]:
        """
        获取 parquet 文件的基本信息，包括压缩信息
        
        Returns:
            包含文件信息的字典
        """
        try:
            # 使用 pyarrow 读取文件元数据，不加载数据
            parquet_file = pq.ParquetFile(self.file_path)
            
            # 获取文件大小
            file_size = os.path.getsize(self.file_path)
            
            # 获取行数
            num_rows = parquet_file.metadata.num_rows
            
            # 获取列信息
            schema = parquet_file.schema_arrow
            columns = [field.name for field in schema]
            
            # 获取压缩信息
            compression_info = self._get_compression_info(parquet_file)
            
            return {
                "file_path": self.file_path,
                "file_size_mb": round(file_size / (1024 * 1024), 2),
                "total_rows": num_rows,
                "columns": columns,
                "num_columns": len(columns),
                "row_groups": parquet_file.num_row_groups,
                "compression": compression_info
            }
        except Exception as e:
            raise Exception(f"读取文件信息失败: {str(e)}")
    
    def _get_compression_info(self, parquet_file) -> Dict[str, Any]:
        """
        获取文件的压缩信息
        
        Args:
            parquet_file: pyarrow ParquetFile 对象
            
        Returns:
            压缩信息字典
        """
        try:
            metadata = parquet_file.metadata
            compression_info = {
                "overall_compression": "unknown",
                "column_compression": {},
                "has_compression": False
            }
            
            # 检查每个 row group 的压缩信息
            if metadata.num_row_groups > 0:
                row_group = metadata.row_group(0)
                compression_types = set()
                
                for i in range(row_group.num_columns):
                    column = row_group.column(i)
                    compression = column.compression
                    column_name = column.path_in_schema
                    
                    if compression and compression != "UNCOMPRESSED":
                        compression_info["has_compression"] = True
                        compression_types.add(compression)
                        compression_info["column_compression"][column_name] = compression
                    else:
                        compression_info["column_compression"][column_name] = "UNCOMPRESSED"
                
                # 确定整体压缩类型
                if len(compression_types) == 1:
                    compression_info["overall_compression"] = list(compression_types)[0]
                elif len(compression_types) > 1:
                    compression_info["overall_compression"] = "mixed"
                else:
                    compression_info["overall_compression"] = "UNCOMPRESSED"
            
            return compression_info
            
        except Exception as e:
            return {
                "overall_compression": "unknown",
                "column_compression": {},
                "has_compression": False,
                "error": str(e)
            }
    
    def read_top_rows(self, num_rows: int = 10) -> List[Dict[str, Any]]:
        """
        读取文件前 N 行数据，使用切片读取避免读取整个文件
        
        Args:
            num_rows: 要读取的行数，默认 10
            
        Returns:
            包含数据的字典列表
        """
        try:
            # 使用 pyarrow 读取前 N 行
            parquet_file = pq.ParquetFile(self.file_path)
            
            # 读取前 num_rows 行
            table = parquet_file.read_row_group(0).slice(0, num_rows)
            
            # 如果第一个 row group 不够，继续读取后续的
            if table.num_rows < num_rows:
                remaining_rows = num_rows - table.num_rows
                for i in range(1, min(parquet_file.num_row_groups, 3)):  # 最多读取前3个row group
                    if remaining_rows <= 0:
                        break
                    next_table = parquet_file.read_row_group(i).slice(0, remaining_rows)
                    table = table.concat_tables([next_table])
                    remaining_rows -= next_table.num_rows
            
            # 转换为 pandas DataFrame
            df = table.to_pandas()
            
            # 转换为字典列表
            result = df.head(num_rows).to_dict('records')
            
            # 处理数据类型，确保可以 JSON 序列化
            return self._serialize_data(result)
            
        except Exception as e:
            raise Exception(f"读取数据失败: {str(e)}")
    
    def read_slice(self, start_row: int, num_rows: int) -> List[Dict[str, Any]]:
        """
        读取指定范围的行数据
        
        Args:
            start_row: 起始行号（从0开始）
            num_rows: 要读取的行数
            
        Returns:
            包含数据的字典列表
        """
        try:
            parquet_file = pq.ParquetFile(self.file_path)
            
            # 计算需要读取的 row groups
            current_row = 0
            tables = []
            
            for i in range(parquet_file.num_row_groups):
                row_group = parquet_file.read_row_group(i)
                row_group_size = row_group.num_rows
                
                # 检查这个 row group 是否包含我们需要的行
                if current_row + row_group_size > start_row:
                    # 计算在这个 row group 中的起始和结束位置
                    local_start = max(0, start_row - current_row)
                    local_end = min(row_group_size, start_row + num_rows - current_row)
                    
                    if local_end > local_start:
                        slice_table = row_group.slice(local_start, local_end - local_start)
                        tables.append(slice_table)
                
                current_row += row_group_size
                
                # 如果已经读取了足够的行，就停止
                if len(tables) > 0 and sum(t.num_rows for t in tables) >= num_rows:
                    break
            
            if not tables:
                return []
            
            # 合并所有表格
            if len(tables) == 1:
                table = tables[0]
            else:
                table = tables[0].concat_tables(tables[1:])
            
            # 转换为 pandas DataFrame
            df = table.to_pandas()
            
            # 转换为字典列表
            result = df.head(num_rows).to_dict('records')
            
            return self._serialize_data(result)
            
        except Exception as e:
            raise Exception(f"读取切片数据失败: {str(e)}")
    
    def _serialize_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        处理数据，确保可以 JSON 序列化
        
        Args:
            data: 原始数据
            
        Returns:
            处理后的数据
        """
        def convert_value(value):
            """转换单个值，确保可以 JSON 序列化"""
            if pd.isna(value):
                return None
            elif isinstance(value, (int, float, str, bool)):
                return value
            elif isinstance(value, (pd.Timestamp, pd.DatetimeTZDtype)):
                return str(value)
            elif hasattr(value, 'item'):  # numpy 类型
                return value.item()
            else:
                return str(value)
        
        result = []
        for row in data:
            converted_row = {}
            for key, value in row.items():
                converted_row[str(key)] = convert_value(value)
            result.append(converted_row)
        
        return result
    
    def get_column_stats(self) -> Dict[str, Any]:
        """
        从parquet文件元数据获取列统计信息
        
        Returns:
            列统计信息
        """
        try:
            parquet_file = pq.ParquetFile(self.file_path)
            metadata = parquet_file.metadata
            
            stats = {}
            
            # 获取schema信息
            schema = metadata.schema
            
            # 遍历所有列
            for i, column_name in enumerate(schema.names):
                column = schema.column(i)
                
                # 获取列的基本信息
                column_stats = {
                    "dtype": str(column.physical_type),
                    "logical_type": str(column.logical_type) if column.logical_type else None,
                    "null_count": 0,
                    "unique_count": None,  # 元数据中通常不包含唯一值统计
                    "sample_values": [],
                    "compression": None,
                    "total_size": 0,
                    "min_value": None,
                    "max_value": None
                }
                
                # 从所有row groups中收集统计信息
                total_null_count = 0
                total_size = 0
                min_values = []
                max_values = []
                
                for rg_idx in range(metadata.num_row_groups):
                    row_group = metadata.row_group(rg_idx)
                    
                    # 获取该列在row group中的统计信息
                    col_chunk = row_group.column(i)
                    
                    # 累加空值数量
                    if col_chunk.statistics:
                        stats_info = col_chunk.statistics
                        if hasattr(stats_info, 'null_count'):
                            total_null_count += stats_info.null_count
                        
                        # 收集最小值和最大值
                        if hasattr(stats_info, 'min') and stats_info.min is not None:
                            min_values.append(stats_info.min)
                        if hasattr(stats_info, 'max') and stats_info.max is not None:
                            max_values.append(stats_info.max)
                    
                    # 累加大小
                    total_size += col_chunk.total_compressed_size
                    
                    # 获取压缩信息
                    if column_stats["compression"] is None:
                        column_stats["compression"] = col_chunk.compression
                
                # 更新统计信息
                column_stats["null_count"] = total_null_count
                column_stats["total_size"] = total_size
                
                # 计算全局最小值和最大值
                if min_values:
                    column_stats["min_value"] = min(min_values)
                if max_values:
                    column_stats["max_value"] = max(max_values)
                
                # 尝试获取一些示例值（从第一个row group）
                try:
                    if metadata.num_row_groups > 0:
                        first_rg = metadata.row_group(0)
                        if first_rg.num_rows > 0:
                            # 读取前几行来获取示例值
                            sample_data = self.read_top_rows(min(5, first_rg.num_rows))
                            if sample_data and column_name in sample_data[0]:
                                sample_values = []
                                for row in sample_data:
                                    if column_name in row and row[column_name] is not None:
                                        val = row[column_name]
                                        if hasattr(val, 'item'):  # numpy类型
                                            sample_values.append(val.item())
                                        else:
                                            sample_values.append(val)
                                        if len(sample_values) >= 3:  # 最多3个示例值
                                            break
                                column_stats["sample_values"] = sample_values
                except:
                    # 如果获取示例值失败，忽略
                    pass
                
                stats[column_name] = column_stats
            
            return stats
            
        except Exception as e:
            raise Exception(f"获取列统计信息失败: {str(e)}")


def main():
    """测试函数"""
    import sys
    
    if len(sys.argv) != 2:
        print("用法: python parquet_reader.py <parquet_file_path>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    try:
        reader = ParquetReader(file_path)
        
        # 获取文件信息
        info = reader.get_file_info()
        print("文件信息:")
        print(json.dumps(info, indent=2, ensure_ascii=False))
        
        # 读取前 10 行
        print("\n前 10 行数据:")
        data = reader.read_top_rows(10)
        print(json.dumps(data, indent=2, ensure_ascii=False))
        
    except Exception as e:
        print(f"错误: {e}")


if __name__ == "__main__":
    main() 