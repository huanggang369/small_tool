#!/usr/bin/env python3
"""
通用文件读取器，支持多种文件格式
"""

import os
import json
import pandas as pd
from typing import Dict, Any, List, Optional
import pyarrow.parquet as pq


class FileReader:
    """通用文件读取器"""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.file_type = self._detect_file_type()
    
    def _detect_file_type(self) -> str:
        """检测文件类型"""
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"文件不存在: {self.file_path}")
        
        # 获取文件扩展名
        _, ext = os.path.splitext(self.file_path)
        ext = ext.lower()
        
        if ext == '.parquet':
            return 'parquet'
        elif ext in ['.json', '.jsonl', '.ndjson']:
            return 'json'
        elif ext in ['.txt', '.csv', '.log']:
            return 'text'
        else:
            # 所有其他格式都当作txt文件处理
            return 'text'
    
    def get_file_info(self) -> Dict[str, Any]:
        """获取文件基本信息"""
        try:
            file_size = os.path.getsize(self.file_path)
            file_size_mb = round(file_size / (1024 * 1024), 2)
            
            if self.file_type == 'parquet':
                return self._get_parquet_info()
            elif self.file_type == 'json':
                return self._get_json_info()
            else:
                return self._get_text_info()
                
        except Exception as e:
            raise Exception(f"获取文件信息失败: {str(e)}")
    
    def _get_parquet_info(self) -> Dict[str, Any]:
        """获取parquet文件信息"""
        parquet_file = pq.ParquetFile(self.file_path)
        metadata = parquet_file.metadata
        
        # 获取压缩信息
        compression_info = self._get_compression_info(parquet_file)
        
        return {
            "file_type": "parquet",
            "file_size_mb": round(os.path.getsize(self.file_path) / (1024 * 1024), 2),
            "total_rows": metadata.num_rows,
            "num_columns": len(metadata.schema.names),
            "columns": metadata.schema.names,
            "row_groups": metadata.num_row_groups,
            "compression": compression_info
        }
    
    def _get_json_info(self) -> Dict[str, Any]:
        """获取JSON文件信息"""
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # 尝试解析为JSON
            json_data = self._parse_json_file()
            
            if isinstance(json_data, list):
                total_rows = len(json_data)
                if total_rows > 0:
                    columns = list(json_data[0].keys()) if isinstance(json_data[0], dict) else []
                else:
                    columns = []
            else:
                total_rows = 1
                columns = list(json_data.keys()) if isinstance(json_data, dict) else []
            
            return {
                "file_type": "json",
                "file_size_mb": round(os.path.getsize(self.file_path) / (1024 * 1024), 2),
                "total_rows": total_rows,
                "num_columns": len(columns),
                "columns": columns,
                "row_groups": 1,
                "compression": {"overall_compression": "UNCOMPRESSED", "has_compression": False}
            }
        except Exception as e:
            raise Exception(f"解析JSON文件失败: {str(e)}")
    
    def _get_text_info(self) -> Dict[str, Any]:
        """获取文本文件信息"""
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            return {
                "file_type": "text",
                "file_size_mb": round(os.path.getsize(self.file_path) / (1024 * 1024), 2),
                "total_rows": len(lines),
                "num_columns": 1,
                "columns": ["content"],
                "row_groups": 1,
                "compression": {"overall_compression": "UNCOMPRESSED", "has_compression": False}
            }
        except Exception as e:
            raise Exception(f"读取文本文件失败: {str(e)}")
    
    def _get_compression_info(self, parquet_file) -> Dict[str, Any]:
        """获取压缩信息（仅用于parquet文件）"""
        metadata = parquet_file.metadata
        compression_info = {
            "overall_compression": "unknown",
            "column_compression": {},
            "has_compression": False
        }
        
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
            
            if len(compression_types) == 1:
                compression_info["overall_compression"] = list(compression_types)[0]
            elif len(compression_types) > 1:
                compression_info["overall_compression"] = "mixed"
            else:
                compression_info["overall_compression"] = "UNCOMPRESSED"
        
        return compression_info
    
    def read_top_rows(self, num_rows: int = 10) -> List[Dict[str, Any]]:
        """读取前N行数据"""
        try:
            if self.file_type == 'parquet':
                return self._read_parquet_top_rows(num_rows)
            elif self.file_type == 'json':
                return self._read_json_top_rows(num_rows)
            else:
                return self._read_text_top_rows(num_rows)
        except Exception as e:
            raise Exception(f"读取数据失败: {str(e)}")
    
    def _read_parquet_top_rows(self, num_rows: int) -> List[Dict[str, Any]]:
        """读取parquet文件前N行"""
        parquet_file = pq.ParquetFile(self.file_path)
        table = parquet_file.read_row_group(0)
        df = table.to_pandas()
        
        # 转换为字典列表
        result = df.head(num_rows).to_dict('records')
        return self._serialize_data(result)
    
    def _read_json_top_rows(self, num_rows: int) -> List[Dict[str, Any]]:
        """读取JSON文件前N行"""
        json_data = self._parse_json_file()
        
        if isinstance(json_data, list):
            # 多行JSON文件
            return json_data[:num_rows]
        else:
            # 单行JSON文件
            return [json_data]
    
    def _read_text_top_rows(self, num_rows: int) -> List[Dict[str, Any]]:
        """读取文本文件前N行"""
        with open(self.file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        result = []
        for i, line in enumerate(lines[:num_rows]):
            result.append({
                "line_number": i + 1,
                "content": line.rstrip('\n')
            })
        
        return result
    
    def _parse_json_file(self) -> Any:
        """解析JSON文件"""
        with open(self.file_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
        
        # 尝试解析为单行JSON
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            pass
        
        # 尝试解析为多行JSON
        try:
            lines = content.split('\n')
            result = []
            for line in lines:
                line = line.strip()
                if line:
                    result.append(json.loads(line))
            return result
        except json.JSONDecodeError:
            raise Exception("无法解析JSON文件格式")
    
    def _serialize_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """处理数据，确保可以JSON序列化"""
        def convert_value(value):
            if pd.isna(value):
                return None
            elif isinstance(value, (int, float, str, bool)):
                return value
            elif isinstance(value, (pd.Timestamp, pd.DatetimeTZDtype)):
                return str(value)
            elif hasattr(value, 'item'):  # numpy类型
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
        """获取列统计信息（仅支持parquet文件）"""
        if self.file_type != 'parquet':
            return {}
        
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
                    "unique_count": None,
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
                    col_chunk = row_group.column(i)
                    
                    if col_chunk.statistics:
                        stats_info = col_chunk.statistics
                        if hasattr(stats_info, 'null_count') and stats_info.null_count is not None:
                            total_null_count += stats_info.null_count
                        
                        if hasattr(stats_info, 'min') and stats_info.min is not None:
                            min_values.append(stats_info.min)
                        if hasattr(stats_info, 'max') and stats_info.max is not None:
                            max_values.append(stats_info.max)
                    
                    total_size += col_chunk.total_compressed_size
                    
                    if column_stats["compression"] is None:
                        column_stats["compression"] = col_chunk.compression
                
                # 更新统计信息
                column_stats["null_count"] = total_null_count
                column_stats["total_size"] = total_size
                
                if min_values:
                    column_stats["min_value"] = min(min_values)
                if max_values:
                    column_stats["max_value"] = max(max_values)
                
                # 尝试获取示例值
                try:
                    if metadata.num_row_groups > 0:
                        first_rg = metadata.row_group(0)
                        if first_rg.num_rows > 0:
                            sample_data = self.read_top_rows(min(5, first_rg.num_rows))
                            if sample_data and column_name in sample_data[0]:
                                sample_values = []
                                for row in sample_data:
                                    if column_name in row and row[column_name] is not None:
                                        val = row[column_name]
                                        if hasattr(val, 'item'):
                                            sample_values.append(val.item())
                                        else:
                                            sample_values.append(val)
                                        if len(sample_values) >= 3:
                                            break
                                column_stats["sample_values"] = sample_values
                except:
                    pass
                
                stats[column_name] = column_stats
            
            return stats
            
        except Exception as e:
            raise Exception(f"获取列统计信息失败: {str(e)}")
    
    def read_slice(self, start_row: int, num_rows: int) -> List[Dict[str, Any]]:
        """读取指定范围的数据（仅支持parquet文件）"""
        if self.file_type != 'parquet':
            return self.read_top_rows(num_rows)
        
        try:
            parquet_file = pq.ParquetFile(self.file_path)
            table = parquet_file.read_row_group(0)
            df = table.to_pandas()
            
            # 转换为字典列表
            result = df.iloc[start_row:start_row + num_rows].to_dict('records')
            return self._serialize_data(result)
            
        except Exception as e:
            raise Exception(f"读取切片数据失败: {str(e)}")


def main():
    """测试函数"""
    import sys
    
    if len(sys.argv) != 2:
        print("用法: python file_reader.py <file_path>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    try:
        reader = FileReader(file_path)
        
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