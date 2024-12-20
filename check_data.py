import json
import logging
import os
from collections import defaultdict
from typing import Dict, List, Tuple
from glob import glob

from pydantic import BaseModel, ValidationError
from charset_mnbvc import api

from data_types import CodeData, ForumData, ParallelData, QaData, MultiQaData, \
    CommonData, MultiModelDataModel, CommitDataModel, expected_fields


class DataChecker:
    def __init__(self) -> None:
        self.type_list: List[BaseModel] = [QaData, CodeData, ForumData, ParallelData,
                                           CommonData, MultiModelDataModel, CommitDataModel]
        self.max_file_size = 512 * 1024 * 1024

    def check_file_size(self, file_path):
        file_size = os.path.getsize(file_path)
        if file_size > self.max_file_size:
            logger.error(f"文件大小超出限制, 最大限制为{self.max_file_size/1024/1024}MB, 文件大小为{file_size/1024/1024:.2f}MB.")
            raise ValueError(f"文件大小超出限制, 最大限制为{self.max_file_size/1024/1024}MB, 文件大小为{file_size/1024/1024:.2f}MB.")

    def read_head(self, dataset_path: str, k: int):
        with open(dataset_path, 'r', encoding='utf-8') as f:
            for idx, line in enumerate(f):
                if k is not None and k > 0 and idx >= k:
                    break
                yield json.loads(line)

    def read_parquet_head(self, dataset_path: str, k: int):
        parquet_file = pq.ParquetFile(dataset_path)
        columns = parquet_file.schema.names
        data = []
        for idx in range(parquet_file.num_row_groups):
            if k is not None and k > 0 and idx >= k:
                break
            table = parquet_file.read_row_group(idx).to_pandas()
            # table = row_group.to_table(schema=parquet_file.schema, columns=columns)
            for _, row in table.iterrows():
                row_data = {column: row[column] for column in columns}
            yield row_data

    def get_keys(self, data: Dict, prefix='', depth=1, max_depth=1):
        if depth > max_depth:
            return []
        keys = []
        for key, value in data.items():
            if prefix:
                key = f"{prefix}.{key}"
            keys.append(key)
            if isinstance(value, dict):
                sub_keys = self.get_keys(value, prefix=key, depth=depth+1, max_depth=max_depth)
                keys = keys + sub_keys
        return set(keys)
    
    def check_language_ratio(self, text: bytes) -> Tuple[int, float]:
        '''检查中英文比例，要求输入为 bytes'''
        ret, percentage = api.check_zh_en(text)
        return ret, percentage

    def get_data_type(self, data: Dict) -> Tuple[BaseModel, float]:
        type_cls = None
        for data_type in self.type_list:
            try:
                data_type(**data)
                type_cls = data_type
            except:
                continue
        
        if type_cls:
            return type_cls, 1.0
        
        keys = self.get_keys(data, depth=1)
        type_match_scores = []
        for data_type in self.type_list:
            type_keys = set(data_type.model_fields.keys())
            distance = len(type_keys - keys) + len(keys - type_keys)
            score = 1 - (distance / len((type_keys | keys)))
            type_match_scores.append((data_type, score))
        type_match_scores = sorted(type_match_scores, key=lambda x: x[1], reverse=True)
        type_cls, score = type_match_scores[0]
        if 'id' in data and isinstance(data['id'], str):
            type_cls = MultiQaData
        return type_cls, score

    def parser_errors(self, validation_error: ValidationError):
        error_map = defaultdict(set)
        for error in validation_error.errors():
            if isinstance(error['loc'], str):
                error_info = error['loc']
            else:
                error_info = '.'.join(filter(lambda x: isinstance(x, str), error['loc']))
            error_map[error['type']].add(error_info)
        errors = []
        for key, values in error_map.items():
            error_info = ", ".join(list(values)[:3])
            if len(values) >= 3:
                error_info += ', ...'
            if key == 'missing':
                errors.append(f'丢失部分字段, 丢失字段为: [{error_info}]')
            elif 'type' in key:
                expected_type, _ = key.split('_')
                errors.append(f'数据类型错误, 错误的 keys: [{error_info}], 可接受的类型 `{expected_type}`')
            else:
                errors.append(f'其他错误, 错误的 keys: [{error_info}], 错误信息: {key}')
        return '; '.join(errors)
    
    def check_line(
        self, 
        data: Dict, 
        type_cls: BaseModel,
    ):
        try:
            type_cls(**data)
            return True, ''
        except ValidationError as e:
            error_info = self.parser_errors(e)
            return False, error_info

    def check_file(self, dataset_path: str, k: int):
        logger.info(f'开始检查: {dataset_path}')
        self.check_file_size(dataset_path)
        if dataset_path.endswith('.parquet'):
            self.check_parquet(dataset_path, k)
        else:
            self.check_jsonl(dataset_path, k)

    def check_jsonl(self, dataset_path: str, k: int):
        dataset_name = os.path.basename(dataset_path)
        datasets = self.read_head(dataset_path, k)
        right_num_line = 0
        num_line = 0
        zh_en_num = 0
        perc_sum = 0

        not_zh_en_line = ''
        for idx, line_data in enumerate(datasets):
            num_line += 1
            line_data_bytes = json.dumps(line_data, ensure_ascii=False).encode()
            ret, perc = self.check_language_ratio(line_data_bytes)
            perc_sum += perc
            if ret:
                zh_en_num += 1
            else:
                not_zh_en_line = line_data

            if idx == 0:
                first = line_data
                type_cls, score = self.get_data_type(first)        
                if score == 1.0:
                    logger.info(f"数据集 {dataset_name} 的语料类型为 {type_cls.name()}")
                elif score > 0:
                    logger.warning(f"不符合任意一个语料类型, 数据集 {dataset_name} 最接近的语料类型为 {type_cls.name()}, 相似的为 {score:.4f}.")
                else:
                    logger.error(f"数据集 {dataset_name} 不符合任意一个语料类型，也没有相近的语料类型.")
            is_matched, info = self.check_line(line_data, type_cls)
            if not is_matched:
                logger.error(f"数据集 {dataset_name} 错误, 行号: {idx}, 错误信息: {info}")
            else:
                right_num_line += 1
        
        text_percent = perc_sum / num_line
        logger.info(f"数据集 {dataset_name} 检查完毕, 正确行数 {right_num_line} / 总行数 {idx + 1}")
        logger.info(f"检查每行信息是否为中文或英文信息，总共检查{num_line}条，中英文数据{zh_en_num}条，中英文行数占比{zh_en_num/num_line*100:.2f}%, "
                    f"中英文文本总量{text_percent:.2f}%")
        if not_zh_en_line:
            logger.info(f"非中英文示例数据: {str(not_zh_en_line)[:1000]}")
    
    def check_parquet(self, dataset_path: str, k: int):
        dataset_name = os.path.basename(dataset_path)
        schema = pq.read_schema(dataset_path) 
        actual_fields = {field.name: str(field.type) for field in schema}  
    
        lost_keys = []
        not_match_keys = []
        for name, expected_type in expected_fields.items():
            if name not in actual_fields:
                lost_keys.append(f"丢失的字段: {name}")
                continue
            if actual_fields[name] != expected_type and actual_fields[name] != "null":
                not_match_keys.append(f"字段 {name} 数据格式不符合, 允许的类型: {expected_type}, 实际类型: {actual_fields[name]}"  )  

        if len(lost_keys) or len(not_match_keys):
            error = '\n'.join(lost_keys + not_match_keys)
            logger.warning(error)
        logger.info(f"数据集抬头 {dataset_name} 检查完毕, schema 格式正确")

        datasets = self.read_parquet_head(dataset_path, k)
        right_num_line = 0
        num_line = 0
        zh_en_num = 0
        perc_sum = 0

        not_zh_en_line = ''
        for idx, line_data in enumerate(datasets):
            num_line += 1
            line_data_bytes = json.dumps(line_data, ensure_ascii=False).encode()
            ret, perc = self.check_language_ratio(line_data_bytes)
            perc_sum += perc
            if ret:
                zh_en_num += 1
            else:
                not_zh_en_line = line_data

            if idx == 0:
                first = line_data
                type_cls, score = self.get_data_type(first)        
                if score == 1.0:
                    logger.info(f"数据集 {dataset_name} 格式为 {type_cls.name()}, 符合预期")
                elif score > 0:
                    logger.warning(f"不符合任意一个语料类型, 数据集 {dataset_name} 最接近的语料类型为 {type_cls.name()}, 相似的为 {score:.4f}.")
                else:
                    logger.error(f"数据集 {dataset_name} 不符合任意一个语料类型，也没有相近的语料类型.")
            is_matched, info = self.check_line(line_data, type_cls)
            if not is_matched:
                logger.error(f"数据集 {dataset_name} 错误, 行号: {idx}, 错误信息: {info}")
            else:
                right_num_line += 1

        text_percent = perc_sum / num_line
        logger.info(f"数据集 {dataset_name} 检查完毕, 正确行数 {right_num_line} / 总行数 {idx + 1}")
        logger.info(f"检查每行信息是否为中文或英文信息，总共检查{num_line}条，中英文数据{zh_en_num}条，中英文行数占比{zh_en_num/num_line*100:.2f}%, "
                    f"中英文文本总量{text_percent:.2f}%")
        if not_zh_en_line:
            logger.info(f"非中英文示例数据: {str(not_zh_en_line)[:1000]}")



    def check_folder(
        self, 
        dataset_dir: str, 
        k: int=10
    ):
        for file_path in sorted(glob(f"{dataset_dir}/*.json*")):
            if not os.path.isfile(file_path):
                continue
            self.check_file(file_path, k = k)

        for file_path in sorted(glob(f"{dataset_dir}/*.parquet")):
            if not os.path.isfile(file_path):
                continue
            self.check_file(file_path, k = k)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', type=str, required=True, help='数据集目录或路径')
    parser.add_argument('--k', type=int, required=False, help='检查前 k 行, 默认检查所有行')
    parser.add_argument('--use_pyarrow', action='store_true', help='检查 parquet 文件时选择')
    args = parser.parse_args()
    
    if args.use_pyarrow:
        import pyarrow.parquet as pq

    log_dir = './logs/'
    os.makedirs(log_dir, exist_ok=True)
    fh = logging.FileHandler(os.path.join(log_dir, 'check_log.txt'))
    fh.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)


    logger = logging.getLogger(name='__checker__')
    logger.setLevel(logging.INFO)
    logger.addHandler(fh)
    if args.k:
        logger.addHandler(ch)

    dk = DataChecker()
    if os.path.isfile(args.dataset):
        dk.check_file(args.dataset, k=args.k)
    elif os.path.isdir(args.dataset):
        dk.check_folder(args.dataset, k=args.k)

