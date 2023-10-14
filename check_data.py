import json
import logging
import os
from collections import defaultdict
from typing import Dict, List, Tuple
from glob import glob

from pydantic import BaseModel, ValidationError

from data_types import CodeData, ForumData, ParallelData, QaData


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(name='__checker__')


class DataChecker:
    def __init__(self) -> None:
        self.type_list: List[BaseModel] = [QaData, CodeData, ForumData, ParallelData]

    def read_head(self, dataset_path: str, k: int = 100):
        with open(dataset_path, 'r', encoding='utf-8') as f:
            for idx, line in enumerate(f):
                if k > 0 and idx >= k:
                    break
                yield json.loads(line)
    
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
            type_keys = set(data_type.__fields__.keys())
            distance = len(type_keys - keys) + len(keys - type_keys)
            score = 1 - (distance / len((type_keys | keys)))
            type_match_scores.append((data_type, score))
        type_match_scores = sorted(type_match_scores, key=lambda x: x[1], reverse=True)
        return type_match_scores[0]

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
                errors.append(f'missing error, missing keys: [{error_info}]')
            elif 'type' in key:
                expected_type, _ = key.split('_')
                errors.append(f'type error, error keys: [{error_info}], expected type `{expected_type}`')
            else:
                errors.append(f'other error, error keys: [{error_info}]')
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

    def check_file(self, dataset_path: str, k: int=10):
        logger.info(f'checking dataset: {dataset_path}')
        dataset_name = os.path.basename(dataset_path)
        datasets = self.read_head(dataset_path, k)
        right_num_line = 0
        for idx, line_data in enumerate(datasets):
            if idx == 0:
                first = line_data
                type_cls, score = self.get_data_type(first)        
                if score == 1.0:
                    logger.info(f"the type of dataset {dataset_name} is {type_cls.name()}")
                elif score > 0:
                    logger.warning(f"can not match data type, the most similar type of dataset {dataset_name} is {type_cls.name()}, similar score is {score:.4f}.")
                else:
                    logger.error("can not match any data type and can not find similar data type.")
            is_matched, info = self.check_line(line_data, type_cls)
            if not is_matched:
                logger.error(f" dataset {dataset_name} line {idx}: {info}")
            else:
                right_num_line += 1
        logger.info(f"check dataset {dataset_name} finished, right line {right_num_line} / total check line {idx + 1}")
    
    def check_folder(
        self, 
        dataset_dir: str, 
        k: int=10
    ):
        for file_path in sorted(glob(f"{dataset_dir}/*.json*")):
            if not os.path.isfile(file_path):
                continue
            self.check_file(file_path, k = k)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', type=str, required=True, help='dataset path or dataset dir')
    parser.add_argument('--k', type=int, default=10, help='check top k line of each file')
    args = parser.parse_args()

    dk = DataChecker()
    if os.path.isfile(args.dataset):
        dk.check_file(args.dataset, k=args.k)
    elif os.path.isdir(args.dataset):
        dk.check_folder(args.dataset, k = args.k)
