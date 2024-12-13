import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, field_validator


class QaMetaData(BaseModel):
    create_time: str
    问题明细: str
    回答明细: str
    扩展字段: Optional[str]

    @field_validator("create_time")
    def time_format(cls, v):
        format_string = "%Y%m%d %H:%M:%S"
        try:
            datetime.strptime(v, format_string)
            return v
        except Exception as e:
            raise e

class QaData(BaseModel):
    id: int
    问: str
    答: str
    来源: str
    元数据: QaMetaData
    时间: str

    @classmethod
    def name(cls):
        return '对话/问答语料数据'


class MultiQAExtension(BaseModel):
    会话: int
    多轮序号: int


class MultiQaMetaData(BaseModel):
    create_time: str
    问题明细: str
    回答明细: str
    扩展字段: str

    @field_validator("create_time")
    def time_format(cls, v):
        format_string = "%Y%m%d %H:%M:%S"
        datetime.strptime(v, format_string)
        return v

    
    @field_validator("扩展字段")
    def extension_format(cls, v):
        MultiQAExtension.model_validate_json(v)
        return v


class MultiQaData(BaseModel):
    id: str
    问: str
    答: str
    来源: str
    元数据: MultiQaMetaData
    时间: str

    @classmethod
    def name(cls):
        return '多轮对话/问答语料数据'


class CodeData(BaseModel):
    来源: str
    仓库名: str
    path: str
    文件名: str
    ext: str
    size: int
    原始编码: str
    md5:str
    text: str
    时间: str

    @classmethod
    def name(cls):
        return '代码语料数据'


class ForumResponse(BaseModel):
    楼ID: str
    回复: str
    扩展字段: str


class ForumData(BaseModel):
    ID: int
    主题: str
    来源: str
    回复: List[ForumResponse]
    元数据: Any
    时间: str

    @classmethod
    def name(cls):
        return '论坛语料格式'


class ParallelData(BaseModel):
    文件名: str
    是否待查文件: bool
    是否重复文件: bool
    段落数: int
    去重段落数: int
    低质量段落数: int
    行号: int
    是否重复: bool
    是否跨文件重复: bool
    it_text: str
    zh_text: str
    en_text: str
    ar_text: str
    nl_text: str
    de_text: str
    eo_text: str
    fr_text: str
    he_text: str
    ja_text: str
    pt_text: str
    ru_text: str
    es_text: str
    sv_text: str
    ko_text: str
    th_text: str
    id_text: str
    cht_text: str
    vi_text: str
    扩展字段: str
    时间: str
    zh_text_md5: str

    @classmethod
    def name(cls):
        return '平行语料格式'


class CommonParagraph(BaseModel):
    行号: int
    是否重复: bool
    是否跨文件重复: bool
    md5: str
    内容: str
    扩展字段: str


class CommonData(BaseModel):
    文件名: str
    是否待查文件: bool
    是否重复文件: bool
    文件大小: int
    simhash: int
    最长段落长度: int
    段落数: int
    去重段落数: int
    低质量段落数: int
    段落: List[CommonParagraph]
    扩展字段: str
    时间: str

    @classmethod
    def name(cls):
        return '通用语料格式'

class MultiModelDataModel(BaseModel):
    实体ID: Union[str, None]
    块ID: Union[int, None]
    时间: str
    扩展字段: str
    文本: Union[str, None]
    图片: Any
    OCR文本: Union[str, None]
    音频: Any
    STT文本: Union[str, None]
    块类型: str
    md5: str
    页ID: Union[int, None]

    @classmethod
    def name(cls):
        return '多模态语料格式'


class CommitDataModel(BaseModel):
    '''每行是一个文本的数据，对应一个代码仓库里的一个文本文件的变更。'''
    来源: str
    仓库名: str
    path: str
    文件名: str
    ext: str
    index: str
    message: str
    diff: str
    原始编码: str
    md5: str
    时间: str
    扩展字段: str

    @classmethod
    def name(cls):
        return 'Commit 语料格式'

expected_fields = {  
    "实体ID": "string",  
    "块ID": "int64",  
    "时间": "string", 
    "扩展字段": "string",
    "文本": "string",  
    "图片": "binary",
    "OCR文本": "string",  
    "音频": "binary",
    "STT文本": 'string',
    "块类型": "string",  
    "md5": "string",   
    "页ID":  "int64",
}  


if __name__ == "__main__":
    import json
    json_data = """{
        "id":123456,
        "问":"写一个超短小说",
        "答":"他们相遇，又别离。岁月如梭，情感却不减。",
        "来源":"wikihow",
        "元数据":{
            "create_time":"20230511 15:56:03",
            "问题明细":"",
            "回答明细":"",
            "扩展字段":""
        }
    }"""
    data = QaData(**json.loads(json_data))
    print(data)
    print('----------------------------------------')
    print(data.model_dump())
    print('----------------------------------------')
    print(data.model_dump_json())
    print(QaData.__name__)
