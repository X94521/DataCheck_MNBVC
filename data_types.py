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

class ParallelParagraph(BaseModel):
    行号: int
    是否重复: bool
    是否跨文件重复: bool
    zh_text_md5: str
    zh_text: str
    en_text: str
    ar_text: Optional[str] = ''
    nl_text: Optional[str] = ''
    de_text: Optional[str] = ''
    eo_text: Optional[str] = ''
    fr_text: Optional[str] = ''
    he_text: Optional[str] = ''
    it_text: Optional[str] = ''
    ja_text: Optional[str] = ''
    pt_text: Optional[str] = ''
    ru_text: Optional[str] = ''
    es_text: Optional[str] = ''
    sv_text: Optional[str] = ''
    ko_text: Optional[str] = ''
    th_text: Optional[str] = ''
    id_text: Optional[str] = ''
    vi_text: Optional[str] = ''
    cht_text: Optional[str] = ''
    other1_text: Optional[str] = ''
    other2_text: Optional[str] = ''
    扩展字段: Optional[str] = ''


class ParallelData(BaseModel):
    文件名: str
    是否待查文件: bool
    是否重复文件: bool
    段落数: int
    去重段落数: int
    低质量段落数: int
    段落: List[ParallelParagraph]
    扩展字段: Optional[str] = ''
    时间: str

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
