from pydantic import BaseModel
from typing import List, Union, Optional


class QaMetaData(BaseModel):
    create_time: str
    问题明细: str
    回答明细: str
    扩展字段: Optional[str]


class QaData(BaseModel):
    id: Union[int, str]
    问: str
    答: str
    来源: str
    元数据: QaMetaData

    @classmethod
    def name(cls):
        return '对话/问答语料数据'


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

    @classmethod
    def name(cls):
        return '代码语料数据'


class ForumMetaData(BaseModel):
    楼ID: Union[int, str]
    回复: str
    扩展字段: str


class ForumData(BaseModel):
    ID: Union[str, int]
    主题: str
    来源: str
    回复: List
    元数据: ForumMetaData

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
    ar_text: str
    nl_text: str
    de_text: str
    eo_text: str
    fr_text: str
    he_text: str
    it_text: str
    ja_text: str
    pt_text: str
    ru_text: str
    es_text: str
    sv_text: str
    ko_text: str
    th_text: str
    other1_text: str
    other2_text: str


class ParallelData(BaseModel):
    文件名: str
    是否待查文件: bool
    是否重复文件: bool
    段落数: int
    去重段落数: int
    低质量段落数: int
    段落: List[ParallelParagraph]

    @classmethod
    def name(cls):
        return '平行语料格式'


if __name__ == "__main__":
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
    data = QaData.parse_raw(json_data)
    print(data)
    print('----------------------------------------')
    print(data.dict())
    print('----------------------------------------')
    print(data.json(ensure_ascii=False))
    print(QaData.__name__)
