# DataCheck_MNBVC

## 项目描述
针对MNBVC 项目下的数据进行格式检查，要求数据格式满足[现有语料格式](https://wiki.mnbvc.org/doku.php/%E7%8E%B0%E6%9C%89%E8%AF%AD%E6%96%99%E6%A0%BC%E5%BC%8F):


- [QaData-单论对话语料](https://github.com/esbatmop/WikiHowQAExtractor-mnbvc)
- [QaData-多轮对话语料](https://github.com/pany8125/ShareGPTQAExtractor-mnbvc)
- [CodeData-代码语料](https://github.com/esbatmop/githubcode_extractor_mnbvc)
- [ForumData-论坛语料](https://github.com/aplmikex/forum_dialogue_mnbvc)
- [ParallelData-平行语料](https://github.com/liyongsea/parallel_corpus_mnbvc)


## 环境安装
```bash
pip install -r requirements.txt
```

## 运行
```bash
python check_data.py  --dataset data/
```