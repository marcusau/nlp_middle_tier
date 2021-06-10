import dataclasses
import os, pathlib, sys,fire,logging

sys.path.append(os.getcwd())

parent_path = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(parent_path))

master_path = parent_path.parent
sys.path.append(str(master_path))

project_path = master_path.parent
sys.path.append(str(project_path))

import yaml2pyclass


class Config(yaml2pyclass.CodeGenerator):
    @dataclasses.dataclass
    class NlpEngineClass:
        @dataclasses.dataclass
        class TablesClass:
            @dataclasses.dataclass
            class UtilClass:
                NameEntity_type: str
                pos: str
            
            @dataclasses.dataclass
            class VocabClass:
                stocknames: str
                NameEntites: str
                stopwords: str
            
            util: UtilClass
            vocab: VocabClass
        
        schema: str
        tables: TablesClass
    
    @dataclasses.dataclass
    class NewsClass:
        @dataclasses.dataclass
        class TablesClass:
            @dataclasses.dataclass
            class VecfilterClass:
                categoryCd: str
                topic: str
                topicID: str
                words: str
            
            articles: str
            category: str
            topic: str
            embeddings: str
            name_entity_scores: str
            related_articles: str
            related_category: str
            related_keywords: str
            vecfilter: VecfilterClass
        
        schema: str
        tables: TablesClass
    
    @dataclasses.dataclass
    class LifestyleClass:
        @dataclasses.dataclass
        class TablesClass:
            @dataclasses.dataclass
            class VecfilterClass:
                catid: str
                words: str
            
            articles: str
            section: str
            category: str
            related_category: str
            tortags: str
            tortags_tags: str
            related_articles: str
            related_keywords: str
            embeddings: str
            name_entity_scores: str
            vecfilter: VecfilterClass
        
        schema: str
        tables: TablesClass
    
    host: str
    password: str
    port: str
    user: str
    nlp_engine: NlpEngineClass
    news: NewsClass
    lifestyle: LifestyleClass
