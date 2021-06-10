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
    class StocknamesClass:
        @dataclasses.dataclass
        class HkClass:
            Chi: str
            Eng: str
        
        @dataclasses.dataclass
        class ShanghaiClass:
            Chi: str
            Eng: str
        
        @dataclasses.dataclass
        class ShenzhenClass:
            Chi: str
            Eng: str
        
        HK: HkClass
        Shanghai: ShanghaiClass
        Shenzhen: ShenzhenClass
    
    @dataclasses.dataclass
    class LifestyleClass:
        @dataclasses.dataclass
        class UrlsClass:
            articles: str
            category: str
            single_article: str
            master: str
            section: str
        
        @dataclasses.dataclass
        class ParamsClass:
            articles: str
            category: str
            single_article: str
            section: str
        
        urls: UrlsClass
        params: ParamsClass
    
    @dataclasses.dataclass
    class NewsClass:
        @dataclasses.dataclass
        class UrlsClass:
            @dataclasses.dataclass
            class ContentClass:
                byID: str
            
            @dataclasses.dataclass
            class ThumbnailClass:
                byTime: str
                byID: str
            
            content: ContentClass
            Thumbnail: ThumbnailClass
        
        @dataclasses.dataclass
        class ParamsClass:
            limitno: str
        
        urls: UrlsClass
        params: ParamsClass
    
    stocknames: StocknamesClass
    lifestyle: LifestyleClass
    news: NewsClass
