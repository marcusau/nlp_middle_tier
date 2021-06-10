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
    class LifestyleClass:
        @dataclasses.dataclass
        class ArticlesClass:
            Dayback_minutes: str
            day_of_week: str
            hour: str
            id: str
            minute: str
            trigger: str
        
        @dataclasses.dataclass
        class CategoryClass:
            day_of_week: str
            hour: str
            id: str
            minute: str
            trigger: str
        
        @dataclasses.dataclass
        class SectionClass:
            day_of_week: str
            hour: str
            id: str
            minute: str
            trigger: str
        
        articles: ArticlesClass
        category: CategoryClass
        section: SectionClass
    
    @dataclasses.dataclass
    class FinancialNewsClass:
        @dataclasses.dataclass
        class ArticlesClass:
            Dayback_minutes: str
            day_of_week: str
            hour: str
            id: str
            minute: str
            trigger: str
        
        articles: ArticlesClass
    
    lifestyle: LifestyleClass
    financial_news: FinancialNewsClass
