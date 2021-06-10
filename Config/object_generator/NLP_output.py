import dataclasses
import os,pathlib,sys
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
   class NewsClass:
      @dataclasses.dataclass
      class WordWeightsClass:
         label: float
         len: float
         headline: float
         span: float
      
      @dataclasses.dataclass
      class ScheClass:
         dayback: int
         recur_limit: int
         trigger: str
         day_of_week: str
         hour: str
         minute: str
         name: str
      
      @dataclasses.dataclass
      class CacheClass:
         nonvec: int
         vec: int
         fail: int
      
      word_weights: WordWeightsClass
      topK: int
      topN: int
      verbose: bool
      sche: ScheClass
      cache: CacheClass
   
   @dataclasses.dataclass
   class LifestyleClass:
      @dataclasses.dataclass
      class WordWeightsClass:
         label: float
         len: float
         title: float
         intro: float
         span: float
      
      @dataclasses.dataclass
      class ScheClass:
         dayback: int
         recur_limit: int
         trigger: str
         day_of_week: str
         hour: str
         minute: str
         name: str
      
      @dataclasses.dataclass
      class CacheClass:
         nonvec: int
         vec: int
         fail: int
      
      word_weights: WordWeightsClass
      topK: int
      topN: int
      verbose: bool
      sche: ScheClass
      cache: CacheClass
   
   news: NewsClass
   lifestyle: LifestyleClass
