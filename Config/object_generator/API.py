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
   class RoutineClass:
      @dataclasses.dataclass
      class NerClass:
         main: str
         simple: str
      
      @dataclasses.dataclass
      class SegClass:
         cut: str
         pseg: str
         addword: str
      
      @dataclasses.dataclass
      class FuzzClass:
         word_sim: str
         words_sim: str
         check_dupe: str
         dedupe: str
      
      @dataclasses.dataclass
      class W2VClass:
         v2v_sim: str
         words_sim: str
         word_mostsim: str
         word_topn: str
         wordtovec: str
         check_dupe: str
         dedupe: str
      
      root: str
      ner: NerClass
      seg: SegClass
      fuzz: FuzzClass
      w2v: W2VClass
   
   host: str
   port: str
   routine: RoutineClass
