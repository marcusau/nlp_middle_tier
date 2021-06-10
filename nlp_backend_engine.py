#/usr/bin/env python
# -*- coding: utf-8 -*-
import os, pathlib, sys,fire,logging

sys.path.append(os.getcwd())

parent_path = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(parent_path))

master_path = parent_path.parent
sys.path.append(str(master_path))

project_path = master_path.parent
sys.path.append(str(project_path))

from typing import Union,List,Dict,Optional,Tuple

from tqdm import tqdm
from datetime import datetime
import time


from Config.SQL_Config import Info as SQL_config

import fire,requests

import pymysql
from sql_db import SQL_connection,schema,tables
import tools


from Config.API_Config import Info as API_config

API_routes=API_config.routine
API_host=API_config.host
API_port=API_config.port

root=f'http://{API_host}:{API_port}{API_routes.root}'
ner_route=API_routes.ner
seg_route=API_routes.seg
fuzz_route=API_routes.fuzz
w2v_route=API_routes.w2v

###-----------  1. Web Server URLs Components   -------------------------------------------------

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
def retry_session(retries, session=None, backoff_factor=0.3):
    session = session or requests.Session()
    retry = Retry( total=retries, read=retries,  connect=retries,   backoff_factor=backoff_factor,   allowed_methods=False, )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

##--------------------------------------------


def ner_scan(text:str,simple:bool=False)->Dict:
    session = retry_session(retries=5)
    text = text.strip()
    ner_simple_api=root+ner_route.simple
    ner_main_api=root+ner_route.main
    ner_api_url = ner_simple_api if simple else ner_main_api
    url_response = session.post(ner_api_url,  json={'text': text})
    resp=url_response.json()

    session.close()
    return resp['data']

def seg(text:str,pos:bool=False):
    session = retry_session(retries=5)
    text = text.strip()
    cut_api=root+seg_route.cut
    pseg_api=root+seg_route.pseg
    seg_api_url = pseg_api  if pos else cut_api
    url_response = session.post(seg_api_url,  json={'words': text})
    resp=url_response.json()

    session.close()
    return resp['data']

###--------------------------------------------------------------------------
def fuzz_dedupe(words:List, threshold:float= 0.6):
    session = retry_session(retries=5)
    url_response = session.post(f'{root}{fuzz_route.dedupe}', json={'words':words, 'threshold':threshold})
    resp = url_response.json()
    session.close()
    return resp

def fuzz_checkdup(word:str, words:Union[List[str],str], threshold:float=0.6):
    session = retry_session(retries=5)
    url_response = session.post(f'{root}{fuzz_route.check_dupe}', json={'word':word,'words':words, 'threshold':threshold})
    resp = url_response.json()
    session.close()
    return resp

def fuzz_wordsim(wordA:str, wordB:str):
    session = retry_session(retries=5)
    url_response = session.post(f'{root}{fuzz_route.word_sim}', json={'wordA':wordA,'wordB':wordB})
    resp = url_response.json()
    session.close()
    return resp

def fuzz_wordsims(word:str, words:Union[List[str],str],TopN:Optional[Union[int,None]]=None):
    session = retry_session(retries=5)
    url_response = session.post(f'{root}{fuzz_route.words_sim}', json={'word':word,'words':words, 'TopN':TopN})
    resp = url_response.json()
    session.close()
    return resp

###----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def w2v_topN(word:str,TopN:int=10,):
    session = retry_session(retries=5)
    url_response = session.post(f'{root}{w2v_route.word_topn}', json={'word':word, 'TopN':TopN})
    resp = url_response.json()
    session.close()
    return resp

def w2v_mostsim(word:str, words:Union[str,List]):
    session = retry_session(retries=5)
    url_response = session.post(f'{root}{w2v_route.word_mostsim}', json={'word':word, 'words':words})
    resp = url_response.json()
    session.close()
    return resp

def w2v_wordsims(word:str, words:Union[str,List]):
    session = retry_session(retries=5)
    url_response = session.post(f'{root}{w2v_route.words_sim}', json={'word':word, 'words':words})
    resp = url_response.json()
    session.close()
    return resp


def w2v_checkdup(word:str, words:Union[str,List],threshold:float=0.6):
    session = retry_session(retries=5)
    url_response = session.post(f'{root}{w2v_route.check_dupe}', json={'word':word,'words':words, 'threshold':threshold})
    resp = url_response.json()
    session.close()
    return resp



def w2v_dedupe( words:Union[str,List],threshold:float=0.6):
    session = retry_session(retries=5)
    url_response = session.post(f'{root}{w2v_route.dedupe}', json={'words':words, 'threshold':threshold})
    resp = url_response.json()
    session.close()
    return resp

def w2v_tovec( words:Union[str,List]):
    session = retry_session(retries=5)
    url_response = session.post(f'{root}{w2v_route.wordtovec}', json={'words':words})
    resp = url_response.json()
    session.close()
    return resp


if __name__=='__main__':
    fire.Fire({"ner":ner_scan,'seg':seg,"fuzz_dedupe":fuzz_dedupe,'topN':w2v_topN})
   # print(fuzz_dedupe(words= ['普華永道中天會計師事務所', '羅兵咸永道會計師事務所', '畢馬威華振會計師事務所', '畢馬威會計師事務所', '特殊普通合夥', '股東周年大會', '國資委', '董事會', '退任'], threshold= 0.8))
    #print(fuzz_wordsims(word='畢馬威會計師事務所', words= ['普華永道中天會計師事務所', '羅兵咸永道會計師事務所', '畢馬威華振會計師事務所', '畢馬威會計師事務所', '特殊普通合夥', '股東周年大會', '國資委', '董事會', '退任'], TopN=3))
    # print(fuzz_wordsim(wordA='畢馬威會計師事務所',wordB='畢馬威華振會計師事務所'))

    # print(fuzz_checkdup(word='畢馬威會計師事務所',
    #                     words=['普華永道中天會計師事務所', '羅兵咸永道會計師事務所', '畢馬威華振會計師事務所', '畢馬威會計師事務所', '特殊普通合夥', '股東周年大會', '國資委',
    #                            '董事會', '退任'], threshold= 0.8))
   # print(w2v_wordsims(word='畢馬威會計師事務所',  words=['普華永道中天會計師事務所', '羅兵咸永道會計師事務所', '畢馬威華振會計師事務所', '畢馬威會計師事務所', '特殊普通合夥', '股東周年大會', '國資委',  '董事會', '退任']))
   # print(w2v_checkdup(word='畢馬威會計師事務所',      words=['普華永道中天會計師事務所', '羅兵咸永道會計師事務所', '畢馬威華振會計師事務所', '畢馬威會計師事務所', '特殊普通合夥', '股東周年大會', '國資委',  '董事會', '退任'],threshold=0.6))

    #print(w2v_dedupe(    words=['普華永道中天會計師事務所', '羅兵咸永道會計師事務所', '畢馬威華振會計師事務所', '畢馬威會計師事務所', '特殊普通合夥', '股東周年大會', '國資委', '董事會', '退任'], threshold=0.7))

#     print(ner_scan(text='''《經濟通通訊社２６日專訊》歐盟執委會主席馮德萊恩周二（２５日）在布魯塞爾舉行的歐
# 盟２７國領導人會議後表示，簡化歐盟出行手續的「疫苗護照」相關基礎設施配套將於下月在歐
# 盟層面準備就緒。（ａｌ）''',simple=False))