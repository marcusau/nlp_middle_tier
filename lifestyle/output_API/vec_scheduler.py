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

from typing import Union,List,Dict,Tuple

import pickle
import numpy as np

from tqdm import tqdm
from datetime import datetime,timedelta,date
import time


from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

from lifestyle.ETL import update_query,sql_query
from lifestyle.output_API import nlp_handle as nlp
from Config.NLP_output_Config import Info as NLP_Config

lifestyle_NLP_Config=NLP_Config.lifestyle

from cacheout import CacheManager

####-------------------------------hyper-parameters----------------------------
dayback=lifestyle_NLP_Config.sche.dayback
end_date = datetime.now()
start_date =end_date-timedelta(days=dayback)

verbose=lifestyle_NLP_Config.verbose

topK=lifestyle_NLP_Config.topK
topN=lifestyle_NLP_Config.topN
recur_lifestyle_limit=lifestyle_NLP_Config.sche.recur_limit

vec_lifestyle_sche_trigger=lifestyle_NLP_Config.sche.trigger
vec_lifestyle_day_of_week=lifestyle_NLP_Config.sche.day_of_week
vec_lifestyle_hour=lifestyle_NLP_Config.sche.hour
vec_lifestyle_minute=lifestyle_NLP_Config.sche.minute
vec_lifestyle_name=lifestyle_NLP_Config.sche.name


cache_nonvec_size=lifestyle_NLP_Config.cache.nonvec
cache_vec_size=lifestyle_NLP_Config.cache.vec
cache_fail_size=lifestyle_NLP_Config.cache.fail
cacheman = CacheManager({'not_vec': {'maxsize': cache_nonvec_size,'ttl':0},  'vec': {'maxsize': cache_vec_size, 'ttl': 0},  'fail': {'maxsize':cache_fail_size, 'ttl': 0},  })

##-----------------------------------------------------------------------------------------------



init_arts = sql_query.get_articles_bydate(start_dt=start_date, end_dt=end_date)
init_arts = {artid: art_info for artid, art_info in init_arts.items() if  art_info['vectorize'] == 'Y' and art_info['fulltext'] not in ['', None] and len(art_info['fulltext']) > 3}

init_vector = sql_query.get_vector_bydate(start_dt=start_date, end_dt=end_date)
init_vector = {artid: vector for artid, vector in init_vector.items() if     vector is not None and isinstance(vector, np.ndarray)}

init_ner = sql_query.get_related_ner_bydate(start_dt=start_date, end_dt=end_date)
init_ner = {artid: ner for artid, ner in init_ner.items() if ner is not None and isinstance(ner, dict) and len(ner) > 0}

init_keywords = sql_query.get_related_keywords_bydate(start_dt=start_date, end_dt=end_date)
init_keywords = {artid: keywords for artid, keywords in init_keywords.items() if   keywords is not None and isinstance(keywords, list) and len(keywords) > 0}

init_nonvec_arts, init_vec_arts = {}, {}
for artid, art_info in init_arts.items():
    art_info['artid'] = artid
    art_info['ner'] = init_ner[artid] if artid in init_ner else {}
    art_info['vector'] = init_vector[artid] if artid in init_vector else None
    art_info['keywords'] = init_keywords[artid] if artid in init_keywords else []

    # print(f"artid:{art_info['artid']} ,  ner: {art_info['ner']} , vector: {type(art_info['vector'])}")
    if len(art_info['ner']) > 0 and len(art_info['keywords']) > 0 and art_info['vector'] is not None and isinstance( art_info['vector'], np.ndarray):  # len(art_info['keywords']) > 0 and
        init_vec_arts[artid] = art_info
    elif art_info['publish_up'] > datetime(year=date.today().year, month=date.today().month, day=date.today().day - 1, hour=0, minute=0, second=0):
        init_nonvec_arts[artid] = art_info
print(f"Numbers of lifestyle articles in the {start_date} to {end_date}:   {len(init_vec_arts)}  ")
print(f"Numbers of non-vectorize lifestyle articles in the {start_date} to {end_date}:   {len(init_nonvec_arts)}  ")

if len(init_vec_arts)>0:
    cacheman['vec'].set_many(init_vec_arts)

if len(init_nonvec_arts)>0:
    cacheman['not_vec'].set_many(init_nonvec_arts)


# ##-----------------------------------------------------------------------------------------------
def update_lifestyle_vec(verbose: bool = verbose,topN:int=100):
    for artid in tqdm(cacheman['not_vec'], desc='vectorize news from non vec cache'):

        notvec_art = cacheman['not_vec'].get(artid)
        notvec_art['artid']=artid
        try:
            notvec_art = nlp.main(art_info=notvec_art, total_arts=dict(cacheman['vec'].items()), topK=topK, topN=topN, verbose=verbose)
            if notvec_art['vector'] is not None and notvec_art['ner'] is not None:
                cacheman['not_vec'].delete(artid)
                cacheman['vec'].set(artid,notvec_art)
            else:
                print(f"error: vector,  artid: {artid}, publish_up :{notvec_art['publish_up']},  arttitle: {notvec_art[ 'arttitle']}, ner:{notvec_art['ner']}, ")
                cacheman['not_vec'].delete(artid)
                cacheman['fail'].set(artid,notvec_art )
        except Exception as e:
           print(f"error: {e},  artid: {artid}, publish_up :{notvec_art['publish_up']},  arttitle: {notvec_art['arttitle']}, ner:{notvec_art['ner']}, ")
           cacheman['not_vec'].delete(artid)
           cacheman['fail'].set(artid, notvec_art)


if len(cacheman['not_vec'])>0:
    update_lifestyle_vec(verbose=verbose)

###---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def regular_update_articles():

    recur_lifestyle={ artid:art_info for artid,art_info in sql_query.get_articles_byorder(limit=recur_lifestyle_limit).items() if art_info['vectorize'] == 'Y'  and art_info['fulltext'] not in ['', None] and len(art_info['fulltext']) > 3 and artid not in cacheman['vec'] and artid not in cacheman['not_vec'] and artid not in cacheman['fail'] }
    print(f'no. of non-vec news from db: {len(recur_lifestyle)}')
    print(f"no. of vec news from db: {len(cacheman['vec'])}")
    if len(recur_lifestyle)>0:
        cacheman['not_vec'].set_many(recur_lifestyle)
        update_lifestyle_vec(verbose=verbose)

# ###---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

executors = {  'default': ThreadPoolExecutor(20), 'processpool': ProcessPoolExecutor(5)}# 默认参数配置
job_defaults = {  'coalesce': False,      'misfire_grace_time': 30 } # 30秒的任务超时容错

def my_listener(event):
    if event.exception:
        print(f'Job {event.job_id} failed...')  # or logger.fatal('The job crashed :(')
        logging.warning(f'Job {event.job_id} failed...')  # .format())
    else:
        print(f'Job {event.job_id} was executed...')
        logging.info(f'Job {event.job_id} was executed...')  # .format(event.job_id))

scheduler = BackgroundScheduler(executors=executors, job_defaults=job_defaults, )

if __name__=='__main__':

    scheduler.add_job(regular_update_articles,  trigger=vec_lifestyle_sche_trigger, day_of_week=vec_lifestyle_day_of_week,  hour=vec_lifestyle_hour, minute=vec_lifestyle_minute, name=vec_lifestyle_name, replace_existing=True, max_instances=10)  # id=news_sched.articles.id,

    scheduler.add_listener(my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    scheduler.print_jobs(jobstore='default')
    scheduler.start()

    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
    try:
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
