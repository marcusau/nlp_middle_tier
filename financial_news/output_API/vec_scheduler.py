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

from typing import Union,List,Dict

import pickle
import numpy as np

from tqdm import tqdm
from datetime import datetime,timedelta,date
import time

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

from financial_news.ETL import update_query,sql_query
from financial_news.output_API import nlp_handle as nlp

from cacheout import CacheManager

from Config.NLP_output_Config import Info as NLP_Config

Sche_Config=NLP_Config.news.sche
news_NLP_Config=NLP_Config.news
####-------------------------------hyper-parameters----------------------------

end_date = datetime.now()
start_date =end_date-timedelta(days=Sche_Config.dayback)
verbose=news_NLP_Config.verbose
TopN = news_NLP_Config.topK
topK=news_NLP_Config.topN

recur_news_limit=Sche_Config.recur_limit
vecnews_sche_trigger=Sche_Config.trigger
vecnews_day_of_week=Sche_Config.day_of_week
vecnews_hour=Sche_Config.hour
vecnews_minute=Sche_Config.minute
vecnews_name=Sche_Config.name


cache_nonvec_size=news_NLP_Config.cache.nonvec
cache_vec_size=news_NLP_Config.cache.vec
cache_fail_size=news_NLP_Config.cache.fail
cacheman = CacheManager({'not_vec': {'maxsize': cache_nonvec_size,'ttl':0},  'vec': {'maxsize': cache_vec_size, 'ttl': 0},    'fail': {'maxsize': cache_fail_size, 'ttl': 0},})
##-----------------------------------------------------------------------------------------------

init_news_data=sql_query.get_articles_bydate(start_dt=start_date,end_dt=end_date,contentReady='Y',vectorize='Y')
init_news_data={newsID: news_art for newsID, news_art in init_news_data.items() if  news_art['content'] not in ['',None] and len(news_art['content'])>3}

init_vector_data=sql_query.get_vector_bydate(start_dt=start_date,end_dt=end_date)
init_vector_data={newsID: vector for newsID, vector in init_vector_data.items() if  vector is not None and isinstance(vector, np.ndarray)  }

init_ner_data=sql_query.get_related_ner_bydate(start_dt=start_date,end_dt=end_date)
init_ner_data={newsID: ner for newsID, ner in init_ner_data.items() if  ner is not None and isinstance(ner, dict) and len(ner)>0  }


init_keywords_data=sql_query.get_related_keywords_bydate(start_dt=start_date,end_dt=end_date)
init_keywords_data={newsID: keywords for newsID, keywords in init_keywords_data.items() if  keywords is not None and isinstance(keywords,list) and len(keywords)>0  }


nonvec_data,vec_data={},{}
for newsID, news_art in init_news_data.items() :
    news_art['ner']=init_ner_data[newsID] if newsID in init_ner_data else {}
    news_art['vector']=init_vector_data[newsID] if newsID in init_vector_data else None
    news_art['keywords']=init_keywords_data[newsID] if newsID in init_keywords_data else []

    if len(news_art['ner'])>0 and len(news_art['keywords'])>0 and news_art['vector'] is not None:
        vec_data[newsID]=news_art
    elif news_art['newsDate']>datetime(year=date.today().year,month=date.today().month,day=date.today().day-1,hour=0,minute=0,second=0):
        nonvec_data[newsID] = news_art
print(f"Numbers of news articles in the {start_date} to {end_date}:   {len(vec_data)}  ")
print(f"Numbers of non-vectorize news articles in the {start_date} to {end_date}:   {len(nonvec_data)}  ")



if len(vec_data)>0:
    cacheman['vec'].set_many(vec_data)

if len(nonvec_data)>0:
    cacheman['not_vec'].set_many(nonvec_data)



# ##-----------------------------------------------------------------------------------------------
def update_new_vec(verbose: bool = verbose):
    for newsID in tqdm(cacheman['not_vec'], desc='vectorize news from non vec cache'):

        notvec_art = cacheman['not_vec'].get(newsID)
        try:
            notvec_art = nlp.main(news_info=notvec_art, news_total=dict(cacheman['vec'].items()), topK=topK, topN=TopN, verbose=verbose)
            if notvec_art['vector'] is not None and notvec_art['ner'] is not None:
                cacheman['not_vec'].delete(newsID)
                cacheman['vec'].set(newsID,notvec_art)
            else:
                print(f"error: vector,  newsID: {newsID}, newsDate :{notvec_art['newsDate']},  headline: {notvec_art[ 'headline']}, ner:{notvec_art['ner']}, ")
                cacheman['not_vec'].delete(newsID)
                cacheman['fail'].set(newsID,notvec_art )
        except Exception as e:
           print(f"error: {e},  newsID: {newsID}, newsDate :{notvec_art['newsDate']},  headline: {notvec_art['headline']}, ner:{notvec_art['ner']}, ")
           cacheman['not_vec'].delete(newsID)
           cacheman['fail'].set(newsID, notvec_art)


if len(cacheman['not_vec'])>0:
    update_new_vec(verbose=verbose)
#
# ###---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def regular_update_articles():

    recur_news={ newsID:news_art for newsID,news_art in sql_query.get_articles_byorder(limit=recur_news_limit,contentReady='Y',vectorize='Y').items() if newsID not in cacheman['vec'] and newsID not in cacheman['not_vec'] and newsID not in cacheman['fail'] and news_art['content'] not in ['',None] and len(news_art['content'])>3}
    print(f'no. of non-vec news from db: {len(recur_news)}')
    print(f"no. of vec news from db: {len(cacheman['vec'])}")
    if len(recur_news)>0:
        cacheman['not_vec'].set_many(recur_news)
        update_new_vec(verbose=verbose)



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

    scheduler.add_job(regular_update_articles,  trigger=Sche_Config.trigger, day_of_week=Sche_Config.day_of_week,  hour=Sche_Config.hour, minute=Sche_Config.minute, name=Sche_Config.name, replace_existing=True, max_instances=10)  # id=news_sched.articles.id,

    scheduler.add_listener(my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    scheduler.print_jobs(jobstore='default')
    scheduler.start()

    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
    try:
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
