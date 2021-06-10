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

from tqdm import tqdm
from datetime import datetime
import time

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

from financial_news.ETL import update_query,sql_query
from Config.Schedule_Config import Info as Sche_config

#from diskcache import Cache
from cacheout import Cache
from cacheout import CacheManager
###---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO)

cacheman = CacheManager({'ready': {'maxsize': 2000,'ttl':0},
                         'notready': {'maxsize': 200, 'ttl': 0},
                         'vecfilter': {'maxsize': 200,'ttl':0}})


###---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# ready_cache=Cache(cull_limit=2000,timeout=0)
# notready_cache=Cache(cull_limit=1000,timeout=0)
# vecfilter_cache=Cache(cull_limit=1000,timeout=0)
#
# for r_newsid, r_v, in tqdm(sql_query.get_articles_bydate(start_dt=60*48,end_dt=datetime.now(),contentReady='Y').items(),desc='load ready news articles to cache'):
#     ready_cache.set(r_newsid,r_v)
#
# for n_newsid, n_v, in tqdm(sql_query.get_articles_bydate(start_dt=6000,end_dt=datetime.now(),contentReady='N').items(),desc='load NOT ready news articles to cache'):
#     notready_cache.set(n_newsid,n_v)
#
# for product_id, p_v in tqdm(sql_query.get_vecfilter().items(),desc='load news vectorize filter data to cache'):
#     if product_id=='vectorize':
#         for vec_col, vec_f_data in p_v.items():
#             vecfilter_cache.set(vec_col, vec_f_data )

cacheman['ready'].set_many(sql_query.get_articles_bydate(start_dt=60*24*1,end_dt=datetime.now(),contentReady='Y'))
cacheman['notready'].set_many(sql_query.get_articles_bydate(start_dt=6000,end_dt=datetime.now(),contentReady='N'))
cacheman['vecfilter'].set_many(sql_query.get_vecfilter()['vectorize'])

# ###---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# #
# # def regular_update_cache(ready:Dict,notready:Dict):
# #     for rid,rv in ready.items():
# #         ready_cache[rid]=rv
# #
# #         if rid in notready_cache:
# #           del  notready_cache[rid]
# #
# #     for nid, nv in notready.items():
# #         notready_cache[nid]=nv
# #         if nid in ready_cache:
# #             del ready_cache[nid]
#
def regular_update_cache(ready:Dict,notready:Dict):

    cacheman['ready'].set_many({rid:rv for rid, rv in ready.items() if not cacheman['ready'].has(rid)})
    cacheman['notready'].delete_many([rid for rid, rv in ready.items() if   cacheman['notready'].has(rid)])

    cacheman['ready'].delete_many([nid for nid, nv in notready.items() if cacheman['ready'].has(nid)])
    cacheman['notready'].set_many({nid:nv for nid, nv in notready.items()  if not cacheman['notready'].has(nid)})
#         ready_cache[rid]=rv


def regular_update_articles(start_dt=30):

    thumbnail_a = update_query.get_thumbnail_bydate(start_dt=start_dt, end_dt=datetime.now())
    ready_a, notready_a, = update_query.deter_ready(thumbnail_a)

  #  notready_newsIDs= list(notready_cache)
    notready_newsIDs=list(cacheman['notready'].keys())
    thumbnail_b = update_query.get_thumbnail_bynewsID(newsIDs=notready_newsIDs)
    ready_b, notready_b, = update_query.deter_ready(thumbnail_b)

    ready_data=update_query.merge_data(ready_a,ready_b)
    notready_data = update_query.merge_data(notready_a,notready_b)

    pending_update_ready_data=update_query.deter_update(query_data=ready_data,stored_data=dict(cacheman['ready'].items()))
    pending_update_notready_data=update_query.deter_update(query_data=notready_data,stored_data=dict(cacheman['notready'].items()))

    update_ready_a = update_query.get_ready_content(pending_update_ready_data)
    update_ready_b = update_query.deter_vectorize(update_ready_a,vecfilter_data=dict(cacheman['vecfilter'].items()))

    regular_update_cache(ready=update_ready_b,notready= pending_update_notready_data)

    update_data_final = update_query.merge_data(update_ready_b, pending_update_notready_data)
    update_query.load_data(update_data_final)

###---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

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

###---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

sche_config=Sche_config.financial_news

scheduler.add_job(regular_update_articles,args=[int(sche_config.articles.Dayback_minutes)], trigger=sche_config.articles.trigger, day_of_week=sche_config.articles.day_of_week, hour=sche_config.articles.hour, minute=sche_config.articles.minute,  name=sche_config.articles.id, replace_existing=True, max_instances=10)# id=news_sched.articles.id,


scheduler.add_listener(my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
scheduler.print_jobs(jobstore='default')
scheduler.start()
print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
try:
    # 这是在这里模拟应用程序活动（使主线程保持活动状态）。
    while True:
        time.sleep(2)
except (KeyboardInterrupt, SystemExit):
    # 关闭调度器
    scheduler.shutdown()
