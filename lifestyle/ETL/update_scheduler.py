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

from Config.Schedule_Config import Info as Sche_Config


import tools
from lifestyle.ETL import update_query,sql_query,input_api

logging.basicConfig(level=logging.INFO)
###----------------------------------------------------------------------------------------------------------------------------------------------------------------
#
# from diskcache import Cache
#
# article_cache=Cache(cull_limit=1000,timeout=0)
# vecfilter_cache=Cache(cull_limit=1000,timeout=0)
#
# for artid,a_data in tqdm(sql_query.get_articles_bydate(start_dt=60*24*5,end_dt=datetime.now()).items(),desc=f'load lifestyle articles data to cache'):
#     article_cache.set(artid,a_data)
#
# for product_id,vec_data in tqdm(sql_query.get_vecfilter().items(),desc=f'load lifestyle vectorize data to cache'):
#     if product_id=='vectorize':
#         for vec_col,v_ in vec_data.items():
#             vecfilter_cache.set(vec_col,v_)

from cacheout import CacheManager
cacheman = CacheManager({'ready': {'maxsize': 2000,'ttl':0},  'vecfilter': {'maxsize': 200,'ttl':0}})

cacheman['ready'].set_many(sql_query.get_articles_bydate(start_dt=60*24*20,end_dt=datetime.now()))
cacheman['vecfilter'].set_many(sql_query.get_vecfilter()['vectorize'])
###----------------------------------------------------------------------------------------------------------------------------------------------------------------

def regular_update_section():
    update_query.update_section()

def regular_update_category():
    update_query.update_category()

# def regular_update_cache(data:Dict):
#     for artid, v in data.items():
#         if artid not in article_cache:
#             article_cache.set(artid,v)

def regular_update_articles():
   # start_dt, end_dt = tools.convert_str2dt(start=start_dt, end=datetime.now())
  #  print(f' Time Range :  {start_dt}  -  {end_dt}')

    #API_data = input_api.get_articles_bydate(start_dt=start_dt, end_dt=end_dt, )
    headline_API_data = input_api.get_headlines()
    update_data = {d: headline_API_data[d] for d in headline_API_data.keys() if not cacheman['ready'].has(int(d))}

    for update_artid in tqdm(update_data,desc='fetch lifestyle content'):
        update_art_content= input_api.get_articles_byid(artid=update_artid)
        update_data.update(update_art_content)

    update_data = update_query.deter_vecfilter(data=update_data,vecfilter_data=dict(cacheman['vecfilter'].items()))

    update_query.load_update_articles(update_data=update_data)


##----------------------------------------------------------------------------------------------------------------------------------------------------------------

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

###----------------------------------------------------------------------------------------------------------------------------------------------------------------
sche_config=Sche_Config.lifestyle

scheduler.add_job(regular_update_articles, trigger=sche_config.articles.trigger , day_of_week=sche_config.articles.day_of_week, hour=sche_config.articles.hour, minute=sche_config.articles.minute,  name=sche_config.articles.id, replace_existing=True, max_instances=10)# id=news_sched.articles.id args=[int(sche_config.articles.Dayback_minutes)],
scheduler.add_job(regular_update_section, trigger=sche_config.section.trigger, day_of_week=sche_config.section.day_of_week, hour=sche_config.section.hour, minute=sche_config.section.minute,  name=sche_config.section.id, replace_existing=True, max_instances=10)
scheduler.add_job(regular_update_category, trigger=sche_config.category.trigger , day_of_week=sche_config.category.day_of_week, hour=sche_config.category.hour, minute=sche_config.category.minute,  name=sche_config.category.id, replace_existing=True, max_instances=10)

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