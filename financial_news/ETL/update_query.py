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

from collections import ChainMap
from typing import Dict,List,Optional,Union


from datetime import timedelta,datetime
from tqdm import tqdm

from financial_news.ETL import input_api, sql_query,utils



###---------------------------------------- E:  sql query ----------------------------------------------------------------------

def get_sql_ready(start_dt=30,end_dt=datetime.now())->Dict:
    return sql_query.get_articles_bydate(start_dt=start_dt,end_dt=end_dt,contentReady='Y')

def get_sql_notready(start_dt=1200,end_dt=datetime.now())->Dict:
    return sql_query.get_articles_bydate(start_dt=start_dt,end_dt=end_dt,contentReady='N')

def get_sql_vecfilter()->Dict:
    return sql_query.get_vecfilter()


###--------------------------------------- E: -thumbnail  ----------------------------------------------------------------------

def get_thumbnail_bynewsID(newsIDs:List)->Dict:
    return input_api.get_thumbnail_byNewsID(newsIDs)

def get_thumbnail_bydate( start_dt=30,end_dt=datetime.now())->Dict:
    thumbnail_data= input_api.get_thumbnail_bydate(start_dt=start_dt, end_dt=end_dt)
    return thumbnail_data


###--------------------------------------- E: CONTENT ----------------------------------------------------------------------

def get_ready_content(data:Dict)->Dict:
    data__ =data.copy()
    Ready_update_content = input_api.get_content_byNewsID(newsID=list(data__.keys()))
    for Ready_newsID, Ready_v in data__.items():
        Ready_v['content'] = Ready_update_content.get(Ready_newsID, '')
    return data__

##-------------------------------T:-----------------

def merge_data(data_a,data_b):
    return dict(ChainMap(data_a,data_b))


def deter_ready(data:Dict):
    Ready, NOTready = input_api.split_ready(data)
    return Ready, NOTready

def deter_update(query_data,stored_data)->Dict:
    return {qid: qv for qid,qv in query_data.items() if stored_data.get(qid) is None}


def deter_vectorize(data:Dict,vecfilter_data,content_incl=False)->Dict:
    data__ =data.copy()
    for Ready_newsID, Ready_v in data__.items():
        Ready_v['vectorize'] = utils.vecFilter_func(article=Ready_v,vecfilter_data=vecfilter_data, content_incl=content_incl)
    return data__


###---------------------------------------------LOAD -----------------------------------

def load_data(data:Dict):

    for update_newsID, update_v in tqdm(data.items(),desc='insert API data to SQL'):

        update_v['refID']=update_v.get('refID',None)
        update_v['language']=update_v.get('language',None)
        update_v['newsID']=update_newsID
        update_v['contentReady']=update_v.get('contentReady',None)
        update_v['newsCode']=update_v.get('newsCode','')
        update_v['newsDate']=update_v.get('newsDate',None)
        update_v['BreakingNews']=update_v.get('BreakingNews',None)
        update_v['datatype']=update_v.get('datatype',None)
        update_v['packageCd']=update_v.get('packageCd',None)
        update_v['topicID']=update_v.get('topicID',None)
        update_v['topic']=update_v.get('topic',None)
        update_v['author']=update_v.get('author',None)
        update_v['thumbnailUrl']=update_v.get('thumbnailUrl',None)
        update_v['headline']=update_v.get('headline','')
        update_v['content']=update_v.get('content','')
        update_v['vectorize']=update_v.get('vectorize','N')
        update_v['relCategory']=update_v.get('',None)
        update_v['update_time']=datetime.now()

        sql_query.insert_articles(data=update_v)
        if update_v['relCategory']:
            related_category_record= [ (update_newsID,cd,update_v['update_time']) for cd in update_v['relCategory']]
            sql_query.insert_related_category(data=related_category_record)
        if update_v['topic']:
            topic_record= [update_v['topicID'],update_v['topic'],update_v['update_time']]
            sql_query.insert_topic(data=topic_record)


##------------------------- ETL : main functions -------------------------------------------------------------


def update_articles_bydate(start_dt=30,end_dt=datetime.now()):

    sql_vecfilter_data=sql_query.get_vecfilter()['vectorize']

    sql_data=get_sql_ready(start_dt=start_dt,end_dt=end_dt)

    thumbnail = get_thumbnail_bydate(start_dt=start_dt,end_dt=end_dt)
    ready_thumbnail, notready_thumbnail,=deter_ready(thumbnail)

    pending_update_data =deter_update(query_data=ready_thumbnail,stored_data=sql_data)

    update_ready_a=get_ready_content(pending_update_data)
    update_ready_b =deter_vectorize(update_ready_a,vecfilter_data=sql_vecfilter_data)

    update_data_final=merge_data(update_ready_b,notready_thumbnail)
    load_data(update_data_final)


def update_notready_articles(start_dt=6000,end_dt=datetime.now()):

    sql_vecfilter_data=sql_query.get_vecfilter()['vectorize']
    sql_notready=get_sql_notready(start_dt=start_dt, end_dt=end_dt)

    thumbnail=get_thumbnail_bynewsID(newsIDs=list(sql_notready.keys()))

    ready_data, notready_data = deter_ready(thumbnail)

    update_ready_a = get_ready_content(ready_data)
    update_ready_b = deter_vectorize(update_ready_a,vecfilter_data=sql_vecfilter_data)
    update_data_final = merge_data(update_ready_b, notready_data)

    load_data(update_data_final)


def update_articles(start_dt=30,end_dt=datetime.now()):

    sql_vecfilter_data = sql_query.get_vecfilter()['vectorize']

    sql_ready_data = get_sql_ready(start_dt=start_dt, end_dt=end_dt)
    thumbnail_a = get_thumbnail_bydate(start_dt=start_dt, end_dt=end_dt)
    ready_a, notready_a, = deter_ready(thumbnail_a)

    sql_notready = get_sql_notready(start_dt=start_dt, end_dt=end_dt)
    thumbnail_b = get_thumbnail_bynewsID(newsIDs=list(sql_notready.keys()))
    ready_b, notready_b, = deter_ready(thumbnail_b)

    ready_data=merge_data(ready_a,ready_b)
    notready_data = merge_data(notready_a,notready_b)

    pending_update_ready_data=deter_update(query_data=ready_data,stored_data=sql_ready_data)
    pending_update_notready_data=deter_update(query_data=notready_data,stored_data=sql_notready)

    update_ready_a = get_ready_content(pending_update_ready_data)
    update_ready_b = deter_vectorize(update_ready_a,vecfilter_data=sql_vecfilter_data)

    update_data_final = merge_data(update_ready_b, pending_update_notready_data)
    load_data(update_data_final)



def update_articles_vecfilter(start_dt=30,end_dt=datetime.now()):

    sql_vecfilter_data=sql_query.get_vecfilter()['vectorize']

    sql_data=sql_query.get_articles_bydate(start_dt=start_dt,end_dt=end_dt,contentReady='Y')
    update_data = deter_vectorize(sql_data, vecfilter_data=sql_vecfilter_data)

    load_data(update_data)



if __name__ == "__main__":
    fire.Fire({'thumbnail_bynewsID':get_thumbnail_bynewsID,'thumbnail_bydate':get_thumbnail_bydate,
               "articles_bydate":update_articles_bydate,'notready_articles':update_notready_articles,  "articles":update_articles,
               "vecfilter":update_articles_vecfilter})
