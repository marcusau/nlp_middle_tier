#/usr/bin/env python
# -*- coding: utf-8 -*-
import os, pathlib, sys,fire,logging,pickle

sys.path.append(os.getcwd())

parent_path = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(parent_path))

master_path = parent_path.parent
sys.path.append(str(master_path))

project_path = master_path.parent
sys.path.append(str(project_path))


from typing import Dict,List,Union,Optional,Tuple
from itertools import chain
import pickle
import numpy as np

from pytime import pytime
from datetime import date,timedelta,datetime
from tqdm import tqdm

import pymysql
from sql_db import SQL_connection,schema,tables

import tools

##--------------------------------------     category  ---------------------------------------------------------------------------------
def get_category()->Dict:
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        select_query = f"""   SELECT *  FROM {schema.News}.{tables.News.category}"""
        cursor.execute(select_query )
        rows={}
        rows={i['categoryCd']:i['categoryTChi'] for i in  cursor.fetchall()}
        SQL_connection.close()
        return rows


##--------------------------------------   related_category  ---------------------------------------------------------------------------------

def get_related_category_byorder(limit:int=500)->Dict:
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        select_query = f""" SELECT a.newsID,group_concat(a.categoryCd SEPARATOR ',') as related_categoryCd
                                        FROM  {schema.News}.{tables.News.related_category}  a
                                        JOIN {schema.News}.{tables.News.articles} b on a.newsID=b.newsID
                                        group by newsID
                                        ORDER BY b.newsDate desc
                                        limit {limit}"""
        cursor.execute(select_query )
        rows={}
        for i in  cursor.fetchall():
            newsID=i['newsID']
            related_categoryCd=i['related_categoryCd'].split(',')
            rows[newsID]=related_categoryCd
        SQL_connection.close()
        return rows


def get_related_category_bydate(start_dt:Union[datetime,str,int],end_dt:Union[datetime,str])->Dict:

    start_dt, end_dt = tools.convert_str2dt(start=start_dt, end=end_dt)
    start_dt = start_dt.strftime('%Y-%m-%d %H:%M:%S')
    end_dt = end_dt.strftime('%Y-%m-%d %H:%M:%S')
    print(f'get related category from SQL in time range : {start_dt} - {end_dt}')

    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        select_query = f""" SELECT a.newsID,group_concat(a.categoryCd SEPARATOR ',') as related_categoryCd
                                        FROM  {schema.News}.{tables.News.related_category}  a
                                        JOIN {schema.News}.{tables.News.articles} b on a.newsID=b.newsID
                                       where b.newsDate >="{start_dt}" and b.newsDate<='{end_dt}'
                                       group by a.newsID
                                       ORDER BY b.newsDate desc
                                        """
        cursor.execute(select_query )
        rows={}
        for i in  cursor.fetchall():
            newsID=i['newsID']
            related_categoryCd=i['related_categoryCd'].split(',')
            rows[newsID]=related_categoryCd
        SQL_connection.close()
        return rows


def get_related_category_byid(newsID:Union[str,int,List[Union[str,int]]])->Dict:
    newsID = [newsID] if type(newsID) in [str, int] else [newsID]
    newsID = ','.join([str(a) for a in newsID])
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        select_query = f""" SELECT a.newsID,group_concat(a.categoryCd SEPARATOR ',') as related_categoryCd
                                        FROM  {schema.News}.{tables.News.related_category}  a
                                        JOIN {schema.News}.{tables.News.articles} b on a.newsID=b.newsID
                                        where b.newsID in ({newsID})
                                        group by newsID
                                        ORDER BY b.newsDate desc"""

        cursor.execute(select_query )
        rows={}
        for i in  cursor.fetchall():
            newsID=i['newsID']
            related_categoryCd=i['related_categoryCd'].split(',')
            rows[newsID]=related_categoryCd
        SQL_connection.close()
        return rows


def insert_category(data):
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        insert_query = f""" insert  into  {schema.News}.{tables.News.category}  (categoryCd ,categoryTChi,update_time) values (%s,%s,%s) ON DUPLICATE KEY UPDATE update_time=values(update_time);"""
        cursor.execute(insert_query, data)
    SQL_connection.close()

def insert_related_category(data):
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        insert_query = f""" insert  into  {schema.News}.{tables.News.related_category}  (newsID,categoryCd ,update_time) values (%s,%s,%s) ON DUPLICATE KEY UPDATE update_time=values(update_time);"""
        cursor.execute(insert_query, data)
    SQL_connection.close()


###-------------------------------------topic ----------------------------------------------------

def get_topic()->Dict:
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        select_query = f"""   SELECT topicID,topic  FROM {schema.News}.{tables.News.topic}"""
        cursor.execute(select_query )
        rows={}
        for i in  cursor.fetchall():
            topicID="empty" if i['topicID'] in [None,'',"NULL","empty"] else i['topicID']
            topic= i['topic']
            topic_key=topicID+' ：'+topic
            rows[topic_key]=i
        SQL_connection.close()
        return rows


def insert_topic(data):
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        insert_query = f""" insert  into  {schema.News}.{tables.News.topic}  (topicID,topic ,update_time) values (%s,%s,%s) ON DUPLICATE KEY UPDATE update_time=values(update_time);"""
        cursor.execute(insert_query, data)
    SQL_connection.close()



##------------------------- Content ---------------------------------------------------------------------------
# -
def get_content_byid(newsID:Union[str,int,List[Union[str,int]]]):

    newsID = [newsID] if type(newsID) in [str, int] else [newsID]
    newsID = ','.join([str(a) for a in newsID])

    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        select_query = f"""  SELECT refID,language,newsID,contentReady, newsDate,content,vectorize, update_time
                                            FROM {schema.News}.{tables.News.articles}
                                            where newsID in ({newsID})
                                            order by newsDate desc
                                            """
        cursor.execute(select_query )
        rows={}
        for i in  cursor.fetchall():
            i['newsID'] = int(i['newsID'])
            newsID=i['newsID']
            rows[newsID]=i
        SQL_connection.close()
        return rows



def get_content_byorder(limit:int=500,contentReady:str=None,vectorize:str=None):

    assert contentReady in ['Y', 'N', None], print('content Ready only takes arugments of Y or N')
    assert vectorize in ['Y', 'N', None], print('vectorize only takes arugments of Y or N')

    if not contentReady and not vectorize:
        select_query = f"""  SELECT refID,language,newsID,contentReady, newsDate,content,vectorize, update_time  FROM {schema.News}.{tables.News.articles}    order by newsDate desc        limit {limit}"""
    elif contentReady and not vectorize:
        select_query = f"""  SELECT refID,language,newsID,contentReady, newsDate,content,vectorize, update_time  FROM {schema.News}.{tables.News.articles}   where contentReady='{contentReady}'  order by newsDate desc    limit {limit}"""
    elif not contentReady and vectorize:
        select_query = f"""  SELECT refID,language,newsID,contentReady, newsDate,content,vectorize, update_time  FROM {schema.News}.{tables.News.articles}   where  vectorize='{vectorize}'  order by newsDate desc    limit {limit}"""
    else:
        select_query = f"""  SELECT refID,language,newsID,contentReady, newsDate,content,vectorize, update_time  FROM {schema.News}.{tables.News.articles} where contentReady='{contentReady}' and vectorize='{vectorize}'  order by newsDate desc        limit {limit}"""

    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )
        rows={}
        for i in  cursor.fetchall():
            i['newsID'] = int(i['newsID'])
            newsID=i['newsID']
            rows[newsID]=i
        SQL_connection.close()
        return rows



def get_content_bydate(start_dt:Union[datetime,str,int] ,end_dt: Union[datetime,str],contentReady:str=None,vectorize:str=None):

    start_dt,end_dt=tools.convert_str2dt(start=start_dt,end=end_dt)

    start_dt=start_dt.strftime('%Y-%m-%d %H:%M:%S')
    end_dt = end_dt.strftime('%Y-%m-%d %H:%M:%S')
    print(f'get news content  from SQL in time range : {start_dt} - {end_dt}')

    assert contentReady in ['Y', 'N', None], print('content Ready only takes arugments of Y or N')
    assert vectorize in ['Y', 'N', None], print('vectorize only takes arugments of Y or N')

    if not contentReady and not vectorize:
        select_query = f"""  SELECT refID,language,newsID,contentReady, newsDate,content,vectorize, update_time  FROM {schema.News}.{tables.News.articles}    where newsDate>='{start_dt} and newsDate<=''{end_dt}'  order by newsDate desc  """
    elif contentReady and not vectorize:
        select_query = f"""  SELECT refID,language,newsID,contentReady, newsDate,content,vectorize, update_time  FROM {schema.News}.{tables.News.articles}    where newsDate>='{start_dt} and newsDate<=''{end_dt}' and contentReady='{contentReady}'   order by newsDate desc  """

    elif not contentReady and vectorize:
        select_query = f"""  SELECT refID,language,newsID,contentReady, newsDate,content,vectorize, update_time  FROM {schema.News}.{tables.News.articles}   where newsDate>='{start_dt} and newsDate<=''{end_dt}' and vectorize='{vectorize}'  order by newsDate desc  """
    else:
        select_query = f"""  SELECT refID,language,newsID,contentReady, newsDate,content,vectorize, update_time  FROM {schema.News}.{tables.News.articles}   where newsDate>='{start_dt} and newsDate<=''{end_dt}' and contentReady='{contentReady}' and vectorize='{vectorize}'  order by newsDate desc  """

    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )
        rows={}
        for i in  cursor.fetchall():
            i['newsID'] = int(i['newsID'])
            newsID=i['newsID']
            rows[newsID]=i
        SQL_connection.close()
        return rows

###-----------------------------------------articles  ------------------------------------------------------

def get_articles_byid(newsID:Union[str,int,List[Union[str,int]]]):
    newsID = [newsID] if type(newsID) in [str, int] else [newsID]
    newsID = ','.join([str(a) for a in newsID])
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        select_query = f"""  SELECT refID,language,newsID,contentReady, newsDate, BreakingNews, packageCd,topicID,topic,author, headline,content,thumbnailUrl,vectorize, update_time
                                            FROM {schema.News}.{tables.News.articles}
                                            where newsID in ({newsID})
                                            order by newsDate desc
                                            """
        cursor.execute(select_query )
        rows={}
        for i in  cursor.fetchall():
            i['newsID'] = int(i['newsID'])
            newsID=i['newsID']
            rows[newsID]=i
        SQL_connection.close()
        return rows




def get_articles_byorder(limit:int=500,contentReady:str=None,vectorize:str=None):

    assert contentReady in ['Y','N',None] , print('content Ready only takes arugments of Y or N')
    assert vectorize in ['Y','N',None], print('vectorize only takes arugments of Y or N')

    if not contentReady and not vectorize:
        select_query = f"""  SELECT refID,language,newsID,contentReady, newsDate, BreakingNews, packageCd,topicID,topic,author, headline,content,thumbnailUrl,vectorize, update_time FROM {schema.News}.{tables.News.articles}  
                                                   order by newsDate desc      limit {limit}   """
    elif contentReady and not vectorize:
        select_query = f"""  SELECT refID,language,newsID,contentReady, newsDate, BreakingNews, packageCd,topicID,topic,author, headline,content,thumbnailUrl,vectorize, update_time  FROM {schema.News}.{tables.News.articles}       
                                                        where contentReady='{contentReady}'     order by newsDate desc          limit {limit}   """

    elif not contentReady and  vectorize:
        select_query = f"""  SELECT refID,language,newsID,contentReady, newsDate, BreakingNews, packageCd,topicID,topic,author, headline,content,thumbnailUrl,vectorize, update_time    FROM {schema.News}.{tables.News.articles}   
                                                        where vectorize='{vectorize}'    order by newsDate desc      limit {limit} """
    else:
        select_query =f"""  SELECT refID,language,newsID,contentReady, newsDate, BreakingNews, packageCd,topicID,topic,author, headline,content,thumbnailUrl,vectorize, update_time FROM {schema.News}.{tables.News.articles}    
                                                    where contentReady='{contentReady}' and vectorize='{vectorize}'    order by newsDate desc       limit {limit}"""

    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )
        rows={}
        for i in  cursor.fetchall():
            i['newsID'] = int(i['newsID'])
            newsID=i['newsID']
            rows[newsID]=i
        SQL_connection.close()
        return rows


def get_articles_bydate(start_dt:Union[datetime,str,int]=60,end_dt:Union[datetime,str]=datetime.now(),contentReady:str=None,vectorize:str=None):

    start_dt, end_dt = tools.convert_str2dt(start=start_dt, end=end_dt)
    start_dt = start_dt.strftime('%Y-%m-%d %H:%M:%S')
    end_dt = end_dt.strftime('%Y-%m-%d %H:%M:%S')
    print(f'get news articles from DB in time range : {start_dt} - {end_dt}')

    assert contentReady in ['Y','N',None] , print('content Ready only takes arugments of Y or N')
    assert vectorize in ['Y','N',None], print('vectorize only takes arugments of Y or N')

    if not contentReady and not vectorize:
        select_query = f"""  SELECT refID,language,newsID,contentReady, newsDate, BreakingNews, packageCd,topicID,topic,author, headline,content,thumbnailUrl,vectorize, update_time    FROM {schema.News}.{tables.News.articles} 
                                                where newsDate>='{start_dt}' and newsDate<='{end_dt}' order by newsDate desc   """

    elif contentReady and not vectorize:
        select_query = f"""  SELECT refID,language,newsID,contentReady, newsDate, BreakingNews, packageCd,topicID,topic,author, headline,content,thumbnailUrl,vectorize, update_time   FROM {schema.News}.{tables.News.articles} 
                                                where newsDate>='{start_dt}' and newsDate<='{end_dt}' and contentReady='{contentReady}' order by newsDate desc   """

    elif not contentReady and  vectorize:
        select_query = f"""  SELECT refID,language,newsID,contentReady, newsDate, BreakingNews, packageCd,topicID,topic,author, headline,content,thumbnailUrl,vectorize, update_time FROM {schema.News}.{tables.News.articles}
                                                where newsDate>='{start_dt}' and newsDate<='{end_dt}' and vectorize='{vectorize}' order by newsDate desc   """

    else:
        select_query = f"""  SELECT refID,language,newsID,contentReady, newsDate, BreakingNews, packageCd,topicID,topic,author, headline,content,thumbnailUrl,vectorize, update_time  FROM {schema.News}.{tables.News.articles} 
                                             where newsDate>='{start_dt}' and newsDate<='{end_dt}' and contentReady='{contentReady}' and vectorize='{vectorize}' order by newsDate desc   """

    #print(f' sql query: {select_query}')
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )
        rows={}
        for i in  tqdm(cursor.fetchall(),desc='get news articles from SQL'):
            i['newsID'] = int(i['newsID'])
            newsID=i['newsID']
            rows[newsID]=i
        SQL_connection.close()
        return rows


def insert_articles(data:Union[Dict,List[Dict]]):

    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        select_query= f"""  select newsID from  {schema.News}.{tables.News.articles}  where newsID ={data['newsID']} """
        cursor.execute(select_query)
        rows= cursor.fetchall()
        if len(rows)>0:
            update_query =f""" update {schema.News}.{tables.News.articles} set  contentReady  = "{data['contentReady']}", content="{str(data['content'])}",newsDate='{data['newsDate']}', update_time='{data['update_time']}'  , vectorize='{data['vectorize']}'   where newsID ={int(data["newsID"])}"""
            cursor.execute(update_query)
        else:

            data=(data['refID'], data['language'], data['newsID'], data['contentReady'], data['newsCode'], data['newsDate'], data['BreakingNews'], data['datatype'], data['packageCd'], data['topicID'], data['topic'], data['author'], data['thumbnailUrl'], str((data['headline'])), str((data['content'])), data['vectorize'],data['update_time'])

            insert_query = f""" insert  into  {schema.News}.{tables.News.articles}  (refID,  language, newsID,   contentReady, newsCode,   newsDate,  BreakingNews,  datatype, packageCd, topicID, topic, author, thumbnailUrl,  headline, content,vectorize,update_time) values  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE content=values(content),contentReady=values(contentReady),newsDate=values(newsDate),update_time=values(update_time),vectorize=values(vectorize);"""
            cursor.execute(insert_query,data)
    SQL_connection.close()



####--------------------------------------------------------------------------------------------------------------------

def get_vecfilter_bytype(datatype:str):
    assert datatype.lower() in ['categorycd', 'topic', 'topicid', 'word', 'words'], print( "datatype must be 'categorycd','topic','topicid','word','words'")

    separator = '\t\t\t'

    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        vec_tables={'categorycd':tables.News.vecfilter.categoryCd, 'topic':tables.News.vecfilter.topic, 'topicid':tables.News.vecfilter.topicID, 'word':tables.News.vecfilter.words}
        vec_cols={'categorycd':'categoryCd', 'topic':'topic', 'topicid':'topicID', 'word':'word'}

        DataType=datatype.lower()
        vec_table=vec_tables[DataType]
        vec_col=vec_cols[DataType]

        select_query = f"""   SELECT product,GROUP_CONCAT({vec_col} SEPARATOR '{separator}') as {vec_col} FROM {schema.News}.{vec_table} group by product"""

        cursor.execute(select_query )
        sql_records=cursor.fetchall()

       # dict__= convert_sql2dict(vec_col=vec_col,sql_records=sql_records)

        rows = {}
        for i in sql_records:
            product_id = str(i['product'])
            vec_values = i[vec_col].split(separator)
            del i['product']
            rows[product_id] = {vec_col:vec_values}

        SQL_connection.close()
        return rows


def get_vecfilter():
    vec_cols = [ 'categoryCd',  'topic', 'topicID', 'word']

    output={}
    for vec_col in vec_cols:
        sql_data=get_vecfilter_bytype(vec_col)
        for product_id,vec_data in sql_data.items():
            if product_id not in output:
                output[product_id]=vec_data
            else:
                output[product_id].update(vec_data)

    return output


def insert_vecfilter(datatype:str,product:str,filter_data:Union[str,int],system_input:str='N'):

    assert datatype.lower() in ['categorycd', 'topic', 'topicid', 'word', 'words'], print( "datatype must be 'categorycd','topic','topicid','word','words'")
    vec_tables = {'categorycd': tables.News.vecfilter.categoryCd,'categoryCd': tables.News.vecfilter.categoryCd,
                   'topicid': tables.News.vecfilter.topicID,  'topicID': tables.News.vecfilter.topicID,
                  'word': tables.News.vecfilter.words,                'words': tables.News.vecfilter.words,
                'topic': tables.News.vecfilter.topic,
                  }
    vec_cols = {'categorycd': 'categoryCd','categoryCd': 'categoryCd', 'topic': 'topic', 'topicid': 'topicID', 'topicID': 'topicID', 'word': 'word', 'words': 'word'}

    DataType = datatype.lower()
    vec_table = vec_tables[DataType]
    vec_col = vec_cols[DataType]


    data=[product,filter_data,system_input,datetime.now()]
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        insert_query = f""" insert  into  {schema.News}.{vec_table}  (product,{vec_col},system_input ,update_time) values (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE update_time=values(update_time);"""
        cursor.execute(insert_query, data)
    SQL_connection.close()


###------------------------------------------------------------------------






###-----------------related keywords------------------------------------------------

def get_related_keywords_byid(newsID:Union[str,int,List[Union[str,int]]]):
    newsID = [newsID] if type(newsID) in [str, int] else [newsID]
    newsID = ','.join([str(a) for a in newsID])
    select_query = f"""  SELECT a.newsID, a.word 
                                            FROM {schema.News}.{tables.News.related_keywords} a
                                            join  {schema.News}.{tables.News.articles}  b on a.newsID=b.newsID
                                            where a.newsID in ({newsID})
                                            group by a.newsID
                                            order by b.newsDate desc"""
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )
        rows={}
        for i in  cursor.fetchall():
            newsID= int(i['newsID'])
            if newsID not in rows:
                rows[newsID]=[str(i['word'])]
            else:
                rows[newsID].append(str(i['word']))
        SQL_connection.close()
        return rows


def get_related_keywords_byorder(limit:int=500):
    select_query = f"""  SELECT a.newsID, a.word 
                                            FROM {schema.News}.{tables.News.related_keywords} a
                                            join  {schema.News}.{tables.News.articles}  b on a.newsID=b.newsID
                                            GROUP BY b.newsID  order by b.newsDate desc  limit {limit}"""
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )
        rows={}
        for i in  cursor.fetchall():
            newsID= int(i['newsID'])
            if newsID not in rows:
                rows[newsID]=[str(i['word'])]
            else:
                rows[newsID].append(str(i['word']))
        SQL_connection.close()
        return rows


def get_related_keywords_bydate(start_dt:Union[datetime,str,int],end_dt:Union[datetime,str]):

    start_dt, end_dt = tools.convert_str2dt(start=start_dt, end=end_dt)
    start_dt, end_dt = start_dt.strftime('%Y-%m-%d %H:%M:%S'),end_dt.strftime('%Y-%m-%d %H:%M:%S')
    print(f'get news related keywords from DB in time range : {start_dt} - {end_dt}')

    select_query = f"""  SELECT a.newsID, a.word
                                            FROM {schema.News}.{tables.News.related_keywords} a
                                            join  {schema.News}.{tables.News.articles}  b on a.newsID=b.newsID
                                            where newsDate>='{start_dt} and newsDate<=''{end_dt}'
                                            GROUP BY b.newsID  order by b.newsDate desc  """
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )

        rows={}
        for i in  cursor.fetchall():
            newsID= int(i['newsID'])
            if newsID not in rows:
                rows[newsID]=[str(i['word'])]
            else:
                rows[newsID].append(str(i['word']))

        SQL_connection.close()
        return rows

###--------------------------keywords------------------------------


def get_keywords_byorder(limit:int=500):
    select_query = f"""  SELECT a.word ,group_concat(a.newsID SEPARATOR ',') as newsIDs
                                            FROM {schema.News}.{tables.News.related_keywords} a
                                            join  {schema.News}.{tables.News.articles}  b on a.newsID=b.newsID
                                            group by a.word  order by b.newsDate desc  limit {limit}"""
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )
        rows={}
        for i in  cursor.fetchall():
            i['word'] = str(i['word'])
            word=i['word']
            newsIDs =i["newsIDs"].split(',')
            rows[word]= newsIDs
        SQL_connection.close()
        return rows


def get_keywords_byword(word:Union[str,int,List[Union[str,int]]]):
    word = [word] if type(word) in [str, int] else [word]
    word= ["'"+w+"'" for w in word]
    word = ','.join([str(a) for a in word])

    select_query = f"""  SELECT a.word ,group_concat(a.newsID SEPARATOR ',') as newsIDs
                                            FROM {schema.News}.{tables.News.related_keywords} a
                                            join  {schema.News}.{tables.News.articles}  b on a.newsID=b.newsID
                                            where a.word in ({word})
                                            group by a.word  order by b.newsDate desc  """

    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )
        rows={}
        for i in  cursor.fetchall():
            i['word'] = str(i['word'])
            word=i['word']
            newsIDs =i["newsIDs"].split(',')
            rows[word]= newsIDs
        SQL_connection.close()
        return rows

def insert_keywords(newsID,keywords:Union[List,str]):
    news_kw=[keywords] if type(keywords) ==str else keywords
    data=[(newsID,word,datetime.now()) for word in news_kw ]
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
            SQL_connection.ping(reconnect=True)
            insert_query = f""" insert  into  {schema.News}.{tables.News.related_keywords}  ( newsID,  word,update_time) values  (%s,%s,%s) ON DUPLICATE KEY UPDATE update_time=values(update_time);"""
            cursor.executemany(insert_query,data)
    SQL_connection.close()


##-------------Related Name Entity --------------------------------------------------------------

def get_related_ner_byid(newsID:Union[str,int,List[Union[str,int]]]):
    newsID = [newsID] if type(newsID) in [str, int] else [newsID]
    newsID = ','.join([str(a) for a in newsID])

    select_query = f"""SELECT a.newsID,a.name_entity_json
                                            FROM {schema.News}.{tables.News.embeddings} a
                                            join  {schema.News}.{tables.News.articles}  b on a.newsID=b.newsID
                                            where a.newsID in ({newsID})
                                            order by b.newsDate desc """
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )
        rows={}
        for i in  cursor.fetchall():
            newsID ,name_entity_json= int(i['newsID']),i["name_entity_json"]
            rows[newsID] = None if name_entity_json is None else pickle.loads(name_entity_json)

        SQL_connection.close()
        return rows



def get_related_ner_byorder(limit:int=500):
    select_query = f"""SELECT a.newsID,a.name_entity_json
                                            FROM {schema.News}.{tables.News.embeddings} a
                                            join  {schema.News}.{tables.News.articles}  b on a.newsID=b.newsID
                                            order by b.newsDate desc  limit {limit}"""
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )
        rows={}
        for i in  cursor.fetchall():
            newsID ,name_entity_json= int(i['newsID']),i["name_entity_json"]
            rows[newsID] = None if name_entity_json is None else pickle.loads(name_entity_json)

        SQL_connection.close()
        return rows


def get_related_ner_bydate(start_dt:Union[datetime,str,int],end_dt:Union[datetime,str]):

    start_dt, end_dt = tools.convert_str2dt(start=start_dt, end=end_dt)
    start_dt = start_dt.strftime('%Y-%m-%d %H:%M:%S')
    end_dt = end_dt.strftime('%Y-%m-%d %H:%M:%S')
    print(f'get news vectors from DB in time range : {start_dt} - {end_dt}')

    select_query = f"""SELECT a.newsID,a.name_entity_json
                                            FROM {schema.News}.{tables.News.embeddings} a
                                            join  {schema.News}.{tables.News.articles}  b on a.newsID=b.newsID
                                            where newsDate>='{start_dt} and newsDate<=''{end_dt}'
                                            order by b.newsDate desc """
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )

        rows={}
        for i in  cursor.fetchall():
            newsID ,name_entity_json= int(i['newsID']),i["name_entity_json"]
            rows[newsID] = None if name_entity_json is None else pickle.loads(name_entity_json)
        SQL_connection.close()
        return rows

##----------  vector ---------------------------------------------------------------------------


def get_vector_byid(newsID:Union[str,int,List[Union[str,int]]]):
    newsID = [newsID] if type(newsID) in [str, int] else [newsID]
    newsID = ','.join([str(a) for a in newsID])

    select_query = f"""SELECT a.newsID,a.vector
                                            FROM {schema.News}.{tables.News.embeddings} a
                                            join  {schema.News}.{tables.News.articles}  b on a.newsID=b.newsID
                                            where a.newsID in ({newsID})
                                            order by b.newsDate desc """
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )
        rows={}
        for i in  cursor.fetchall():
            newsID ,vector= int(i['newsID']),i["vector"]
            vector = None if vector is None else pickle.loads(vector)
            rows[newsID]=  vector
        SQL_connection.close()
        return rows



def get_vector_byorder(limit:int=500):
    select_query = f"""SELECT a.newsID,a.vector
                                            FROM {schema.News}.{tables.News.embeddings} a
                                            join  {schema.News}.{tables.News.articles}  b on a.newsID=b.newsID
                                            order by b.newsDate desc  limit {limit}"""
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )
        rows={}
        for i in  cursor.fetchall():
            newsID ,vector= int(i['newsID']),i["vector"]
            vector = None if vector is None else pickle.loads(vector)
          #  vector = pickle.loads(vector) if type(vector)== bytes else vector
            rows[newsID]=  vector
        SQL_connection.close()
        return rows


def get_vector_bydate(start_dt:Union[datetime,str,int],end_dt:Union[datetime,str]):

    start_dt, end_dt = tools.convert_str2dt(start=start_dt, end=end_dt)
    start_dt = start_dt.strftime('%Y-%m-%d %H:%M:%S')
    end_dt = end_dt.strftime('%Y-%m-%d %H:%M:%S')
    print(f'get news vectors from DB in time range : {start_dt} - {end_dt}')

    select_query = f"""SELECT a.newsID,a.vector
                                            FROM {schema.News}.{tables.News.embeddings} a
                                            join  {schema.News}.{tables.News.articles}  b on a.newsID=b.newsID
                                            where newsDate>='{start_dt} and newsDate<=''{end_dt}'
                                            order by b.newsDate desc """
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )
        rows={}
        for i in  cursor.fetchall():
            newsID ,vector= int(i['newsID']),i["vector"]
            vector = None if vector is None else pickle.loads(vector)
        #    vector = pickle.loads(vector) if type(vector)== bytes else vector
            rows[newsID]=  vector
        SQL_connection.close()
        return rows



def insert_vector(data):
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
            SQL_connection.ping(reconnect=True)
            ner,vector=data.get('ner'),data.get('vector')
            ner=pickle.dumps(ner) if type(ner)==dict else ner
            vector=pickle.dumps(vector) if type(vector)==np.ndarray else vector
         #   data=(data['refID'], data['language'], data['newsID'], data['contentReady'], data['newsCode'], data['newsDate'], data['BreakingNews'], data['datatype'], data['packageCd'], data['topicID'], data['topic'], data['author'], data['thumbnailUrl'], str((data['headline'])), str((data['content'])), data['vectorize'],data['update_time'])
            data=[data.get('newsID'),ner,vector ,datetime.now()]
            insert_query = f""" insert  into  {schema.News}.{tables.News.embeddings}  ( newsID,   name_entity_json, vector,update_time) values  (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE update_time=values(update_time);"""
            cursor.execute(insert_query,data)
    SQL_connection.close()

###-----------------------------NameEntity -------------------------------------------------------



def get_NameEntity_byorder(limit:int=500):
    select_query = f"""SELECT a.newsID,a.name_entity_json as ner
                                            FROM {schema.News}.{tables.News.embeddings} a
                                            join  {schema.News}.{tables.News.articles}  b on a.newsID=b.newsID
                                            order by b.newsDate desc  limit {limit}"""
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )
        rows={}
        for i in  cursor.fetchall():
            newsID = int(i['newsID'])

            ner=i["ner"]
            if ner in [None,'null','']:
                ner=[()]
            else:
                ner=pickle.loads(ner)
                if type(ner) in [bytes,bytearray]:
                    ner = pickle.loads(ner)
                else:
                    ner=ner
                if ner in [None, 'null', '']:
                    ner=[()]
                else:
                    ner= [(str(k),v) for k,v in ner.items()]

            rows[newsID]=  ner
        SQL_connection.close()
        return rows



def get_NameEntity_bydate(start_dt:Union[datetime,str,int],end_dt:Union[datetime,str]):
    start_dt, end_dt = tools.convert_str2dt(start=start_dt, end=end_dt)
    start_dt = start_dt.strftime('%Y-%m-%d %H:%M:%S')
    end_dt = end_dt.strftime('%Y-%m-%d %H:%M:%S')
    print(f'get news Name Entity from DB in time range : {start_dt} - {end_dt}')

    select_query = f"""SELECT a.newsID,a.name_entity_json as ner
                                            FROM {schema.News}.{tables.News.embeddings} a
                                            join  {schema.News}.{tables.News.articles}  b on a.newsID=b.newsID
                                            where b.newsDate>='{start_dt} and b.newsDate<=''{end_dt}'
                                            order by b.newsDate desc """
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )
        rows={}
        for i in  cursor.fetchall():
            newsID = int(i['newsID'])

            ner=i["ner"]
            if ner in [None,'null','']:
                ner=[()]
            else:
                ner=pickle.loads(ner)
                if type(ner) in [bytes,bytearray]:
                    ner = pickle.loads(ner)
                else:
                    ner=ner
                if ner in [None, 'null', '']:
                    ner=[()]
                else:
                    ner= [(str(k),v) for k,v in ner.items()]

            rows[newsID]=  ner
        SQL_connection.close()
        return rows





def get_NameEntity_byid(newsID:Union[str,int,List[Union[str,int]]]):
    newsID = [newsID] if type(newsID) in [str, int] else [newsID]
    newsID = ','.join([str(a) for a in newsID])


    select_query = f"""SELECT a.newsID,a.name_entity_json as ner
                                            FROM {schema.News}.{tables.News.embeddings} a
                                            join  {schema.News}.{tables.News.articles}  b on a.newsID=b.newsID
                                            where a.newsID in ({newsID})
                                            order by b.newsDate desc """
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )
        rows={}
        for i in  cursor.fetchall():
            newsID = int(i['newsID'])

            ner=i["ner"]
            if ner in [None,'null','']:
                ner=[()]
            else:
                ner=pickle.loads(ner)
                if type(ner) in [bytes,bytearray]:
                    ner = pickle.loads(ner)
                else:
                    ner=ner
                if ner in [None, 'null', '']:
                    ner=[()]
                else:
                    ner= [(str(k),v) for k,v in ner.items()]

            rows[newsID]=  ner
        SQL_connection.close()
        return rows



def insert_NameEntity(data:Dict):
    data_ = [(word, word_data.get('label'), datetime.now()) for word,word_data in data.items()]
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
            SQL_connection.ping(reconnect=True)

            insert_query = f""" insert  into  {schema.vocab}.{tables.vocab.NameEntites}  (word,label,update_time) values  (%s,%s,%s) ON DUPLICATE KEY UPDATE update_time=values(update_time);"""
            cursor.executemany(insert_query,data_)
    SQL_connection.close()


###---------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def insert_related_news(data:Union[List[Tuple],Tuple]):
    data=[data] if type(data) ==tuple else data
    data=[(id,id_comp,sim,datetime.now()) for (id,id_comp,sim) in data ]
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
            SQL_connection.ping(reconnect=True)
            insert_query = f""" insert  into  {schema.News}.{tables.News.related_articles}  ( newsID, newsID_comp,sim,update_time) values  (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE update_time=values(update_time);"""
            cursor.executemany(insert_query,data)
    SQL_connection.close()

###---------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def get_nertype_scores():
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        select_query = f"""   SELECT label,score,ranking_score  FROM {schema.News}.{tables.News.name_entity_scores}"""
        cursor.execute(select_query )

        rows= {i['label']:i for i in  cursor.fetchall()}

        SQL_connection.close()
        return rows


###




##---------------------------------------------------------------------------------------------------------------
if __name__=='__main__':
     fire.Fire()