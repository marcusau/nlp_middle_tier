# /usr/bin/env python
# -*- coding: utf-8 -*-

import os, pathlib, sys,fire,logging

sys.path.append(os.getcwd())

parent_path = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(parent_path))

master_path = parent_path.parent
sys.path.append(str(master_path))

project_path = master_path.parent
sys.path.append(str(project_path))

from typing import Dict,List,Union,Optional,Tuple
from itertools import chain
import operator

from datetime import date,timedelta,datetime
from tqdm import tqdm
import pickle

import pymysql
from sql_db import SQL_connection,schema,tables
import tools

##--------------------------------------------------------------------------------------------------------------------------------------------

def get_section():
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        select_query = f"""   SELECT *  FROM {schema.lifestyle}.{tables.lifestyle.section}"""
        cursor.execute(select_query )
        rows={i['secid']:i for i in  tqdm(cursor.fetchall(),desc=f'get lifestyle section ,  db : {schema.lifestyle}, table: {tables.lifestyle.section}')}
        SQL_connection.close()
        return rows

def insert_section(data):
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        insert_query = f""" insert into  {schema.lifestyle}.{tables.lifestyle.section}  (secid,name,alias,seccolor,update_time) values (%s,%s,%s,%s,%s)  ON DUPLICATE KEY UPDATE update_time=values(update_time); """
        cursor.execute(insert_query,data )

    SQL_connection.close()


##--------------------------------------------------------------------------------------------------------------------------------------------
def get_category():
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        select_query =f""" SELECT *     FROM {schema.lifestyle}.{tables.lifestyle.category} """
        cursor.execute(select_query)
        rows={}
        for i in tqdm(cursor.fetchall(),desc=f'get lifestyle category ,  db : {schema.lifestyle}, table: {tables.lifestyle.category}'):
            catid=int(i['catid'])
            rows[catid]=i
        SQL_connection.close()
        return rows


def insert_category(data):
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        insert_query = f""" insert into  {schema.lifestyle}.{tables.lifestyle.category}  (catid,secid,cattitle,catalias,catdesc,author,status,allow_comment,updatefreq,source,source_url,update_time) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE update_time=values(update_time); """
        cursor.execute(insert_query,data )

    SQL_connection.close()

###----------------------------------------------------------------------------------------------------------------------------
def get_related_category():
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        select_query =f""" SELECT  catid,GROUP_CONCAT(related_catid SEPARATOR ',') as related_catids      FROM {schema.lifestyle}.{tables.lifestyle.related_category} group by catid """
        cursor.execute(select_query)
        rows={int(i['catid']):[int(id) for id in i['related_catids'].split(',')] for i in tqdm(cursor.fetchall(),desc=f'get lifestyle  relatedcategory ,  db : {schema.lifestyle}, table: {tables.lifestyle.related_category}')}
        SQL_connection.close()
        return rows

def insert_related_category(data):
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        insert_query= f""" insert into  {schema.lifestyle}.{tables.lifestyle.related_category}  (catid,related_catid ,update_time) values (%s,%s,%s) ON DUPLICATE KEY UPDATE update_time=values(update_time);"""
        cursor.execute(insert_query, data)

    SQL_connection.close()

##---------------------------------------------------------------------------------------------------------------

def get_tortags_bytitle(titles:Union[str,List[str]]=None):
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        if titles:
            titles=[titles] if isinstance(titles,str) else titles
            titles=','.join(['"'+t+'"' for t in titles])
            select_query = f"""   SELECT *  FROM {schema.lifestyle}.{tables.lifestyle.tortags_tags} where tag_title in ({titles})"""

        else:
            select_query = f"""   SELECT *  FROM {schema.lifestyle}.{tables.lifestyle.tortags_tags}"""
        print(select_query)
        cursor.execute(select_query )
        rows={i['tagid']:i for i in  tqdm(cursor.fetchall(),desc=f'get lifestyle tortags_tags_bytitle,  schema: {schema.lifestyle}, table: {tables.lifestyle.tortags_tags}')}
        SQL_connection.close()
        return rows

def get_tortags_byorder(limit:int=None):
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        if limit:
            select_query = f"""   SELECT *  FROM {schema.lifestyle}.{tables.lifestyle.tortags_tags} limit {limit}"""
        else:
            select_query = f"""   SELECT *  FROM {schema.lifestyle}.{tables.lifestyle.tortags_tags}"""
        cursor.execute(select_query )
        rows={i['tagid']:i for i in tqdm( cursor.fetchall(),desc=f' get lifestyle  tortags_tags  schema: {schema.lifestyle}  table:  {tables.lifestyle.tortags_tags}')}
        SQL_connection.close()
        return rows

def insert_tortags(data):
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        insert_query= f""" insert into  {schema.lifestyle}.{tables.lifestyle.tortags_tags}  (tagid,tag_title,hits ,update_time) values (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE update_time=values(update_time);"""
        cursor.execute(insert_query, data)
    SQL_connection.close()

###---------------------------------------------------------------------------------------------------------------------------

def get_related_tortags_bydate(start_dt:str,end_dt:str,):
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        select_query=f"""  SELECT b.artid,group_concat(c.tagid SEPARATOR ',') as tagids, group_concat(c.tag_title SEPARATOR '        ') as tag_titles
                                        FROM {schema.lifestyle}.{tables.lifestyle.tortags} a
                                        join {schema.lifestyle}.{tables.lifestyle.articles} b on a.artid=b.artid 
                                        join {schema.lifestyle}.{tables.lifestyle.tortags_tags} c on c.tagid=a.tagid
                                        where b.publish_up>='{start_dt}' and b.publish_up<='{end_dt}'
                                        group by b.artid
                                        order by b.publish_up desc """
        cursor.execute(select_query)
        #rows =cursor.fetchall()
        rows={}
        for i in tqdm(cursor.fetchall(),desc=f'get lifestyle related tortags , schema: {schema.lifestyle} table: {tables.lifestyle.tortags_tags}'):
            artid=int(i.get('artid',None))
            tagids = i.get('tagids', None)
            tag_titles= i.get('tag_titles', None)

            tagids=tagids.split(',')
            tag_titles=tag_titles.split('        ')
            tortags={int(tagid): tag_title for tagid, tag_title in zip(tagids,tag_titles)}
            rows[artid]=tortags

    SQL_connection.close()
    return rows

def insert_related_tortags(data):
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        insert_query= f""" insert into  {schema.lifestyle}.{tables.lifestyle.tortags}  (tagid,artid ,update_time) values (%s,%s,%s) ON DUPLICATE KEY UPDATE update_time=values(update_time);"""
        cursor.execute(insert_query, data)
    SQL_connection.close()


##---------------------------------------------------------------------------------------------------------------

def get_articles_byorder(limit:int=None):
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        if limit:
            select_query = f"""   SELECT *  FROM {schema.lifestyle}.{tables.lifestyle.articles}  order by publish_up desc limit {limit}"""
        else:
            select_query = f"""   SELECT *  FROM {schema.lifestyle}.{tables.lifestyle.articles} order by publish_up desc"""
        cursor.execute(select_query )
        rows={}
        for i in  tqdm(cursor.fetchall(),desc=f' get lifestyle article from DB, schema : {schema.lifestyle}, Tables: {tables.lifestyle.articles}'):
            artid=int(i['artid'])
            del i['artid']
            rows[artid]=i
        SQL_connection.close()
        return rows


def get_articles_byid(artid:Union[str,int,List[Union[str,int]]]):

    artid = [artid] if type(artid) in [str, int] else [artid]
    artid = ','.join([str(a) for a in artid])
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        select_query = f"""   SELECT *  FROM {schema.lifestyle}.{tables.lifestyle.articles} where artid in ({artid}) order by publish_up desc """
        cursor.execute(select_query )
        rows={}
        for i in  tqdm(cursor.fetchall(),desc=f' get lifestyle article from DB, schema : {schema.lifestyle}, Tables: {tables.lifestyle.articles}'):
            artid=int(i['artid'])
            del i['artid']
            rows[artid]=i
        SQL_connection.close()
        return rows


def get_articles_bydate(start_dt: Union[datetime,str,int] = 30,end_dt: Union[datetime,str] = datetime.now()):
    start_dt, end_dt = tools.convert_str2dt(start=start_dt, end=end_dt)
    print(f' get lifestyle articles from db in Time Range :  {start_dt}  -  {end_dt}')

    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        select_query = f"""   SELECT *  FROM {schema.lifestyle}.{tables.lifestyle.articles} where publish_up>='{start_dt}' and publish_up <='{end_dt}' order by publish_up desc """

        cursor.execute(select_query )
        rows={}
        for i in tqdm(cursor.fetchall(),desc=f' get lifestyle article from DB, schema : {schema.lifestyle}, Tables: {tables.lifestyle.articles}'):
            artid=int(i['artid'])
            del i['artid']
            rows[artid]=i
        SQL_connection.close()
        return rows

def insert_articles(data):
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        insert_query = f""" insert  into  {schema.lifestyle}.{tables.lifestyle.articles}  (artid , language , catid , publish_up , arttitle , introtext ,  {schema.lifestyle}.{tables.lifestyle.articles}.fulltext , author , isHealth , isHot , isFocus , images , images_m , share_url ,update_time,vectorize) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE publish_up=values(publish_up), update_time=values(update_time), vectorize=values(vectorize);"""
        cursor.execute(insert_query, data)
    SQL_connection.close()

###------------------------------------------------------------------------------------------------------------------------
def get_vecfilter_bytype(datatype:str):
    assert datatype.lower() in ['catid',  'word', 'words'], print( "datatype must be 'catid',  'word'")

    separator = '\t\t\t'

    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        vec_tables={'catid':tables.lifestyle.vecfilter.catid,   'word':tables.lifestyle.vecfilter.words}
        vec_cols={'catid':'catid',  'word':'word'}

        DataType=datatype.lower()
        vec_table=vec_tables[DataType]
        vec_col=vec_cols[DataType]

        select_query = f"""   SELECT product,GROUP_CONCAT({vec_col} SEPARATOR '{separator}') as {vec_col} FROM {schema.lifestyle}.{vec_table} group by product"""

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
    vec_cols = [ 'catid',  'word']

    output={}
    for vec_col in vec_cols:
        sql_data=get_vecfilter_bytype(vec_col)
        for product_id,vec_data in sql_data.items():
            if product_id not in output:
                output[product_id]=vec_data
            else:
                output[product_id].update(vec_data)

    return output


def insert_vecfilter(datatype:str,product:str,filter_data:Union[str,int],system_input='N'):

    assert datatype.lower() in ['catid',  'word', 'words'], print( "datatype must be 'catid',  'word'")

    vec_tables = {'catid': tables.lifestyle.vecfilter.catid, 'word': tables.lifestyle.vecfilter.words}
    vec_cols = {'catid': 'catid', 'word': 'word'}

    DataType = datatype.lower()
    vec_table = vec_tables[DataType]
    vec_col = vec_cols[DataType]

    data=[product,filter_data,system_input,datetime.now()]
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        insert_query= f""" insert into  {schema.lifestyle}.{vec_table}  (product,{vec_col},system_input,update_time) values (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE update_time=values(update_time);"""
        cursor.execute(insert_query, data)
    SQL_connection.close()



##---------------------------------------------------------------------------------------------------------------

def get_fullarticles_bydate(start_dt: Union[datetime,str,int] = 30,end_dt: Union[datetime,str] = datetime.now()):

    start_dt, end_dt = tools.convert_str2dt(start=start_dt, end=end_dt)
    print(f' Time Range :  {start_dt}  -  {end_dt}')

    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        select_query = f""" SELECT a.artid, a.language, a.publish_up,d.secid,d.name as section_name, a.catid,  b.cattitle, GROUP_CONCAT(c.related_catid SEPARATOR ',') as related_catid, e.tags ,a.arttitle, a.introtext, a.fulltext, a.author, a.isHealth, a.isHot, a.isFocus,a.images_url, a.images_m_url,  a.share_url, a.vectorize,a.update_time  
                                    FROM {schema.lifestyle}.{tables.lifestyle.articles} a
                                    join  {schema.lifestyle}.{tables.lifestyle.category} b on a.catid=b.catid
                                    join  {schema.lifestyle}.{tables.lifestyle.related_category} c on b.catid=c.catid
                                    join  {schema.lifestyle}.{tables.lifestyle.section} d on b.secid=d.secid
                                    join 
                                                (SELECT a.artid,  GROUP_CONCAT(f.tag_title SEPARATOR '\t') as tags 
                                                 FROM {schema.lifestyle}.{tables.lifestyle.articles} a
		                                join {schema.lifestyle}.{tables.lifestyle.tortags} e on e.artid=a.artid
		                                join  {schema.lifestyle}.{tables.lifestyle.tortags_tags} f on f.tagid=e.tagid
		                                where publish_up>='{start_dt}' and publish_up <='{end_dt}' group by a.artid ) as e 
		                    on e.artid=a.artid	                                
		                    where publish_up>='{start_dt}' and publish_up <='{end_dt}'
		                    group by a.artid 
		                    order by publish_up desc"""
        cursor.execute(select_query )
        rows={}
        for i in  tqdm(cursor.fetchall(),desc=f' get lifestyle article from DB, schema : {schema.lifestyle}, Tables: {tables.lifestyle.articles}'):
            artid=i['artid']
            i['related_catid']=i['related_catid'].split(',')
            i['tags'] = i['tags'].split('\t')
            del i['artid']
            rows[artid]=i
        SQL_connection.close()
        return rows


def get_fullarticles_byid(artid:Union[str,int,List[Union[str,int]]]):
    artid = [artid] if type(artid) in [str, int] else [artid]
    artid = ','.join([str(a) for a in artid])
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        select_query = f""" SELECT a.artid, a.language, a.publish_up,d.secid,d.name as section_name, a.catid,  b.cattitle, GROUP_CONCAT(c.related_catid SEPARATOR ',') as related_catid, e.tags ,a.arttitle, a.introtext, a.fulltext, a.author, a.isHealth, a.isHot, a.isFocus,a.images_url, a.images_m_url,  a.share_url, a.vectorize,a.update_time  
                                    FROM {schema.lifestyle}.{tables.lifestyle.articles} a
                                    join  {schema.lifestyle}.{tables.lifestyle.category} b on a.catid=b.catid
                                    join  {schema.lifestyle}.{tables.lifestyle.related_category} c on b.catid=c.catid
                                    join  {schema.lifestyle}.{tables.lifestyle.section} d on b.secid=d.secid
                                    join 
                                                (SELECT a.artid,  GROUP_CONCAT(f.tag_title SEPARATOR '\t') as tags 
                                                 FROM {schema.lifestyle}.{tables.lifestyle.articles} a
		                                join {schema.lifestyle}.{tables.lifestyle.tortags} e on e.artid=a.artid
		                                join  {schema.lifestyle}.{tables.lifestyle.tortags_tags} f on f.tagid=e.tagid
		                                where  a.artid in ({artid})  group by a.artid ) as e 
		                    on e.artid=a.artid	                                
		                    where  a.artid in ({artid}) 
		                    group by a.artid 
		                    order by publish_up desc"""

        cursor.execute(select_query )
        rows={}
        for i in  tqdm(cursor.fetchall(),desc=f' get lifestyle article from DB, schema : {schema.lifestyle}, Tables: {tables.lifestyle.articles}'):
            artid=i['artid']
            i['related_catid']=i['related_catid'].split(',')
            i['tags'] = i['tags'].split('\t')
            del i['artid']
            rows[artid]=i
        SQL_connection.close()
        return rows


##------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def get_NameEntity_byorder(limit:int=None):
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        if limit:
            select_query = f"""   SELECT artid,name_entity_json  FROM {schema.lifestyle}.{tables.lifestyle.embeddings} limit {limit}"""
        else:
            select_query = f"""    SELECT artid,name_entity_json  FROM {schema.lifestyle}.{tables.lifestyle.embeddings}"""
        cursor.execute(select_query )
        rows = {}
        for i in  tqdm(cursor.fetchall(),desc=f' get lifestyle name entity from DB, schema : {schema.lifestyle}, Tables: {tables.lifestyle.embeddings}'):
            artid=i['artid']
            name_entity_json=pickle.loads(i['name_entity_json'])
            if name_entity_json:
                name_entity=[(k,v) for k,v in name_entity_json.items()]
            else:
                name_entity =[]
            rows[artid]=name_entity
        SQL_connection.close()
        return rows

def get_NameEntity_byid(artid:Union[str,int,List[Union[str,int]]]):

    artid = [artid] if type(artid) in [str, int] else [artid]
    artid = ','.join([str(a) for a in artid])

    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        select_query = f"""   SELECT artid,name_entity_json  FROM {schema.lifestyle}.{tables.lifestyle.embeddings} where artid in ({artid})"""

        cursor.execute(select_query )
        rows = {}
        for i in  tqdm(cursor.fetchall(),desc=f' get lifestyle name entity from DB, schema : {schema.lifestyle}, Tables: {tables.lifestyle.embeddings}'):
            artid=i['artid']
            name_entity_json=pickle.loads(i['name_entity_json'])
            if name_entity_json:
                name_entity=[(k,v) for k,v in name_entity_json.items()]
            else:
                name_entity =[]
            rows[artid]=name_entity
        SQL_connection.close()
        return rows



def insert_NameEntity(data:Dict):
    data_ = [(word, word_data.get('label'), datetime.now()) for word,word_data in data.items()]
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
            SQL_connection.ping(reconnect=True)

            insert_query = f""" insert  into  {schema.vocab}.{tables.vocab.NameEntites}  (word,label,update_time) values  (%s,%s,%s) ON DUPLICATE KEY UPDATE update_time=values(update_time);"""
            cursor.executemany(insert_query,data_)
    SQL_connection.close()

###-------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def get_nertype_scores():

    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        select_query = f"""   SELECT label,score,ranking_score  FROM {schema.lifestyle}.{tables.lifestyle.name_entity_scores}"""
        cursor.execute(select_query )
        rows= {i['label']:i for i in  cursor.fetchall()}
        SQL_connection.close()
        return rows

###-------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def get_keywords_byorder(limit:int=500):
    select_query = f"""  SELECT a.word ,group_concat(a.artid SEPARATOR ',') as artids
                                            FROM {schema.lifestyle}.{tables.lifestyle.related_keywords} a
                                            join  {schema.lifestyle}.{tables.lifestyle.articles}  b on a.artid=b.artid
                                            group by a.word  order by b.publish_up desc  limit {limit}"""
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )
        rows={}
        for i in  cursor.fetchall():
            i['word'] = str(i['word'])
            word=i['word']
            newsIDs =i["artids"].split(',')
            rows[word]= newsIDs
        SQL_connection.close()
        return rows


def get_keywords_byword(word:Union[str,int,List[Union[str,int]]]):
    word = [word] if type(word) in [str, int] else [word]
    word= ["'"+w+"'" for w in word]
    word = ','.join([str(a) for a in word])

    select_query = f"""  SELECT a.word ,group_concat(a.artid SEPARATOR ',') as artids
                                            FROM {schema.lifestyle}.{tables.lifestyle.related_keywords} a
                                            join  {schema.lifestyle}.{tables.lifestyle.articles}  b on a.artid=b.artid
                                            where a.word in ({word})
                                            group by a.word  order by b.publish_up desc  """

    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )
        rows={}
        for i in  cursor.fetchall():
            i['word'] = str(i['word'])
            word=i['word']
            newsIDs =i["artids"].split(',')
            rows[word]= newsIDs
        SQL_connection.close()
        return rows

def insert_keywords(artid,keywords:Union[List,str]):
    lifestyle_kw=[keywords] if type(keywords) ==str else keywords
    data=[(artid,word,datetime.now()) for word in lifestyle_kw ]
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
            SQL_connection.ping(reconnect=True)
            insert_query = f""" insert  into  {schema.lifestyle}.{tables.lifestyle.related_keywords}  (artid,  word,update_time) values  (%s,%s,%s) ON DUPLICATE KEY UPDATE update_time=values(update_time);"""
            cursor.executemany(insert_query,data)
    SQL_connection.close()
###############--------------------------------------------



def get_related_keywords_byorder(limit:int=10):
    select_query = f"""  SELECT a.artid ,a.word 
                                            FROM {schema.lifestyle}.{tables.lifestyle.related_keywords} a
                                            join  {schema.lifestyle}.{tables.lifestyle.articles}  b on a.artid=b.artid
                                             order by b.publish_up desc  limit {limit}"""
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )

        rows={}
        for i in  cursor.fetchall():
            artid,word=i['artid'],i["word"]
            if artid in rows:
                rows[artid].append(word)
            else:
                rows[artid] = [word]

        SQL_connection.close()
        return rows


def get_related_keywords_byid(artid:Union[str,int,List[Union[str,int]]]):

    artid = [artid] if type(artid) in [str, int] else [artid]
    artid = ','.join([str(a) for a in artid])

    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        select_query = f"""   SELECT a.artid ,a.word 
                                                FROM {schema.lifestyle}.{tables.lifestyle.related_keywords} a
                                                 join  {schema.lifestyle}.{tables.lifestyle.articles}  b 
                                                 on a.artid=b.artid
                                                 where artid in ({artid})"""

        cursor.execute(select_query )


        rows = {}
        for i in cursor.fetchall():
            artid, word = i['artid'], i["word"]
            if artid in rows:
                rows[artid].append(word)
            else:
                rows[artid] = [word]
        SQL_connection.close()
        return rows


def get_related_keywords_bydate(start_dt:Union[datetime,str,int],end_dt:Union[datetime,str]):

    start_dt, end_dt = tools.convert_str2dt(start=start_dt, end=end_dt)
    start_dt = start_dt.strftime('%Y-%m-%d %H:%M:%S')
    end_dt = end_dt.strftime('%Y-%m-%d %H:%M:%S')
    print(f'get related_keywords  from DB in time range : {start_dt} - {end_dt}')

    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        select_query = f"""   SELECT a.artid ,a.word 
                                                FROM {schema.lifestyle}.{tables.lifestyle.related_keywords} a
                                                 join  {schema.lifestyle}.{tables.lifestyle.articles}  b 
                                                 on a.artid=b.artid
                                                where b.publish_up>='{start_dt} and b.publish_up<=''{end_dt}'"""

        cursor.execute(select_query )

        rows = {}
        for i in cursor.fetchall():
            artid, word = i['artid'], i["word"]
            if artid in rows:
                rows[artid].append(word)
            else:
                rows[artid] = [word]
        SQL_connection.close()
        return rows


##----------  vector ---------------------------------------------------------------------------




def get_related_articles_byorder(limit:int=10):
    select_query = f"""  SELECT a.artid ,a.artid_comp, a.sim
                                            FROM {schema.lifestyle}.{tables.lifestyle.related_articles} a
                                            join  {schema.lifestyle}.{tables.lifestyle.articles}  b on a.artid=b.artid
                                             order by b.publish_up desc  limit {limit}"""
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )

        rows={}
        for i in  cursor.fetchall():
            artid,artid_comp,sim=i['artid'],i["artid_comp"],float(i["sim"])
            if artid in rows:
                rows[artid].update({artid_comp:sim})
            else:
                rows[artid] = {artid_comp:sim}

        for artid, related_arts in rows.items():
            rows[artid] = dict(sorted(related_arts.items(), key=operator.itemgetter(1), reverse=True))

        SQL_connection.close()
        return rows


def get_related_articles_byid(artid:Union[str,int,List[Union[str,int]]]):

    artid = [artid] if type(artid) in [str, int] else [artid]
    artid = ','.join([str(a) for a in artid])

    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        select_query = f"""   SELECT a.artid ,a.artid_comp, a.sim
                                                FROM {schema.lifestyle}.{tables.lifestyle.related_articles} a
                                                 join  {schema.lifestyle}.{tables.lifestyle.articles}  b 
                                                 on a.artid=b.artid
                                                 where a.artid in ({artid})"""

        cursor.execute(select_query )

        rows={}
        for i in  cursor.fetchall():
            artid,artid_comp,sim=i['artid'],i["artid_comp"],float(i["sim"])
            if artid in rows:
                rows[artid].update({artid_comp:sim})
            else:
                rows[artid] = {artid_comp:sim}

        for artid, related_arts in rows.items():
            rows[artid] = dict(sorted(related_arts.items(), key=operator.itemgetter(1), reverse=True))

        SQL_connection.close()
        return rows


def get_related_articles_bydate(start_dt:Union[datetime,str,int],end_dt:Union[datetime,str]):

    start_dt, end_dt = tools.convert_str2dt(start=start_dt, end=end_dt)
    start_dt = start_dt.strftime('%Y-%m-%d %H:%M:%S')
    end_dt = end_dt.strftime('%Y-%m-%d %H:%M:%S')
    print(f'get related_keywords  from DB in time range : {start_dt} - {end_dt}')

    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        select_query = f"""   SELECT  a.artid ,a.artid_comp, a.sim
                                                FROM {schema.lifestyle}.{tables.lifestyle.related_articles} a
                                                 join  {schema.lifestyle}.{tables.lifestyle.articles}  b 
                                                 on a.artid=b.artid
                                                where b.publish_up>='{start_dt} and b.publish_up<=''{end_dt}'"""

        cursor.execute(select_query )

        rows={}
        for i in  cursor.fetchall():
            artid,artid_comp,sim=i['artid'],i["artid_comp"],float(i["sim"])
            if artid in rows:
                rows[artid].update({artid_comp:sim})
            else:
                rows[artid] = {artid_comp:sim}

        for artid, related_arts in rows.items():
            rows[artid] = dict(sorted(related_arts.items(), key=operator.itemgetter(1), reverse=True))

        SQL_connection.close()
        return rows


def insert_related_articles(data:List[Tuple]):
    data=[ (artid,artid_comp,sim,datetime.now())  for (artid,artid_comp,sim) in data]
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
            SQL_connection.ping(reconnect=True)
            insert_query = f""" insert  into  {schema.lifestyle}.{tables.lifestyle.related_articles}  ( artid,  artid_comp,sim,update_time) values  (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE  sim=values(sim),update_time=values(update_time);"""
            cursor.executemany(insert_query,data)
    SQL_connection.close()


##-------------Related Name Entity --------------------------------------------------------------

def get_related_ner_byid(artid:Union[str,int,List[Union[str,int]]]):
    artid = [artid] if type(artid) in [str, int] else [artid]
    artid = ','.join([str(a) for a in artid])

    select_query = f"""SELECT a.artid,a.name_entity_json
                                            FROM {schema.lifestyle}.{tables.lifestyle.embeddings} a
                                            join  {schema.lifestyle}.{tables.lifestyle.articles}  b on a.artid=b.artid
                                            where a.artid in ({artid})
                                            order by b.publish_up desc """
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )
        rows={}
        for i in  cursor.fetchall():
            artid ,name_entity_json= int(i['artid']),i["name_entity_json"]
            rows[artid] = None if name_entity_json is None else pickle.loads(name_entity_json)

        SQL_connection.close()
        return rows



def get_related_ner_byorder(limit:int=500):
    select_query = f"""SELECT a.artid,a.name_entity_json
                                            FROM {schema.lifestyle}.{tables.lifestyle.embeddings} a
                                            join  {schema.lifestyle}.{tables.lifestyle.articles}  b on a.artid=b.artid
                                            order by b.publish_up desc  limit {limit}"""
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )

        rows={}
        for i in  cursor.fetchall():
            artid ,name_entity_json= int(i['artid']),i["name_entity_json"]
            rows[artid] = None if name_entity_json is None else pickle.loads(name_entity_json)

        SQL_connection.close()
        return rows


def get_related_ner_bydate(start_dt:Union[datetime,str,int],end_dt:Union[datetime,str]):

    start_dt, end_dt = tools.convert_str2dt(start=start_dt, end=end_dt)
    start_dt = start_dt.strftime('%Y-%m-%d %H:%M:%S')
    end_dt = end_dt.strftime('%Y-%m-%d %H:%M:%S')
    print(f'get articles ner  from DB in time range : {start_dt} - {end_dt}')

    select_query = f"""SELECT a.artid,a.name_entity_json
                                            FROM {schema.lifestyle}.{tables.lifestyle.embeddings} a
                                            join  {schema.lifestyle}.{tables.lifestyle.articles}  b on a.artid=b.artid
                                            where publish_up>='{start_dt} and publish_up<=''{end_dt}'
                                            order by b.publish_up desc """
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )

        rows={}
        for i in  cursor.fetchall():
            artid ,name_entity_json= int(i['artid']),i["name_entity_json"]
            rows[artid] = None if name_entity_json is None else pickle.loads(name_entity_json)

        SQL_connection.close()
        return rows

##----------  vector ---------------------------------------------------------------------------


def get_vector_byid(artid:Union[str,int,List[Union[str,int]]]):
    artid = [artid] if type(artid) in [str, int] else [artid]
    artid = ','.join([str(a) for a in artid])

    select_query = f"""SELECT a.artid,a.vector        FROM {schema.lifestyle}.{tables.lifestyle.embeddings} a
                                            join  {schema.lifestyle}.{tables.lifestyle.articles}  b on a.artid=b.artid
                                            where a.artid in ({artid})
                                            order by b.publish_up desc """
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )

        rows={}
        for i in  cursor.fetchall():
            newsID ,vector= int(i['artid']),i["vector"]
            vector= None if vector is None else pickle.loads(vector)
            rows[newsID]=  vector

        SQL_connection.close()
        return rows



def get_vector_byorder(limit:int=500):
    select_query = f"""SELECT a.artid,a.vector   FROM {schema.lifestyle}.{tables.lifestyle.embeddings} a
                                            join  {schema.lifestyle}.{tables.lifestyle.articles}  b on a.artid=b.artid
                                            order by b.publish_up desc  limit {limit}"""
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )

        rows={}
        for i in  cursor.fetchall():
            newsID ,vector= int(i['artid']),i["vector"]
            vector= None if vector is None else pickle.loads(vector)
            rows[newsID]=  vector
        SQL_connection.close()
        return rows


def get_vector_bydate(start_dt:Union[datetime,str,int],end_dt:Union[datetime,str]):

    start_dt, end_dt = tools.convert_str2dt(start=start_dt, end=end_dt)
    start_dt = start_dt.strftime('%Y-%m-%d %H:%M:%S')
    end_dt = end_dt.strftime('%Y-%m-%d %H:%M:%S')
    print(f'get news vectors from DB in time range : {start_dt} - {end_dt}')

    select_query = f"""SELECT a.artid,a.vector
                                            FROM {schema.lifestyle}.{tables.lifestyle.embeddings} a
                                            join  {schema.lifestyle}.{tables.lifestyle.articles}  b on a.artid=b.artid
                                            where b.publish_up>='{start_dt} and b.publish_up<=''{end_dt}'
                                            order by b.publish_up desc """
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        SQL_connection.ping(reconnect=True)
        cursor.execute(select_query )
        rows={}
        for i in  cursor.fetchall():
            newsID ,vector= int(i['artid']),i["vector"]
            vector= None if vector is None else pickle.loads(vector)
            rows[newsID]=  vector

        SQL_connection.close()
        return rows



def insert_vector(data):
    with SQL_connection.cursor(pymysql.cursors.DictCursor) as cursor:
            SQL_connection.ping(reconnect=True)
            data=[data.get('artid'),pickle.dumps(data.get('ner')),pickle.dumps(data.get('vector')),datetime.now()]
            insert_query = f""" insert  into  {schema.lifestyle}.{tables.lifestyle.embeddings}  (artid,   name_entity_json, vector,update_time) values  (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE name_entity_json=values(name_entity_json), vector=values(vector), update_time=values(update_time);"""
            cursor.execute(insert_query,data)
    SQL_connection.close()











if __name__=='__main__':
     fire.Fire()

