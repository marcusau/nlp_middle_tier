import os, pathlib, sys,fire,logging

sys.path.append(os.getcwd())

parent_path = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(parent_path))

master_path = parent_path.parent
sys.path.append(str(master_path))

project_path = master_path.parent
sys.path.append(str(project_path))

from typing import Union,Dict,List

from datetime import datetime
from tqdm import tqdm

import tools
from lifestyle.ETL import input_api, sql_query,util

sql_vecfilter_data=sql_query.get_vecfilter()['vectorize']



def update_section():
    API_data= input_api.get_section()
    sql_data= sql_query.get_section()
    update_data={d:API_data.get(d) for d in API_data.keys() if sql_data.get(d) is None}
    if update_data is None:
        print('no section data update in API')
        pass
    else:
        for k,v in tqdm(update_data.items(),desc='update lifestyle section from API to SQL'):
            secid,name,alias,secolor,update_time=v.get('secid'),v.get('name'),v.get('alias'),v.get('seccolor'),datetime.now()
            records=[secid,name,alias,secolor,update_time]
            sql_query.insert_section(data=records)

def  update_category():
    API_data= input_api.get_category()
    SQL_data= sql_query.get_category()
    update_data={d:API_data[d] for d in API_data.keys()  if SQL_data.get(int(d),None) is None}

    for k,v in tqdm(update_data.items(),desc='update lifestyle category from API to SQL'):
        catid=v.get('catid',None)
        secid=v.get('secid',None)
        cattitle=v.get('cattitle',None)
        catalias=v.get('catalias',None)
        catdesc=v.get('catdesc',None)
        author=v.get('author',None)
        status=v.get('status',None)
        allow_comment=v.get('allow_comment',None)
        updatefreq=v.get('updatefreq',None)
        source=v.get('source',None)
        source_url=v.get('source_url',None)
        related_catids = v.get('related_catids', None)


        category_record=[catid,secid,cattitle,catalias,catdesc,author,status,allow_comment,updatefreq,source,source_url,datetime.now()]
        sql_query.insert_category(category_record)

        if related_catids:
            related_catids_record=  [ (catid,r,datetime.now()) for r in related_catids]
            sql_query.insert_related_category(data=related_catids_record)


###---------------------------------------------------------------------------------------------------------

def deter_vecfilter(data:Dict,vecfilter_data=sql_vecfilter_data):
    for k, v in data.items():
        v['vectorize'] = util.vecFilter_func(article=v, vecfilter_data=vecfilter_data)
    return data

def load_update_articles(update_data:Dict):

    for artid ,v in tqdm(update_data.items(),desc='update lifestyle articles in '):
       # artid = v.get('artid', None)
        catid = v.get('catid', None)
        publish_up = v.get('publish_up', None)
        arttitle = v.get('arttitle', None)
        introtext = v.get('introtext', None)
        fulltext = v.get('fulltext', None)
        author = v.get('author', None)
        isHealth = v.get('isHealth', None)
        isHot = v.get('isHot', None)
        isFocus = v.get('isFocus', None)
        images= v.get('images', None)
        images_m = v.get('images_m', None)
        share_url = v.get('share_url', None)
        tag = v.get('tag', None)
        vectorize=v.get('vectorize', 'N')

        article_records=[  artid , 'TC',  catid ,  publish_up , arttitle , introtext , fulltext , author ,  isHealth ,  isHot ,  isFocus ,  images ,  images_m , share_url ,datetime.now(),vectorize]
        sql_query.insert_articles(data=article_records)

        if tag:
            for tagid,tag_title in tag.items():
                related_tortags_record=[tagid,artid,datetime.now()]
                sql_query.insert_related_tortags(data=related_tortags_record)


                tortags_record = [tagid,tag_title, 0,datetime.now()]
                sql_query.insert_tortags(data=tortags_record)

###------------------------------------------------------------------------------------------------------------------------------

def update_articles(start_dt: Union[datetime,str,int] = 20000,end_dt: Union[datetime,str] = datetime.now()):

    start_dt, end_dt = tools.convert_str2dt(start=start_dt, end=end_dt)
    #print(f' Time Range :  {start_dt}  -  {end_dt}')
    SQL_data = sql_query.get_articles_bydate(start_dt=start_dt, end_dt=end_dt)
    headline_API_data= input_api.get_headlines()

    update_data={d:headline_API_data[d] for d in headline_API_data.keys()  if SQL_data.get(int(d),None) is None}

    for update_artid in tqdm(update_data,desc='fetch lifestyle content'):
        update_art_content= input_api.get_articles_byid(artid=update_artid)
        update_data.update(update_art_content)

  #  return update_data
    update_data = deter_vecfilter(update_data)
    load_update_articles(update_data=update_data)

def update_article_vecfilter(start_dt=30,end_dt=datetime.now()):
    start_dt, end_dt = tools.convert_str2dt(start=start_dt, end=end_dt)
    print(f' Time Range :  {start_dt}  -  {end_dt}')
    SQL_data = sql_query.get_articles_bydate(start_dt=start_dt, end_dt=end_dt)
    update_data = deter_vecfilter(SQL_data)
    load_update_articles(update_data=update_data)


if __name__=="__main__":
    fire.Fire({'section':update_section,"category":update_category,"articles":update_articles,   "vecfilter":update_article_vecfilter})
#
#
