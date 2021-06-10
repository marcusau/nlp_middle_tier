import os, pathlib, sys,fire,logging

sys.path.append(os.getcwd())

parent_path = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(parent_path))

master_path = parent_path.parent
sys.path.append(str(master_path))

project_path = master_path.parent
sys.path.append(str(project_path))

from typing import List,Union, Optional,Dict

from pytime import pytime
from datetime import datetime,  timedelta
from tqdm import tqdm

import requests

from Config.InputAPI_Config import Info as InputAPI_config
import tools


InputAPI_urls=InputAPI_config.lifestyle.urls
InputAPI_param=InputAPI_config.lifestyle.params

###-----------------------------------------------------------------------------------------------------
def get_requests(url:str)->Dict:
        resp_info = requests.get( url)
        if resp_info.status_code == 200:
            return resp_info.json()
        else:
            print('Error: ',resp_info.status_code, ' url:',url)
            return {}

###-----------------------------------------------------------------------------------------------------
def get_section()->Dict:
    rows = get_requests(InputAPI_urls.section).get(InputAPI_param.section, None)
    if len(rows) >0 :
        return {int(r['secid']):r for r in rows}
    else:
        print(f'no section data is from {InputAPI_urls.section}')
        return {}

def get_category()->Dict:
        rows=get_requests(InputAPI_urls.category).get(InputAPI_param.category, None)
        if len(rows) >0:
            output={}
            data=[row for row_id,(name,row) in enumerate(rows.items()) if row_id % 2 !=0]
            for d in data:
                catid=int(d['catid'])
                d['catdesc']=tools.normalize_texts(d['catdesc'])
            #    print(d)
                d['related_catids']=[int(r) for r in d['related_catids']]  if d.get('related_catids',None) else []
                output[catid]=d
            return output
        else:
            print(f'no category data is from {InputAPI_urls.category}')
            return {}

###-----------------------------------------------------------------------------------------------------

def get_articles_byid(artid:Union[str,int,List[Union[str,int]]])->Dict:
    artid = [artid] if type(artid) in [str, int] else [artid]

    output={}
    for id in artid:#,desc=f'get lifestyle single articles from API : {InputAPI_urls.single_article}'):
        url = InputAPI_urls.single_article+ str(id)
        single_article = get_requests(url).get(InputAPI_param.single_article, None)
        if not single_article:
            print(f' No lifestyle article from url {url }  ')
            pass
        else:
            for k,v in single_article.items():
                v['introtext']=tools.normalize_texts(v['introtext'])
                v['fulltext'] = tools.Clean_lifestyleArticle_func(v['fulltext'])
                v['publish_up'] = pytime.parse(v["real_publish_up"])
                del v["real_publish_up"],v["sectionid"],v['keywords'],v['tid'],v['replies'],v['artcredits'],v['related_seid']

            output.update(single_article)
    return output


def get_articles_bylimit() -> Dict:
    rows = get_requests(InputAPI_urls.articles).get(InputAPI_param.articles, None)
    if not rows:
        print(f' No lifestyle article from url {InputAPI_urls.articles}  ')
        return {}
    else:
        rows2={}
        for r in tqdm(rows,desc=f'get lifestyle articles from API : {InputAPI_urls.articles}'):
            artid=r['artid']
            r2=get_articles_byid(artid=artid)[artid]
            artid = int(r['artid'])
            r.update(r2)
            del r['abstract'], r['real_publish_up'], r['has_video'], r['replies'], r['status']
            rows2[artid]=r
        return rows2


def get_articles_bydate(start_dt: Union[datetime,str,int] = 30,end_dt: Union[datetime,str] = datetime.now())->Dict:

    start_dt, end_dt = tools.convert_str2dt(start=start_dt, end=end_dt)
    print(f' get lifestyle articles from API at Time Range :  {start_dt}  -  {end_dt}')

    rows = get_requests(InputAPI_urls.articles).get(InputAPI_param.articles, None)
    if not rows:
        print(f' No lifestyle article from url {InputAPI_urls.articles}  ')
        return {}
    else:
        rows_intime = {r['artid']:r  for r in rows  if pytime.parse(r["real_publish_up"])>=start_dt and pytime.parse(r["real_publish_up"])<= end_dt}

        for artid,r in rows_intime.items():
            r['abstract'] = tools.normalize_texts(r['abstract'])
            r['publish_up'] = pytime.parse(r["real_publish_up"])

            r2 = get_articles_byid(artid=artid)[artid]
            r.update(r2)
            del r['abstract'],r['real_publish_up'],r['has_video'],r['replies'],r['status']

        return  rows_intime

def get_headlines()->Dict:


    rows = get_requests(InputAPI_urls.articles).get(InputAPI_param.articles, None)
    if not rows:
        print(f' No lifestyle article from url {InputAPI_urls.articles}  ')
        return {}
    else:
        rows= {r['artid']: r for r in rows if  pytime.parse(r["real_publish_up"])}
        for artid,r in rows.items():
            r['abstract'] = tools.normalize_texts(r['abstract'])
      #      r['publish_up'] = pytime.parse(r["real_publish_up"])

         #   r2 = get_articles_byid(artid=artid)[artid]
           # r.update(r2)
            del r['abstract'],r['real_publish_up'],r['has_video'],r['replies'],r['status']

        return  rows


if __name__=="__main__":
    fire.Fire({'section': get_section, "category": get_category, "articles_byid": get_articles_byid,
               "articles_bylimit": get_articles_bylimit,'articles_bydate':get_articles_bydate,'headlines':get_headlines})