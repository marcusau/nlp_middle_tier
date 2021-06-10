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


import requests,re,json
from typing import List,Union, Optional,Dict

from pytime import pytime
from datetime import datetime, timedelta
from tqdm import tqdm

###------------- API url and Parameterss Config -----------------------------

import tools
from Config.InputAPI_Config import Info as InputAPI_config

InputAPI_urls=InputAPI_config.news.urls
Thumbnail_API_urls=InputAPI_urls.Thumbnail
Content_API_urls=InputAPI_urls.content

InputAPI_params=InputAPI_config.news.params


def decode_json(API_resp):
    _json = API_resp.json()
    return _json['rows'] if _json['succeed'] == 'true' else []

def Clean_data(data):
    for d in data:
        if d.get('headline'):
            d['headline'] = tools.Clean_NewsArticle_Func(d['headline'])
            d['headline'] = re.sub("\t|\s|\n", "", d['headline'])

        if d.get('topic'):
            d['topic'] = tools.normalize_text(d['topic'])
            d['topic'] = re.sub("\t|\s|\n|【|】|《|》", "", d['topic'])

        if d.get('fulltext'):
            d['fulltext'] = tools.Clean_NewsArticle_Func(d['fulltext'])
            d['fulltext'] = re.sub("\t|\s|\n", "", d['fulltext'])

        if d.get("relProducts"):
            del d["relProducts"]
        yield d

def get_requests(url:str):
    API_resp = requests.get(url)
    if API_resp.status_code is not 200:
        print(f'Error :{url.status_code} ,   Thumbnail API: {url} ', )
        return []
    else:
        API_json = decode_json(API_resp)
        return list(Clean_data(API_json)) if API_json else []


def get_thumbnail_byNewsID(newsID:Union[str,int,List[Union[str,int]]]):
    newsID= [newsID] if type(newsID) in [str,int] else newsID
    print(newsID)
    output={}
    for newsid in tqdm(newsID,desc=f'get thumbnail in API :{Thumbnail_API_urls.byID}'):
        url=Thumbnail_API_urls.byID.format(newsid)
        resp_result=get_requests(url)

        if len(resp_result)==0:
            print(f'empty record, newsID: {newsid} , url: {url}')
            pass
        else:
            d=resp_result[0]
            newsID_=d["newsID"]
            d['refID']=int(d['refID'])
            d['newsDate']=pytime.parse(d['newsDate'])
            del d['newsID']
            output.update({newsID_:d})
    return output

def get_thumbnail_bydate(start_dt: Union[datetime,str,int] = 30,end_dt: Union[datetime,str] = datetime.now() ,num_limit:int=InputAPI_params.limitno):
    start_dt, end_dt = tools.convert_str2dt(start=start_dt, end=end_dt)

    print(f' Time Range :  {start_dt}  -  {end_dt}')
    currnet_p,dateback_p = tools.API_TimeSlot(end_dt),tools.API_TimeSlot(start_dt)
    updonfrom = f'{dateback_p.date_str}+{dateback_p.hour}%3A{dateback_p.minute}%3A{dateback_p.second}'  # '2020-10-27+11%3A30%3A00'
    updonto = f'{currnet_p.date_str}+{currnet_p.hour}%3A{currnet_p.minute}%3A{currnet_p.second}'  # '2020-10-27+12%3A00%3A00'
    InputAPI_url = Thumbnail_API_urls.byTime.format(dateback_p.date_str, currnet_p.date_str, updonfrom,  updonto, num_limit)

    rows=get_requests(InputAPI_url)
    rows=[r for r in rows if (pytime.parse(r['newsDate']) >= start_dt and pytime.parse(r['newsDate']) <= end_dt)]
    output={}
    for d in tqdm(rows,desc=f'get thumbnail from API :{InputAPI_url}'):
        newsID = int(d['newsID'])
        d['refID'] = int(d['refID'])
        d['newsDate'] = pytime.parse(d['newsDate'])
        del d['newsID']
        output.update({newsID: d})
    return output
###------------------------------------------


def get_content_byNewsID(newsID:Union[str,int,List[Union[str,int]]]):
    newsID = [newsID] if type(newsID) in [str, int] else newsID


    output={}
    for newsid in tqdm(newsID,desc=f'get news content in API {InputAPI_urls.content.byID}'):
        url=InputAPI_urls.content.byID.format(str(newsid))
        resp=requests.get(url)
        if resp.status_code is not 200:
            print(f'error with status code : {resp.status_code} ,  newsid :{newsid}, url:{url} ,')
            output.update({int(newsid): ''})
        else:
            content=json.loads(resp.content.decode("utf-8"))['content']
            if content is "" or len(content)==0 or content is None:
                print(f' no content, newsid :{newsid},   url:{url} ,')
                output.update({int(newsid):''})
            else:
                output.update({int(newsid):tools.Clean_NewsArticle_Func(content)})

    return output


def split_ready(d:Dict):
    ready,NOTready={},{}
    for newsID, v in d.items():
        v['content']=''
        if v['contentReady'] == 'Y':
            ready[newsID]=v
        else:
            NOTready[newsID]=v
    return ready,NOTready

if __name__=='__main__':
    fire.Fire()