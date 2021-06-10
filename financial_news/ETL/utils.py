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


import unicodedata
import re,json
from typing import List,Dict,Union,Optional


def vecFilter_func(article:Dict, vecfilter_data:Dict, content_incl:bool=False):

    if  article.get('relCategory',None):
        categoryCd_check=[categoryCd in vecfilter_data['categoryCd'] for categoryCd in article['relCategory']]
    else:
        categoryCd_check =[False]
    topicID_check=[article['topicID'] in vecfilter_data['topicID']]
    topic_check =[news_topic in article['topic'] for news_topic in vecfilter_data['topic']]
    Headline_word_check =[news_Headline in article['headline'] for news_Headline in vecfilter_data['word']]
    Content_word_check =[news_Content in article['content'] for news_Content in vecfilter_data['word']]

    if content_incl:
        total_check=categoryCd_check + topicID_check + topic_check + Headline_word_check + Content_word_check
    else:
        total_check = categoryCd_check + topicID_check + topic_check + Headline_word_check
    if any(total_check):
        return 'N'
    else:
        return 'Y'

