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

from typing import List,Dict
import operator,pickle
import numpy as np

from tqdm import tqdm
from datetime import datetime,date, timedelta
import time

from pymcdm import methods as mcdm_methods
from pyflashtext import KeywordProcessor

from financial_news.ETL import sql_query
import nlp_backend_engine as nlp
from Config.NLP_output_Config import Info as NLP_Config

from scipy.spatial import distance

news_NLP_Config=NLP_Config.news

nertag_scores=sql_query.get_nertype_scores()

score_weights=np.array([news_NLP_Config.word_weights.label,news_NLP_Config.word_weights.len,news_NLP_Config.word_weights.headline,news_NLP_Config.word_weights.span])
score_types = np.array([1, 1, 1,1])

def related_art_func(art_obj:Dict, art_objs:Dict,TopN:int=1000)->Dict:


    art_ids ,art_Matrix= list(art_objs.keys()) , np.vstack([art_v['vector'] for artid,art_v in art_objs.items() if art_v['vector'] is not None and  np.array(art_v['vector']).shape==(300,)])

    distances =distance.cdist([art_obj['vector'] ], art_Matrix, "cosine" )[0]


    results ={art_id:float(1 - d) for (art_id,  d) in  zip(art_ids, distances) if float(1 - d) < 0.99  }

    return   dict(sorted(results.items(), key=lambda x: x[1], reverse=True)[:TopN])


def keywords_func(headline:str,content:str,fuzz_threhold:float=0.55,w2v_threhold:float=0):

    full_text=headline+"。"+content

    ner=nlp.ner_scan(full_text,simple=False)

    flashtext_proc = KeywordProcessor(case_sensitive=True)
    flashtext_proc.add_keywords_from_list(list(ner.keys()))
    ner_content_span_scores = {i[0]:1-(i[1]/len(content)) for i in flashtext_proc.extract_keywords(content, span_info=True) }
    ner_headline_scores = {i:1 for i in flashtext_proc.extract_keywords(headline) }

    temp_scores={}
    for word,label in ner.items():

        label_score=nertag_scores.get(label,None)
        if label_score and label_score.get('ranking_score',0)>0:
            temp_scores[word] =[float(label_score['score']), len(word), ner_headline_scores.get(word,0), ner_content_span_scores.get(word,0)]

    del flashtext_proc,ner_content_span_scores,ner_headline_scores
    score_matrix=np.array([vv for vv in temp_scores.values()])

    topsis = mcdm_methods.TOPSIS()
    topsis_results=topsis(score_matrix, score_weights, score_types)
    word_scores=dict(zip(list(temp_scores.keys()),topsis_results))
    del temp_scores,score_matrix
    word_scores = dict(sorted(word_scores.items(), key=operator.itemgetter(1), reverse=True))

    dedupe_words = nlp.fuzz_dedupe(words=list(word_scores.keys()), threshold=fuzz_threhold)
    dedupe_words = nlp.w2v_dedupe(words=dedupe_words, threshold=w2v_threhold) if w2v_threhold>0 else dedupe_words

  #  topK_words=dedupe_words[:topK]  if topK else dedupe_words
    dedupe_words ={w: {'label': ner[w], 'score': word_scores.get(w) if word_scores.get(w)>0 else 0.1} for w in dedupe_words}

    # del dedupe_words,word_scores
    #

   # for w,w_info in topK_words.items():
   #     w_info['score']=w_info['score']/total_topK_scores

    return {i[0]:i[1] for i in sorted(dedupe_words.items(), key=lambda x: x[1]['score'],reverse=True) }


def ner2vec_func(keywords:Dict,topV:12):
    topK = min(len(keywords), topV)
    vec_keywords = dict(sorted(keywords.items(), key=lambda x: x[1]['score'], reverse=True)[:topK])
    doc_vec=[]
    for word, v in vec_keywords.items():
        w_vector=np.array(nlp.w2v_tovec(words=word))
        w_score=v['score']
        doc_vec.append(w_vector*w_score)
    doc_vec=np.array(doc_vec)
    return sum(doc_vec)

def vec_func(article:Dict,topK:int=12,fuzz_threhold:float=0.55,w2v_threhold:float=0.6,verbose:bool=False):
    headline,content=article.get('headline',''),article.get('content','')
    if headline and content:
        keywords=keywords_func(headline=headline,content=content,fuzz_threhold=fuzz_threhold,w2v_threhold=w2v_threhold)
        vec=ner2vec_func(keywords,topV=topK)
        return keywords,vec
    else:
        return None,None



def main(news_info:Dict,news_total:Dict,topK:int=12,topN:int=500,verbose:bool=True):
    try:
        news_info['ner'], news_info['vector']= vec_func(news_info, topK=topK, fuzz_threhold=0.55, w2v_threhold=0.6)

        if not (isinstance(news_info['ner'], dict) and len(news_info['ner']) > 0):

            news_info['ner'],news_info['keywords'],news_info['vector'],news_info['related_news']=None,None,None,None
            print(f"error on NER :  newsID : {news_info['newsID']},  newsDate :{news_info['newsDate']}, newsDate :{news_info[ 'newsDate']}, headline :{news_info['headline']}, content :{news_info['content'][:50]}, ner: {news_info.get('ner', None)} ")

        else:
            sql_query.insert_NameEntity(news_info['ner'])
            news_info['keywords']= [i[0] for i in   sorted(news_info['ner'].items(), key=lambda x: x[1]['score'], reverse=True)[:min(len(news_info['ner']),topK)]]
            sql_query.insert_keywords(newsID=news_info['newsID'], keywords=news_info['keywords'])

            if verbose:
                print(news_info['newsID'], news_info['newsDate'], news_info['headline'])
                for topK_idx, (word, w_info) in enumerate(news_info['ner'].items()):
                    if word in news_info['keywords']:
                        print(f" No. {topK_idx+1} keyword: {word} --> label:  {w_info['label']} , score : { w_info['score']}")
                print('\n')

            if news_info['vector'].shape[0] != 300:
                news_info['vector'], news_info[ 'related_news'] = None, None
                print(f"error on vector :  newsID : {news_info['newsID']},  newsDate :{news_info['newsDate']}, newsDate :{news_info[ 'newsDate']}, headline :{news_info['headline']}, content :{news_info['content'][:50]})  vector shape: {news_info['vector'], type( news_info['vector'])},'\n")

            else:
                sql_query.insert_vector(data=news_info)

                compare_base_news = {newsID_comp: news_art_comp for newsID_comp, news_art_comp in news_total.items() if  news_info['newsDate'] >= news_art_comp['newsDate']}
                compare_news = related_art_func(news_info,compare_base_news, TopN=topN)
                if len(compare_news) <=0:
                    news_info['related_news'] = None
                else:

                    records = [(news_info['newsID'], newsID_comp, sim) for newsID_comp, sim in compare_news.items()]
                    sql_query.insert_related_news(data=records)
                    news_info['related_news']=compare_news


                    if verbose:
                        for top_N, newsID_comp in enumerate(compare_news):
                            if top_N<20:
                                news_comp, news_sim = compare_base_news[newsID_comp], compare_news[newsID_comp]
                                print(f" top_N:{top_N + 1},  newsID: {newsID_comp}, sim : {news_sim}  , newsDate:  {news_comp['newsDate']} ,headline: {news_comp['headline']}, content :{news_comp['content'][:50]}")

        return news_info

    except Exception as e:

        news_info['ner'], news_info['keywords'], news_info['vector'], news_info['related_news'] = None, None, None, None
        print(f"error on {e} :  newsID : {news_info['newsID']},  newsDate :{news_info['newsDate']}, newsDate :{ news_info['newsDate']}, headline :{news_info['headline']}, content :{news_info['content'][  :50]})  vector shape: {news_info['vector'], type(news_info['vector'])},'\n")

        return news_info

if __name__=='__main__':
    topK=news_NLP_Config.topK
    TopN = news_NLP_Config.topN


    end_date=datetime.now()
    start_date=end_date-timedelta(days=news_NLP_Config.sche.dayback)
    trace_date=end_date-timedelta(days=1)
    verbose=news_NLP_Config.verbose

    whole_data=sql_query.get_articles_bydate(start_dt=start_date,end_dt=end_date,contentReady='Y',vectorize='Y')
    vec_data=sql_query.get_vector_bydate(start_dt=start_date,end_dt=end_date)

    base_news,pending_data = {},{}
    for newsID, news_art in whole_data.items() :
        if newsID in vec_data and news_art['content'] not in ['',None] and isinstance(vec_data[newsID], np.ndarray) and vec_data[newsID] is not None:
            news_art['vector']=vec_data[newsID]
            base_news[newsID]=news_art

        if newsID not in vec_data and  news_art['content'] not in ['',None] and  news_art['newsDate']>trace_date:
            pending_data[newsID]=news_art
    print(f"Numbers of news articles in the time range: {len(base_news)}")


    for newsID, news_info in tqdm(pending_data.items(),desc='vectorizing news'):
        news_info=main(news_info=news_info,news_total=base_news,topK=topK, topN=TopN,verbose=verbose)
        if news_info['vector'] is not None and news_info['ner'] is not None:
            base_news[newsID]=news_info



    #         try:
    #             ners,docvec = vec_func(news_info, topK=topK, fuzz_threhold=0.55, w2v_threhold=0.6)
    #             news_info['ner'], news_info['vector']=ners,docvec
    #
    #             if not (isinstance(ners,dict) and len(ners)>0):
    #                 print(f"error on NER :  newsID : {newsID},  newsDate :{news_info['newsDate']}, newsDate :{news_info[  'newsDate']}, headline :{news_info['headline']}, content :{news_info['content'][ :50]}, ner: {news_info.get('ner',   None)} ")
    #             else:
    #                 sql_query.insert_NameEntity(news_info['ner'])
    #
    #                 news_keywords = [i[0] for i in sorted(news_info['ner'].items(), key=lambda x: x[1]['score'], reverse=True)[:topK]]
    #                 sql_query.insert_keywords(newsID=newsID,keywords=news_keywords)
    #
    #                 print(newsID, news_info['newsDate'], news_info['headline'])
    #                 display_keywords={word:w_info for word,w_info in news_info['ner'].items() if word in news_keywords}
    #                 for word, w_info in display_keywords.items():
    #                     print(word,w_info)
    #                 print('\n')
    #
    #
    #                 if  news_info['vector'].shape==(300,):
    #
    #                     sql_query.insert_vector(data=news_info)
    #                   #  print(f"success : insert vector  , newsID :{newsID}, vector shape :{news_info['vector'].shape}")
    #
    #                     compare_base_news={newsID_comp :news_art_comp for newsID_comp , news_art_comp in base_news.items() if news_info['newsDate']>news_art_comp['newsDate']}
    #                     compare_news = related_art_func(news_info,compare_base_news, TopN=TopN)
    #                     records = [(newsID, newsID_comp, sim) for newsID_comp, sim in compare_news.items()]
    #                     sql_query.insert_related_news(data=records)
    #                     for top_N, newsID_comp in enumerate(compare_news):
    #                             news_comp,news_sim=compare_base_news[newsID_comp], compare_news[newsID_comp]
    #                             print(f" top_N:{top_N+1},  newsID: {newsID_comp}, sim : {news_sim}  , newsDate:  {news_comp['newsDate']} ,headline: {news_comp['headline']}, content :{news_comp['content'][:50]}")
    #
    #                     base_news[newsID]=news_info
    #                 else:
    #                     print(f"error on vector :  newsID : {newsID},  newsDate :{news_info['newsDate']}, newsDate :{news_info[ 'newsDate']}, headline :{news_info['headline']}, content :{news_info['content'][:50]})  vector shape: {news_info['vector'], type(news_info['vector'])},'\n")
    #
    #         except Exception as e:
    #             print(f"error1: {e},  newsID : {newsID},  newsDate :{news_info['newsDate']}, newsDate :{news_info['newsDate']}, headline :{news_info['headline']}, content :{news_info['content'][:50]}, ner: {news_info.get('ner',None)}, vector shape: {news_info.get('vector',None)},'\n")
    #
    #
    # # # vec_news,non_vec_news={},{}
    # # for newsID, news_art in whole_data.items():
    # #     vector=vec_data.get(newsID)
    # #     if vector is not None and news_art['content'] not in ['',None] and np.array(vector).shape==(300,):
    # #         news_art['vector']=vector
    # #         vec_news[newsID]=news_art
    # #     elif  news_art['content'] not in ['',None]:
    # #         non_vec_news[newsID]=news_art
    # #     else:
    # #         pass
    #
    # # for newsID in tqdm(non_vec_news):
    # #     news_art=non_vec_news[newsID]
    # #     try:
    # #         news_art['ner'],news_art['vector']=vec_func(news_art,topK=topK,fuzz_threhold=0.55,w2v_threhold=0.6)
    # #         if news_art['ner']:
    # #             sql_query.insert_vector(data=news_art)
    # #             sql_query.insert_NameEntity(news_art['ner'])
    # #
    # #             news_keywords =[i[0]  for i in sorted(news_art['ner'].items(), key=lambda x: x[1]['score'], reverse=True)[:topK]]
    # #             sql_query.insert_keywords(newsID=newsID,keywords=news_keywords)
    # #
    # #             news_art.pop('vector')
    # #             print(newsID, news_art['newsDate'],news_art['headline'])
    # #             display_keywords={word:w_info for word,w_info in news_art['ner'].items() if word in news_keywords}
    # #             for word, w_info in display_keywords.items():
    # #                 print(word,w_info)
    # #             print('\n')
    # #     except:
    # #         print('error:', newsID,news_art['headline'])
    # #
    # # data=sql_query.get_articles_byorder(limit=5000,contentReady='Y',vectorize='Y')
    # # vec_data=sql_query.get_vector_byorder(5000)
    #
    # # sim_news={newsID: news_art for newsID, news_art in vec_news.items() if news_art['newsDate']>=sim_date}
    # # for newsID in tqdm(sim_news):
    # #     news_art=vec_news[newsID]
    # #     print(f"  newsID: {newsID},  newsDate:  {news_art[   'newsDate']} ,headline: {news_art['headline']}, content :{news_art['content'][:50]}")
    # #
    # #     compare_news=related_art_func(news_art,vec_news,TopN=TopN)
    # #     if compare_news:
    # #         records=[(newsID,newsID_comp,sim ) for newsID_comp,sim in compare_news.items()]
    # #         sql_query.insert_related_news(data=records)
    # #         for top_N,newsID_comp in enumerate(compare_news):
    # #
    # #             news_comp,news_sim=vec_news[newsID_comp], compare_news[newsID_comp]
    # #             print(f" top_N:{top_N+1},  newsID: {newsID_comp}, sim : {news_sim}  , newsDate:  {news_comp['newsDate']} ,headline: {news_comp['headline']}, content :{news_comp['content'][:50]}")
    # #
    # #     print('\n')
    #
