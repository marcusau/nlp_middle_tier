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
import operator
import numpy as np

from tqdm import tqdm
from datetime import datetime,date,timedelta
import time

from pymcdm import methods as mcdm_methods
from pyflashtext import KeywordProcessor

from lifestyle.ETL import sql_query
import nlp_backend_engine as nlp
from scipy.spatial import distance


from Config.NLP_output_Config import Info as NLP_Config

lifestyle_NLP_Config=NLP_Config.lifestyle

nertag_scores=sql_query.get_nertype_scores()

score_weights=np.array([lifestyle_NLP_Config.word_weights.label, lifestyle_NLP_Config.word_weights.len,lifestyle_NLP_Config.word_weights.title,lifestyle_NLP_Config.word_weights.intro,lifestyle_NLP_Config.word_weights.span])
score_types = np.array([1, 1, 1,1,1])


def related_art_func(art_obj:Dict, art_objs:Dict,TopN:int=1000)->Dict:


    art_ids ,art_Matrix= list(art_objs.keys()) , np.vstack([art_v['vector'] for artid,art_v in art_objs.items() if art_v['vector'] is not None and  art_v['vector'].shape[0]==300])

    distances = distance.cdist([art_obj['vector'] ], art_Matrix, "cosine" )[0]


    results ={art_id:float(1 - d) for (art_id,  d) in  zip(art_ids, distances) if float(1 - d) < 0.99  }

    return   dict(sorted(results.items(), key=lambda x: x[1], reverse=True)[:TopN])




def keywords_func(arttitle:str,introtext:str,fulltext:str,fuzz_threhold:float=0.55,w2v_threhold:float=0.6):

    full_text=arttitle+"。"+introtext+"。"+fulltext

    ner=nlp.ner_scan(full_text,simple=False)

    flashtext_proc = KeywordProcessor(case_sensitive=True)
    flashtext_proc.add_keywords_from_list(list(ner.keys()))

    ner_arttitle_scores = {i:1 for i in flashtext_proc.extract_keywords(arttitle) }
    ner_introtext_scores = {i:1 for i in flashtext_proc.extract_keywords(introtext) }
    ner_fulltext_span_scores = {i[0]:1-(i[1]/len(fulltext)) for i in flashtext_proc.extract_keywords(fulltext, span_info=True) }


    temp_scores={}
    for word,label in ner.items():
        label_score=nertag_scores.get(label,None)
        if label_score and label_score.get('ranking_score',0)>0:
            temp_scores[word] =[float(label_score['score']), len(word), ner_arttitle_scores.get(word,0), ner_introtext_scores.get(word,0),ner_fulltext_span_scores.get(word,0)]

    del flashtext_proc,ner_arttitle_scores,ner_introtext_scores,ner_fulltext_span_scores
    score_matrix=np.array([vv for vv in temp_scores.values()])

    topsis = mcdm_methods.TOPSIS()
    topsis_results=topsis(score_matrix, score_weights, score_types)
    word_scores=dict(zip(list(temp_scores.keys()),topsis_results))
    del temp_scores,score_matrix
    word_scores = dict(sorted(word_scores.items(), key=operator.itemgetter(1), reverse=True))

    dedupe_words = nlp.fuzz_dedupe(words=list(word_scores.keys()), threshold=fuzz_threhold)
    dedupe_words = nlp.w2v_dedupe(words=dedupe_words, threshold=w2v_threhold) if w2v_threhold>0 else dedupe_words

  #  topK_words=dedupe_words[:topK]  if topK else dedupe_words
    dedupe_words ={w: {'label': ner[w], 'score': word_scores.get(w)} for w in dedupe_words}

    # del dedupe_words,word_scores
    #
    # total_topK_scores=sum(v['score'] if v['score']>0.05 else 0.2 for v in topK_words.values())
    # for w,w_info in topK_words.items():
    #     w_info['score']=w_info['score']/total_topK_scores

    return {i[0]:i[1] for i in sorted(dedupe_words.items(), key=lambda x: x[1]['score'],reverse=True) }

def ner2vec_func(keywords:Dict,topK:int=12):
    topK = min(len(keywords), topK)
    vec_keywords = dict(sorted(keywords.items(), key=lambda x: x[1]['score'], reverse=True)[:topK])
    doc_vec=[]
    for word, v in vec_keywords.items():
        w_vector=np.array(nlp.w2v_tovec(words=word))
        w_score=v['score']
        doc_vec.append(w_vector*w_score)
    doc_vec=np.array(doc_vec)
    return sum(doc_vec)


def vec_func(article:Dict,topK:int=12, fuzz_threhold:float=0.55,w2v_threhold:float=0.6,verbose:bool=False):
    arttitle,introtext,fulltext=article.get('arttitle',''),article.get('introtext',''),article.get('fulltext','')
    if arttitle and introtext and fulltext:
        keywords=keywords_func(arttitle=arttitle,introtext=introtext,fulltext=fulltext,fuzz_threhold=fuzz_threhold,w2v_threhold=w2v_threhold)

        vec = ner2vec_func(keywords, topK=topK)
        return keywords, vec
    else:
        return None, None




def main(art_info:Dict,total_arts:Dict,topK:int=12,topN:int=100,verbose:bool=True):
    try:
        art_info['ner'], art_info['vector']= vec_func(art_info, topK=topK, fuzz_threhold=0.55, w2v_threhold=0.6)

        if not (isinstance(art_info['ner'], dict) ):

            art_info['ner'],art_info['keywords'],art_info['vector'],art_info['related_articles']=None,None,None,None
            print(f"error on NER :  artid : {art_info['artid']},  publish_up :{art_info['publish_up']},  arttitle :{art_info['arttitle']}, fulltext :{art_info['fulltext'][:50]}, ner: {art_info.get('ner', None)} ")

        else:

            sql_query.insert_NameEntity(art_info['ner'])
            print(f"success :  insert NameEntity of artid :{art_info['artid']}")
            art_info['keywords']= [i[0] for i in  sorted(art_info['ner'].items(), key=lambda x: x[1]['score'], reverse=True)[:min(len(art_info['ner']),topK)]]
            sql_query.insert_keywords(artid=art_info['artid'], keywords=art_info['keywords'])
            print(f"success :  insert keywords of artid :{art_info['artid']}")

            if verbose:
                print(art_info['artid'], art_info['publish_up'], art_info['arttitle'])
                for topK_idx, (word, w_info) in enumerate(art_info['ner'].items()):
                    if word in art_info['keywords']:
                        print(f" No. {topK_idx+1} keyword: {word} --> label:  {w_info['label']} , score : { w_info['score']}")


            if art_info['vector'].shape[0] != 300:
                art_info['vector'], art_info[ 'related_articles'] = None, None
                print(f"error on vector :  artid : {art_info['artid']},  publish_up :{art_info['publish_up']}, arttitle :{art_info['arttitle']}, fulltext :{art_info['fulltext'][:50]})  vector shape: {art_info['vector'], type( art_info['vector'])},'\n")

            else:
                sql_query.insert_vector(data=art_info)
                print(f"success :  insert vector of artid :{art_info['artid']}")

                compare_base = {artid_comp: art_comp for artid_comp, art_comp in total_arts.items() if  art_info['publish_up'] >= art_comp['publish_up']}
                compare_arts = related_art_func(art_info,compare_base, TopN=topN)

                if len(compare_arts) <=0:
                    art_info['related_articles'] = None
                else:

                    records = [(art_info['artid'], artid_comp, sim) for artid_comp, sim in compare_arts.items()]
                    sql_query.insert_related_articles(data=records)
                    art_info['related_articles']=compare_arts

                    if verbose:
                        for top_N, (artid_comp,art_sim ) in enumerate(compare_arts.items()):
                            if top_N<10:
                                art_comp = compare_base[artid_comp]
                                print(f" top_N:{top_N + 1},  artid: {artid_comp}, sim : {art_sim}  , publish_up:  {art_comp['publish_up']} ,arttitle: {art_comp['arttitle']}, fulltext :{art_comp['fulltext'][:50]}")
                    print('\n')
        return art_info

    except Exception as e:

        art_info['ner'], art_info['keywords'], art_info['vector'], art_info['related_articles'] = None, None, None, None
        print(f"error on {e} :  artid : {art_info['artid']},  publish_up :{art_info['publish_up']},  arttitle :{art_info['arttitle']}, fulltext :{art_info['fulltext'][  :50]})  vector shape: { type(art_info['vector'])},'\n")

        return art_info


if __name__=='__main__':
    verbose=NLP_Config.lifestyle.verbose
    topK=NLP_Config.lifestyle.topK
    topN=NLP_Config.lifestyle.topN

    end_date=datetime.now()
    start_date = end_date-timedelta(days=NLP_Config.lifestyle.sche.dayback)

    init_arts = sql_query.get_articles_bydate(start_dt=start_date,end_dt=end_date)
    init_arts = {artid: art_info for artid, art_info in init_arts.items()  if art_info['vectorize'] == 'Y' and art_info['fulltext'] not in ['', None] and len( art_info['fulltext']) > 3}

    init_vector=sql_query.get_vector_bydate(start_dt=start_date,end_dt=end_date)
    init_vector= {artid: vector for artid,vector in init_vector.items() if vector is not None  and isinstance(vector, np.ndarray)}

    init_ner = sql_query.get_related_ner_bydate(start_dt=start_date, end_dt=end_date)
    init_ner = {artid: ner for artid, ner in init_ner.items() if  ner is not None and isinstance(ner, dict) and len(ner) > 0}

    init_keywords = sql_query.get_related_keywords_bydate(start_dt=start_date, end_dt=end_date)
    init_keywords = {artid: keywords for artid, keywords in init_keywords.items() if   keywords is not None and isinstance(keywords, list) and len(keywords) > 0}

    nonvec_arts, vec_arts = {}, {}
    for artid, art_info in init_arts.items():
        art_info['artid'] =artid
        art_info['ner'] = init_ner[artid] if artid in init_ner else {}
        art_info['vector'] = init_vector[artid] if artid in init_vector else None
        art_info['keywords'] = init_keywords[artid] if artid in init_keywords else []

       # print(f"artid:{art_info['artid']} ,  ner: {art_info['ner']} , vector: {type(art_info['vector'])}")
        if len(art_info['ner']) > 0 and len(art_info['keywords']) > 0 and  art_info['vector'] is not None and isinstance( art_info['vector'], np.ndarray): # len(art_info['keywords']) > 0 and
            vec_arts[artid] = art_info
        elif art_info['publish_up'] > datetime(year=date.today().year, month=date.today().month, day=date.today().day - 3, hour=0, minute=0, second=0):
            nonvec_arts[artid] = art_info
    print(f"Numbers of lifestyle articles in the {start_date} to {end_date}:   {len(vec_arts)}  ")
    print(f"Numbers of non-vectorize lifestyle articles in the {start_date} to {end_date}:   {len(nonvec_arts)}  ")




    for artid, art_info in tqdm(init_arts.items(),desc='vectorizing news'):
        art_info=main(art_info=art_info,total_arts=init_arts,topK=topK, topN=topN,verbose=verbose)
        if art_info['vector'] is not None and isinstance(art_info['vector'],np.ndarray) and art_info['ner'] is not None and len(art_info['ner'])>0 and isinstance(art_info['ner'],dict):
            vec_arts[artid]=art_info
