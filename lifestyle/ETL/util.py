import os, pathlib, sys,fire,logging

sys.path.append(os.getcwd())

parent_path = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(parent_path))

master_path = parent_path.parent
sys.path.append(str(master_path))

project_path = master_path.parent
sys.path.append(str(project_path))

from typing import Union,Dict

from datetime import datetime
from tqdm import tqdm




def vecFilter_func(article: Dict, vecfilter_data ):

    catid_check = [str(article['catid']) in vecfilter_data['catid']]
    #title_word_check = [word in article['arttitle'] for word in vecfilter_data['word']]

    total_check = catid_check#+title_word_check
    if any(total_check):
        return 'Y'
    else:
        return 'N'
