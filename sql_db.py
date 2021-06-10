
import os, pathlib, sys,fire,logging

sys.path.append(os.getcwd())

parent_path = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(parent_path))

master_path = parent_path.parent
sys.path.append(str(master_path))

project_path = master_path.parent
sys.path.append(str(project_path))

import pymysql.cursors
from Config.SQL_Config import Info as SQL_setting


class schema:
    util=SQL_setting.nlp_engine.schema
    vocab=SQL_setting.nlp_engine.schema
    News=SQL_setting.news.schema
    lifestyle=SQL_setting.lifestyle.schema



class tables:
    util=SQL_setting.nlp_engine.tables.util
    vocab = SQL_setting.nlp_engine.tables.vocab
    News=SQL_setting.news.tables
    lifestyle = SQL_setting.lifestyle.tables




SQL_connection =  pymysql.connect(
            host=SQL_setting.host,
            port=int(SQL_setting.port),
            user=SQL_setting.user,
            password=SQL_setting.password,autocommit=True)

#