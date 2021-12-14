# nlp_middle_tier

- Server IP: 10.200.23.42 (UAT)
- 

# NLP workflow
![](pic/nlp_workflow.jpg)


### structure of project
- [Config](https://github.com/etnetapp-dev/nlp_middle_tier/tree/master/Config) - stores configuation of the project in [yaml files](https://github.com/etnetapp-dev/nlp_middle_tier/tree/master/Config/yamls) and contains scripts to convert configuration into reusable python objects.
- [financial_news](https://github.com/etnetapp-dev/nlp_middle_tier/tree/master/financial_news) - contains [ETL](https://github.com/etnetapp-dev/nlp_middle_tier/tree/master/financial_news/ETL) for data retrival of etnet financial news and [output_API](https://github.com/etnetapp-dev/nlp_middle_tier/tree/master/financial_news/output_API) to perform NLP application to news data
- [lifestyle](https://github.com/etnetapp-dev/nlp_middle_tier/tree/master/lifestyle) - contains [ETL](https://github.com/etnetapp-dev/nlp_middle_tier/tree/master/lifestyle/ETL) for data retrival of lifestyle articles and [output_API](https://github.com/etnetapp-dev/nlp_middle_tier/tree/master/flifestyle/output_API) to perform NLP application to lifestyle textual data
- [nlp_backend_engine.py](https://github.com/etnetapp-dev/nlp_middle_tier/blob/master/nlp_backend_engine.py) - acts as bridge to call functional API from [nlp_backend server](https://github.com/etnetapp-dev/nlp_backend) and to use backend functional locally.
- [tools.py](https://github.com/etnetapp-dev/nlp_middle_tier/blob/master/tools.py) and [sql_db.py](https://github.com/etnetapp-dev/nlp_middle_tier/blob/master/sql_db.py) - provides supplemental functions on data preprocessing and set up connection between python server with SQL database. 



## Structure of financial_news and lifestyle module
    - ETL
         input_api.py  (structure requests function of external APIs in python scope)
         sql_query.py (contains object-oriented (OOP) data model structure of data incoming from external APIs)
         update_query.py ( contains tables' structure and relationship of mysql db in term of python scope)
         update_scheduler.py  ( contains data-fetch functions of external APIs and Data I/O functions between modules and SQLDB )
         utils.py  ( contains data-fetch functions of external APIs and Data I/O functions between modules and SQLDB )         

    - output_API
         nlp_handle.py  (structure requests function of external APIs in python scope)
         vec_scheduler.py (contains object-oriented (OOP) data model structure of data incoming from external APIs)

         
         
         
# 1. Data Retrieval (schedulers)

**Pre-requisite procedures**: connection and configuration of SQLDB

###  Structure of data source folders: 
- [news folder](https://github.com/etnetapp-dev/app2app_nlp/tree/master/data_source/news) 
- [lifestyle folder](https://github.com/etnetapp-dev/app2app_nlp/tree/master/data_source/lifestyle) 
- [stockNames](https://github.com/etnetapp-dev/app2app_nlp/tree/master/data_source/stocknames) 
- [theme](https://github.com/etnetapp-dev/app2app_nlp/tree/master/data_source/theme) 

#### Each of data source folders contain four key scripts:
    - source_api.py  (structure requests function of external APIs in python scope)
    - schema.py (contains object-oriented (OOP) data model structure of data incoming from external APIs)
    - db_table.py ( contains tables' structure and relationship of mysql db in term of python scope)
    - CRUD.py  ( contains data-fetch functions of external APIs and Data I/O functions between modules and SQLDB )

#### External APIs of data: all are stored in [data_source.yaml](https://github.com/etnetapp-dev/app2app_nlp/blob/master/config/data_source.yaml)
    news:
        Content   : http://10.1.8.51/NewsServer/GetNewsContent.do
        Thumbnail : https://oapi2u.etnet.com.hk/NewsThumbnails/embed/GetNews.do
       
     lifestyle:
         article  : http://www.etnet.com.hk/apps/etnetapp/api/get_columns_data.php?type=latest
         category : http://www.etnet.com.hk/apps/etnetapp/api/get_columns_data.php?type=catinfo
         section  : http://www.etnet.com.hk/apps/etnetapp/api/get_columns_data.php?type=menu
       
     stocknames   : http://10.1.8.158/StreamServer/SortSearchServlet?reqID=6&category={}&sortFieldID=1&sort=A&from=0&size=5000&customFields=1,{}
     themenames   : http://10.1.8.10:5001/app2app/themedetails  
> note: for external API of themenames, the pre-requisite is the kick-start of running of theme_details.py from   [app2app_web module](https://github.com/etnetapp-dev/app2app_web)



Script directory in development server:  /opt/etnet/app2app/text_processing/theme_articles_mapping/

### 4.2. Direct run command : 
     python3.7  /opt/etnet/app2app/text_processing/theme_articles_mapping/api_scheduler.py
     python3.7   <folder path>/text_processing/theme_articles_mapping/api_scheduler.py

### 4.3.Service run command
#### 4.3.1   create and edit “app2app-theme-mapping.service” script
     vim /opt/etnet/scripts/ app2app-theme-mapping.service
![](demo_configs/app2app_theme_mapping.png)     
Note: mainly input “direct run command” in between start) and exit $?

#### 4.3.2. create and edit “app2app-theme-mapping.service” in “/usr/lib/systemd/system/” folder
     vim /usr/lib/systemd/system/ app2app-theme-mapping.service 
![](demo_configs/app2app_theme_mapping_service.png)   
Note: mainly paste the directory of “app2app-theme-mapping.service” of step2.1 into the .service file


#### 4.3.3  run the .service in the background with “systemctl start” command
     systemctl start app2app-theme-mapping.service
