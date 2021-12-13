# nlp_middle_tier

- Server IP: 10.200.23.42 (UAT)
- 

# NLP workflow
![](pic/nlp_workflow.jpg)


 
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
