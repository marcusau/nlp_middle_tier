
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


from pytime import pytime
from datetime import datetime,date,timedelta
from bs4 import BeautifulSoup

import cleantext



class API_TimeSlot(object):
    def __init__(self,timeslot: datetime):
        self.dt=timeslot
        self.dt_str = self.dt.strftime('%Y-%m-%d %H:%M:%S')
        self.date = self.dt.date()
        self.date_str = self.date.strftime('%Y-%m-%d')
        self.hour = "%02d" % self.dt.hour
        self.minute = "%02d" % self.dt.minute
        self.second = "%02d" % self.dt.second

def strQ2B(text):
    """把字符串全角转半角"""
    ss = []
    for s in text:
        tmp = ""
        for uchar in s:
            inside_code = ord(uchar)
            if inside_code == 12288:  # 全角空格直接转换
                inside_code = 32
            elif (inside_code >= 65281 and inside_code <= 65374):  # 全角字符（除空格）根据关系转化
                inside_code -= 65248
            tmp += chr(inside_code)
        ss.append(tmp)
    return ''.join(ss)



class punctuations(object):
    delimiters=['>>','。','｡','？','?','！','!','﹔',';','…', '｜','|','～','~','—','－','／','/','＼','\\','－－－－－－－－－－','------------------------------------------','……','…','...','---','----',    '----------', '......', '...','\n','\n\n']
    sub_delimiters=['，','：','､','、','﹑','·','–','〃','‟','„','〟',]
    #blankets=['〈〉《》「」『』【】〔〕〖〗〘〙〚〛｛｝｟｠｢｣（）“” 〝〞 ｀＇［］‘’  ']
    back_blankets=  ['〉','》','」','』','】','〕','〗','〙','〛','｝','｠','｣','）','”','〞','＇','］','’','}', '⦆', ')', "'", ']']
    front_blankets=['〈','《','「','『','【','〔','〖','〘','〚','｛','｟','｢','（','“','〝','｀','［','‘','{', '⦅', '(', '`', '[']
    non_delimiters = ['，','：','､','、','﹑','‧','·','–','‛','〃','‟','„','〟', ':']
    other_logo=['〾','〿','＠','@', '＊','*','＆','&','＄','$','＃','#','％','%', '＋','+','〰','﹏','_','＿','_','＜','<','＝','=','＞','>','＾','^',',']
    all_exp_logo=delimiters+back_blankets+front_blankets+non_delimiters
    all=delimiters+back_blankets+front_blankets+non_delimiters+other_logo


class Sentencizer:
    def __init__(self, input_text:str, split_characters:List[str]=punctuations.delimiters, delimiter_token='<SPLIT>'):
        self.sentences = []
        self.raw = str(input_text)
        self._split_characters = split_characters
        self._delimiter_token = delimiter_token
        self._index = 0
        self._sentencize()

    def _sentencize(self):
        work_sentence = self.raw
        for character in self._split_characters:
            work_sentence = work_sentence.replace(character, character + "" + self._delimiter_token)
        self.sentences = [x.strip() for x in work_sentence.split(self._delimiter_token) if x != '']

def normalize_text(text:str)->str:
    text = text.replace('\u2013', '-').replace('\xa0', ' ').replace('\u3000', ' ').replace('\ufeff', ' ')
   # text = unidecode(text)
    text = strQ2B(text)
    text = unicodedata.normalize('NFKC', text)
    text = text.strip()
    return text

def normalize_texts(text:str)->str:
    text=BeautifulSoup(text,features="html.parser").text
    if any(i in text for i in punctuations.all_exp_logo):
        ss=Sentencizer(text,split_characters=punctuations.all_exp_logo).sentences
        return  ''.join([normalize_text(s[:-1]) + s[-1] for s in ss if len(s) > 0])
    else:
        return normalize_text(text) if len(text) > 0 else ''



def Remove_html_code(s)->str:
    ss = BeautifulSoup(s, features="html.parser").text
    return ss
#
# def Remove_Email_url(s:str)->str:
#     s = re.sub(r'([\w0-9._-]+@[\w0-9._-]+\.[\w0-9_-]+)', '', s)
#     if not URL_Extract.has_urls(s):
#         return s
#     else:
#         for url in URL_Extract.gen_urls(s):
#             s = s.replace(url, '')  # prints: ['janlipovsky.cz']
#
#         return s

def Remove_StockCode(s:str)->str:
    s1 = re.sub('\(\d{3,6}\)|\（\d{3,6}\）', '', s)
    s2 = re.sub('\(滬:\d+\)|\(深:\d+\)', '', s1)
    s3 = re.sub('\(滬：\d+\)|\(深：\d+\)', '', s2)
    s3 = re.sub('\（滬：\d+\）|\（深：\d+\）', '', s3)

    return s3



def Remove_misc(s:str)->str: #'：',
    s1 = re.sub('\（\w{2,5}\.下同\）','', s)
    s2 = re.sub('﹒','.', s1)
    s2 = re.sub("＇","'", s2)
    s2 = re.sub('％', '%', s2)
    s2 = re.sub('\*', '', s2)
    s2 = re.sub('\(\d\)', '', s2)

    s2 = re.sub('\(\w+圖片\)', '', s2)
    s2 = re.sub('(撰文\：\w{2,5})', '', s2)
    s2 = re.sub('開市Good Morning', '', s2)
    s2 = re.sub('(\(\w+官網截圖\))', '', s2)
    s2 = re.sub('(\(音譯\w+\))', '', s2)
    s2 = re.sub('(\(\w+攝\))', '', s2)
    s2 = re.sub('(\(經濟通HV2系統截圖.+\))|(\(經濟通HV2系統截圖\))', '', s2)


    s2 = re.sub(re.compile('(^\《\w+\》)'), '', s2)
    s2 = re.sub(re.compile('(\《香港經濟日報\》$)'), '', s2)
    s2 = re.sub(re.compile('(\《環富通基金頻道\d+專訊\》)'), '', s2)
    s2 = re.sub(re.compile('(\《經濟通通訊社\》記者\w+筆錄)'), '', s2)

    s2 = re.sub(re.compile('(\(權益披露\w+\))'), '', s2)


    s2 = re.sub(re.compile('(\〈有關牌照詳情\，請閱以下連結\:$)'), '', s2)
    s2 = re.sub(re.compile('(\d{2,4}年\d{1,2}月\d{1,2}日$)'), '', s2)
    s2 = re.sub(re.compile('(\(本欄逢周\w+刊出\))'), '', s2)
    s2 = re.sub(re.compile('(\《香港經濟日報\》$)'), '', s2)
    s2 = re.sub(re.compile('(\(權益披露:\w+\))'), '', s2)

    s2 = s2.replace('---------－',' ')

    s2 = s2.replace('－－－－－－－－－－',' ')
    s2 = s2.replace('————————————',' ')

    s2 = s2.replace('---------－',' ')
    s2 = s2.replace('-----------', ' ')
    s2 = s2.replace('============', ' ')
    s2 = s2.replace('----－', ' ')
    s2 = s2.replace('-----', ' ')

    s2 =  re.sub(r'\(專頁\w+\) $|\(專頁\:\w+\)$|\(影音\:\w+\)$|\(影音\w+\)$|\(群組\:\w+\)$|\(群組\w+\)$|\(筆者電郵\:\w+\)$|\(筆者電郵\w+\)$', '',s2.strip())
    s2 = re.sub(r'(\(\))|(\(\）)|(\( \）)|(\《  \》)|(\《 及  \》)|(\《  \》 \）)|(\《 及  )|(\《 及  \》 \）)|(\《  \》)', '',s2.strip())



    s2 =  re.sub(r'\(\w+\）$|\(\w+\)$|\(\w+\、\w+\)$|\(al\)', '',s2.strip())
    s2 =  re.sub(r'(\(\w+\:\) \）$)|(\(\w+\)\）$)|(\(\w+\) \）$)|(\(\w+\、\w+\) \）$)|(\》\）$)|\《  \》', '',s2.strip())
    s2 =  re.sub(r'\》\）', '\》',s2.strip())

    s2 = re.sub(r' ','', s2.strip())
    s2=re.sub('\《經濟通通訊社\w?\w?日專訊\》','',s2)
    #s_list=DummySentencizer(s2, split_characters=['。」','。',';','!','*']).sentences
    return  s2 #s_list

def Remove_Disclaimer(s:str)->str:

    s=s.replace('有關業績的詳情，請參閱公司正式之通告','')

    s1 = s.replace( '《編者按》本欄搜羅即日熱門傳聞，惟消息未經證實，《經濟通》亦不保證內容之準確性本文只供參考之用，並不構成要約、招攬或邀請、誘使、任何不論種類或形式之申述或訂立任何建議及推薦，讀者務請運用個人獨立思考能力自行作出投資決定，如因相關建議招致損失，概與《經濟通》、編者及作者無涉。', ' ')
    s2 = s1.replace('《編者按》本欄搜羅即日熱門傳聞，惟消息未經證實，《經濟通》亦不保證內容之準確性本文只供參考之用，並不構成要約、招攬或邀請、誘使、任何不論種類或形式之申述或訂立任何建議及推薦，讀者務請運用個人獨立思考能力自行作出投資決定，如因相關建議招致損失，概與《經濟通》、編者及作者無涉。', ' ')

    s3 = s2.replace( '編者按:本文只供參考之用，並不構成要約、招攬或邀請、誘使、任何不論種類或形式之申述或訂立任何建議及推薦，讀者務請運用個人獨立思考能力自行作出投資決定，如因相關建議招致損失，概與《經濟通》、編者及作者無涉。', ' ')
    s4 = s3.replace( '編者按:本文只供參考之用，並不構成要約、招攬或邀請、誘使、任何不論種類或形式之申述或訂立任何建議及推薦，讀者務請運用個人獨立思考能力自行作出投資決定，如因相關建議招致損失，概與《環富通》、編者及作者無涉。', ' ')
    s5 = s4.replace( '編者按:本文只供參考之用，並不構成要約、招攬或邀請、誘使、任何不論種類或形式之申述或訂立任何建議及推薦，讀者務請運用個人獨立思考能力自行作出投資決定，如因相關建議招致損失，概與《經濟通通訊社》、編者及作者無涉。',  ' ')


    s6 = s5.replace( '編者按: 本文只供參考之用，並不構成要約、招攬或邀請、誘使、任何不論種類或形式之申述或訂立任何建議及推薦，讀者務請運用個人獨立思考能力自行作出投資決定，如因相關建議招致損失，概與《經濟通通訊社》、編者及作者無涉。', ' ')

    s7 = s6.replace( '＊《編者按》本欄搜羅即日熱門傳聞，惟消息未經證實，《經濟通》亦不保證內容之準確性本文只供參考之用，並不構成要約、招攬或邀請、誘使、任何不論種類或形式之申述或訂立任何建議及推薦，讀者務請運用個人獨立思考能力自行作出投資決定，如因相關建議招致損失，概與《經濟通》、編者及作者無涉。', ' ')
    s8 = s7.replace( '＊編者按:本文只供參考之用，並不構成要約、招攬或邀請、誘使、任何不論種類或形式之申述或訂立任何建議及推薦，讀者務請運用個人獨立思考能力自行作出投資決定，如因相關建議招致損失，概與《經濟通》、編者及作者無涉。', ' ')
    s9 = s8.replace( '＊編者按:本文只供參考之用，並不構成要約、招攬或邀請、誘使、任何不論種類或形式之申述或訂立任何建議及推薦，讀者務請運用個人獨立思考能力自行作出投資決定，如因相關建議招致損失，概與《經濟通通訊社》、編者及作者無涉。',' ')

    s10 = s9.replace('有關業績的詳情，請參閱該公司之正式通告', ' ')
    s11 = s10.replace('有關業績的備註，請參閱該公司之正式通告', ' ')

    s12 = s11.replace('上述報價只供參考用', ' ')
    s13 = s12.replace('筆者並無持有上述股份', ' ')
    s14 = s13.replace('編者按：本文只供參考之用，並不構成要約、招攬或邀請、誘使、任何不論種類或形式之申述或訂立任何建議及推薦，讀者務請運用個人獨立思考能力自行作出投資決定，如因相關建議招致損失，概與《經濟通通訊社》、編者及作者無涉。','')
    s14=s14.replace('《經濟通及交易通首席顧問梁業豪》','')
    s14=re.sub('\（\w\w\：patreon\.BennyLeung\.com\）','',s14)
    return s14

def Remove_Advertisement(s:str)->str:

    s0 = re.sub('\（\d{1,2}日\）', ' ',s)
    s1 = s0.replace('【etnet一App通天下】', ' ')
    s2 = s1.replace('【etnetApp】', ' ')

    s3 = s2.replace('立即下載>>', ' ')
    s4 = s3.replace('立即下載 >>', ' ')
    s5 = s4.replace('etnet財經.生活AppiOS/', ' ')
    s6 = s5.replace('etnet財經.生活AppiOS:', ' ')
    s6 = s5.replace('強化版AppiOS:', ' ')

    s6 = s5.replace('etnet財經.生活AppiOS', ' ')


    s7 = s6.replace('Android:', ' ')
    s8 = s7.replace('iOS:', ' ')
    s9 = s8.replace('Huawei:', ' ')
    s10 = s9.replace('Huawei:', ' ')

    s11 = s10.replace('獨家「銀行匯率比較」功能:', ' ')
    s12 = s11.replace('匯市靚價一眼通，一按直達交易商，買入賣出好輕鬆！', ' ')

    s13 = s12.replace('etnetMQ強化版App', ' ')
    s14 = s13.replace('etnet強化版MQ', ' ')

    s15 = s14.replace('精明外匯買賣三招', ' ')
    s16 = s15.replace('「睇圖 - 比較匯率 - 預設提示」', ' ')
    s17 = s16.replace('「睇圖-比較匯率-預設提示」', ' ')
    s18 = s17.replace('精明外匯買賣三招「睇圖 - 比較匯率 - 預設提示」', ' ')

    s19 = s18.replace('【etnetApp】精明外匯買賣三招「睇圖-比較匯率-預設提示」立即下載>>etnet財經.生活AppiOS/Huawei:iOS/:', ' ')
    s20 = s19.replace('【etnetApp】精明外匯買賣三招「睇圖 - 比較匯率 - 預設提示」', ' ')
    s21 = s20.replace('請大家早上8:55分，準時觀看《經濟通》NewsTV直播節目', ' ')

    return s21

def Remove_ReporterNote(s:str)->str:
    s1=s.strip(' ').strip()
    s2=re.sub('\(\w\w\)$|\＊$','',s1)
    s3=re.sub('\(經濟通通訊\w{2,4}\）$','',s2)
    s4=re.sub('\（\w{2,8}\）$','',s3)
    return s4



def Clean_NewsArticle_Func(s:str)->str:
    if s is None:
        return None
    else:
        s1=normalize_texts(s)
        s1 = cleantext.replace_urls(s1, '')
        s2 = cleantext.replace_emails(s1, '')
    #    s2 = Remove_Email_url(s1)
        s3 = Remove_StockCode(s2.strip())
        s4 = Remove_Advertisement(s3.strip().strip(' '))
        s5 = Remove_misc(s4.strip().strip(' '))
        s6 = Remove_Disclaimer(s5.strip().strip(' '))
        s7 = Remove_ReporterNote(s6.strip().strip(' '))
       # s7 = cleantext.fix_bad_unicode(s7)
      #  s7 = cleantext.to_ascii_unicode(s7, lang="zh")
        #s7 = cleantext.fix_strange_quotes(s7)
        return s7

def Clean_lifestyleArticle_func(ss):
    ss1=Remove_html_code(ss)
   # ss2=Remove_Email_url(ss1)
    ss3=Remove_StockCode(ss1)
    ss4= re.sub('\s|\t|\n','',ss3)
    ss5=cleantext.replace_urls(ss4,'')
    ss6=cleantext.replace_emails(ss5,'')
   # ss7=cleantext.fix_bad_unicode(ss6)

 #   ss7=cleantext.fix_strange_quotes(ss7)

    return ss6


def str_isDate(word:str)->bool:
    single_date_regex = re.compile('^[廿|一|二|三|四|五|六|七|八|九|十|\d]{1,4}(個?)(年|月|日|天|小時|分鐘|秒|星期|禮拜)$')
    double_date_regex = re.compile('^第[廿|一|二|三|四|五|六|七|八|九|十|\d]{1,5}(個?)(年|月|日|天|小時|分鐘|秒|星期|禮拜)$')
    mid_date_regex = re.compile('(^[廿|一|二|三|四|五|六|七|八|九|十|\d]{1,5}(個?)(年|月|日|天|小時|分鐘|秒|星期|禮拜))([廿|一|二|三|四|五|六|七|八|九|十|\d]{1,5}(個?)(年|月|日|天|小時|分鐘|秒|星期|禮拜))$')
    long_date_regex = re.compile('(^[廿|一|二|三|四|五|六|七|八|九|十|\d]{1,5}(個?)(年|月|日|天|小時|分鐘|秒|星期|禮拜))([廿|一|二|三|四|五|六|七|八|九|十|\d]{1,5}(個?)(年|月|日|天|小時|分鐘|秒|星期|禮拜))([廿|一|二|三|四|五|六|七|八|九|十|\d]{1,5}(個?)(年|月|日|天|小時|分鐘|秒|星期|禮拜))$')
    return any([re.fullmatch(pattern=pattern, string=word) for pattern in [single_date_regex,double_date_regex,mid_date_regex ,long_date_regex]])

def str_isNumber(word)->bool:
    try:
        float(word)
        return True
    except ValueError:
        return False

def convert_str2dt(start:Union[datetime,str,int],end:Union[datetime,str]):
    if type(end) is datetime:
        end = end
    elif type(end) is str:
        end = pytime.parse(end)
        if type(end) is date:
            end=datetime(end.year,end.month,end.day,hour=0,minute=0,second=0)
        else:
            end=end

    if type(start) is datetime:
        start = start
    elif type(start) is int or len(start) < 8:
        start = end- timedelta(minutes=int(start))
    else:
        start = pytime.parse(start)

        if type(start) is date:
            start = datetime(start.year, start.month, start.day, hour=0, minute=0, second=0)
        else:
            start =start

    return start,end

