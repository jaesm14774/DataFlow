import pandas as pd
import numpy as np
import datetime
from common.config import *

user_agent=pd.read_csv(user_agent_path,sep=r'\n').useragent.tolist()


class NewsLogic(object):
    _page=3 #抓取幾頁文章url
    _source='' #來源
    _headers={
        'User-Agent': str(np.random.choice(user_agent)),
        'Referer': 'https://www.google.com.tw/?hl=zh_TW'
    }
    _timeout=30 #requests timeout
    _domain_url='' #dns
    _now_time=format(datetime.datetime.now()+datetime.timedelta(hours=8),'%Y-%m-%d %H:%M:%S')
    
    def get_article_url_from(self):
        pass
    
    def get_article_info(self):
        pass