import pandas as pd
import numpy as np
import datetime
from common.config import *

user_agent=pd.read_csv(user_agent_path,sep=r'\n').useragent.tolist()

_ARTICLE_COLUMNS = [
    'article_id', 'title', 'created_at', 'create_time',
    'content', 'author', 'category', 'keyword',
    'article_url', 'img', 'source',
]


class NewsLogic(object):
    _page=3
    _source=''
    _timeout=30
    _domain_url=''

    def __init__(self):
        self._headers={
            'User-Agent': str(np.random.choice(user_agent)),
            'Referer': 'https://www.google.com.tw/?hl=zh_TW'
        }
        self._now_time=format(datetime.datetime.now()+datetime.timedelta(hours=8),'%Y-%m-%d %H:%M:%S')

    def get_article_url_from(self):
        pass

    def get_article_info(self):
        pass

    def build_article_dataframe(self, *, article_id, title, created_at,
                                content, author, category, keyword,
                                article_url, img):
        return pd.DataFrame({
            'article_id': article_id,
            'title': title,
            'created_at': created_at,
            'create_time': self._now_time,
            'content': content,
            'author': author,
            'category': category,
            'keyword': keyword,
            'article_url': article_url,
            'img': img,
            'source': self._source,
        }, index=[1])