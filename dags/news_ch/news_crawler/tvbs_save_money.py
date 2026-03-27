import requests
from bs4 import BeautifulSoup
import re
import datetime
import pandas as pd
import json
from news_ch.news_crawler.base_process import *

class TVBS_SaveMoney(NewsLogic):
    _source='tvbs_優惠好康情報'
    _domain_url='https://news.tvbs.com.tw/pack/packdetail/841' #只抓優惠活動，其他未更新
    _page=1

    def parse_json(self, soup_format_str):
        json_str=soup_format_str.text.strip()
        json_str=re.sub(string=json_str, pattern=r'\n\s+|\s+\n', repl='')
        
        return json.loads(json_str)    

    def get_article_url_from(self):
        from lib.get_sql import get_sql
        from common.config import sql_configure_path

        connection=pd.read_csv(sql_configure_path,encoding='utf_8_sig',index_col='name')
        conn,cursor,engine=get_sql(connection.loc['host','value'],connection.loc['port','value'],
                                    connection.loc['user','value'],connection.loc['password','value'],
                                    'news_ch')

        for page_num in range(1,self._page+1):
            response=requests.post(f'{self._domain_url}/{page_num}',headers=self._headers, timeout=20)
            soup=BeautifulSoup(response.text,'lxml')

        json_data_list=[self.parse_json(section) for section in soup.find_all('script', {'type':'application/ld+json'}) if 'itemListElement' in self.parse_json(section)]
        
        if not json_data_list:
            print(f'[中文新聞-TVBS] 無 JSON-LD itemList（頁面結構變更）| base={self._domain_url}')
            return pd.DataFrame(columns=['article_id', 'author', 'created_at', 'title', 'brief_content', 'views', 'img_url', 'article_url', 'create_time', 'source'])
        
        data=json_data_list[0]
        data=data['itemListElement']
        article_id_list=[d['url'].split('/')[-1] for d in data]
        
        if not article_id_list:
            print(f'[中文新聞-TVBS] JSON-LD 無文章 ID | base={self._domain_url}')
            return pd.DataFrame(columns=['article_id', 'author', 'created_at', 'title', 'brief_content', 'views', 'img_url', 'article_url', 'create_time', 'source'])
        
        df=pd.read_sql_query(f"""
            select 
                article_id, author, created_at, title, substring(content,1,100) brief_content, 
                NULL views, img img_url, article_url, create_time
            from news 
            where source='TVBS' and article_id in {tuple(article_id_list)}
            order by created_at desc
        """, engine)
        df['source']=self._source

        return df
