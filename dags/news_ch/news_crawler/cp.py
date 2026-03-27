import requests
from bs4 import BeautifulSoup
import re
import datetime
import pandas as pd
import time
import random
from news_ch.news_crawler.base_process import *

class CP(NewsLogic):
    _source='CP值'
    _domain_url='https://cpok.tw/'
    
    def parse_one_block_article_meta(self,part):
        article_id=part.get('id')
        article_url=part.find('a').get('href')
        title=part.find('h2').text.strip()
        brief_content=part.find(class_='archive-content').text.strip()
        views=part.find(class_='views').text.strip()
        img_url=part.find('img').get('src')

        #文章時間無法快速取得，需要進入文章才能獲得完整的時間
        b=requests.get(article_url,headers=self._headers,timeout=self._timeout)
        soup_content=BeautifulSoup(b.text,'lxml')
        metadata=soup_content.find(class_='entry-content').find(class_='begin-single-meta')

        author=metadata.find('a',{'rel':'author'}).text.strip()
        created_at=metadata.find(class_='my-date').text.strip()
        created_at=re.sub(string=created_at,pattern=r'\s*(年|月|日)\s*',repl='-')
        created_at=' '.join(created_at.rsplit('-',maxsplit=1))

        data={
            'article_id':article_id,
            'author':author,
            'created_at':created_at,
            'title':title,
            'brief_content':brief_content,
            'views':int(views.replace(',','')),
            'img_url':img_url,
            'article_url':article_url
        }
        
        return data
    
    def get_article_url_from(self):
        data=[]
        
        for page_num in range(1,self._page+1):
            url=f'https://cpok.tw/discount/page/{page_num}' #所有board皆涵蓋的類別

            a=requests.get(url,headers=self._headers,timeout=self._timeout)
            soup=BeautifulSoup(a.text,'lxml')
            
            for part in soup.find('div',{'id':'content'}).find_all('article'):
                tmp=self.parse_one_block_article_meta(part)
                data.append(pd.DataFrame(tmp,index=[0]))
            
            time.sleep(random.randint(1,3))
            print(f'[中文新聞] 列表頁完成 | 來源=CP值 | page={page_num}')
        
        df=pd.concat(data)
        df['created_at']=df['created_at'].astype('datetime64[ns]')
        df['create_time']=self._now_time
        df=df.sort_values(['created_at'])
        df['source']=self._source

        return df
    
