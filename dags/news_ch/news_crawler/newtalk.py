import requests
from bs4 import BeautifulSoup
import re
import datetime
import pandas as pd
from news_ch.news_crawler.base_process import *

class NewTalk(NewsLogic):
    _source='newtalk'
    _domain_url='https://newtalk.tw'
    _now_date=datetime.datetime.now().strftime('%Y-%m-%d')
    
    def get_article_url_from(self):
        a=requests.get(f'{self._domain_url}/news/summary/{self._now_date}',
                      headers=self._headers,timeout=self._timeout)
        a.encoding='utf8'
        
        soup=BeautifulSoup(a.text,'lxml')

        url=[];tag=[];Time=[];img=[]

        for part in soup.find_all(class_='news-list-item clearfix'):
            #可能是影音專區 or 錯誤時間
            try:
                t=part.find(class_='news_date').text
                t1=re.search(string=t,pattern=r'\d+\.\d+\.\d+').group(0)
                t2=re.search(string=t,pattern=r'\d+:\d+').group(0)
            except Exception:
                continue
            
            url.append(part.find('a').get('href'))

            tag=' '

            Time.append(t1+' '+t2)
            img.append(part.find('img').get('src'))

        TEMP=pd.DataFrame({'source':self._source,
                           'article_url':url,
                           'category':tag,
                           'created_at':Time,
                           'img':img,
                           'keyword':' '})      
        return TEMP

    def get_article_info(self,article_url,tim,img,keyword,category):
        b=requests.get(article_url,headers=self._headers,timeout=self._timeout)
        b.encoding='utf8'
        soup=BeautifulSoup(b.text,'lxml')

        try:
            title=soup.find('h1').text
        except Exception:
            title=' '
            print(f'[中文新聞] 缺少 title（頁面改版或選擇器失效）| url={article_url}')     

        created_at=tim.replace('.','-')

        category=soup.find(class_='tags').find(class_='subcategory_tag').text

        try:
            keyword=';'.join([s.text for s in soup.find(class_='tags').find_all('a')[1:]])
        except Exception:
            keyword=' '
            print(f'[中文新聞] 缺少 keyword | url={article_url}') 

        try:
            author=soup.find(class_='content_reporter').text.strip()
        except Exception:
            author=' '
            print(f'[中文新聞] 缺少 author | url={article_url}') 

        #去除script，有偷藏一個在article content裡，不抓圖片解說文字
        soup.find('div',{'itemprop':'articleBody'}).find('script').decompose()

        for s in soup.find_all(class_='news_img'):
            s.decompose()

        try:
            content='\n'.join([s.text.strip() for s in soup.find('div',{'itemprop':'articleBody'}).find_all('p')])
        except Exception:
            content=' '
            print(f'[中文新聞] 缺少 content | url={article_url}')

        article_id=re.search(string=article_url,pattern=r'view/(\d+-\d+-\d+/\d+)$').group(1)

        return self.build_article_dataframe(
            article_id=article_id, title=title, created_at=created_at,
            content=content, author=author, category=category,
            keyword=keyword, article_url=article_url, img=img)

