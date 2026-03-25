import requests
from bs4 import BeautifulSoup
import re
import datetime
import pandas as pd
import random
from news_ch.news_crawler.base_process import *

class ETtoday(NewsLogic):
    _source='ETtoday'
    _domain_url=f'https://www.ettoday.net'
    _timeout=30
    _now_date=format(datetime.datetime.now(),'%Y-%m-%d')

    def change_time_format(self,txt):
        return re.sub(string=txt,pattern='/',repl='-')
    
    def get_article_url_from(self):
        a=requests.get(f'{self._domain_url}/news/news-list-{self._now_date}-0.htm',headers=self._headers,
                       timeout=self._timeout)
        soup=BeautifulSoup(a.text,'lxml')
        
        TEMP=[]
        for part in soup.find(class_='part_list_2').find_all('h3'):
            title=part.find('a').text.strip()
            
            created_at=part.find(class_='date').text.strip()
            if '-' in created_at:
                created_at=format(datetime.datetime.strptime(created_at,'%Y-%m-%d %H:%M'),'%Y-%m-%d %H:%M:%S')
            else:
                created_at=format(datetime.datetime.strptime(created_at,'%Y/%m/%d %H:%M'),'%Y-%m-%d %H:%M:%S')
            
            category=part.find('em',class_='tag').text
            
            article_url=part.find('a').get('href')
            if not bool(re.search(string=article_url,pattern=r'^https*')):
                article_url=self._domain_url+article_url
            
            TEMP.append(pd.DataFrame({
                'created_at':created_at,
                'category':category,
                'article_url':article_url,
                'source':self._source,
                'keyword':' ',
                'img':' ',
            },index=[1]))
        
        temp_date=re.sub(string=self._now_date,pattern='-',repl='')
        #抓到昨天的數據則停止
        sign=0;index=1
        while sign != 1:
            b=requests.post(f'{self._domain_url}/show_roll.php',headers={
                'user-agent':str(np.random.choice(user_agent)),
                'referer':f'{self._domain_url}/news/news-list-{self._now_date}-0.htm'},data={
                'offset': index,
                'tPage': 3,
                'tFile': f'{temp_date}.xml',
                'tOt': 0,
                'tSi': 100,
                'tAr': 0,
            })
            soup_t=BeautifulSoup(b.text,'lxml')
            
            compare_date=datetime.datetime.strptime(soup_t.find('h3').find(class_='date').text,'%Y/%m/%d %H:%M')
            now_date=datetime.datetime.strptime(self._now_date,'%Y-%m-%d')
            if compare_date < now_date:
                sign=1
            else:
                for part in soup_t.find_all('h3'):
                    title=part.find('a').text.strip()
                    
                    created_at=part.find(class_='date').text.strip()
                    if '-' in created_at:
                        created_at=format(datetime.datetime.strptime(created_at,'%Y-%m-%d %H:%M'),'%Y-%m-%d %H:%M:%S')
                    else:
                        created_at=format(datetime.datetime.strptime(created_at,'%Y/%m/%d %H:%M'),'%Y-%m-%d %H:%M:%S')
                    
                    category=part.find('em',class_='tag').text
                    
                    article_url=part.find('a').get('href')
                    if not bool(re.search(string=article_url,pattern=r'^https*')):
                        article_url=f'{self._domain_url}'+article_url
                    
                    TEMP.append(pd.DataFrame({
                        'created_at':created_at,
                        'category':category,
                        'article_url':article_url,
                        'source':self._source,
                        'keyword':' ',
                        'img':' ',
                    },index=[1]))
                
                index=index+1
        
        TEMP=pd.concat(TEMP)
        TEMP=TEMP[~TEMP.duplicated(subset=['article_url'])]
        TEMP['created_at']=TEMP.created_at.map(self.change_time_format)
        return TEMP.reset_index(drop=True)

    
    def get_article_info(self,article_url,tim,img,keyword,category):
        self._headers['Referer']=self._domain_url
        
        a=requests.get(article_url,headers=self._headers,timeout=self._timeout)
        soup=BeautifulSoup(a.text,'lxml')
        
        #title
        title=soup.find('h1',class_=re.compile('title')).text.strip()
        
        if soup.find(class_='tag') is not None:
            keyword=[s.text.strip() for s in soup.find(class_='tag').find_all('a')]
            keyword=[s for s in keyword if s !='']
            keyword=';'.join(keyword)
        else:
            try:
                keyword=[s.text.strip() for s in soup.find(class_='part_keyword').find_all('a')]
                keyword=[s for s in keyword if s !='']
                keyword=';'.join(keyword)           
            except Exception:
                keyword=' '
        
        if soup.find('div',class_='part_menu_5 clearfix') is not None:
            try:
                category=soup.find('div',class_='part_menu_5 clearfix').find(class_='btn current').text.strip()
            except Exception:
                category=soup.find('div',class_='part_menu_5 clearfix').find('strong').text.strip()
        elif soup.find(class_=re.compile('^logo_|_logo$')) is not None:
            category=soup.find(class_=re.compile('^logo_|_logo$')).text.strip()
        else:
            category=' '
            print(f'[中文新聞] 分類解析失敗 | url={article_url}')
        
        try:
            img=soup.find(class_='story').find('img').get('src')
            if not bool(re.search(string=img,pattern=r'^https*')):
                img='https:'+img
        except Exception:
            img=' '
        
        created_at=soup.find('time',class_=re.compile('news-time|date')).text.strip()
        created_at=re.sub(string=created_at,pattern=r'年|月|日|-|/',repl='')
        try:
            created_at=datetime.datetime.strptime(created_at,'%Y%m%d %H:%M')
        except Exception:
            created_at=datetime.datetime.strptime(created_at,'%Y%m%d %H:%M:%S')
        created_at=format(created_at,'%Y-%m-%d %H:%M:%S')
        
        #content,author
        for s in soup.find_all('img'):
            s.decompose()
        for s in soup.find_all(class_='ad_in_news'):
            s.decompose()
        
        content=[s.text for s in soup.find(class_='story').find_all('p') if not bool(re.search(string=s.text.strip(),
                                                                                               pattern=r'^(▲|▼|►)'))]
        content='\n'.join(content).strip()
        
        try:
            author=re.search(string=content,pattern=r'^(.{0,15}／.{0,15})\n').group(1)
        except Exception:
            author=' '
        
        if author !=' ':
            content=re.sub(string=content,pattern=author,repl='').strip()
        
        #article_id
        if re.search(string=article_url,pattern=r'(\d+)\.htm') is not None:
            article_id=re.search(string=article_url,pattern=r'(\d+)\.htm').group(1)
        else:
            article_id=re.search(string=article_url,pattern=r'/news/(\d+)').group(1)

        return self.build_article_dataframe(
            article_id=article_id, title=title, created_at=created_at,
            content=content, author=author, category=category,
            keyword=keyword, article_url=article_url, img=img)

