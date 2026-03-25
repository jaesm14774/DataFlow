import requests
from bs4 import BeautifulSoup
import re
import datetime
import pandas as pd
from itertools import chain
from news_ch.news_crawler.base_process import *

class ChinaTimes(NewsLogic):
    _source='中時日報'
    _domain_url='https://www.chinatimes.com'
    _page=10
    
    def get_article_url_from(self):
        article_url=[];tag=[]
        
        for page_num in range(0,self._page):
            a=requests.get(f'{self._domain_url}/realtimenews?page={page_num}&chdtv',
                          headers=self._headers,timeout=self._timeout)
            soup=BeautifulSoup(a.text,'lxml')
            
            article_url.append([self._domain_url+s.find('a').get('href') for s in soup.find_all('h3',class_='title')])
            
            tag.append([s.find(class_='category').text for s in soup.find_all('div',class_='meta-info')])
        
        article_url=list(chain.from_iterable(article_url))
        tag=list(chain.from_iterable(tag))        

        TEMP=pd.DataFrame({'source':self._source,
                           'article_url':article_url,
                           'category':tag,
                           'created_at':' ',
                           'img':' ',
                           'keyword':' '})
        return TEMP

    def get_article_info(self,article_url,tim,img,keyword,category):
        a=requests.get(article_url,headers=self._headers,timeout=self._timeout)
        soup=BeautifulSoup(a.text,'lxml')
        
        try:
            title=soup.find('h1').text.strip()
            title=re.sub(string=title,pattern='\u3000|\xa0',repl='')
        except Exception:
            print(f'[中文新聞] 缺少 title（頁面改版或選擇器失效）| url={article_url}')
            title=' '
        
        try:
            created_at=soup.find('time').get('datetime')
            created_at=format(datetime.datetime.strptime(created_at,'%Y-%m-%d %H:%M'),'%Y-%m-%d %H:%M:%S')
        except Exception:
            print(f'[中文新聞] 缺少 created_at | url={article_url}')
            created_at=None
        
        try:
            try:
                s=soup.find('div',class_='meta-info').find(class_='source').text.strip()
            except Exception:
                s=''
            try:
                s2=soup.find('div',class_='meta-info').find(class_='author').text.strip()
            except Exception:
                s2=''
            
            author=s+' '+s2
            if author == '':
                author=' '
        except Exception:
            print(f'[中文新聞] 缺少 author | url={article_url}')
            author=' '
        
        try:
            img=soup.find('figure').find('img').get('src')
        except Exception:
            img=' '
            print(f'[中文新聞] 缺少 img | url={article_url}')

        try:
            for s in soup.find_all(re.compile('img|figure')):
                s.decompose()
            
            content=[re.sub(string=s.text,pattern=r'^\s+|\s+$',repl='') for s in soup.find(class_='article-body').find_all('p',recursive=False)]
            content='\n'.join(content)
            content=content.strip()
        except Exception:
            print(f'[中文新聞] 缺少 content | url={article_url}')
            content=' '            

        #keyword
        try:
            keyword=[t.find('a').text for t in soup.find_all(class_='hash-tag')]
            keyword=';'.join(keyword)
        except Exception:
            keyword=' '
            print(f'[中文新聞] 缺少 keyword | url={article_url}')
        
        article_id=re.search(string=article_url,pattern=r'\d+-\d+').group(0)

        return self.build_article_dataframe(
            article_id=article_id, title=title, created_at=created_at,
            content=content, author=author, category=category,
            keyword=keyword, article_url=article_url, img=img)
        
