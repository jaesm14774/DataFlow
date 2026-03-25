import requests
from bs4 import BeautifulSoup
import re
import datetime
import pandas as pd
from itertools import chain
from news_ch.news_crawler.base_process import *

class TheNewsLens(NewsLogic):
    _source='thenewslens'
    _timeout=30
    _domain_url='https://www.thenewslens.com'
    
    def get_article_url_from(self):
        self._headers['Referer']=self._domain_url
        
        article_url=[];keyword=[];created_at=[];img=[]

        for page_num in range(1,self._page+1):
            b=requests.get(f'{self._domain_url}/news?page={page_num}',
                           headers=self._headers,timeout=self._timeout)
            soup=BeautifulSoup(b.text,'lxml')    

            article_url.append([s.find('a').get('href') for s in soup.find_all('h2',class_='title')])

            for s in soup.find_all('div',class_='col-sm-8'):
                temp_keyword=[]

                try:
                    for part in s.find(class_='tags-box').find_all('a',{'data-gtm-label':'tag'}):
                        temp_keyword.extend([part.text.strip()])
                except Exception:
                    temp_keyword=[' ']

                keyword.append([';'.join(temp_keyword)])

            created_at.append([s.find(class_='time').text.split('|')[0].strip().replace('/','-') for s in soup.find_all(class_='info-box')])

            img.append([s.find('img').get('data-srcset').split(', ')[-1].strip() for s in soup.find_all(class_='img-box')])

        article_url=list(chain.from_iterable(article_url))  
        keyword=list(chain.from_iterable(keyword))
        created_at=list(chain.from_iterable(created_at))
        img=list(chain.from_iterable(img))

        TEMP=pd.DataFrame({'source':[self._source]*len(article_url),
                           'article_url':article_url,
                           'category':' ',
                           'created_at':created_at,
                           'img':img,
                           'keyword':keyword})   

        return TEMP


    def get_article_info(self,article_url,tim,img,keyword,category):
        self._headers['Referer']=self._domain_url
        
        a=requests.get(article_url,headers=self._headers,timeout=self._timeout)
        soup=BeautifulSoup(a.text,'lxml')

        try:
            title=soup.find('h1').text
            title=re.sub(string=title,pattern=r'^\s+|\s+$|\u3000|\xa0',repl='')
        except Exception:
            print(f'[中文新聞] 缺少 title（頁面改版或選擇器失效）| url={article_url}')
            title=' '

        try:
            category=soup.find(class_=re.compile('article.*-box')).find(class_=re.compile('d-inline|cate')).text
            category=category.strip()
        except Exception:
            print(f'[中文新聞] 缺少 category | url={article_url}')
            category=' '

        try:
            tim=datetime.datetime.strptime(tim,'%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S')

            created_at=tim
        except Exception:
            print(f'[中文新聞] 時間欄位無法解析 | url={article_url}')
            created_at=None

        try:
            author=soup.find(class_=re.compile('author-name')).text.strip()
        except Exception:
            print(f'[中文新聞] 缺少 author | url={article_url}')
            author=' '

        for s in soup.find_all(re.compile('img|figure|script')):
            s.decompose()

        try:
            summary=soup.find(class_='WhyNeedKnow').text
        except Exception:
            summary=''

        try:
            content='\n'.join([s.text for s in soup.find(class_='article-content').find_all('p')])
        except Exception:
            print(f'[中文新聞] 缺少 content | url={article_url}')
            content=' '    

        content=summary+'\n\n\n\n\n'+content

        keyword=keyword

        article_id=re.search(string=article_url,pattern=r'/(\d+)$').group(1)

        return self.build_article_dataframe(
            article_id=article_id, title=title, created_at=created_at,
            content=content, author=author, category=category,
            keyword=keyword, article_url=article_url, img=img)

