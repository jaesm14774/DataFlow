import requests
from bs4 import BeautifulSoup
import re
import datetime
import pandas as pd
from news_ch.news_crawler.base_process import *

class PTS(NewsLogic):
    _source='公視新聞'
    _domain_url='https://news.pts.org.tw'
    _page=3
    
    def get_article_url_from(self):
        try:
            tag=[];img=[];article_url=[]
            
            for page_num in range(1,self._page+1):
                a=requests.get(f'{self._domain_url}/dailynews?page={page_num}',headers=self._headers,
                               timeout=self._timeout)
                soup=BeautifulSoup(a.text,'lxml')    
                
                if page_num == 1:
                    #每頁的第一篇新聞
                    if soup.find(class_='breakingnews').find(class_='news-info').find('a') is None:
                        category=' '
                    else:
                        category=soup.find(class_='breakingnews').find(class_='news-info').find('a').text

                    tag.append(category)
                    img.append(soup.find(class_='breakingnews').find('img').get('src'))
                    article_url.append(soup.find('h2').find('a').get('href'))
                else:
                    for part in soup.find(class_='list-unstyled news-list').find_all('li',class_='d-flex'):
                        if part.find(class_='news-info').find('a') is None:
                            category=' '
                        else:
                            category=part.find(class_='news-info').find('a').text

                        article_url.append(part.find('figure').find('a').get('href'))
                        tag.append(category)
                        img.append(part.find('figure').find('img').get('src'))

            TEMP=pd.DataFrame({'source':[self._source]*len(article_url),
                               'article_url':article_url,
                               'category':tag,
                               'created_at':' ',
                               'img':img,
                               'keyword':' '})
            
            return TEMP
        except Exception:
            return pd.DataFrame() 

    def get_article_info(self,article_url,tim,img,keyword,category):
        b=requests.get(article_url,headers=self._headers,timeout=self._timeout)
        soup=BeautifulSoup(b.text,'lxml')
        
        try:
            title=soup.find('h1').text
        except Exception:
            title=' '
            print(f'[中文新聞] 缺少 title（頁面改版或選擇器失效）| url={article_url}')
        
        try:
            author=soup.find('span',class_='article-reporter').text
            author=author.split(' / ')
            if isinstance(author,list):
                author=author[0]
            if author == '':
                author=' '
        except Exception:
            author=' '
            print(f'[中文新聞] 缺少 author | url={article_url}')     

        created_at=soup.find('time').text
        
        if img == '':
            img=' '
        
        category=category
        
        try:
            content=soup.find(class_='post-article').text
            content=re.sub(string=content,pattern='\xa0',repl='').strip()
        except Exception:
            content=' '
            print(f'[中文新聞] 缺少 content | url={article_url}')
        
        #keyword
        try:
            keyword=[s.find('a').text for s in soup.find('ul',class_='tag-list').find_all(class_='blue-tag')]
            keyword=';'.join([s for s in keyword if s !='...'])
        except Exception:
            keyword=' '
            print(f'[中文新聞] 缺少 keyword | url={article_url}')
        
        article_id=re.search(string=article_url,pattern=r'article/(\d+)$').group(1)
        
        return self.build_article_dataframe(
            article_id=article_id, title=title, created_at=created_at,
            content=content, author=author, category=category,
            keyword=keyword, article_url=article_url, img=img)

