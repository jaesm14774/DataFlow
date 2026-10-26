import requests
from bs4 import BeautifulSoup
import re
import datetime
import pandas as pd
from itertools import chain
import time
from news_ch.news_crawler.base_process import *
from news_ch.news_crawler.page_utils import article_metadata, jsonld_nodes, published_time, meta

class Anue(NewsLogic):
    _source='鉅亨網'
    _page=5
    
    def extract_img(self,item):
        try:
            result=item['l']['src']
        except Exception:
            result=' '
        
        return result
    
    def get_article_url_from(self):
        u=(
            f'https://api.cnyes.com/media/api/v1/newslist/category/headline?limit=30'
        )
        a=requests.get(u,headers=self._headers,timeout=self._timeout)
        a.encoding='utf-8-sig'
        a=a.json()
        
        total_page=a['items']['last_page']
        
        url=[];Time=[];img=[]
        
        for page_num in range(1,self._page+1):
            u=(
                f'https://api.cnyes.com/media/api/v1/newslist/category/headline?'
                f'limit=30&page={page_num}'
            )
            
            a=requests.get(u,headers=self._headers,timeout=self._timeout)
            a.encoding='utf-8-sig'
            a=a.json()
            dat=pd.DataFrame(a['items']['data'])
            
            if len(dat) == 0:
                continue
            
            url.append(['https://news.cnyes.com/news/id/%s?exp=a' % ss for ss in dat.newsId])
            Time.append([
                datetime.datetime.fromtimestamp(
                    t, datetime.timezone.utc
                ).astimezone(
                    datetime.timezone(datetime.timedelta(hours=8))
                ).strftime('%Y-%m-%d %H:%M:%S')
                for t in dat.publishAt
            ])
            img.append(dat.coverSrc.map(self.extract_img).tolist())
            time.sleep(3)

        url=list(chain.from_iterable(url))
        img=list(chain.from_iterable(img))
        Time=list(chain.from_iterable(Time))
        tag=''
        
        TEMP=pd.DataFrame({'source':self._source,
                           'article_url':url,
                           'category':tag,
                           'created_at':Time,
                            'img':img,
                           'keyword':' '}
                         )      
        return TEMP
    
    def get_article_info(self,article_url,tim,img,keyword,category):
        a=requests.get(article_url,headers=self._headers,timeout=self._timeout)
        a.raise_for_status()
        soup=BeautifulSoup(a.text,'lxml')

        metadata=article_metadata(soup,article_url)
        if not metadata:
            raise ValueError('Anue article has no matching NewsArticle metadata')
        
        try:
            title=metadata['headline']
            title=re.sub(string=title,pattern=r'\u3000|\xa0|^\s+|\s+$',repl='')
        except Exception:
            print(f'[中文新聞] 缺少 title（頁面改版或選擇器失效）| url={article_url}')
            title=' '
        
        created_at=published_time(metadata.get('datePublished'))
        
        try:
            raw_author=metadata.get('author',[])
            raw_author=raw_author if isinstance(raw_author,list) else [raw_author]
            author=';'.join(item.get('name','') if isinstance(item,dict) else str(item) for item in raw_author)
            author=re.sub(string=author,pattern='鉅亨網|記者',repl='').strip()
        except Exception:
            print(f'[中文新聞] 缺少 author | url={article_url}')
            author=' '
        
        try:
            content=metadata.get('articleBody') or ''
            if not content:
                temp=soup.find('main', {'id': 'article-container'}).find_all('p')
                content='\n'.join(s.get_text(' ',strip=True) for s in temp)
            content=re.sub(pattern=r'\n+$|^\n+',repl='',string=content)
        except Exception:
            print(f'[中文新聞] 缺少 content | url={article_url}')
            content=' '
        
        #keyword
        try:
            keyword=str(metadata.get('keywords') or '').replace(',',';')
        except Exception:
            keyword=' '
            print(f'[中文新聞] 缺少 keyword | url={article_url}')

        article_id=re.search(string=article_url,pattern=r'id/(\d+)').group(1)

        try:
            breadcrumbs=next((n for n in jsonld_nodes(soup) if n.get('@type')=='BreadcrumbList'),{})
            category=';'.join(item.get('name','') for item in breadcrumbs.get('itemListElement',[]) if not item.get('item') or not str(item.get('item')).split('?')[0].rstrip('/').endswith('/id/'+article_id))
            category=category or metadata.get('articleSection','')
        except Exception:
            category=''
            print(f'[中文新聞] 缺少 category | url={article_url}')

        return self.build_article_dataframe(
            article_id=article_id, title=title, created_at=created_at,
            content=content, author=author, category=category,
            keyword=keyword, article_url=article_url, img=img)

