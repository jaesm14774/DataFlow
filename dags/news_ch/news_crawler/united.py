import requests
from bs4 import BeautifulSoup
import re
import datetime
import pandas as pd
from itertools import chain
from urllib.parse import urlsplit
from news_ch.news_crawler.base_process import *
from news_ch.news_crawler.page_utils import meta, published_time, scoped_text

class United(NewsLogic):
    _source='聯合報'
    _timeout=30
    _domain_url='https://udn.com'
    
    def get_article_url_from(self):
        tim=[];img=[];article_url=[]
        for page_num in range(0,self._page):
            a=requests.get(f'https://udn.com/api/more?page={page_num+1}&id=&channelId=1&cate_id=99&type=breaknews',
                           headers=self._headers,
                           timeout=self._timeout)
            a.encoding='utf-8-sig'
            a=a.json()
            a=a['lists']
            
            article_url.append([re.sub(string=self._domain_url+s['titleLink'],
                                       pattern=r'\?from=udn-ch1_breaknews-1-99-news',
                                       repl='') for s in a])
            
            img.append([s['url'] for s in a])
            
            tim.append([s['time']['date'] for s in a])
        
        tim=list(chain.from_iterable(tim))
        img=list(chain.from_iterable(img))
        article_url=list(chain.from_iterable(article_url))

        TEMP=pd.DataFrame({'source':[self._source]*len(article_url),
                           'article_url':article_url,
                           'category':' ',
                           'created_at':tim,
                           'img':img,
                           'keyword':' '})
        
        return TEMP

    def _load_article_page(self,article_url):
        seen=set()
        for _ in range(3):
            if article_url in seen:
                raise ValueError('UDN article redirect loop')
            seen.add(article_url)
            b=requests.get(article_url,headers=self._headers,timeout=self._timeout)
            b.raise_for_status()
            resolved=urlsplit(b.url)
            if resolved.scheme!='https' or resolved.hostname!='udn.com' or not re.fullmatch(r'/news/story/\d+/\d+',resolved.path):
                raise ValueError('UDN article redirected outside public story pages')
            article_url=b.url
            soup=BeautifulSoup(b.content,'lxml')
            if soup.select_one('.article-content__paragraph,.article'):
                return article_url,soup
            # Some breaking-news links are public JavaScript redirect stubs.
            # Read only the explicit literal URL; never execute publisher scripts.
            targets=[]
            redirect_wrapper=soup.select_one('.article-content__wrapper')
            for script in redirect_wrapper.find_all('script') if redirect_wrapper else []:
                match=re.search(r'''\bwindow\.location\.href\s*=\s*(['"])(https://udn\.com/news/story/\d+/\d+)\1\s*;?''',script.get_text())
                if match:
                    targets.append(match.group(2))
            if len(set(targets))!=1:
                raise ValueError('UDN has no recognized public body; metadata description is not full text')
            article_url=targets[0]
        raise ValueError('UDN article exceeded redirect limit')

    def get_article_info(self,article_url,tim,img,keyword,category):
        article_url,soup=self._load_article_page(article_url)
        
        #title
        try:
            title=soup.find('h1',class_=re.compile('title')).text.strip()
        except Exception:
            title=[s.get('content') for s in soup.find_all('meta') if bool(re.search(string=str(s),pattern=r'og:title'))][0]
            title=title.split('|')[0]
            print(f'[中文新聞] 缺少 title（頁面改版或選擇器失效）| url={article_url}') 

        #發表日期
        if tim is None or tim=='' or tim==' ':
            try:
                created_at=soup.find('time',class_='article-content__time').text
                created_at=created_at+':00'
            except Exception:
                try:
                    created_at=soup.find(class_='shareBar__info--author').find('span').text
                    created_at=created_at+':00'
                except Exception:
                    created_at=[s.get('content') for s in soup.find_all('meta') if bool(re.search(string=str(s),pattern='date.available'))][0]
        else:
            created_at=tim
        created_at=published_time(meta(soup,'article:published_time')) or published_time(created_at)

        #圖片
        img=meta(soup,'og:image') or img
        if img is None or img=='' or img==' ':
            try:
                img=soup.find(re.compile('picture|figure')).find('img').get('src')
                if not bool(re.search(string=img,pattern='^https*')):
                    img='https:'+img
            except Exception:
                try:
                    img=[s.get('content') for s in soup.find_all('meta') if bool(re.search(string=str(s),pattern='taboola:image'))][0]
                except Exception:
                    img=' '
                    print(f'[中文新聞] 圖片欄位解析失敗 | url={article_url}')   

        try:
            category=[s.text for s in soup.find_all(class_='breadcrumb-items') if s.get('href') is not None][-1]
        except Exception:
            try:
                category=soup.find(class_=re.compile('breadcrumb')).find('a').text.strip()
            except Exception:
                category=' '
                print(f'[中文新聞] 分類解析失敗 | url={article_url}')

        #author
        try:
            author=soup.find(class_='article-content__author').text.strip()
            author=re.sub(string=author,pattern=r'\s+',repl=' ')
        except Exception:
            try:
                author=soup.find(class_='shareBar__info--author').text
                author=re.sub(string=author,pattern=created_at,repl='')
            except Exception:
                try:
                    author=[s.get('content') for s in soup.find_all('meta') if bool(re.search(string=str(s),pattern=r'name=\"author\"/'))][0]
                except Exception:
                    print(f'[中文新聞] 缺少 author | url={article_url}')
                    author=' '    
                    
        #keyword
        try:
            keyword=';'.join([s.text.strip() for s in soup.find('section',class_='keywords').find_all('a')])
        except Exception:
            keyword=' '
            print(f'[中文新聞] 缺少 keyword | url={article_url}')
        
        #content 
        #去除圖片影響
        for s in soup.find_all(re.compile('^(img|picture|figure)')):
            s.decompose()

        body=soup.select_one('.article-content__paragraph') or soup.select_one('.article')
        if body is None:
            raise ValueError('UDN public article body is missing')
        content=scoped_text(body.select_one('section') or body)
        if not content:
            raise ValueError('UDN public article body is empty')

        article_id=re.search(string=article_url,pattern=r'\d+/\d+').group(0)

        return self.build_article_dataframe(
            article_id=article_id, title=title, created_at=created_at,
            content=content, author=author, category=category,
            keyword=keyword, article_url=article_url, img=img)

