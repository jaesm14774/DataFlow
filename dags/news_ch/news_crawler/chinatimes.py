import requests
from bs4 import BeautifulSoup
import re
import datetime
import pandas as pd
from itertools import chain
import cloudscraper
from news_ch.news_crawler.base_process import *

class ChinaTimes(NewsLogic):
    _source='中時日報'
    _domain_url='https://www.chinatimes.com'
    _page=10
    
    def get_article_url_from(self):
        rows=[]
        
        scraper=cloudscraper.create_scraper()
        for page_num in range(1,self._page+1):
            a=scraper.get(f'{self._domain_url}/realtimenews?page={page_num}&chdtv',
                         headers=self._headers,timeout=self._timeout)
            soup=BeautifulSoup(a.text,'lxml')
            
            items=soup.select('div.articlebox-compact')
            if not items:
                items=soup.find_all('h3',class_='title')
                for item in items:
                    a_tag=item.find('a')
                    if not a_tag:
                        continue
                    href=a_tag.get('href','')
                    if href and not href.startswith('http'):
                        href=self._domain_url+href
                    rows.append({'article_url':href,'category':' '})
            else:
                for item in items:
                    a_tag=item.select_one('h3.title a')
                    if not a_tag:
                        a_tag=item.find('a')
                    if not a_tag:
                        continue
                    href=a_tag.get('href','')
                    if href and not href.startswith('http'):
                        href=self._domain_url+href
                    cat_el=item.select_one('.meta-info .category')
                    cat=cat_el.text.strip() if cat_el else ' '
                    rows.append({'article_url':href,'category':cat})

        if not rows:
            return pd.DataFrame(columns=['source','article_url','category','created_at','img','keyword'])

        TEMP=pd.DataFrame(rows)
        TEMP['source']=self._source
        TEMP['created_at']=' '
        TEMP['img']=' '
        TEMP['keyword']=' '
        return TEMP

    def get_article_info(self,article_url,tim,img,keyword,category):
        scraper=cloudscraper.create_scraper()
        a=scraper.get(article_url,headers=self._headers,timeout=self._timeout)
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
        
