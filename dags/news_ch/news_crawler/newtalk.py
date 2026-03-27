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

        items=soup.find_all(class_='news-list-item clearfix')
        if not items:
            news_ul=soup.select_one('ul.category-list')
            if news_ul:
                items=news_ul.find_all('li',class_=lambda c: c and 'hover-parent-image-scale' in c)
            else:
                items=[]

        for part in items:
            try:
                date_el=part.find(class_='news_date') or part.find('p',class_='date')
                if not date_el:
                    continue
                t=date_el.text
                t=re.sub(r'^.*發布\s*','',t).strip()
                t1_m=re.search(r'\d+\.\d+\.\d+',t)
                t2_m=re.search(r'\d+:\d+',t)
                if not t1_m or not t2_m:
                    continue
                t1=t1_m.group(0)
                t2=t2_m.group(0)
            except Exception:
                continue
            
            a_tag=part.select_one('a[href*="/news/view/"]') or part.find('a')
            if not a_tag:
                continue
            href=a_tag.get('href','')
            if href and not href.startswith('http'):
                href=self._domain_url+href
            url.append(href)

            tag.append(' ')
            Time.append(t1+' '+t2)

            img_el=part.find('img')
            if img_el:
                img.append(img_el.get('data-src') or img_el.get('src') or '')
            else:
                img.append('')

        TEMP=pd.DataFrame({'source':self._source,
                           'article_url':url,
                           'category':' ',
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

        try:
            tags_el=soup.find(class_='tags')
            if tags_el:
                cat_el=tags_el.find(class_='subcategory_tag')
                if cat_el:
                    category=cat_el.text.strip()
                else:
                    first_a=tags_el.find('a')
                    category=first_a.text.strip() if first_a else ' '
            else:
                bc=soup.select_one('.breadcrumb a, nav[aria-label="breadcrumb"] a')
                if bc:
                    bcs=soup.select('.breadcrumb a, nav[aria-label="breadcrumb"] a')
                    category=bcs[-1].text.strip() if bcs else ' '
                else:
                    meta_sec=soup.find('meta',{'property':'article:section'})
                    category=meta_sec['content'].strip() if meta_sec and meta_sec.get('content') else ' '
            if not category:
                category=' '
        except Exception:
            category=' '

        try:
            tags_el=soup.find(class_='tags')
            if tags_el:
                keyword=';'.join([s.text.strip() for s in tags_el.find_all('a')[1:] if s.text.strip()])
            else:
                meta_kw=soup.find('meta',{'name':'keywords'})
                keyword=meta_kw['content'].replace(',',';') if meta_kw and meta_kw.get('content') else ' '
            if not keyword:
                keyword=' '
        except Exception:
            keyword=' '

        try:
            author=soup.find(class_='content_reporter').text.strip()
            if not author:
                author=' '
        except Exception:
            try:
                author_el=soup.select_one('.info .author, .reporter')
                author=author_el.text.strip() if author_el else ' '
            except Exception:
                author=' '

        try:
            body=soup.find('div',{'itemprop':'articleBody'})
            if body:
                for s in body.find_all('script'):
                    s.decompose()
                for s in soup.find_all(class_='news_img'):
                    s.decompose()
                content='\n'.join([s.text.strip() for s in body.find_all('p')])
            else:
                article_el=soup.select_one('article .content, .news_content')
                content=article_el.text.strip() if article_el else ' '
            if not content:
                content=' '
        except Exception:
            content=' '
            print(f'[中文新聞] 缺少 content | url={article_url}')

        m=re.search(string=article_url,pattern=r'view/(\d+-\d+-\d+/\d+)$')
        if m:
            article_id=m.group(1)
        else:
            m2=re.search(string=article_url,pattern=r'/(\d+)$')
            article_id=m2.group(1) if m2 else article_url.split('/')[-1]

        return self.build_article_dataframe(
            article_id=article_id, title=title, created_at=created_at,
            content=content, author=author, category=category,
            keyword=keyword, article_url=article_url, img=img)

