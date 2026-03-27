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
        
        rows=[]

        for page_num in range(1,self._page+1):
            b=requests.get(f'{self._domain_url}/news?page={page_num}',
                           headers=self._headers,timeout=self._timeout)
            soup=BeautifulSoup(b.text,'lxml')    

            items=soup.select('section.item-wrapper.list-item')
            if not items:
                items_old=soup.find_all('h2',class_='title')
                for h2 in items_old:
                    a_tag=h2.find('a')
                    if a_tag:
                        rows.append({'article_url':a_tag.get('href',''),'keyword':' ','created_at':' ','img':' '})
                continue

            for sec in items:
                a_tag=sec.select_one('h3.item-title a[href]')
                if not a_tag:
                    a_tag=sec.select_one('a.img-link[href]')
                if not a_tag:
                    continue
                url=a_tag.get('href','')
                if url and not url.startswith('http'):
                    url=self._domain_url+url

                time_el=sec.select_one('time.time')
                if time_el:
                    t_text=time_el.text.strip().split('|')[0].strip().replace('/','-')
                else:
                    t_text=' '

                img_el=sec.select_one('img.img-cover')
                img_url=img_el.get('src','') if img_el else ' '

                kw_tags=sec.select('ul.tags-wrapper a.hashtag, ul.tags-wrapper a')
                if kw_tags:
                    kw=';'.join([t.text.strip() for t in kw_tags if t.text.strip()])
                else:
                    kw=' '

                rows.append({'article_url':url,'keyword':kw,'created_at':t_text,'img':img_url})

        if not rows:
            return pd.DataFrame(columns=['source','article_url','category','created_at','img','keyword'])

        TEMP=pd.DataFrame(rows)
        TEMP['source']=self._source
        TEMP['category']=' '

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
            cat_el=soup.select_one('.item-froms a[href*="/category/"], .article-info a[href*="/category/"]')
            if cat_el:
                category=cat_el.text.strip()
            else:
                cat_el2=soup.find(class_=re.compile('article.*-box'))
                if cat_el2:
                    c=cat_el2.find(class_=re.compile('d-inline|cate'))
                    category=c.text.strip() if c else ' '
                else:
                    category=' '
        except Exception:
            category=' '

        try:
            if tim and tim.strip() and tim.strip()!=' ':
                tim_clean=tim.strip()
                for fmt in ['%Y-%m-%d','%Y-%m-%d %H:%M:%S','%Y-%m-%d %H:%M']:
                    try:
                        created_at=datetime.datetime.strptime(tim_clean,fmt).strftime('%Y-%m-%d %H:%M:%S')
                        break
                    except ValueError:
                        continue
                else:
                    created_at=None
            else:
                created_at=None
        except Exception:
            created_at=None

        try:
            auth_el=soup.select_one('.author-wrapper a.author, a.author, .author-name, [class*="author-name"]')
            author=auth_el.text.strip() if auth_el else ' '
            if not author:
                meta_a=soup.find('meta',{'name':'author'})
                author=meta_a['content'].strip() if meta_a and meta_a.get('content') else ' '
        except Exception:
            author=' '

        for s in soup.find_all(re.compile('img|figure|script')):
            s.decompose()

        try:
            summary_el=soup.find(class_='WhyNeedKnow')
            summary=summary_el.text if summary_el else ''
        except Exception:
            summary=''

        try:
            body=soup.select_one('section[id^="article-content-"]')
            if not body:
                body=soup.select_one('section.article-body')
            if not body:
                body=soup.select_one('.article-content, .article-body-content')
            if body:
                paras=body.select('p.ck-section')
                if not paras:
                    paras=body.find_all('p')
                content='\n'.join([s.text for s in paras if s.text.strip()])
            else:
                content=' '
        except Exception:
            content=' '

        if summary and content!=' ':
            content=summary+'\n'+content
        elif summary:
            content=summary

        if not content or not content.strip():
            content=' '

        m=re.search(string=article_url,pattern=r'/(\d+)$')
        article_id=m.group(1) if m else article_url.split('/')[-1]

        return self.build_article_dataframe(
            article_id=article_id, title=title, created_at=created_at,
            content=content, author=author, category=category,
            keyword=keyword, article_url=article_url, img=img)

