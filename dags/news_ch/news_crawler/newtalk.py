import requests
from bs4 import BeautifulSoup
import re
import datetime
import pandas as pd
import json
from urllib.parse import urljoin
from news_ch.news_crawler.base_process import *

class NewTalk(NewsLogic):
    _source='newtalk'
    _domain_url='https://newtalk.tw'
    
    def get_article_url_from(self):
        taipei_tz=datetime.timezone(datetime.timedelta(hours=8))
        list_date=datetime.datetime.now(taipei_tz).strftime('%Y-%m-%d')
        response=requests.get(
            f'{self._domain_url}/news/summary/{list_date}/1',
            headers=self._headers,timeout=self._timeout,
        )
        response.raise_for_status()
        response.encoding='utf8'
        soup=BeautifulSoup(response.text,'lxml')

        rows=[]
        seen=set()
        for a_tag in soup.select('a[href*="/news/view/"]'):
            href=urljoin(self._domain_url,a_tag.get('href','').strip())
            if not re.search(r'^https://newtalk\.tw/news/view/\d{4}-\d{2}-\d{2}/\d+$',href):
                continue
            if href in seen:
                continue
            seen.add(href)
            card=a_tag.find_parent('article') or a_tag.parent
            img_el=card.find('img') if card else None
            category_el=card.select_one('.type,.category') if card else None
            rows.append({
                'source':self._source,
                'article_url':href,
                'category':category_el.get_text(' ',strip=True) if category_el else ' ',
                # The current list page does not carry a dependable article time.
                # get_article_info reads the article's published-time metadata.
                'created_at':'',
                'img':(
                    img_el.get('data-src') or img_el.get('src') or ''
                    if img_el else ''
                ),
                'keyword':' ',
            })
        return pd.DataFrame(rows,columns=[
            'source','article_url','category','created_at','img','keyword'
        ])

    def get_article_info(self,article_url,tim,img,keyword,category):
        b=requests.get(article_url,headers=self._headers,timeout=self._timeout)
        b.encoding='utf8'
        soup=BeautifulSoup(b.text,'lxml')

        try:
            title=soup.find('h1').text
        except Exception:
            title=' '
            print(f'[中文新聞] 缺少 title（頁面改版或選擇器失效）| url={article_url}')     

        published_at=None
        time_tag=soup.find('time')
        if time_tag:
            published_at=time_tag.get('datetime') or time_tag.get_text(' ',strip=True)
        if not published_at:
            for script in soup.find_all('script',{'type':'application/ld+json'}):
                try:
                    payload=json.loads(script.string or script.get_text())
                except (TypeError,ValueError):
                    continue
                pending=[payload]
                while pending and not published_at:
                    node=pending.pop()
                    if isinstance(node,dict):
                        published_at=node.get('datePublished')
                        pending.extend(
                            value for value in node.values()
                            if isinstance(value,(dict,list))
                        )
                    elif isinstance(node,list):
                        pending.extend(node)

        created_at=None
        if published_at:
            try:
                parsed_at=datetime.datetime.fromisoformat(
                    str(published_at).strip().replace('Z','+00:00')
                )
                if parsed_at.tzinfo:
                    parsed_at=parsed_at.astimezone(
                        datetime.timezone(datetime.timedelta(hours=8))
                    ).replace(tzinfo=None)
                created_at=parsed_at.strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                created_at=None
        if not created_at:
            print(f'[中文新聞] 缺少有效 published_at | url={article_url}')

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

