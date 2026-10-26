import requests
from bs4 import BeautifulSoup
import re
import datetime
import pandas as pd
import json
from urllib.parse import urlparse
from news_ch.news_crawler.base_process import *


def _news_article_nodes(payload):
    pending=[payload]
    while pending:
        node=pending.pop()
        if isinstance(node,dict):
            article_type=node.get('@type')
            is_article_type=(
                article_type in {'NewsArticle','Article'}
                if isinstance(article_type,str)
                else isinstance(article_type,list)
                and any(value in {'NewsArticle','Article'} for value in article_type)
            )
            if is_article_type:
                yield node
            pending.extend(
                value for value in node.values()
                if isinstance(value,(dict,list))
            )
        elif isinstance(node,list):
            pending.extend(node)


def _published_time(value):
    if not value:
        return None
    try:
        parsed_at=datetime.datetime.fromisoformat(
            str(value).strip().replace('Z','+00:00')
        )
        if parsed_at.tzinfo:
            parsed_at=parsed_at.astimezone(
                datetime.timezone(datetime.timedelta(hours=8))
            ).replace(tzinfo=None)
        return parsed_at.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None

class TVBS(NewsLogic):
    _source='TVBS'
    _timeout=30
    _domain_url='https://news.tvbs.com.tw'
    _date=datetime.datetime.now().strftime('%Y-%m-%d')
    
    def get_article_url_from(self):
        self._headers['Referer']=self._domain_url
        
        article_url=[] ; category=[] ; img=[]

        def empty_result():
            return pd.DataFrame({
                'source': [],
                'article_url': [],
                'category': [],
                'created_at': [],
                'img': [],
                'keyword': [],
            })

        try:
            response=requests.get(
                f'{self._domain_url}/realtime',
                headers=self._headers,timeout=self._timeout,
            )
            response.raise_for_status()
        except requests.RequestException as e:
            print(f'[TVBS] list page request failed; skipping source | {type(e).__name__}: {e}')
            return empty_result()

        soup=BeautifulSoup(response.content, 'lxml')

        seen=set()
        rows=[]
        for script in soup.find_all('script',{'type':'application/ld+json'}):
            try:
                payload=json.loads(script.string or script.get_text())
            except (TypeError,ValueError):
                continue
            for article in _news_article_nodes(payload):
                url=str(article.get('url') or '').strip()
                parsed_url=urlparse(url)
                if (
                    parsed_url.scheme!='https'
                    or parsed_url.netloc!='news.tvbs.com.tw'
                    or not re.search(r'/\d+$',parsed_url.path)
                    or url in seen
                ):
                    continue
                seen.add(url)
                article_url.append(url)
                path_parts=[part for part in parsed_url.path.split('/') if part]
                category.append(path_parts[0] if len(path_parts)>1 else '')
                image=article.get('image')
                if isinstance(image,list):
                    image=image[0] if image else ''
                elif isinstance(image,dict):
                    image=image.get('url') or image.get('contentUrl') or ''
                img.append(str(image or ''))
                published_at=_published_time(
                    article.get('datePublished') or article.get('dateCreated')
                )
                rows.append({
                    'source':self._source,
                    'article_url':url,
                    'category':category[-1],
                    'created_at':published_at or '',
                    'img':img[-1],
                    'keyword':'',
                })

        if not rows:
            print('[TVBS] realtime page contained no NewsArticle JSON-LD entries')
        TEMP=pd.DataFrame(rows,columns=[
            'source','article_url','category','created_at','img','keyword'
        ])
        return TEMP.iloc[:200,:]


    def get_article_info(self,article_url,tim,img,keyword,category):
        self._headers['Referer']=self._domain_url
        
        a=requests.get(article_url,headers=self._headers,timeout=self._timeout)
        a.raise_for_status()
        soup=BeautifulSoup(a.content,'lxml')

        json_data=None
        for script in soup.find_all('script',{'type':'application/ld+json'}):
            try:
                payload=json.loads(script.string or script.get_text())
            except (TypeError,ValueError):
                continue
            json_data=next(_news_article_nodes(payload),None)
            if json_data:
                break
        json_data=json_data or {}
        
        try:
            title=(json_data.get('headline') or soup.find('h1').get_text(' ',strip=True)).removesuffix('│TVBS新聞網').strip()
            title=re.sub(string=title,pattern=r'^\s+|\s+$|\u3000|\xa0',repl='')
        except Exception:
            print(f'[中文新聞] 缺少 title（頁面改版或選擇器失效）| url={article_url}')
            title=''
        
        try:
            created_at=_published_time(
                json_data.get('datePublished') or json_data.get('dateCreated')
            )
            if not created_at:
                raise ValueError('published time is missing or invalid')
        except Exception:
            print(f'[中文新聞] 缺少 created_at | url={article_url}')
            created_at=None

        try:
            author_value=json_data.get('author')
            if isinstance(author_value,list):
                author_value=author_value[0] if author_value else None
            author=(
                author_value.get('name','')
                if isinstance(author_value,dict) else str(author_value or '')
            ).strip()
            if not author:
                raise ValueError('author is missing')
        except Exception:
            print(f'[中文新聞] 缺少 author | url={article_url}')
            author=''

        try:
            content=json_data.get('articleBody')
            if not content:
                article_body=soup.select_one('.article-editor-content')
                content=(
                    '\n'.join(
                        paragraph.get_text(' ',strip=True)
                        for paragraph in article_body.find_all('p')
                        if paragraph.get_text(' ',strip=True)
                    )
                    if article_body else ''
                )
            content=str(content or '').strip()
            if not content:
                raise ValueError('article body is missing')
        except Exception:
            print(f'[中文新聞] 缺少 content | url={article_url}')
            content=' '    

        try:
            keywords=json_data.get('keywords') or []
            keyword=';'.join(str(item) for item in keywords) if isinstance(keywords,list) else str(keywords)
        except Exception:
            print(f'[中文新聞] 缺少 keyword | url={article_url}')
            keyword=''

        match=re.search(string=article_url,pattern=r'/(\d+)$')
        article_id=match.group(1) if match else article_url

        return self.build_article_dataframe(
            article_id=article_id, title=title, created_at=created_at,
            content=content, author=author, category=category,
            keyword=keyword, article_url=article_url, img=img)

