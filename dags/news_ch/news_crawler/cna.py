import requests
from bs4 import BeautifulSoup
import re
import datetime
import pandas as pd
from news_ch.news_crawler.base_process import *

class CNA(NewsLogic):
    _source='中央社'
    _page=1
    
    def get_article_url_from(self):
        self._headers['Referer']='https://www.cna.com.tw/list/aall.aspx'

        a=requests.get('https://www.cna.com.tw/list/aall.aspx',headers=self._headers,timeout=self._timeout)
        soup=BeautifulSoup(a.text,'lxml')
        
        article_list=soup.find('ul',{'id':'jsMainList'}).find_all('li')
        
        tim=[part.find(class_='date').text.replace('/','-') for part in article_list]
        tag=''
        article_url=[part.find('a').get('href') for part in article_list]
        
        img=[]
        for part in article_list:
            if part.find('img') is None:
                img.append('')
            elif part.find('img').get('src') is None:
                img.append(part.find('img').get('data-src'))
            else:
                img.append(part.find('img').get('src'))

        TEMP=pd.DataFrame({'source':[self._source]*len(article_url),'article_url':article_url,
                           'category':tag,'created_at':tim,'img':img,'keyword':''})
        return TEMP

    def get_article_info(self,article_url,tim,img,keyword,category):
        self._headers['Referer']='https://www.google.com.tw/?hl=zh_TW'
        
        b=requests.get(article_url,headers=self._headers,timeout=self._timeout)
        soup=BeautifulSoup(b.text,'lxml')
        
        try:
            title=soup.find('h1').text
        except Exception:
            title=' '
            print(f'[中文新聞] 缺少 title（頁面改版或選擇器失效）| url={article_url}')       

        created_at=tim
        
        category=soup.find(class_='breadcrumb').find_all('a')[-1].text.strip()
        
        try:
            content=''
            for s in soup.find('div',class_='paragraph').find_all('p',recursive=False):
                content=content+'\n'+s.text
                content=re.sub(string=content,pattern=r'^\s+|\s+$',repl='')
            if content == '':
                content=' '
                print(f'[中文新聞] 缺少 content | url={article_url}')
        except Exception:
            try:
                content=''
                for s in soup.find('article').find(class_="wrapper").find_all('p',recursive=False):
                    content=content+'\n'+s.text
                    content=re.sub(string=content,pattern=r'^\s+|\s+$',repl='')
                if content == '':
                    content=' '
                    print(f'[中文新聞] 缺少 content | url={article_url}')
            except Exception:
                content=' '
                print(f'[中文新聞] 缺少 content | url={article_url}')
        
        try:
            author=re.search(string=content,pattern=r'^〔[^〕]+〕|^［[^］]+］|^（[^）]+）').group(0)
            content=re.sub(pattern=author,repl='',string=content)
        except Exception:
            try:
                author=soup.find(class_='author').text
                author=author.split('／')[0]
                author=re.sub(string=author,pattern=r'中央社|^\s+|\s+$',repl='')
            except Exception:
                print(f'[中文新聞] 缺少 author | url={article_url}')
                author=' '  
        
        keyword=';'.join([k.text.replace('#','').strip() for k in soup.find_all(class_='keywordTag')])
        
        article_id=re.search(string=article_url,pattern=r'(\d+)(\.aspx)*$').group(1)
        
        return self.build_article_dataframe(
            article_id=article_id, title=title, created_at=created_at,
            content=content, author=author, category=category,
            keyword=keyword, article_url=article_url, img=img)
        
