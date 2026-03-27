import requests
from bs4 import BeautifulSoup
import re
import datetime
import pandas as pd
import time
from news_ch.news_crawler.base_process import *

class Apple(NewsLogic):
    _source='蘋果日報'
    _domain_url=f'https://tw.appledaily.com'
    _timeout=30
    
    def get_article_url_from(self):
        TEMP=[]
        for page_num in range(self._page):
            a=requests.get(f'{self._domain_url}/realtime/new/{page_num}',
                           headers=self._headers,
                           timeout=self._timeout)
            soup=BeautifulSoup(a.text,'lxml')    
            
            #解析
            article_url=[];created_at=[];img=[]

            for part in soup.find(class_='stories-container').find_all(class_='story-card'):        
                img_url=part.find('img').get('data-src')

                a_u=f'{part.get("href")}'
                if not bool(re.search(string=a_u,pattern=r'^http')):
                    a_u=f'{self._domain_url}/{a_u}'

                t=part.find(class_='timestamp').text.split('：')[1]
                t=format(datetime.datetime.strptime(t,'%Y/%m/%d %H:%M'),'%Y-%m-%d %H:%M:%S')

                article_url.append(a_u)
                img.append(img_url)
                created_at.append(t)

            TEMP.append(
                pd.DataFrame({
                    'created_at':created_at,
                    'category':' ',
                    'article_url':article_url,
                    'source':self._source,
                    'keyword':' ',
                    'img':img,
                })
            )

            time.sleep(2)

        return pd.concat(TEMP)
    
    def get_article_info(self,article_url,tim,img,keyword,category):
        a=requests.get(article_url,headers=self._headers,timeout=self._timeout)
        soup=BeautifulSoup(a.text,'lxml')
        
        #title
        try:
            title=soup.find('h1').text.strip()
        except Exception:
            print(f'[中文新聞] 缺少 title（頁面改版或選擇器失效）| url={article_url}')
            title=' '
        
        #keyword
        try:
            keyword=[s.text.strip() for s in soup.find(class_='tags-container').find_all('a')]
            keyword=';'.join(keyword)
        except Exception:
            print(f'[中文新聞] 缺少 keyword | url={article_url}')
            keyword=' '

        #category
        try:
            category=soup.find(class_='section-name-container-underscore').text.strip()
        except Exception:
            print(f'[中文新聞] 缺少 category | url={article_url}')
            category=' '

        created_at=tim

        #內文
        try:
            #將網址刪除
            for u in soup.find_all(re.compile('figure|img')):
                u.decompose()

            content=soup.find('div',{'id':'articleBody','class':'article_body'}).find('section').find_next('section').find_all('p',recursive=False)

            content='\n'.join([s.text.strip() for s in content])

            content='\n'.join(content.split('熱門新聞：')).strip()
        except Exception:
            print(f'[中文新聞] 缺少 content | url={article_url}')
            content=' '

        try:
            author=re.search(string=content,pattern=r'（.*）')

            if author:
                author=author.group(0)
                author=re.sub(string='（|）',repl='')
            else:
                author=' '

            content=content.replace(author,'').strip()
        except Exception:
            print(f'[中文新聞] 缺少 author | url={article_url}')
            author=' ' 

        #article_id
        article_id=article_url.split('/')[-1]

        return self.build_article_dataframe(
            article_id=article_id, title=title, created_at=created_at,
            content=content, author=author, category=category,
            keyword=keyword, article_url=article_url, img=img)

