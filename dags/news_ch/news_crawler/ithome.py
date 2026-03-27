import requests
from bs4 import BeautifulSoup
import re
import datetime
import pandas as pd
from itertools import chain
from news_ch.news_crawler.base_process import *

class iThome(NewsLogic):
    _source='iThome'
    _domain_url='https://www.ithome.com.tw'
    _page=5
    
    def get_article_url_from(self):
        article_url=[];keyword=[]
        
        for page_num in range(0,self._page):
            b=requests.get(f'{self._domain_url}/news?page={page_num}',
                           headers=self._headers,timeout=self._timeout)
            soup=BeautifulSoup(b.text,'lxml')    
            
            article_url.append([self._domain_url+s.find('a').get('href') for s in soup.find_all('div',class_='item')])
            
            keyword.append([s.find(class_='category').text for s in soup.find_all('div',class_='item')])
        
        article_url=list(chain.from_iterable(article_url))  
        keyword=list(chain.from_iterable(keyword))  
        
        W=[]
        for w in keyword:
            temp=[]
            for split_w in w.split('|'):
                split_w=re.sub(string=split_w,pattern=r'^\s+|\s+$',repl='')
                temp.append(split_w)
                
            W.append(';'.join(temp))   
        
        TEMP=pd.DataFrame({'source':[self._source]*len(article_url),
                           'article_url':article_url,
                           'category':' ',
                           'created_at':' ',
                           'img':' ',
                           'keyword':W})
        
        return TEMP

    def get_article_info(self,article_url,tim,img,keyword,category):
        a=requests.get(article_url,headers=self._headers,timeout=self._timeout)
        soup=BeautifulSoup(a.text,'lxml')
        
        try:
            title=soup.find('h1').text
            title=re.sub(string=title,pattern=r'^\s+|\s+$|\u3000|\xa0',repl='')
        except Exception:
            print(f'[中文新聞] 缺少 title（頁面改版或選擇器失效）| url={article_url}')
            title=' '
        
        try:
            created_at=soup.find(class_='submitted').find(class_='created').text
            created_at=format(datetime.datetime.strptime(created_at,'%Y-%m-%d'),'%Y-%m-%d %H:%M:%S')
        except Exception:
            print(f'[中文新聞] 時間欄位無法解析 | url={article_url}')
            created_at=None
        
        try:
            author=soup.find(class_='submitted').find(class_='author').text.strip()
        except Exception:
            print(f'[中文新聞] 缺少 author | url={article_url}')
            author=' '
        
        try:
            img=soup.find(class_='img-wrapper').find('img').get('src')
        except Exception:
            img=' '

        try:
            bc=soup.find(class_='breadcrumb')
            if bc:
                links=bc.find_all('a')
                if len(links)>1:
                    category=links[-1].text.strip()
                elif links:
                    category=links[0].text.strip()
            if not category or category==' ':
                cat_el=soup.select_one('.article-type a, .field-name-field-category a')
                if cat_el:
                    category=cat_el.text.strip()
        except Exception:
            pass
        if not category or category==' ':
            category=keyword.split(';')[0] if keyword and keyword!=' ' else ' '
       
        for s in soup.find_all(re.compile('img|figure')):
            s.decompose()
        
        try:
            content=''
            temp=soup.find(class_='field field-name-body field-type-text-with-summary field-label-hidden').find(class_='field-item even').find_all(re.compile('^(p|h3|h2|ul)'))
            for s in temp:
                if bool(re.search(string=s.text,pattern=r'^圖片來源')):
                    content=content
                else:
                    content=content+re.sub(string=s.text,pattern=r'更多內容$',repl='')+'\n'
            
            content=content.strip()
        except Exception:
            print(f'[中文新聞] 缺少 content | url={article_url}')
            content=' '    
        
        article_id=re.search(string=article_url,pattern=r'/(\d+)$').group(1)
        
        return self.build_article_dataframe(
            article_id=article_id, title=title, created_at=created_at,
            content=content, author=author, category=category,
            keyword=keyword, article_url=article_url, img=img)
    
