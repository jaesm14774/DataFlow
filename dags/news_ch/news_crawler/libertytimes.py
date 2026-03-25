import requests
from bs4 import BeautifulSoup
import re
import datetime
import pandas as pd
from news_ch.news_crawler.base_process import *

class LibertyTimes(NewsLogic):
    _source='自由時報'
    _domain_url='https://news.ltn.com.tw'
    _page=5
    
    def get_article_url_from(self):
        TEMP=[]
        for page_num in range(1,self._page+1):
            #第一頁資料沒有編號，其他頁a['data']後不是list，還是json檔，key是吐出資料的序號
            response=requests.get(f'{self._domain_url}/ajax/breakingnews/all/{page_num}',
                          headers=self._headers,timeout=self._timeout)
            response.encoding='utf-8-sig'
            a=response.json()
            
            if isinstance(a['data'],list):
                TEMP.append(pd.DataFrame(a['data']))
            elif isinstance(a['data'],dict):
                rs=[]
                for k in a['data'].keys():
                    rs.append(a['data'][k])
                TEMP.append(pd.DataFrame(rs))
            else:
                raise RuntimeError('Strange data format received from LTN!')
        
        TEMP=pd.concat(TEMP)
        #取需要的變數
        TEMP=TEMP.loc[:,['title','photo_S','url','tagText']]
        TEMP.columns=['title','img','article_url','category']
        TEMP['source']=self._source
        TEMP['created_at']=' '
        TEMP['keyword']=' '
        TEMP.drop(['title'],axis=1,inplace=True)
        
        return TEMP

    def get_article_info(self,article_url,tim,img,keyword,category):
        b=requests.get(article_url,headers=self._headers,timeout=self._timeout)
        soup=BeautifulSoup(b.text,'lxml')
        
        try:
            title=soup.find(re.compile('h1|title')).text.strip()
        except Exception:
            title=' '
            print(f'[中文新聞] 缺少 title（頁面改版或選擇器失效）| url={article_url}')
        
        try:
            #created_at
            created_at=[s.text.strip() for s in soup.find_all(class_='time')]
            created_at=[s for s in created_at if s != ''][0]

            created_at=re.search(string=created_at,pattern=r'\d+(/|-)\d+(/|-)\d+ \d+:\d+').group(0)
            created_at=re.sub(string=created_at,pattern='/',repl='-')
            created_at=format(datetime.datetime.strptime(created_at,'%Y-%m-%d %H:%M'),'%Y-%m-%d %H:%M:%S')
        except Exception:
            print(f'[中文新聞] 缺少 created_at | url={article_url}')
            created_at=None

        try:
            img=soup.find('div',class_='photo boxTitle').find('img').get('src')
        except Exception:
            try:
                img=soup.find('span',class_='ph_i').find('img').get('data-original')
                if bool(re.search(string=img,pattern=r'^//')):
                    img='https:'+img
            except Exception:
                img=' '       

        for i in soup.find_all('img'):
            i.decompose()
        for i in soup.find_all(class_='ph_b ph_d1'):
            i.decompose()

        temp=soup.find('div',{'data-desc':re.compile('內文|內容頁')}).find_all('p',recursive=False)

        if len(temp) == 0:
            try:
                temp=soup.find('div',{'data-desc':re.compile('內文|內容頁')}).find(class_='text').find_all('p',recursive=False)
            except Exception:
                temp=soup.find_all(class_='text')
                temp=[s for s in temp if s.text.strip() != ''][0]
                temp=temp.find_all('p',recursive=False)        

        temp=[s.text for s in temp if not bool(re.search(string=str(s),pattern=r'before_ir|after_ir|appE1121'))]
        temp=[s for s in temp if s !='']
        content='\n'.join(temp)
        
        try:
            author=re.search(string=content,pattern=r'^(〔|［).{0,30}(〕|］)').group(0)
            content=re.sub(string=content,pattern=author,repl='')
            content=re.sub(string=content,pattern=r'^\s+|\s+$',repl='')
        except Exception:
            try:
                author=re.sub(string=soup.find(class_='author').text,pattern=r'^\s+|\s+$',repl='')
            except Exception:
                author=' '
            print(f'[中文新聞] 缺少 content | url={article_url}')
            content=' '  

        article_id=re.search(string=article_url,pattern=r'/(\d+)$').group(1)

        return self.build_article_dataframe(
            article_id=article_id, title=title, created_at=created_at,
            content=content, author=author, category=category,
            keyword=keyword, article_url=article_url, img=img)
        
