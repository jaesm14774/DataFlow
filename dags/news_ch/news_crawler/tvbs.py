import requests
from bs4 import BeautifulSoup
import re
import datetime
import pandas as pd
import json
from news_ch.news_crawler.base_process import *

class TVBS(NewsLogic):
    _source='TVBS'
    _timeout=30
    _domain_url='https://news.tvbs.com.tw'
    _date=datetime.datetime.now().strftime('%Y-%m-%d')
    
    def get_article_url_from(self):
        self._headers['Referer']=self._domain_url
        
        article_url=[] ; category=[] ; img=[]

        response=requests.get(f'{self._domain_url}/realtime/news/{self._date}',
                   headers=self._headers,timeout=self._timeout)
        soup=BeautifulSoup(response.text, 'lxml')
        
        section=soup.find(class_='news_list_menu').find_next(class_='list').find_all('li')
        
        for part in section:
            try:
                # 提取超連結
                try:
                    url=self._domain_url + part.find('a')['href'].strip()
                    article_url.append(url)
                except Exception:
                    continue

                # 提取類別
                try:
                    category_tmp=part.find('div', class_='type').text.strip()
                except Exception:
                    category_tmp=''
                
                category.append(category_tmp)

                # 提取圖片 URL
                try:
                    img_tmp=part.find('img', class_='lazyimage')['data-original']
                except Exception:
                    img_tmp=''
                
                img.append(img_tmp)
            except Exception as e:
                print(f'[中文新聞] 列表區塊解析失敗 | 來源={self._source} | {type(e).__name__}: {e}')

        TEMP=pd.DataFrame({'source':[self._source]*len(article_url),
                           'article_url':article_url,
                           'category':category,
                           'created_at':['']*len(article_url),
                           'img':img,
                           'keyword':['']*len(article_url)})
        
        if len(TEMP)> 200:
            TEMP=TEMP.iloc[:200,:]
        
        return TEMP


    def get_article_info(self,article_url,tim,img,keyword,category):
        self._headers['Referer']=self._domain_url
        
        a=requests.get(article_url,headers=self._headers,timeout=self._timeout)
        soup=BeautifulSoup(a.text,'lxml')

        json_str=soup.find('script', {'type':'application/ld+json'}).text.strip()
        json_str=re.sub(string=json_str, pattern=r'\n\s+|\s+\n', repl='')

        json_data=json.loads(json_str)
        
        try:
            title=json_data['headline'].strip('│TVBS新聞網')
            title=re.sub(string=title,pattern=r'^\s+|\s+$|\u3000|\xa0',repl='')
        except Exception:
            print(f'[中文新聞] 缺少 title（頁面改版或選擇器失效）| url={article_url}')
            title=''
        
        try:
            created_at=datetime.datetime.strptime(json_data['dateCreated'], '%Y-%m-%dT%H:%M:%S+08:00').strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            print(f'[中文新聞] 缺少 created_at | url={article_url}')
            created_at=None

        try:
            author=json_data['author']['name'].strip()
        except Exception:
            print(f'[中文新聞] 缺少 author | url={article_url}')
            author=''

        try:
            content=json_data['articleBody'].strip()
        except Exception:
            print(f'[中文新聞] 缺少 content | url={article_url}')
            content=' '    

        try:
            keyword=';'.join(json_data['keywords'])
        except Exception:
            print(f'[中文新聞] 缺少 keyword | url={article_url}')
            keyword=''

        article_id=re.search(string=article_url,pattern=r'/(\d+)$').group(1)

        return self.build_article_dataframe(
            article_id=article_id, title=title, created_at=created_at,
            content=content, author=author, category=category,
            keyword=keyword, article_url=article_url, img=img)

