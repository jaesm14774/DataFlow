import requests
from bs4 import BeautifulSoup
import re
import datetime
import pandas as pd
import json
from news_ch.news_crawler.base_process import *

class SETN(NewsLogic):
    _source='三立新聞'
    _domain_url='https://www.setn.com'
    _page=4
    
    def get_article_url_from(self):
        TEMP=[]
        for page_num in range(1,self._page+1):
            a=requests.get(f'{self._domain_url}/ViewAll.aspx?p={page_num}',
                           headers=self._headers,timeout=self._timeout)
            soup=BeautifulSoup(a.text,'lxml')
            
            title=[];url=[];category=[]
            #娛樂網址會特地轉換呈現方式，將其轉回原本格式，以免格式不依
            for it in soup.find_all(class_='newsItems'):
                try:
                    u=it.find(class_='view-li-title').find('a').get('href')
                    ti=it.find(class_='view-li-title').text
                    ca=it.find('div',class_='newslabel-tab').text
                except Exception:
                    u=it.find(class_='news-word').find('a').get('href')
                    ti=it.find(class_='news-word').text
                    ca=it.find(class_='news-label').text
                
                ti=re.sub(string=ti,pattern=r'^\s+|\s+$',repl='')

                if bool(re.search(string=u,pattern=r'^https://')):
                    try:
                        article_id=re.search(string=u,pattern=r'(news|News)/(\d+)\?').group(2)
                        url.append(f'{self._domain_url}/News.aspx?NewsID={article_id}&utm_source=setn.com&utm_medium=viewall&utm_campaign=viewallnews')
                    except Exception:
                        print(f'[中文新聞-SETN] 無法從列表 URL 解析 NewsID，略過 | url={u!r}')
                        continue
                else:
                    url.append('https://www.setn.com'+u)
                title.append(ti)
                category.append(ca)
            
            TEMP.append(pd.DataFrame({
                'category':category,
                'article_url':url,
                'img':' ',
                'keyword':' ',
                'created_at':' ',
                'source':self._source,
            }))
        
        TEMP=pd.concat(TEMP)
        
        return TEMP[~TEMP.duplicated(subset=['article_url'])]

    def get_article_info(self,article_url,tim,img,keyword,category):
        self._headers['Referer']=self._domain_url
        
        a=requests.get(article_url,headers=self._headers,timeout=self._timeout)
        soup=BeautifulSoup(a.text,'lxml')
        
        #title
        title=soup.find('h1').text.strip()
        title=re.sub(string=title,pattern='\u3000',repl='    ')
        
        #keyword,category,img,created_at
        temp=str(soup)
        temp=temp[temp.find('<script type="application/ld+json">')+36:]
        temp=temp[:temp.find('</script>')]
        temp=temp.strip()
        temp=temp.replace('\r\n','').replace('  ','')
        temp=json.loads(temp[:temp.find(',"description')]+'}')

        keyword=';'.join(temp['keywords'])

        if category is None:
            category=temp['articleSection']
        
        img=temp['image']['url']

        try:
            created_at=temp['datePublished']
            created_at=format(datetime.datetime.strptime(created_at,'%Y-%m-%dT%H:%M:%S+00:00'),'%Y-%m-%d %H:%M:%S')
        except Exception:
            created_at=None
            print(f'[中文新聞-SETN] JSON-LD 時間解析失敗 | url={article_url}')

        #content,author
        temp=soup.find('div',{'id':'Content1'}).find_all('p')

        #找尋作者
        if soup.find(class_='reporter') is not None:
            if soup.find(class_='reporter').text == '中央社':
                author='中央社'
            else:
                author=''
        else:
            author=''

        if author == '中央社':
            content=[re.sub(string=s.text,pattern=r'^\s+|\s+$',repl='') for s in temp]
            content=[s for s in content if not bool(re.search(string=s,pattern=r'^(▲|▼|►)'))]
            content='\n'.join(content)        
        else:
            author=temp[0].text
            if len(author)>25: #代表第一行非作者
                author=' '
                content=[re.sub(string=s.text,pattern=r'^\s+|\s+$',repl='') for s in temp]
                content=[s for s in content if not bool(re.search(string=s,pattern=r'^(▲|▼|►)'))]
                content='\n'.join(content)        
            else:
                content=[re.sub(string=s.text,pattern=r'^\s+|\s+$',repl='') for s in temp[1:]]
                content=[s for s in content if not bool(re.search(string=s,pattern=r'^(▲|▼|►)'))]
                content='\n'.join(content) 

        article_id=re.search(string=article_url,pattern=r'NewsID=(\d+)').group(1)

        return self.build_article_dataframe(
            article_id=article_id, title=title, created_at=created_at,
            content=content, author=author, category=category,
            keyword=keyword, article_url=article_url, img=img)

