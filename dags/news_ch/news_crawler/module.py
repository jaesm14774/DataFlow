import requests
from bs4 import BeautifulSoup
import datetime
import pandas as pd
from itertools import chain
import re
import random
import json
import time
import cloudscraper
from news_ch.news_crawler.base_process import *

class TheNewsLens(NewsLogic):
    _source='thenewslens'
    _timeout=30
    _domain_url='https://www.thenewslens.com'
    
    def get_article_url_from(self):
        self._headers['Referer']=self._domain_url
        
        article_url=[];keyword=[];created_at=[];img=[]

        for page_num in range(1,self._page+1):
            b=requests.get(f'{self._domain_url}/news?page={page_num}',
                           headers=self._headers,timeout=self._timeout)
            soup=BeautifulSoup(b.text,'lxml')    

            article_url.append([s.find('a').get('href') for s in soup.find_all('h2',class_='title')])

            for s in soup.find_all('div',class_='col-sm-8'):
                temp_keyword=[]

                try:
                    for part in s.find(class_='tags-box').find_all('a',{'data-gtm-label':'tag'}):
                        temp_keyword.extend([part.text.strip()])
                except:
                    temp_keyword=[' ']

                keyword.append([';'.join(temp_keyword)])

            created_at.append([s.find(class_='time').text.split('|')[0].strip().replace('/','-') for s in soup.find_all(class_='info-box')])

            img.append([s.find('img').get('data-srcset').split(', ')[-1].strip() for s in soup.find_all(class_='img-box')])

        article_url=list(chain.from_iterable(article_url))  
        keyword=list(chain.from_iterable(keyword))
        created_at=list(chain.from_iterable(created_at))
        img=list(chain.from_iterable(img))

        TEMP=pd.DataFrame({'source':[self._source]*len(article_url),
                           'article_url':article_url,
                           'category':' ',
                           'created_at':created_at,
                           'img':img,
                           'keyword':keyword})   

        return TEMP


    def get_article_info(self,article_url,tim,img,keyword,category):
        self._headers['Referer']=self._domain_url
        
        a=requests.get(article_url,headers=self._headers,timeout=self._timeout)
        soup=BeautifulSoup(a.text,'lxml')

        try:
            title=soup.find('h1').text
            title=re.sub(string=title,pattern=r'^\s+|\s+$|\u3000|\xa0',repl='')
        except:
            print('Can not find title for url: '+article_url)
            title=' '

        try:
            category=soup.find(class_=re.compile('article.*-box')).find(class_=re.compile('d-inline|cate')).text
            category=category.strip()
        except:
            print('Can not find category for url: '+article_url)
            category=' '

        try:
            tim=datetime.datetime.strptime(tim,'%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S')

            created_at=tim
        except:
            print('Can not find dt_publish for url: '+article_url)
            created_at=None

        try:
            author=soup.find(class_=re.compile('author-name')).text.strip()
        except:
            print('Can not find author for url: '+article_url)
            author=' '

        for s in soup.find_all(re.compile('img|figure|script')):
            s.decompose()

        try:
            summary=soup.find(class_='WhyNeedKnow').text
        except:
            summary=''

        try:
            content='\n'.join([s.text for s in soup.find(class_='article-content').find_all('p')])
        except:
            print('Can not find content for url: '+article_url)
            content=' '    

        content=summary+'\n\n\n\n\n'+content

        keyword=keyword

        article_id=re.search(string=article_url,pattern=r'/(\d+)$').group(1)

        return pd.DataFrame({
            'article_id':article_id,
            'title':title,
            'created_at':created_at,
            'create_time':self._now_time,
            'content':content,
            'author':author,
            'category':category,
            'keyword':keyword,
            'article_url':article_url,
            'img':img,
            'source':self._source,
        },index=[1]) 


class Anue(NewsLogic):
    _source='鉅亨網'
    _page=5
    
    def extract_img(self,item):
        try:
            result=item['l']['src']
        except:
            result=' '
        
        return result
    
    def get_article_url_from(self):
        u=(
            f'https://api.cnyes.com/media/api/v1/newslist/category/headline?limit=30'
        )
        a=requests.get(u,headers=self._headers,timeout=self._timeout)
        a=a.json()
        
        total_page=a['items']['last_page']
        
        url=[];Time=[];img=[]
        
        for page_num in range(1,self._page+1):
            u=(
                f'https://api.cnyes.com/media/api/v1/newslist/category/headline?'
                f'limit=30&page={page_num}'
            )
            
            a=requests.get(u,headers=self._headers,timeout=self._timeout)           
            a=a.json()
            dat=pd.DataFrame(a['items']['data'])
            
            if len(dat) == 0:
                continue
            
            url.append(['https://news.cnyes.com/news/id/%s?exp=a' % ss for ss in dat.newsId])
            Time.append([format(datetime.datetime.fromtimestamp(t),'%Y-%m-%d %H:%M:%S') for t in dat.publishAt])
            img.append(dat.coverSrc.map(self.extract_img).tolist())
            time.sleep(3)

        url=list(chain.from_iterable(url))
        img=list(chain.from_iterable(img))
        Time=list(chain.from_iterable(Time))
        tag=''
        
        TEMP=pd.DataFrame({'source':self._source,
                           'article_url':url,
                           'category':tag,
                           'created_at':Time,
                            'img':img,
                           'keyword':' '}
                         )      
        return TEMP
    
    def get_article_info(self,article_url,tim,img,keyword,category):
        a=requests.get(article_url,headers=self._headers,timeout=self._timeout)
        soup=BeautifulSoup(a.text,'lxml')
        
        try:
            title=soup.find('h1').text
            title=re.sub(string=title,pattern=r'\u3000|\xa0|^\s+|\s+$',repl='')
        except:
            print('Can not find title for url: '+article_url)
            title=' '
        
        created_at=tim
        
        try:
            author=soup.find(class_='alr4vq1').text
            author=re.sub(string=author,pattern='鉅亨網|記者',repl='').strip()
        except:
            print('Can not find author for url: '+article_url)
            author=' '
        
        try:
            content=''
            temp=soup.find('main', {'id': 'article-container'}).find_all('p')
            for s in temp:
                content=content+s.text+'\n'
            content=re.sub(pattern=r'\n+$|^\n+',repl='',string=content)
        except:
            print('Can not find content for url: '+article_url)
            content=' '
        
        #keyword
        try:
            keyword=[s.text for s in soup.find_all(class_='t1v4wtvw')]
            keyword=';'.join(keyword)
        except:
            keyword=' '
            print('Can not find keyword for url: '+article_url)

        article_id=re.search(string=article_url,pattern=r'id/(\d+)').group(1)

        source='鉅亨網'

        try:
            category=soup.find('title').text.split('|')[1].split('-')[1].strip()
        except:
            category=''
            print('Can not find category for url: '+article_url)

        D_temp=pd.DataFrame({
            'article_id':article_id,
            'title':title,
            'created_at':created_at,
            'create_time':self._now_time,
            'content':content,
            'author':author,
            'category':category,
            'keyword':keyword,
            'article_url':article_url,
            'img':img,
            'source':source},index=[1])
        
        return(D_temp)


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
        except:
            print('Can not find title for url: '+article_url)
            title=' '
        
        #keyword
        try:
            keyword=[s.text.strip() for s in soup.find(class_='tags-container').find_all('a')]
            keyword=';'.join(keyword)
        except:
            print('Can not find keyword for url: '+article_url)
            keyword=' '

        #category
        try:
            category=soup.find(class_='section-name-container-underscore').text.strip()
        except:
            print('Can not find category for url: '+article_url)
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
        except:
            print('Can not find content for url: '+article_url)
            content=' '

        try:
            author=re.search(string=content,pattern=r'（.*）')

            if author:
                author=author.group(0)
                author=re.sub(string='（|）',repl='')
            else:
                author=' '

            content=content.replace(author,'').strip()
        except:
            print('Can not find author for url: '+article_url)
            author=' ' 

        #article_id
        article_id=article_url.split('/')[-1]

        return pd.DataFrame({
                    'article_id':article_id,
                    'title':title,
                    'created_at':created_at,
                    'create_time':self._now_time,
                    'content':content,
                    'author':author,
                    'category':category,
                    'keyword':keyword,
                    'article_url':article_url,
                    'img':img,
                    'source':self._source
                },index=[1])   


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

        for part in soup.find_all(class_='news-list-item clearfix'):
            #可能是影音專區 or 錯誤時間
            try:
                t=part.find(class_='news_date').text
                t1=re.search(string=t,pattern=r'\d+\.\d+\.\d+').group(0)
                t2=re.search(string=t,pattern=r'\d+:\d+').group(0)
            except:
                continue
            
            url.append(part.find('a').get('href'))

            tag=' '

            Time.append(t1+' '+t2)
            img.append(part.find('img').get('src'))

        TEMP=pd.DataFrame({'source':self._source,
                           'article_url':url,
                           'category':tag,
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
        except:
            title=' '
            print('Can not find title for url: '+article_url)     

        created_at=tim.replace('.','-')

        category=soup.find(class_='tags').find(class_='subcategory_tag').text

        try:
            keyword=';'.join([s.text for s in soup.find(class_='tags').find_all('a')[1:]])
        except:
            keyword=' '
            print('Can not find keyword for url: '+article_url) 

        try:
            author=soup.find(class_='content_reporter').text.strip()
        except:
            author=' '
            print('Can not find author for url: '+article_url) 

        #去除script，有偷藏一個在article content裡，不抓圖片解說文字
        soup.find('div',{'itemprop':'articleBody'}).find('script').decompose()

        for s in soup.find_all(class_='news_img'):
            s.decompose()

        try:
            content='\n'.join([s.text.strip() for s in soup.find('div',{'itemprop':'articleBody'}).find_all('p')])
        except:
            content=' '
            print('Can not find content for url: '+article_url)

        article_id=re.search(string=article_url,pattern=r'view/(\d+-\d+-\d+/\d+)$').group(1)

        return pd.DataFrame({
            'article_id':article_id,
            'title':title,
            'created_at':created_at,
            'create_time':self._now_time,
            'content':content,
            'author':author,
            'category':category,
            'keyword':keyword,
            'article_url':article_url,
            'img':img,
            'source':self._source,
        },index=[1])


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
                except:
                    u=it.find(class_='news-word').find('a').get('href')
                    ti=it.find(class_='news-word').text
                    ca=it.find(class_='news-label').text
                
                ti=re.sub(string=ti,pattern=r'^\s+|\s+$',repl='')

                if bool(re.search(string=u,pattern=r'^https://')):
                    try:
                        article_id=re.search(string=u,pattern=r'(news|News)/(\d+)\?').group(2)
                        url.append(f'{self._domain_url}/News.aspx?NewsID={article_id}&utm_source=setn.com&utm_medium=viewall&utm_campaign=viewallnews')
                    except:
                        print(u)
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
        except:
            created_at=None
            print(f'Error for setn created_at for url : {article_url}')

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

        return pd.DataFrame({
            'article_id':article_id,
            'title':title,
            'created_at':created_at,
            'create_time':self._now_time,
            'content':content,
            'author':author,
            'category':category,
            'keyword':keyword,
            'article_url':article_url,
            'img':img,
            'source':self._source,
        },index=[1])


class PTS(NewsLogic):
    _source='公視新聞'
    _domain_url='https://news.pts.org.tw'
    _page=3
    
    def get_article_url_from(self):
        try:
            tag=[];img=[];article_url=[]
            
            for page_num in range(1,self._page+1):
                a=requests.get(f'{self._domain_url}/dailynews?page={page_num}',headers=self._headers,
                               timeout=self._timeout)
                soup=BeautifulSoup(a.text,'lxml')    
                
                if page_num == 1:
                    #每頁的第一篇新聞
                    if soup.find(class_='breakingnews').find(class_='news-info').find('a') is None:
                        category=' '
                    else:
                        category=soup.find(class_='breakingnews').find(class_='news-info').find('a').text

                    tag.append(category)
                    img.append(soup.find(class_='breakingnews').find('img').get('src'))
                    article_url.append(soup.find('h2').find('a').get('href'))
                else:
                    for part in soup.find(class_='list-unstyled news-list').find_all('li',class_='d-flex'):
                        if part.find(class_='news-info').find('a') is None:
                            category=' '
                        else:
                            category=part.find(class_='news-info').find('a').text

                        article_url.append(part.find('figure').find('a').get('href'))
                        tag.append(category)
                        img.append(part.find('figure').find('img').get('src'))

            TEMP=pd.DataFrame({'source':[self._source]*len(article_url),
                               'article_url':article_url,
                               'category':tag,
                               'created_at':' ',
                               'img':img,
                               'keyword':' '})
            
            return TEMP
        except:
            return pd.DataFrame() 

    def get_article_info(self,article_url,tim,img,keyword,category):
        b=requests.get(article_url,headers=self._headers,timeout=self._timeout)
        soup=BeautifulSoup(b.text,'lxml')
        
        try:
            title=soup.find('h1').text
        except:
            title=' '
            print('Can not find title for url: '+article_url)
        
        try:
            author=soup.find('span',class_='article-reporter').text
            author=author.split(' / ')
            if isinstance(author,list):
                author=author[0]
            if author == '':
                author=' '
        except:
            author=' '
            print('Can not find author for url: '+article_url)     

        created_at=soup.find('time').text
        
        if img == '':
            img=' '
        
        category=category
        
        try:
            content=soup.find(class_='post-article').text
            content=re.sub(string=content,pattern='\xa0',repl='').strip()
        except:
            content=' '
            print('Can not find content for url: '+article_url)
        
        #keyword
        try:
            keyword=[s.find('a').text for s in soup.find('ul',class_='tag-list').find_all(class_='blue-tag')]
            keyword=';'.join([s for s in keyword if s !='...'])
        except:
            keyword=' '
            print('Can not find keyword for url: '+article_url)
        
        article_id=re.search(string=article_url,pattern=r'article/(\d+)$').group(1)
        
        return pd.DataFrame({
            'article_id':article_id,
            'title':title,
            'created_at':created_at,
            'create_time':self._now_time,
            'content':content,
            'author':author,
            'category':category,
            'keyword':keyword,
            'article_url':article_url,
            'img':img,
            'source':self._source,
        },index=[1])


class United(NewsLogic):
    _source='聯合報'
    _timeout=30
    _domain_url='https://udn.com'
    
    def get_article_url_from(self):
        tim=[];img=[];article_url=[]
        for page_num in range(0,self._page):
            a=requests.get(f'https://udn.com/api/more?page={page_num+1}&id=&channelId=1&cate_id=99&type=breaknews',
                           headers=self._headers,
                           timeout=self._timeout)
            a=a.json()
            a=a['lists']
            
            article_url.append([re.sub(string=self._domain_url+s['titleLink'],
                                       pattern=r'\?from=udn-ch1_breaknews-1-99-news',
                                       repl='') for s in a])
            
            img.append([s['url'] for s in a])
            
            tim.append([s['time']['date'] for s in a])
        
        tim=list(chain.from_iterable(tim))
        img=list(chain.from_iterable(img))
        article_url=list(chain.from_iterable(article_url))

        TEMP=pd.DataFrame({'source':[self._source]*len(article_url),
                           'article_url':article_url,
                           'category':' ',
                           'created_at':tim,
                           'img':img,
                           'keyword':' '})
        
        return TEMP

    def get_article_info(self,article_url,tim,img,keyword,category):
        b=requests.get(article_url,headers=self._headers,timeout=self._timeout)
        soup=BeautifulSoup(b.text,'lxml')
        
        #title
        try:
            title=soup.find('h1',class_=re.compile('title')).text.strip()
        except:
            title=[s.get('content') for s in soup.find_all('meta') if bool(re.search(string=str(s),pattern=r'og:title'))][0]
            title=title.split('|')[0]
            print('Can not find title for url: '+article_url) 

        #發表日期
        if tim is None or tim=='' or tim==' ':
            try:
                created_at=soup.find('time',class_='article-content__time').text
                created_at=created_at+':00'
            except:
                try:
                    created_at=soup.find(class_='shareBar__info--author').find('span').text
                    created_at=created_at+':00'
                except:
                    created_at=[s.get('content') for s in soup.find_all('meta') if bool(re.search(string=str(s),pattern='date.available'))][0]
        else:
            created_at=tim

        #圖片
        if img is None or img=='' or img==' ':
            try:
                img=soup.find(re.compile('picture|figure')).find('img').get('src')
                if not bool(re.search(string=img,pattern='^https*')):
                    img='https:'+img
            except:
                try:
                    img=[s.get('content') for s in soup.find_all('meta') if bool(re.search(string=str(s),pattern='taboola:image'))][0]
                except:
                    img=' '
                    print(f'Error for img for {article_url}')   

        try:
            category=[s.text for s in soup.find_all(class_='breadcrumb-items') if s.get('href') is not None][-1]
        except:
            try:
                category=soup.find(class_=re.compile('breadcrumb')).find('a').text.strip()
            except:
                category=' '
                print(f'Error for category for {article_url}')

        #author
        try:
            author=soup.find(class_='article-content__author').text.strip()
            author=re.sub(string=author,pattern=r'\s+',repl=' ')
        except:
            try:
                author=soup.find(class_='shareBar__info--author').text
                author=re.sub(string=author,pattern=created_at,repl='')
            except:
                try:
                    author=[s.get('content') for s in soup.find_all('meta') if bool(re.search(string=str(s),pattern=r'name=\"author\"/'))][0]
                except:
                    print('Can not find author for url: '+article_url)
                    author=' '    
                    
        #keyword
        try:
            keyword=';'.join([s.text.strip() for s in soup.find('section',class_='keywords').find_all('a')])
        except:
            keyword=' '
            print('Can not find keyword for url: '+article_url)
        
        #content 
        #去除圖片影響
        for s in soup.find_all(re.compile('^(img|picture|figure)')):
            s.decompose()

        content=''
        try:
            for s in soup.find(class_='article-content__paragraph').find('section').find_all('p',recursive=False):
                content=content+'\n'+re.sub(string=s.text,pattern=r'^\s+|\s+$',repl='')
        except:
            try:
                for s in soup.find(class_='article').find_all('p',recursive=False):
                    content=content+'\n'+re.sub(string=s.text,pattern=r'^\s+|\s+$',repl='')
                content=content.strip()
            except:
                try:
                    content=[s.get('content') for s in soup.find_all('meta') if bool(re.search(string=str(s),pattern='og:description'))][0]
                    content=content.strip()
                except:
                    content=' '
                    print('Can not find content for url: '+article_url)

        article_id=re.search(string=article_url,pattern='\d+/\d+').group(0)

        return pd.DataFrame({
            'article_id':article_id,
            'title':title,
            'created_at':created_at,
            'create_time':self._now_time,
            'content':content,
            'author':author,
            'category':category,
            'keyword':keyword,
            'article_url':article_url,
            'img':img,
            'source':self._source,
        },index=[1])       


class LibertyTimes(NewsLogic):
    _source='自由時報'
    _domain_url='https://news.ltn.com.tw'
    _page=5
    
    def get_article_url_from(self):
        TEMP=[]
        for page_num in range(1,self._page+1):
            #第一頁資料沒有編號，其他頁a['data']後不是list，還是json檔，key是吐出資料的序號
            a=requests.get(f'{self._domain_url}/ajax/breakingnews/all/{page_num}',
                          headers=self._headers,timeout=self._timeout).json()
            
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
        except:
            title=' '
            print('Can not find title for url: '+article_url)
        
        try:
            #created_at
            created_at=[s.text.strip() for s in soup.find_all(class_='time')]
            created_at=[s for s in created_at if s != ''][0]

            created_at=re.search(string=created_at,pattern=r'\d+(/|-)\d+(/|-)\d+ \d+:\d+').group(0)
            created_at=re.sub(string=created_at,pattern='/',repl='-')
            created_at=format(datetime.datetime.strptime(created_at,'%Y-%m-%d %H:%M'),'%Y-%m-%d %H:%M:%S')
        except:
            print('Can not find created_at for url: '+article_url)
            created_at=None

        try:
            img=soup.find('div',class_='photo boxTitle').find('img').get('src')
        except:
            try:
                img=soup.find('span',class_='ph_i').find('img').get('data-original')
                if bool(re.search(string=img,pattern=r'^//')):
                    img='https:'+img
            except:
                img=' '       

        for i in soup.find_all('img'):
            i.decompose()
        for i in soup.find_all(class_='ph_b ph_d1'):
            i.decompose()

        temp=soup.find('div',{'data-desc':re.compile('內文|內容頁')}).find_all('p',recursive=False)

        if len(temp) == 0:
            try:
                temp=soup.find('div',{'data-desc':re.compile('內文|內容頁')}).find(class_='text').find_all('p',recursive=False)
            except:
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
        except:
            try:
                author=re.sub(string=soup.find(class_='author').text,pattern=r'^\s+|\s+$',repl='')
            except:
                author=' '
            print('Can not find content for url: '+article_url)
            content=' '  

        article_id=re.search(string=article_url,pattern=r'/(\d+)$').group(1)

        return pd.DataFrame({
            'article_id':article_id,
            'title':title,
            'created_at':created_at,
            'create_time':self._now_time,
            'content':content,
            'author':author,
            'category':category,
            'keyword':keyword,
            'article_url':article_url,
            'img':img,
            'source':self._source,
        },index=[1])
        

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
        except:
            print('Can not find title for url: '+article_url)
            title=' '
        
        try:
            created_at=soup.find(class_='submitted').find(class_='created').text
            created_at=format(datetime.datetime.strptime(created_at,'%Y-%m-%d'),'%Y-%m-%d %H:%M:%S')
        except:
            print('Can not find dt_publish for url: '+article_url)
            created_at=None
        
        try:
            author=soup.find(class_='submitted').find(class_='author').text.strip()
        except:
            print('Can not find author for url: '+article_url)
            author=' '
        
        try:
            img=soup.find(class_='img-wrapper').find('img').get('src')
        except:
            img=' '
       
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
        except:
            print('Can not find content for url: '+article_url)
            content=' '    
        
        article_id=re.search(string=article_url,pattern=r'/(\d+)$').group(1)
        
        return pd.DataFrame({
            'article_id':article_id,
            'title':title,
            'created_at':created_at,
            'create_time':self._now_time,
            'content':content,
            'author':author,
            'category':category,
            'keyword':keyword,
            'article_url':article_url,
            'img':img,
            'source':self._source,
        },index=[1]) 
    
        
class ETtoday(NewsLogic):
    _source='ETtoday'
    _domain_url=f'https://www.ettoday.net'
    _timeout=30
    _now_date=format(datetime.datetime.now(),'%Y-%m-%d')

    def change_time_format(self,txt):
        return re.sub(string=txt,pattern='/',repl='-')
    
    def get_article_url_from(self):
        a=requests.get(f'{self._domain_url}/news/news-list-{self._now_date}-0.htm',headers=self._headers,
                       timeout=self._timeout)
        soup=BeautifulSoup(a.text,'lxml')
        
        TEMP=[]
        for part in soup.find(class_='part_list_2').find_all('h3'):
            title=part.find('a').text.strip()
            
            created_at=part.find(class_='date').text.strip()
            if '-' in created_at:
                created_at=format(datetime.datetime.strptime(created_at,'%Y-%m-%d %H:%M'),'%Y-%m-%d %H:%M:%S')
            else:
                created_at=format(datetime.datetime.strptime(created_at,'%Y/%m/%d %H:%M'),'%Y-%m-%d %H:%M:%S')
            
            category=part.find('em',class_='tag').text
            
            article_url=part.find('a').get('href')
            if not bool(re.search(string=article_url,pattern=r'^https*')):
                article_url=self._domain_url+article_url
            
            TEMP.append(pd.DataFrame({
                'created_at':created_at,
                'category':category,
                'article_url':article_url,
                'source':self._source,
                'keyword':' ',
                'img':' ',
            },index=[1]))
        
        temp_date=re.sub(string=self._now_date,pattern='-',repl='')
        #抓到昨天的數據則停止
        sign=0;index=1
        while sign != 1:
            b=requests.post(f'{self._domain_url}/show_roll.php',headers={
                'user-agent':str(np.random.choice(user_agent)),
                'referer':f'{self._domain_url}/news/news-list-{self._now_date}-0.htm'},data={
                'offset': index,
                'tPage': 3,
                'tFile': f'{temp_date}.xml',
                'tOt': 0,
                'tSi': 100,
                'tAr': 0,
            })
            soup_t=BeautifulSoup(b.text,'lxml')
            
            compare_date=datetime.datetime.strptime(soup_t.find('h3').find(class_='date').text,'%Y/%m/%d %H:%M')
            now_date=datetime.datetime.strptime(self._now_date,'%Y-%m-%d')
            if compare_date < now_date:
                sign=1
            else:
                for part in soup_t.find_all('h3'):
                    title=part.find('a').text.strip()
                    
                    created_at=part.find(class_='date').text.strip()
                    if '-' in created_at:
                        created_at=format(datetime.datetime.strptime(created_at,'%Y-%m-%d %H:%M'),'%Y-%m-%d %H:%M:%S')
                    else:
                        created_at=format(datetime.datetime.strptime(created_at,'%Y/%m/%d %H:%M'),'%Y-%m-%d %H:%M:%S')
                    
                    category=part.find('em',class_='tag').text
                    
                    article_url=part.find('a').get('href')
                    if not bool(re.search(string=article_url,pattern=r'^https*')):
                        article_url=f'{self._domain_url}'+article_url
                    
                    TEMP.append(pd.DataFrame({
                        'created_at':created_at,
                        'category':category,
                        'article_url':article_url,
                        'source':self._source,
                        'keyword':' ',
                        'img':' ',
                    },index=[1]))
                
                index=index+1
        
        TEMP=pd.concat(TEMP)
        TEMP=TEMP[~TEMP.duplicated(subset=['article_url'])]
        TEMP['created_at']=TEMP.created_at.map(self.change_time_format)
        return TEMP.reset_index(drop=True)

    
    def get_article_info(self,article_url,tim,img,keyword,category):
        self._headers['Referer']=self._domain_url
        
        a=requests.get(article_url,headers=self._headers,timeout=self._timeout)
        soup=BeautifulSoup(a.text,'lxml')
        
        #title
        title=soup.find('h1',class_=re.compile('title')).text.strip()
        
        if soup.find(class_='tag') is not None:
            keyword=[s.text.strip() for s in soup.find(class_='tag').find_all('a')]
            keyword=[s for s in keyword if s !='']
            keyword=';'.join(keyword)
        else:
            try:
                keyword=[s.text.strip() for s in soup.find(class_='part_keyword').find_all('a')]
                keyword=[s for s in keyword if s !='']
                keyword=';'.join(keyword)           
            except:
                keyword=' '
        
        if soup.find('div',class_='part_menu_5 clearfix') is not None:
            try:
                category=soup.find('div',class_='part_menu_5 clearfix').find(class_='btn current').text.strip()
            except:
                category=soup.find('div',class_='part_menu_5 clearfix').find('strong').text.strip()
        elif soup.find(class_=re.compile('^logo_|_logo$')) is not None:
            category=soup.find(class_=re.compile('^logo_|_logo$')).text.strip()
        else:
            category=' '
            print(f'Error for category for {article_url}')
        
        try:
            img=soup.find(class_='story').find('img').get('src')
            if not bool(re.search(string=img,pattern=r'^https*')):
                img='https:'+img
        except:
            img=' '
        
        created_at=soup.find('time',class_=re.compile('news-time|date')).text.strip()
        created_at=re.sub(string=created_at,pattern=r'年|月|日|-|/',repl='')
        try:
            created_at=datetime.datetime.strptime(created_at,'%Y%m%d %H:%M')
        except:
            created_at=datetime.datetime.strptime(created_at,'%Y%m%d %H:%M:%S')
        created_at=format(created_at,'%Y-%m-%d %H:%M:%S')
        
        #content,author
        for s in soup.find_all('img'):
            s.decompose()
        for s in soup.find_all(class_='ad_in_news'):
            s.decompose()
        
        content=[s.text for s in soup.find(class_='story').find_all('p') if not bool(re.search(string=s.text.strip(),
                                                                                               pattern=r'^(▲|▼|►)'))]
        content='\n'.join(content).strip()
        
        try:
            author=re.search(string=content,pattern=r'^(.{0,15}／.{0,15})\n').group(1)
        except:
            author=' '
        
        if author !=' ':
            content=re.sub(string=content,pattern=author,repl='').strip()
        
        #article_id
        if re.search(string=article_url,pattern=r'(\d+)\.htm') is not None:
            article_id=re.search(string=article_url,pattern=r'(\d+)\.htm').group(1)
        else:
            article_id=re.search(string=article_url,pattern=r'/news/(\d+)').group(1)

        return pd.DataFrame({
            'article_id':article_id,
            'title':title,
            'created_at':created_at,
            'create_time':self._now_time,
            'content':content,
            'author':author,
            'category':category,
            'keyword':keyword,
            'article_url':article_url,
            'img':img,
            'source':self._source,
        },index=[1])


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
        except:
            title=' '
            print('Can not find title for url: '+article_url)       

        created_at=tim
        
        category=soup.find(class_='breadcrumb').find_all('a')[-1].text.strip()
        
        try:
            content=''
            for s in soup.find('div',class_='paragraph').find_all('p',recursive=False):
                content=content+'\n'+s.text
                content=re.sub(string=content,pattern=r'^\s+|\s+$',repl='')
            if content == '':
                content=' '
                print('Can not find content for url: '+article_url)
        except:
            try:
                content=''
                for s in soup.find('article').find(class_="wrapper").find_all('p',recursive=False):
                    content=content+'\n'+s.text
                    content=re.sub(string=content,pattern=r'^\s+|\s+$',repl='')
                if content == '':
                    content=' '
                    print('Can not find content for url: '+article_url)
            except:
                content=' '
                print('Can not find content for url: '+article_url)
        
        try:
            author=re.search(string=content,pattern=r'^〔[^〕]+〕|^［[^］]+］|^（[^）]+）').group(0)
            content=re.sub(pattern=author,repl='',string=content)
        except:
            try:
                author=soup.find(class_='author').text
                author=author.split('／')[0]
                author=re.sub(string=author,pattern=r'中央社|^\s+|\s+$',repl='')
            except:
                print('Can not find author for url: '+article_url)
                author=' '  
        
        keyword=';'.join([k.text.replace('#','').strip() for k in soup.find_all(class_='keywordTag')])
        
        article_id=re.search(string=article_url,pattern=r'(\d+)(\.aspx)*$').group(1)
        
        return pd.DataFrame({
            'article_id':article_id,
            'title':title,
            'created_at':created_at,
            'create_time':self._now_time,
            'content':content,
            'author':author,
            'category':category,
            'keyword':keyword,
            'article_url':article_url,
            'img':img,
            'source':self._source,
        },index=[1])
        
class ChinaTimes(NewsLogic):
    _source='中時日報'
    _domain_url='https://www.chinatimes.com'
    _page=10
    
    def get_article_url_from(self):
        article_url=[];tag=[]
        
        for page_num in range(0,self._page):
            a=requests.get(f'{self._domain_url}/realtimenews?page={page_num}&chdtv',
                          headers=self._headers,timeout=self._timeout)
            soup=BeautifulSoup(a.text,'lxml')
            
            article_url.append([self._domain_url+s.find('a').get('href') for s in soup.find_all('h3',class_='title')])
            
            tag.append([s.find(class_='category').text for s in soup.find_all('div',class_='meta-info')])
        
        article_url=list(chain.from_iterable(article_url))
        tag=list(chain.from_iterable(tag))        

        TEMP=pd.DataFrame({'source':self._source,
                           'article_url':article_url,
                           'category':tag,
                           'created_at':' ',
                           'img':' ',
                           'keyword':' '})
        return TEMP

    def get_article_info(self,article_url,tim,img,keyword,category):
        a=requests.get(article_url,headers=self._headers,timeout=self._timeout)
        soup=BeautifulSoup(a.text,'lxml')
        
        try:
            title=soup.find('h1').text.strip()
            title=re.sub(string=title,pattern='\u3000|\xa0',repl='')
        except:
            print('Can not find title for url: '+article_url)
            title=' '
        
        try:
            created_at=soup.find('time').get('datetime')
            created_at=format(datetime.datetime.strptime(created_at,'%Y-%m-%d %H:%M'),'%Y-%m-%d %H:%M:%S')
        except:
            print('Can not find created_at for url: '+article_url)
            created_at=None
        
        try:
            try:
                s=soup.find('div',class_='meta-info').find(class_='source').text.strip()
            except:
                s=''
            try:
                s2=soup.find('div',class_='meta-info').find(class_='author').text.strip()
            except:
                s2=''
            
            author=s+' '+s2
            if author == '':
                author=' '
        except:
            print('Can not find author for url: '+article_url)
            author=' '
        
        try:
            img=soup.find('figure').find('img').get('src')
        except:
            img=' '
            print('Can not find img for url: '+article_url)

        try:
            for s in soup.find_all(re.compile('img|figure')):
                s.decompose()
            
            content=[re.sub(string=s.text,pattern=r'^\s+|\s+$',repl='') for s in soup.find(class_='article-body').find_all('p',recursive=False)]
            content='\n'.join(content)
            content=content.strip()
        except:
            print('Can not find content for url: '+article_url)
            content=' '            

        #keyword
        try:
            keyword=[t.find('a').text for t in soup.find_all(class_='hash-tag')]
            keyword=';'.join(keyword)
        except:
            keyword=' '
            print('Can not find keyword for url: '+article_url)
        
        article_id=re.search(string=article_url,pattern=r'\d+-\d+').group(0)

        return pd.DataFrame({
            'article_id':article_id,
            'title':title,
            'created_at':created_at,
            'create_time':self._now_time,
            'content':content,
            'author':author,
            'category':category,
            'keyword':keyword,
            'article_url':article_url,
            'img':img,
            'source':self._source,
        },index=[1])
        
class CP(NewsLogic):
    _source='CP值'
    _domain_url='https://cpok.tw/'
    
    def parse_one_block_article_meta(self,part):
        article_id=part.get('id')
        article_url=part.find('a').get('href')
        title=part.find('h2').text.strip()
        brief_content=part.find(class_='archive-content').text.strip()
        views=part.find(class_='views').text.strip()
        img_url=part.find('img').get('src')

        #文章時間無法快速取得，需要進入文章才能獲得完整的時間
        b=requests.get(article_url,headers=self._headers,timeout=self._timeout)
        soup_content=BeautifulSoup(b.text,'lxml')
        metadata=soup_content.find(class_='entry-content').find(class_='begin-single-meta')

        author=metadata.find('a',{'rel':'author'}).text.strip()
        created_at=metadata.find(class_='my-date').text.strip()
        created_at=re.sub(string=created_at,pattern=r'\s*(年|月|日)\s*',repl='-')
        created_at=' '.join(created_at.rsplit('-',maxsplit=1))

        data={
            'article_id':article_id,
            'author':author,
            'created_at':created_at,
            'title':title,
            'brief_content':brief_content,
            'views':int(views.replace(',','')),
            'img_url':img_url,
            'article_url':article_url
        }
        
        return data
    
    def get_article_url_from(self):
        data=[]
        
        for page_num in range(1,self._page+1):
            url=f'https://cpok.tw/discount/page/{page_num}' #所有board皆涵蓋的類別

            a=requests.get(url,headers=self._headers,timeout=self._timeout)
            soup=BeautifulSoup(a.text,'lxml')
            
            for part in soup.find('div',{'id':'content'}).find_all('article'):
                tmp=self.parse_one_block_article_meta(part)
                data.append(pd.DataFrame(tmp,index=[0]))
            
            time.sleep(random.randint(1,3))
            print(f'Done CP值 page : {page_num}')
        
        df=pd.concat(data)
        df['created_at']=df['created_at'].astype('datetime64[ns]')
        df['create_time']=self._now_time
        df=df.sort_values(['created_at'])
        df['source']=self._source

        return df
    
    
class InfoTalk(NewsLogic):
    _source='好康情報誌'
    _domain_url='https://info.talk.tw/' #只抓優惠活動，其他未更新
    _page=3
    
    def parse_one_block_article_meta(self, part):
        article_data = {}
        
        # 提取標題及連結
        title_tag = part.find('h2', class_='entry-title')
        if title_tag and title_tag.a:
            article_data['title'] = title_tag.a.text.strip()
            article_data['article_url'] = title_tag.a['href']
        
        # # 提取分類
        # category_tag = part.select_one('.meta-categories a')
        # if category_tag:
        #     article_data['category'] = category_tag.text.strip()
        
        # 提取圖片 URL
        img_url = part.find('img', class_='wp-post-image')
        if img_url:
            article_data['img_url'] = img_url['src']
        
        # 提取摘要
        brief_content = part.find('div', class_='entry-excerpt')
        if brief_content:
            article_data['brief_content'] = brief_content.text.strip()
        
        # 提取作者名稱及連結
        author = part.select_one('.meta-author a')
        if author:
            article_data['author'] = author.text.strip()
        
        # 提取發佈日期
        created_at = part.select_one('.meta-date time')
        if created_at:
            created_at = created_at.text.strip()
            if '年' in created_at and '月' in created_at and '日' in created_at:
                created_at = datetime.datetime.strptime(created_at, '%Y 年 %m 月 %d 日').strftime('%Y-%m-%d')

            article_data['created_at']=created_at

        article_data['views'] = None
        article_data['article_id'] = article_data.get('article_url', '').split('/')[-2]
        
        return article_data
    
    def get_article_url_from(self):
        data=[]
        scraper = cloudscraper.create_scraper()

        for page_num in range(1,self._page+1):
            response=scraper.post(f'https://info.talk.tw/page/{page_num}/',headers=self._headers)
            soup=BeautifulSoup(response.text, 'lxml')
            for part in soup.find(class_='entries', attrs={"data-cards": "boxed"}).find_all('article'):
                tmp=self.parse_one_block_article_meta(part)
                data.append(tmp)
            
            time.sleep(random.randint(1,3))
            print(f'Done {self._source} page : {page_num}')
        
        df=pd.DataFrame(data)
        df['created_at']=df['created_at'].astype('datetime64[ns]')
        df['create_time']=self._now_time
        df=df.sort_values(['created_at'])
        df['source']=self._source

        return df

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
                except:
                    continue

                # 提取類別
                try:
                    category_tmp=part.find('div', class_='type').text.strip()
                except:
                    category_tmp=''
                
                category.append(category_tmp)

                # 提取圖片 URL
                try:
                    img_tmp=part.find('img', class_='lazyimage')['data-original']
                except:
                    img_tmp=''
                
                img.append(img_tmp)
            except Exception as e:
                print(e)

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
        except:
            print('Can not find title for url: '+article_url)
            title=''
        
        try:
            created_at=datetime.datetime.strptime(json_data['dateCreated'], '%Y-%m-%dT%H:%M:%S+08:00').strftime('%Y-%m-%d %H:%M:%S')
        except:
            print('Can not find created_at for url: '+article_url)
            created_at=None

        try:
            author=json_data['author']['name'].strip()
        except:
            print('Can not find author for url: '+article_url)
            author=''

        try:
            content=json_data['articleBody'].strip()
        except:
            print('Can not find content for url: '+article_url)
            content=' '    

        try:
            keyword=';'.join(json_data['keywords'])
        except:
            print('Can not find keyword for url: '+article_url)
            keyword=''

        article_id=re.search(string=article_url,pattern=r'/(\d+)$').group(1)

        return pd.DataFrame({
            'article_id':article_id,
            'title':title,
            'created_at':created_at,
            'create_time':self._now_time,
            'content':content,
            'author':author,
            'category':category,
            'keyword':keyword,
            'article_url':article_url,
            'img':img,
            'source':self._source,
        },index=[1]) 


class TVBS_SaveMoney(NewsLogic):
    _source='tvbs_優惠好康情報'
    _domain_url='https://news.tvbs.com.tw/pack/packdetail/841' #只抓優惠活動，其他未更新
    _page=1

    def parse_json(self, soup_format_str):
        json_str=soup_format_str.text.strip()
        json_str=re.sub(string=json_str, pattern=r'\n\s+|\s+\n', repl='')
        
        return json.loads(json_str)    

    def get_article_url_from(self):
        from lib.get_sql import get_sql
        from common.config import sql_configure_path

        connection=pd.read_csv(sql_configure_path,encoding='utf_8_sig',index_col='name')
        conn,cursor,engine=get_sql(connection.loc['host','value'],connection.loc['port','value'],
                                    connection.loc['user','value'],connection.loc['password','value'],
                                    'news_ch')

        for page_num in range(1,self._page+1):
            response=requests.post(f'{self._domain_url}/{page_num}',headers=self._headers, timeout=20)
            soup=BeautifulSoup(response.text,'lxml')

        data=[self.parse_json(section) for section in soup.find_all('script', {'type':'application/ld+json'}) if 'itemListElement' in self.parse_json(section)][0]
        data=data['itemListElement']
        article_id_list=[d['url'].split('/')[-1] for d in data]
        df=pd.read_sql_query(f"""
            select 
                article_id, author, created_at, title, substring(content,1,100) brief_content, 
                NULL views, img img_url, article_url, create_time
            from news 
            where source='TVBS' and article_id in {tuple(article_id_list)}
            order by created_at desc
        """, engine)
        df['source']=self._source

        return df