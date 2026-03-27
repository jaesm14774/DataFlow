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
            title=soup.find('h1').text.strip()
            if not title:
                title=soup.find('title').text.strip()
        except Exception:
            title=' '
            print(f'[中文新聞] 缺少 title（頁面改版或選擇器失效）| url={article_url}')
        
        try:
            meta_time=soup.find('meta',{'property':'article:published_time'})
            if meta_time and meta_time.get('content'):
                raw=meta_time['content']
                dt=datetime.datetime.fromisoformat(raw.replace('Z','+00:00'))
                created_at=dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                meta_pub=soup.find('meta',{'name':'pubdate'})
                if meta_pub and meta_pub.get('content'):
                    raw=meta_pub['content']
                    dt=datetime.datetime.fromisoformat(raw.replace('Z','+00:00'))
                    created_at=dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    time_els=[s.text.strip() for s in soup.find_all(class_=re.compile('time|article_time'))]
                    time_els=[s for s in time_els if s]
                    if time_els:
                        t=re.search(string=time_els[0],pattern=r'\d+[/\-]\d+[/\-]\d+ \d+:\d+')
                        if t:
                            raw=t.group(0).replace('/','-')
                            created_at=format(datetime.datetime.strptime(raw,'%Y-%m-%d %H:%M'),'%Y-%m-%d %H:%M:%S')
                        else:
                            created_at=None
                    else:
                        created_at=None
        except Exception:
            print(f'[中文新聞] 缺少 created_at | url={article_url}')
            created_at=None

        try:
            og_img=soup.find('meta',{'property':'og:image'})
            if og_img and og_img.get('content'):
                img=og_img['content']
            else:
                lazy=soup.find('img',class_='lazy_imgs_ltn')
                if lazy and lazy.get('data-src'):
                    img=lazy['data-src']
                elif soup.find('div',class_='photo boxTitle') and soup.find('div',class_='photo boxTitle').find('img'):
                    img=soup.find('div',class_='photo boxTitle').find('img').get('src','')
                else:
                    img=' '
            if img and img.startswith('//'):
                img='https:'+img
        except Exception:
            img=' '

        for i in soup.find_all('img'):
            i.decompose()
        for i in soup.find_all(class_='ph_b ph_d1'):
            i.decompose()

        content=' '
        try:
            text_node=None
            wc=soup.select_one('div.whitecon[data-desc="內文"]:not(.template) > div.text')
            if wc:
                text_node=wc
            if not text_node:
                wc2=soup.select_one('div.whitecon[data-desc="內容頁"]:not(.template) > div.text')
                if wc2:
                    text_node=wc2
            if not text_node:
                dd=soup.find('div',{'data-desc':re.compile('內文|內容頁')})
                if dd:
                    inner=dd.find(class_='text')
                    text_node=inner if inner else dd
            if not text_node:
                text_node=soup.select_one('div.whitecon.article div.text')
            if not text_node:
                text_node=soup.select_one('div.whitecon:not(.template) div.text')

            if text_node:
                temp=text_node.find_all('p',recursive=False)
                if not temp:
                    temp=text_node.find_all('p')
                temp=[s.text for s in temp if not bool(re.search(string=str(s),pattern=r'before_ir|after_ir|appE1121'))]
                temp=[s for s in temp if s.strip()]
                content='\n'.join(temp).strip()
            if not content:
                content=' '
        except Exception:
            print(f'[中文新聞] 缺少 content | url={article_url}')
            content=' '
        
        try:
            author=re.search(string=content,pattern=r'^(〔|［).{0,30}(〕|］)').group(0)
            content=re.sub(string=content,pattern=re.escape(author),repl='').strip()
        except Exception:
            try:
                ae=soup.find(class_='article_edit')
                if ae:
                    author=ae.text.strip()
                else:
                    ac=soup.find(class_='author')
                    author=ac.text.strip() if ac else ' '
            except Exception:
                author=' '

        try:
            meta_kw=soup.find('meta',{'name':'keywords'})
            if meta_kw and meta_kw.get('content'):
                keyword=meta_kw['content'].replace(',',';').strip()
            else:
                keyword=' '
        except Exception:
            keyword=' '

        article_id=re.search(string=article_url,pattern=r'/(\d+)$').group(1)

        return self.build_article_dataframe(
            article_id=article_id, title=title, created_at=created_at,
            content=content, author=author, category=category,
            keyword=keyword, article_url=article_url, img=img)
        
