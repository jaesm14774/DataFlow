import requests
from bs4 import BeautifulSoup
import re
import datetime
import pandas as pd
from news_ch.news_crawler.base_process import *

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
                    bn=soup.find(class_='breakingnews')
                    if bn:
                        info=bn.find(class_='news-info')
                        cat_a=info.find('a') if info else None
                        category=cat_a.text.strip() if cat_a else ' '
                        tag.append(category)

                        bn_img=bn.find('img')
                        img.append(bn_img.get('src','') if bn_img else '')

                        h1_a=bn.find('h1')
                        if h1_a and h1_a.find('a'):
                            article_url.append(h1_a.find('a').get('href'))
                        else:
                            h2_a=bn.find('h2')
                            if h2_a and h2_a.find('a'):
                                article_url.append(h2_a.find('a').get('href'))

                news_list=soup.select_one('ul.news-list')
                if not news_list:
                    news_list=soup.find('ul',class_=lambda c: c and 'news-list' in c)
                
                if news_list:
                    for part in news_list.find_all('li',class_='d-flex'):
                        info=part.find(class_=lambda c: c and 'news-info' in c)
                        if info:
                            cat_a=info.find('a',href=lambda h: h and '/category/' in h)
                            if not cat_a:
                                cat_a=info.find('a')
                            category=cat_a.text.strip() if cat_a else ' '
                        else:
                            category=' '

                        fig=part.find('figure')
                        if fig and fig.find('a'):
                            article_url.append(fig.find('a').get('href'))
                        else:
                            h2=part.find('h2')
                            if h2 and h2.find('a'):
                                article_url.append(h2.find('a').get('href'))
                            else:
                                continue
                        tag.append(category)
                        fig_img=fig.find('img') if fig else None
                        img.append(fig_img.get('src','') if fig_img else '')

            TEMP=pd.DataFrame({'source':[self._source]*len(article_url),
                               'article_url':article_url,
                               'category':tag,
                               'created_at':' ',
                               'img':img,
                               'keyword':' '})
            
            return TEMP
        except Exception as e:
            print(f'[中文新聞] PTS 列表取得失敗 | {type(e).__name__}: {e}')
            return pd.DataFrame()

    def get_article_info(self,article_url,tim,img,keyword,category):
        b=requests.get(article_url,headers=self._headers,timeout=self._timeout)
        soup=BeautifulSoup(b.text,'lxml')
        
        try:
            title=soup.find('h1').text
        except Exception:
            title=' '
            print(f'[中文新聞] 缺少 title（頁面改版或選擇器失效）| url={article_url}')
        
        try:
            author_a=soup.select_one('.article_authors .reporter-container a')
            if not author_a:
                author_a=soup.select_one('.article-reporter a[href*="/author/"]')
            if author_a:
                author=author_a.text.strip()
            else:
                meta_a=soup.find('meta',{'name':'author'})
                if meta_a and meta_a.get('content'):
                    author=meta_a['content'].strip()
                else:
                    old=soup.find('span',class_='article-reporter')
                    if old:
                        author=old.text.split(' / ')[0].strip()
                    else:
                        author=' '
            if not author:
                author=' '
        except Exception:
            author=' '
            print(f'[中文新聞] 缺少 author | url={article_url}')     

        created_at=soup.find('time').text
        
        if img == '':
            img=' '
        
        category=category
        
        try:
            content=soup.find(class_='post-article').text
            content=re.sub(string=content,pattern='\xa0',repl='').strip()
        except Exception:
            content=' '
            print(f'[中文新聞] 缺少 content | url={article_url}')
        
        #keyword
        try:
            keyword=[s.find('a').text for s in soup.find('ul',class_='tag-list').find_all(class_='blue-tag')]
            keyword=';'.join([s for s in keyword if s !='...'])
        except Exception:
            keyword=' '
            print(f'[中文新聞] 缺少 keyword | url={article_url}')
        
        article_id=re.search(string=article_url,pattern=r'article/(\d+)$').group(1)
        
        return self.build_article_dataframe(
            article_id=article_id, title=title, created_at=created_at,
            content=content, author=author, category=category,
            keyword=keyword, article_url=article_url, img=img)

