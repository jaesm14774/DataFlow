import re
from bs4 import BeautifulSoup
import datetime
import pandas as pd
import time
import random
import cloudscraper
from news_ch.news_crawler.base_process import *

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
            print(f'[中文新聞] 列表頁完成 | 來源={self._source} | page={page_num}')
        
        df=pd.DataFrame(data)
        df['created_at']=df['created_at'].astype('datetime64[ns]')
        df['create_time']=self._now_time
        df=df.sort_values(['created_at'])
        df['source']=self._source

        return df

