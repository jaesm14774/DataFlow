from bs4 import BeautifulSoup
import re
import datetime
import pandas as pd
import json
from urllib.parse import urljoin
from news_ch.news_crawler.base_process import *
from news_ch.news_crawler.page_utils import safe_publisher_get, validate_publisher_url

class SETN(NewsLogic):
    _source='三立新聞'
    _domain_url='https://www.setn.com'
    _page=4

    _taipei_tz=datetime.timezone(datetime.timedelta(hours=8))

    @staticmethod
    def _relative_age_window(label):
        match=re.search(r'(\d+)\s*(分鐘|小時|天)前',str(label or ''))
        if not match:
            return None
        amount=int(match.group(1))
        unit=match.group(2)
        if unit == '分鐘':
            # Eight sampled first-party pages showed about 3.5 minutes of list lag;
            # retain list corroboration while allowing a 5-minute upper bound.
            return max(0,amount-2),amount+5
        if unit == '小時':
            return max(0,amount*60-5),(amount+1)*60+5
        return max(0,amount*1440-5),(amount+1)*1440+5

    @staticmethod
    def _list_time_evidence(value):
        match=re.match(
            r'^__setn_relative_age__=(.*?)\|observed_at=(\d{4}-\d{2}-\d{2} '
            r'\d{2}:\d{2}:\d{2})$',str(value or '').strip()
        )
        return (match.group(1),match.group(2)) if match else None

    @classmethod
    def _matches_list_relative_time(cls,published_wall_time,observed_at,label):
        age_window=cls._relative_age_window(label)
        if age_window is None or not observed_at:
            return False
        try:
            observed=datetime.datetime.fromisoformat(str(observed_at))
            if observed.tzinfo is not None:
                observed=observed.astimezone(cls._taipei_tz).replace(tzinfo=None)
            age_minutes=(observed-published_wall_time).total_seconds()/60
            return age_window[0] <= age_minutes <= age_window[1]
        except (TypeError,ValueError):
            return False

    @staticmethod
    def _jsonld_article(soup):
        pending=[]
        for script in soup.find_all('script',{'type':'application/ld+json'}):
            try:
                pending.append(json.loads(script.string or script.get_text()))
            except (TypeError,ValueError):
                continue
        while pending:
            node=pending.pop()
            if isinstance(node,dict):
                if node.get('@type') in ('NewsArticle','Article') and node.get('datePublished'):
                    return node
                pending.extend(
                    value for value in node.values()
                    if isinstance(value,(dict,list))
                )
            elif isinstance(node,list):
                pending.extend(node)
        return None

    def get_article_url_from(self):
        TEMP=[]
        seen_urls=set()
        for page_num in range(1,self._page+1):
            response=safe_publisher_get(
                f'{self._domain_url}/viewall?p={page_num}',
                allowed_hosts={'www.setn.com'},
                article_path=re.compile(r'/viewall',re.I),
                headers=self._headers,timeout=self._timeout,
            )
            soup=BeautifulSoup(response.content,'lxml')
            observed_at=datetime.datetime.now(self._taipei_tz).replace(
                tzinfo=None
            ).strftime('%Y-%m-%d %H:%M:%S')
            rows=[]
            for card in soup.select('.news_list_item'):
                article_link=next((
                    link for link in card.select('a[href]')
                    if re.search(r'/(?:star/)?news/\d+(?:[?#]|$)',link.get('href',''))
                ),None)
                if article_link is None:
                    continue
                href=urljoin(self._domain_url,article_link.get('href',''))
                try:
                    href=validate_publisher_url(
                        href,allowed_hosts={'www.setn.com','star.setn.com'},
                        article_path=re.compile(r'/(?:star/)?news/\d+',re.I),
                    )
                except ValueError:
                    continue
                category_el=card.select_one('.time_box .tab')
                time_el=card.select_one('.time_box .time')
                image_el=card.select_one('img')
                relative_label=time_el.get_text(' ',strip=True) if time_el else ''
                rows.append({
                    'category':category_el.get_text(' ',strip=True) if category_el else ' ',
                    'article_url':href,
                    'img':image_el.get('src',' ') if image_el else ' ',
                    # Temporary list evidence; replaced by article keywords on parse.
                    'keyword':(
                        f'__setn_relative_age__={relative_label}'
                        f'|observed_at={observed_at}'
                    ),
                    'created_at':' ',
                    'source':self._source,
                })
            if rows:
                new_rows=[]
                for row in rows:
                    if row['article_url'] not in seen_urls:
                        new_rows.append(row)
                        seen_urls.add(row['article_url'])
                if not new_rows:
                    print(
                        f'[中文新聞-SETN] 列表分頁重複，停止後續頁面 '
                        f'| page={page_num} repeated_urls={len(rows)}'
                    )
                    break
                TEMP.append(pd.DataFrame(new_rows))
                continue

            if TEMP:
                break
            raise ValueError('SETN current listing page has no recognized article cards')

        TEMP=pd.concat(TEMP)

        return TEMP[~TEMP.duplicated(subset=['article_url'])]

    def get_article_info(self,article_url,tim,img,keyword,category):
        self._headers['Referer']=self._domain_url

        a=safe_publisher_get(
            article_url,allowed_hosts={'www.setn.com','star.setn.com'},
            article_path=re.compile(r'/(?:star/)?news/\d+',re.I),
            headers=self._headers,timeout=self._timeout,
        )
        soup=BeautifulSoup(a.text,'lxml')

        #title
        title_el=soup.find('h1')
        if title_el is None:
            title_el=soup.select_one('.newsTitle,.news-title,.pageTitle')
        if title_el is None:
            raise ValueError('SETN article page has no h1 title')
        title=title_el.text.strip()
        title=re.sub(string=title,pattern='\u3000',repl='    ')

        #keyword,category,img,created_at
        metadata=self._jsonld_article(soup)
        if metadata is None:
            # The housing vertical exposes a visible wall-clock date and OG metadata.
            # Its published_time falsely labels local time as Z, just like main SETN.
            body=soup.select_one('#ckuse')
            visible_time=soup.select_one('.page-date,time.pageDate')
            if body is None or visible_time is None:
                raise ValueError('SETN article page has no recognized metadata/body')
            wall_time=datetime.datetime.strptime(visible_time.get_text(strip=True),'%Y/%m/%d %H:%M')
            time_evidence=self._list_time_evidence(keyword)
            if not time_evidence or not self._matches_list_relative_time(wall_time,time_evidence[1],time_evidence[0]):
                raise ValueError('SETN housing publication time lacks matching list evidence')
            image_tag=soup.find('meta',{'property':'og:image'})
            author_tag=soup.select_one('.date-box .author,.date-box .reporter')
            metadata={'datePublished':wall_time.isoformat(),
                      'image':image_tag.get('content','') if image_tag else '',
                      'author':author_tag.get_text(' ',strip=True) if author_tag else ''}
        _source_keyword=keyword
        keywords=metadata.get('keywords',[])
        keyword=(keywords if isinstance(keywords,str) else
                 ';'.join(str(value) for value in keywords))

        if not category or not str(category).strip():
            category=metadata.get('articleSection',' ')

        image=metadata.get('image',' ')
        if isinstance(image,dict):
            img=image.get('url',' ')
        elif isinstance(image,list):
            first_image=image[0] if image else ' '
            img=first_image.get('url',' ') if isinstance(first_image,dict) else first_image
        else:
            img=image

        created_at=None
        try:
            raw_created_at=str(metadata['datePublished']).strip().replace('Z','+00:00')
            parsed_created_at=datetime.datetime.fromisoformat(raw_created_at)
            time_evidence=self._list_time_evidence(_source_keyword)
            if parsed_created_at.tzinfo is None:
                corroborated=bool(time_evidence) and self._matches_list_relative_time(
                    parsed_created_at,time_evidence[1],time_evidence[0]
                )
                if corroborated:
                    created_at=parsed_created_at.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    print(
                        f'[中文新聞-SETN][time] unzoned timestamp withheld '
                        f'without matching list-relative evidence | url={article_url}'
                    )
            elif parsed_created_at.utcoffset()==datetime.timedelta(0):
                wall_time=parsed_created_at.replace(tzinfo=None)
                corroborated=bool(time_evidence) and self._matches_list_relative_time(
                    wall_time,time_evidence[1],time_evidence[0]
                )
                if corroborated:
                    created_at=wall_time.strftime('%Y-%m-%d %H:%M:%S')
                    print(
                        '[中文新聞-SETN][time] JSON-LD +00:00 wall time '
                        'matches list-relative age; source offset conflict retained'
                    )
                else:
                    print(
                        f'[中文新聞-SETN][time] offset-zero timestamp withheld '
                        f'without matching list-relative evidence | url={article_url}'
                    )
            else:
                created_at=parsed_created_at.astimezone(
                    self._taipei_tz
                ).strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            created_at=None
            print(f'[中文新聞-SETN] JSON-LD 時間解析失敗 | url={article_url}')

        #content,author
        content_root=(
            soup.select_one('.article_content_area')
            or soup.find('div',{'id':'Content1'})
        )
        if content_root is None:
            # SETN's entertainment subdomain uses a separate article wrapper.
            content_root=soup.select_one('article.printdiv')
        if content_root is None:
            content_root=soup.select_one('#ckuse')
        if content_root is None:
            raise ValueError('SETN article page has no recognized article body')
        paragraphs=content_root.find_all('p')

        #找尋作者
        if soup.find(class_='reporter') is not None:
            if soup.find(class_='reporter').text.strip() == '中央社':
                author='中央社'
            else:
                author=''
        else:
            raw_author=metadata.get('author','')
            if isinstance(raw_author,dict):
                author=raw_author.get('name','')
            elif isinstance(raw_author,list):
                author=';'.join(
                    str(item.get('name','')) if isinstance(item,dict) else str(item)
                    for item in raw_author
                )
            else:
                author=str(raw_author or '')

        if author == '中央社':
            content=[re.sub(string=s.text,pattern=r'^\s+|\s+$',repl='') for s in paragraphs]
            content=[s for s in content if not bool(re.search(string=s,pattern=r'^(▲|▼|►)'))]
            content='\n'.join(content)
        else:
            content=[re.sub(string=s.text,pattern=r'^\s+|\s+$',repl='') for s in paragraphs]
            content=[s for s in content if not bool(re.search(string=s,pattern=r'^(▲|▼|►)'))]
            content='\n'.join(s for s in content if s)

        article_match=re.search(r'/(?:star/)?news/(\d+)',article_url,re.I)
        if article_match is None:
            raise ValueError(f'Unable to parse SETN article id from URL: {article_url}')
        article_id=article_match.group(1)

        return self.build_article_dataframe(
            article_id=article_id, title=title, created_at=created_at,
            content=content, author=author, category=category,
            keyword=keyword, article_url=article_url, img=img)

