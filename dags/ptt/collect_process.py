import re
import requests
import datetime
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import time
from lib.common_tool import task_wrapper
from lib.get_sql import get_sql
from lib.log_process_execution import BaseLogRecord
from common.config import *

app_name = 'ptt'
database_name = 'crawler'
table_name = 'ptt_article'

#初始化log
log_record=BaseLogRecord(process_date=(datetime.datetime.now()+datetime.timedelta(hours=8)).strftime('%Y-%m-%d'),
                         app_name=app_name)

#連接mysql
connection=pd.read_csv(sql_configure_path,index_col='name')
conn,cursor,engine=get_sql(connection.loc['host','value'],
                           int(connection.loc['port','value']),
                           connection.loc['user','value'],
                           connection.loc['password','value'], database_name)

before_count=pd.read_sql_query(f'select count(id) as N from {table_name}',engine)['N'].iloc[0]
log_record.set_before_count(before_count)
print(f'PTT文章，處理前總數為 : {before_count}')

class PttScraper:
    board_id=''

    def __init__(self):
        self.base_url = 'https://www.ptt.cc'
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
            'referer': self.base_url,
            'cookie': 'over18=1'
        }

        self.board=pd.read_sql_query('select id, board_id, crawler_page from ptt_board where status=1',engine)
        self.delete_count = 0
        self.insert_count = 0

    def fetch_hot_boards(self):
        response = requests.get(f'{self.base_url}/hotboards.html', headers=self.headers, timeout=20)
        soup = BeautifulSoup(response.text, 'lxml')

        board_df_list = []

        for b_ent in soup.find_all(class_='b-ent'):
            board_name = b_ent.find(class_='board-name').text
            board_url = f"{self.base_url}{b_ent.find(class_='board').get('href')}"
            board_class = b_ent.find(class_='board-class').text
            board_title = b_ent.find(class_='board-title').text

            title_regx = re.search(string=board_title, pattern=r'◎(《|\[|【)(.*)(》|\]|】)')

            if title_regx is None:
                board_title = board_class
            else:
                board_title = title_regx.group(2) 

            board_df = pd.DataFrame({
                'board_id': board_name,
                'name': board_name,
                'title': board_title,
                'board_url': board_url
            }, index=[0])
            board_df_list.append(board_df)

        return pd.concat(board_df_list).reset_index(drop=True)

    def fetch_article_urls(self, max_page=1):
        response = requests.get(f'{self.base_url}/bbs/{self.board_id}/index.html', headers=self.headers, timeout=20)
        soup = BeautifulSoup(response.text, 'lxml')

        for s in soup.find_all(class_='item'):
            s.decompose()

        last_page_no = int([re.search(string=s.get('href'), pattern=r'\d+').group(0) for s in soup.find(class_='action-bar').find_all('a') if s.text == '‹ 上頁'][0])+1

        max_page = last_page_no if max_page is None else max_page

        article_url_list = []

        for n, ii in enumerate(range(last_page_no - max_page + 1, last_page_no + 1)):
            if n == 0:
                article_url_list.extend(
                    [f"{self.base_url}{s.get('href')}" if not bool(re.search(string=s.get('href'), pattern=r'^http')) else s.get('href') for s in
                     soup.find(class_='r-list-container').find_all('a')])

            else:
                response = requests.get(f'{self.base_url}/bbs/{self.board_id}/index{ii}.html', headers=self.headers, timeout=20)

                soup = BeautifulSoup(response.text, 'lxml')

                for s in soup.find_all(class_='item'):
                    s.decompose()

                article_url_list.extend(
                    [f"{self.base_url}{s.get('href')}" if not bool(re.search(string=s.get('href'), pattern=r'^http')) else s.get('href') for s in
                     soup.find(class_='r-list-container').find_all('a')])

        return article_url_list

    def parse_article(self, article_url):
        article_id = re.search(string=article_url, pattern=r'/([A-z0-9\.]+)\.html$').group(1)

        a = requests.get(article_url, headers=self.headers, timeout=20)

        if a.status_code == 404:
            return pd.DataFrame(), pd.DataFrame()

        soup = BeautifulSoup(a.text, 'lxml')

        meta_dict = self._extract_meta_data(soup)

        author = meta_dict.get('作者', '')
        board_name = meta_dict.get('看板', '')
        title = meta_dict.get('標題', '')
        created_at = self._format_created_at(meta_dict.get('時間'))

        if created_at is None:
            return pd.DataFrame(), pd.DataFrame()

        self._remove_metaline_elements(soup)

        content = soup.find('div', {'id': 'main-content'}).text.strip().split('\n')

        p = self._find_separator_position(content)

        if p is None:
            print(f'Check website for unexpected format of content: {article_url}')
            return pd.DataFrame(), pd.DataFrame()

        comment = content[p+1:]
        content = content[:p+1]

        IP = self._extract_ip_from_content(content)

        content = [s for s in content if s != '']
        if content[-1] == '--':
            content = content[:-1]

        content = '\n'.join(content).strip()

        article = pd.DataFrame({
            'board_id': self.board_id,
            'article_id': article_id,
            'author': author,
            'board': board_name,
            'title': title,
            'content': content,
            'created_at': created_at,
            'ip': IP,
            'article_url': article_url,
        }, index=[0])

        article_additional_info, comment_df = self._parse_comments(comment, article_id)
        
        article=pd.merge(left=article, right=article_additional_info,on=['board_id','article_id'],how='left')
        
        return article, comment_df

    def _extract_meta_data(self, soup):
        meta_dict = {}

        for i, j in zip(soup.find_all(class_='article-meta-tag'), soup.find_all(class_='article-meta-value')):
            meta_dict[i.text] = j.text.strip()

        return meta_dict

    def _format_created_at(self, time_str):
        try:
            created_at = format(datetime.datetime.strptime(time_str, '%a %b %d %H:%M:%S %Y'), '%Y-%m-%d %H:%M:%S')
            return created_at
        except Exception as e:
            print(e)
            return None

    def _remove_metaline_elements(self, soup):
        for s in soup.find_all(class_=re.compile('article-metaline')):
            s.decompose()

    def _remove_item_elements(self, soup):
        for s in soup.find_all(class_='item'):
            s.decompose()

    def _find_separator_position(self, content):
        p = [pos for pos, s in enumerate(content) if re.search(string=s, pattern=r'^※ (發信|開獎|編輯)站*')]

        if not p:
            p = [pos for pos, s in enumerate(content) if re.search(string=s, pattern=r'^※ [\u4e00-\u9fa5]+')]
            if len(p) == 1:
                p = p[0]
            else:
                return None
        else:
            p = p[0]

        return p

    def _extract_ip_from_content(self, content):
        try:
            IP = re.search(string=content[-1], pattern=r'\d+\.\d+\.\d+\.\d+').group(0)
            content = content[:-1]
        except:
            IP = ''

        return IP

    def _parse_comments(self, comment, article_id):
        comment_ip = []
        comment_time = []
        comment_tag = []
        comment_author = []
        comment_content = []

        for s in [s for s in comment if re.search(string=s, pattern=r'^(→|推|噓)')]:
            ipdatetime = re.search(string=s, pattern=r'(\d+\.\d+\.\d+\.\d+)* *\d+/\d+ \d*:*\d*$')

            if ipdatetime is None:
                ipdatetime = ''
            else:
                ipdatetime = ipdatetime.group(0)

            ip = re.search(string=ipdatetime, pattern=r'\d+\.\d+\.\d+\.\d+')
            comment_ip.append(ip.group(0) if ip else '')

            date = re.search(string=ipdatetime, pattern=r'\d{2}/\d{2} \d*:*\d*')
            comment_time.append(date.group(0) if date else '')

            if ipdatetime:
                s = re.sub(string=s, pattern=re.escape(ipdatetime), repl='')

            comment_content.append(s.split(':')[-1])
            comment_tag.append(s[0])
            comment_author.append(s.split(':')[0][2:])

        co = pd.DataFrame({
            'board_id': self.board_id,
            'article_id': article_id,
            'tag': comment_tag,
            'author': [w.strip() for w in comment_author],
            'ip': comment_ip,
            'comment_time': comment_time,
            'content': comment_content
        })

        co.loc[co.tag == '→', 'tag'] = '中立'

        comment_count = co.shape[0]
        like_count = co[co.tag == '推'].shape[0]
        dislike_count = co[co.tag == '噓'].shape[0]
        neutral_count = co[co.tag == '中立'].shape[0]

        tm = format(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')

        co['create_time'] = tm
        article_df = pd.DataFrame({
            'board_id': self.board_id,
            'article_id': article_id,
            'create_time': tm,
            'comment_count': comment_count,
            'like_count': like_count,
            'dislike_count': dislike_count,
            'neutral_count': neutral_count
        },index=[0])

        return article_df, co

    def is_leap_year(self, year):
        year = int(year)
        """Check if a year is a leap year."""
        return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


    def estimate_time(self, comment_time, article_time):
        year = article_time[:4]
        t = f"{year}/{comment_time}"

        try:
            r = datetime.datetime.strptime(t, '%Y/%m/%d %H:%M').strftime('%Y-%m-%d %H:%M:%S')
        except ValueError as e:
            if 'day is out of range for month' in str(e) and comment_time.startswith("02/29"):
                # Check if the year is a leap year
                if not self.is_leap_year(year):
                    # If it's not a leap year, set to February 28
                    t = f"{year}/02/28{comment_time[5:]}"
                    r = datetime.datetime.strptime(t, '%Y/%m/%d %H:%M').strftime('%Y-%m-%d %H:%M:%S')
            else:
                # Handle other ValueError exceptions
                r = datetime.datetime.strptime(t, '%Y/%m/%d %H::%S').strftime('%Y-%m-%d %H:%M:%S')
            
        return r

    def revise_data(self, dat):
        dat = dat.sort_values(['estimate_time'])
        dat['content'] = [s.strip() for s in dat.content]

        t = np.array(dat.estimate_time.astype('datetime64[ns]').tolist())

        if len(t) == 1:
            return dat.drop(['created_at'], axis=1)
        else:
            t = t[1:] - t[:(len(t)-1)]
            t = [re.search(string=s, pattern=r'\d+ days \d+:\d+:\d+$').group(0) for s in t.astype('str')]

            n_day = [re.search(string=s, pattern=r'(\d+) days').group(1) for s in t]
            n_hour = [re.search(string=s, pattern=r'days (\d+)').group(1) for s in t]
            n_minute = [re.search(string=s, pattern=r'days \d+:(\d+)').group(1) for s in t]

            deter = []

            for i, j, k in zip(n_day, n_hour, n_minute):
                i = int(i)
                j = int(j)
                k = int(k)

                if i > 0 or j > 0 or k > 2:
                    deter.append(False)
                else:
                    deter.append(True)

            dat['deter'] = deter + [False]  # 最後一句一定沒有下一句
            dat = dat.reset_index(drop=True)

            cut_point = dat[~dat.deter].index.tolist()

            dat['group'] = pd.cut(x=dat.index, bins=[-1] + cut_point, right=True,
                                  labels=[s for s in range(0, len(cut_point))])

            revise = []

            for i in dat.group.unique():
                temp = dat[dat.group == i].copy().reset_index(drop=True)
                temp.loc[0, 'content'] = ''.join(temp.content.tolist())
                revise.append(temp.iloc[[0], :])

            revise = pd.concat(revise).reset_index(drop=True)
            revise = revise.drop(['deter', 'group', 'created_at'], axis=1)

            return revise

    def update_table(self, df, table_name, diff_table_name, primary_keys, log_record=False):
        # Step 1: Truncate and insert data into temp table
        cursor.execute(f'TRUNCATE TABLE {diff_table_name}')
        conn.commit()

        df.loc[:,primary_keys].to_sql(diff_table_name, engine, if_exists='append', index=False)

        # Step 2: Delete already existing data
        primary_key_conditions = ' AND '.join([f'tb.{pk} = tb2.{pk}' for pk in primary_keys])
        delete_sql = f"""
            DELETE tb
            FROM {table_name} tb
            WHERE EXISTS (
                SELECT 1
                FROM {diff_table_name} tb2
                WHERE {primary_key_conditions}
            )
        """
        delete_count=cursor.execute(delete_sql)
        conn.commit()

        # Step 3: Insert new data
        df.to_sql(table_name, engine, index=False, if_exists='append')

        if log_record:
            self.delete_count+=delete_count
            self.insert_count+=df.shape[0]

    def etl_process(self):
        try:
            for i in range(0,len(self.board)):
                parent_id=self.board['id'].iloc[i]
                self.board_id=self.board['board_id'].iloc[i]
                max_page=self.board['crawler_page'].iloc[i]
                
                url_lists=self.fetch_article_urls(max_page=max_page)
                url_lists=np.unique(url_lists).tolist()

                article_data=[]
                comment_data=[]

                for url in url_lists:
                    try:
                        article,comment=self.parse_article(article_url=url)
                    except Exception as e:
                        print(f'Error ptt article url : {url}', '\n', e)
                        continue
                    article_data.append(article)
                    comment_data.append(comment)
                
                article_data=[part for part in article_data if len(part) != 0]
                comment_data=[part for part in comment_data if len(part) != 0]

                if len(article_data)>0:
                    article_data=pd.concat(article_data)
                    article_data=article_data[article_data.title.map(len) < 100]
                    article_data=article_data[~article_data.duplicated(subset=['article_id'])]
                    article_data=article_data.reset_index(drop=True)
                    
                    article_data['parent_id']=parent_id
                    self.update_table(article_data, 'ptt_article', 'ptt_article_diff', ['article_id'],log_record=True)

                if len(comment_data)>0:
                    comment_data=pd.concat(comment_data)
                    comment_data=comment_data.reset_index(drop=True)

                    #依照相同時間(2分內)、作者，合併留言成一則留言(PTT會將長留言分成好幾段)
                    #刪除沒有留言時間或留言的文章，且留言隔式有誤也刪除
                    comment_data=comment_data[~comment_data.content.isnull()]

                    comment_data=comment_data[~(comment_data['comment_time'].map(len) != 11)]

                    comment_data=pd.merge(left=comment_data,right=article_data.loc[:,['article_id','created_at']],
                            on=['article_id'],how='left')

                    comment_data['estimate_time']=[self.estimate_time(i,j) for i,j in zip(comment_data.comment_time,
                                                                                            comment_data.created_at)]

                    comment_data=comment_data[~comment_data.duplicated(subset=['author', 'estimate_time'])]
                    
                    comment_data=comment_data.sort_values(['author','estimate_time'])

                    comment_data=comment_data.groupby(['author']).apply(self.revise_data).reset_index(drop=True)   
                
                    self.update_table(comment_data, 'ptt_comment', 'ptt_comment_diff', ['author', 'estimate_time'])
                
                print(f'Done crawler ptt board : {self.board_id}')
            
            log_record.set_delete_count(self.delete_count)
            print(f'PTT 刪除資料筆數 : {self.delete_count}')
            log_record.set_insert_count(self.insert_count)
            print(f'PTT 新增資料筆數 : {self.insert_count}')
            after_count=pd.read_sql_query(f'select count(1) as N from {table_name}',engine)['N'].iloc[0]  
            log_record.set_after_count(after_count)
            log_record.success = 1
            
            print(f'PTT 爬蟲後筆數 : {after_count}')
            print('收集成功!')
        except Exception as e:
            log_record.raise_error(repr(e))
            print('任務失敗')
            raise e        
        finally:
            log_record.insert_to_log_record()
            conn.close()
            engine.dispose()

@task_wrapper
def collect_all_ptt():
    process=PttScraper()
    process.etl_process()
