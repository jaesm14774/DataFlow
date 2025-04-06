import re
import requests
import pandas as pd
import numpy as np
import datetime
import random
import time
from lib.get_sql import get_sql
from lib.log_process_execution import BaseLogRecord
from common.config import *

app_name = 'youtube'
database_name = 'crawler'
table_name = 'youtube_video'

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
print(f'Youtube影片，處理前總數為 : {before_count}')

youtube_key_list=pd.read_csv(youtube_token_path)['token'].tolist()

class YoutubeCrawler():
    bool_comment_comment=True
    comment_max_page=1
    comment_comment_max_page = 1
    
    def __init__(self):
        self.youtube_key_list=youtube_key_list
        self.domain_url='https://youtube.googleapis.com/youtube/v3'
        self.n_query=0
        
        self.channel=pd.read_sql_query(f"""
            select id, channel_id, name, crawler_page 
            from youtube_channel where status=1
        """,engine)
        self.delete_count = 0
        self.insert_count = 0

    #抓取頻道資訊
    def get_channel_info(self,channel_id):
        self.channel_id=channel_id
        
        youtube_key=random.choice(self.youtube_key_list)
        
        domain_url=self.domain_url
        
        url=f'{domain_url}/channels?part={"snippet,statistics"}&id={channel_id}&key={youtube_key}'

        #get json format of channel info
        a=requests.get(url, timeout=20).json()
        
        self.n_query=self.n_query+1
        
        if a['pageInfo']['totalResults'] == 0:
            print(f'Not find any channel for channel id = {channel_id}')
        elif a['pageInfo']['totalResults'] == 1:
            #channel basic info

            a=a['items'][0]
            kind=a['kind']
            channel_name=a['snippet']['title']
            channel_description=a['snippet']['description']
            if 'customUrl' not in a['snippet'].keys():
                fake_id=channel_id
            else:
                fake_id=a['snippet']['customUrl']
                
            created_at=a['snippet']['publishedAt']
            #2020-02-20T08:41:41.734861Z
            created_at=re.sub(string=created_at,pattern=r'\.\d+Z$',repl='Z')
            created_at=format(datetime.datetime.strptime(created_at,'%Y-%m-%dT%H:%M:%SZ')+datetime.timedelta(hours=8),'%Y-%m-%d %H:%M:%S')

            image_url=a['snippet']['thumbnails']['medium']['url']
            
            if 'country' in a['snippet'].keys():
                country=a['snippet']['country']
            else:
                country=''

            #channel statistic info
            view_count=a['statistics']['viewCount']
            
            if 'subscriberCount' in a['statistics'].keys():
                subscriber_count=a['statistics']['subscriberCount']
            else:
                subscriber_count=np.nan
                
            hidden_subscriber_count=a['statistics']['hiddenSubscriberCount']
                
            video_count=a['statistics']['videoCount']

            return pd.DataFrame({
                'kind':kind,
                'channel_id':channel_id,
                'fake_id':fake_id,
                'name':channel_name,
                'description':channel_description,
                'created_at':created_at,
                'image_url':image_url,
                'country':country,
                'view_count':view_count,
                'subscriber_count':subscriber_count,
                'hidden_subscriber_count':hidden_subscriber_count,
                'video_count':video_count,
            },index=[0])
        else:
            raise RuntimeError(f'難道channel_id不是unique_id?請檢查:{channel_id}')
        
    #抓取channel的video id list
    def get_videolist(self,channel_id,max_page=1):
        """
        The propose of function is to get last videolist(video id) for 1 page.
        If max_page is None then to get all.

        channel_id: Unique channel id for youtube
        max_page: Get the last <max_page> th page for video id. If want all,then set max_page=None
        """
        youtube_key=random.choice(self.youtube_key_list)
        
#         print('now:',youtube_key)
        
        domain_url=self.domain_url
        
        session=requests.session()
        #首先須先拿到upload_id
        url=f'https://www.googleapis.com/youtube/v3/channels?part=contentDetails&id={channel_id}&key={youtube_key}'

        a=session.get(url).json()
        
        self.n_query=self.n_query+1

        #沒有上傳任何影片
        if 'items' not in a.keys():
            return []
        
        upload_id=a['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        #抓取全部的影片

        #google容忍一次最多可以收集資料的數量
        max_result=50

        url=f'{domain_url}/playlistItems?part=snippet&maxResults={max_result}&playlistId={upload_id}&key={youtube_key}'

        n_page=1

        if max_page is None:
            max_page=9999

        #判斷是否跳出迴圈
        deter=0

        v_id=[]
        #收集資訊皆忽略，只需要保留videoid，後來會用video api得到更詳細的資訊
        while n_page<= max_page:

            if n_page == 1:
                a=session.get(url).json()
                
                self.n_query=self.n_query+1

                #假設發生錯誤
                if 'error' in a.keys():
                    return []
                
                total_n=a['pageInfo']['totalResults']

                #找到最終頁數，並更新max_page
                temp_n=int(np.ceil(total_n/max_result))

                if max_page<temp_n:
                    max_page=max_page
                else:
                    max_page=temp_n
            else:
                if nextPageToken is not None:
                    a=session.get(f'{url}&pageToken={nextPageToken}').json()
                    
                    self.n_query=self.n_query+1
                else: #沒有下一頁
                    deter=1

            #沒有下一頁跳出迴圈
            if deter == 1 and n_page != 1:
                break  
                
            if 'nextPageToken' in a.keys():
                nextPageToken=a['nextPageToken']
            else:
                nextPageToken=None

            v_id.extend([s['snippet']['resourceId']['videoId'] for s in a['items']])

            n_page=n_page+1

#             print(f'Done page:{n_page-1} and total(page,result)={(max_page,total_n)}. Now token is {nextPageToken}')       

        return v_id
    
    #抓取特定影片資訊(可以一次吃多個，最多50個，video id 必須要是list type)
    def get_videoinfo(self,video_id):
        youtube_key=random.choice(self.youtube_key_list)
        domain_url=self.domain_url
        
#         print('now:',youtube_key)
        
        assert isinstance(video_id,list),'video_id must be list type.Ex:["7wvNwOPprBE"]'
        
        #整理成最多50個一組
        n_stage=int(np.ceil(len(video_id)/50))
        video_r=[','.join(video_id[(i*50):(i+1)*50]) for i in range(0,n_stage)]
        
        v=[]
        
        for vid_r in video_r:
            url=f'{domain_url}/videos?key={youtube_key}&part=snippet,statistics,contentDetails&id={vid_r}'

            a=requests.get(url, timeout=20).json()
            
            self.n_query=self.n_query+1
            
            for part in a['items']:
                #video info
                kind=part['kind']
                created_at=part['snippet']['publishedAt']
                created_at=format(datetime.datetime.strptime(created_at,'%Y-%m-%dT%H:%M:%SZ')+datetime.timedelta(hours=8),'%Y-%m-%d %H:%M:%S')
                channel_id=part['snippet']['channelId']
                title=part['snippet']['title']
                description=part['snippet']['description']
                image_url=part['snippet']['thumbnails']['medium']['url']

                if 'tags' in part['snippet'].keys():
                    tags=';'.join(part['snippet']['tags'])
                else:
                    tags=''

                if 'defaultAudioLanguage' in part['snippet'].keys():
                    default_language=part['snippet']['defaultAudioLanguage']
                else:
                    default_language=''
                
                if 'liveBroadcastContent' in part['snippet'].keys():
                    live_broadcast_content=part['snippet']['liveBroadcastContent']
                else:
                    live_broadcast_content=None

                #video statistic info
                if 'viewCount' in part['statistics'].keys():
                    view_count=part['statistics']['viewCount']
                else:
                    view_count=np.nan 
  
                if 'dislikeCount' in part['statistics'].keys():
                    dislike_count=part['statistics']['dislikeCount']
                else:
                    dislike_count=np.nan               

                if 'likeCount' in part['statistics'].keys():
                    like_count=part['statistics']['likeCount']
                else:
                    like_count=np.nan

                if 'commentCount' in part['statistics'].keys():
                    comment_count=part['statistics']['commentCount']
                else:
                    comment_count=np.nan

                #補影片的長度
                if 'contentDetails' in part.keys():
                    duration=part['contentDetails'].get('duration', 0)
                else:
                    duration=None
                    
                v.append(pd.DataFrame({
                        'kind':kind,
                        'channel_id':channel_id,
                        'video_id':part['id'],
                        'created_at':created_at,
                        'title':title,
                        'description':description,
                        'image_url':image_url,
                        'tags':tags,
                        'default_language':default_language,
                        'live_broadcast_content':live_broadcast_content,
                        'view_count':view_count,
                        'like_count':like_count,
                        'dislike_count':dislike_count,
                        'comment_count':comment_count,
                        'duration':duration,
                    },index=[0]))
        
        if len(v) == 0:
            v=pd.DataFrame()
        else:
            v=pd.concat(v)
            v=v.reset_index(drop=True)
        
        return v
    
    #抓取影片留言資訊
    def get_commentinfo(self,video_id,max_page=1):
        youtube_key=random.choice(self.youtube_key_list)
        
#         print('now:',youtube_key)
        
        domain_url=self.domain_url
        
        #google容忍一次最多可以收集資料的數量
        max_result=100

        url=f'{domain_url}/commentThreads?key={youtube_key}&part=snippet&videoId={video_id}&maxResults={max_result}&textFormat=plainText&order=time'

        session=requests.session()

        if max_page is None:
            max_page=99999

        n_page=1

        #判斷是否跳出迴圈
        deter=0
        
        co=[]
        #依據頁數，收集留言
        while n_page<= max_page:
            if n_page == 1:
                a=session.get(url).json()
                
                self.n_query=self.n_query+1
                # 註:youtube在commentthread沒有提供正確的result count，因此無法找到最終頁數，更新max_page
            else:
                if nextPageToken is not None:
                    a=session.get(f'{url}&pageToken={nextPageToken}').json()
                    self.n_query=self.n_query+1
                else: #沒有下一頁
                    deter=1

            if 'nextPageToken' in a.keys():
                nextPageToken=a['nextPageToken']
            else:
                nextPageToken=None

            #沒有下一頁跳出迴圈
            if deter == 1 and n_page != 1:
                break
            
            #假設此影片禁用留言，會產生error
            if 'error' in a.keys(): 
                return pd.DataFrame()
            
            for part in a['items']:
                part=part['snippet']
                kind=part['topLevelComment']['kind']
                comment_id=part['topLevelComment']['id']
                content=part['topLevelComment']['snippet']['textOriginal']
                author=part['topLevelComment']['snippet']['authorDisplayName']
                author_image=part['topLevelComment']['snippet']['authorProfileImageUrl']
                author_channel=part['topLevelComment']['snippet']['authorChannelUrl']

                if 'authorChannelId' in part['topLevelComment']['snippet'].keys():
                    author_channel_id=part['topLevelComment']['snippet']['authorChannelId']['value']
                else:
                    author_channel_id=''

                like_count=part['topLevelComment']['snippet']['likeCount']

                created_at=part['topLevelComment']['snippet']['publishedAt']
                created_at=format(datetime.datetime.strptime(created_at,'%Y-%m-%dT%H:%M:%SZ')+datetime.timedelta(hours=8),'%Y-%m-%d %H:%M:%S')

                modified_time=part['topLevelComment']['snippet']['updatedAt']
                modified_time=format(datetime.datetime.strptime(modified_time,'%Y-%m-%dT%H:%M:%SZ')+datetime.timedelta(hours=8),'%Y-%m-%d %H:%M:%S')

                reply_count=part['totalReplyCount']

                co.append(pd.DataFrame({
                    'kind':kind,
                    'video_id':video_id,
                    'comment_id':comment_id,
                    'content':content,
                    'author':author,
                    'author_image':author_image,
                    'author_channel':author_channel,
                    'author_channel_id':author_channel_id,
                    'like_count':like_count,
                    'created_at':created_at,
                    'modified_time':modified_time,
                    'reply_count':reply_count
                },index=[0]))

            n_page=n_page+1

#             print(f'Done page:{n_page-1}. Now token is {nextPageToken}')
    

        if len(co)>0:
            co=pd.concat(co)
            co=co.reset_index(drop=True)
        else:
            co=pd.DataFrame()

        return co   
    
    #抓取留言的留言資訊
    def get_comment_comment(self,parent_id,max_page=1):
        youtube_key=random.choice(self.youtube_key_list)
        
        domain_url=self.domain_url
        
        max_result=100
        url=f'{domain_url}/comments?key={youtube_key}&part=snippet&maxResults={max_result}&parentId={parent_id}&textFormat=plainText'

        session=requests.session()

        if max_page is None:
            max_page=99999

        n_page=1

        #判斷是否跳出迴圈
        deter=0
        
        cc=[]
        #依據頁數，收集留言的留言
        while n_page<= max_page:
            if n_page == 1:
                a=session.get(url).json()
                
                self.n_query=self.n_query+1
                # 註:youtube在commentthread沒有提供正確的result count，因此無法找到最終頁數，更新max_page
            else:
                if nextPageToken is not None:
                    a=session.get(f'{url}&pageToken={nextPageToken}').json()
                    self.n_query=self.n_query+1
                else: #沒有下一頁
                    deter=1

            #沒有下一頁跳出迴圈
            if deter == 1 and n_page != 1:
                break  
                
            if 'nextPageToken' in a.keys():
                nextPageToken=a['nextPageToken']
            else:
                nextPageToken=None

            for part in a['items']:
                kind=part['kind']
                comment_id=part['id']
                content=part['snippet']['textOriginal']
                author=part['snippet']['authorDisplayName']
                author_image=part['snippet']['authorProfileImageUrl']
                author_channel=part['snippet']['authorChannelUrl']

                if 'authorChannelId' in part['snippet'].keys():
                    author_channel_id=part['snippet']['authorChannelId']['value']
                else:
                    author_channel_id=''

                like_count=part['snippet']['likeCount']

                created_at=part['snippet']['publishedAt']
                created_at=format(datetime.datetime.strptime(created_at,'%Y-%m-%dT%H:%M:%SZ')+datetime.timedelta(hours=8),'%Y-%m-%d %H:%M:%S')

                modified_time=part['snippet']['updatedAt']
                modified_time=format(datetime.datetime.strptime(modified_time,'%Y-%m-%dT%H:%M:%SZ')+datetime.timedelta(hours=8),'%Y-%m-%d %H:%M:%S')

                cc.append(pd.DataFrame({
                    'kind':kind,
                    'source_id':parent_id,
                    'comment_id':comment_id,
                    'content':content,
                    'author':author,
                    'author_image':author_image,
                    'author_channel':author_channel,
                    'author_channel_id':author_channel_id,
                    'like_count':like_count,
                    'created_at':created_at,
                    'modified_time':modified_time,
                },index=[0]))

            n_page=n_page+1

#             print(f'Done page:{n_page-1}. Now token is {nextPageToken}')

        if len(cc)>0:
            cc=pd.concat(cc)
            cc=cc.reset_index(drop=True)
        else:
            cc=pd.DataFrame()

        return cc      

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
            for i in range(0,len(self.channel)):
                parent_id=self.channel['id'].iloc[i]
                self.channel_id=self.channel['channel_id'].iloc[i]
                name=self.channel['name'].iloc[i]
                max_page=self.channel['crawler_page'].iloc[i]
                            
                #得到頻道資訊
                channel_info=self.get_channel_info(channel_id=self.channel_id)

                #得到影片id
                video_list=self.get_videolist(channel_id=self.channel_id,
                                              max_page=max_page)
                
                if len(video_list)>5:
                    video_list=video_list[:10]

                #沒有任一影片時
                if len(video_list) == 0:
                    V=[]
                    co=[]
                    cc=[]
                else:
                    #收集影片資訊
                    V=self.get_videoinfo(video_id=video_list)
                    if V.shape[0]>0:
                        for s in ['view_count','like_count','dislike_count','comment_count']:
                            V[s]=V[s].astype('float')  
                    else:
                        V=[]

                    #收集有留言影片的資訊
                    if not isinstance(V,pd.DataFrame):
                        co=[]
                    else:
                        temp=V[V.comment_count != 0]
                        
                        if temp.shape[0]>0:
                            co=[]

                            for vid in temp.video_id:
                                co.append(self.get_commentinfo(video_id=vid,
                                                               max_page=1))
                                time.sleep(random.randint(3,10)/10)

                            co=pd.concat(co)
                        else:
                            co=[]
                    
                    #是否收集留言的留言
                    if self.bool_comment_comment:
                        #收集留言的留言
                        if not isinstance(co,pd.DataFrame) or len(co) == 0:
                            cc=[]
                        else:
                            temp=co[co.reply_count>0]

                            if temp.shape[0]>0:
                                cc=[]

                                for cid in temp.comment_id:
                                    cc.append(self.get_comment_comment(parent_id=cid,
                                                                       max_page=self.comment_comment_max_page))

                                cc=pd.concat(cc)    
                            else:
                                cc=[]
                    else:
                        cc=[]
                
                print(f'Done of collecting {name} and total query_cost : {self.n_query} at this times.')

                if len(channel_info)>0:
                    channel_info=channel_info.reset_index(drop=True)
                    channel_info['create_date']=format(datetime.datetime.now(),'%Y-%m-%d')
                    
                    channel_info=channel_info.where(~channel_info.isnull(),None)
                    channel_info['parent_id']=parent_id

                    self.update_table(
                        df= channel_info, table_name='youtube_infochannel',
                        diff_table_name='youtube_infochannel_diff',
                        primary_keys=['channel_id','create_date']
                    )

                if len(V)>0:
                    V=V.reset_index(drop=True)
                    V=V.astype('object').where(pd.notnull(V),None)
                    V=V.replace(np.nan,None)
                    V['parent_id']=parent_id

                    self.update_table(
                        df= V, table_name='youtube_video',
                        diff_table_name='youtube_video_diff',
                        primary_keys=['video_id','parent_id'],
                        log_record=True
                    )

                if len(co)>0:
                    co=co.reset_index(drop=True)
                    co=co.astype('object').where(pd.notnull(co),None)
                    co=co.replace(np.nan,None)

                    self.update_table(
                        df= co, table_name='youtube_comment',
                        diff_table_name='youtube_comment_diff',
                        primary_keys=['comment_id','video_id']
                    )
                    
                if len(cc)>0:
                    cc=cc.reset_index(drop=True)
                    cc=cc.astype('object').where(pd.notnull(cc),None)
                    cc=cc.replace(np.nan,None)

                    self.update_table(
                        df= cc, table_name='youtube_commentcomment',
                        diff_table_name='youtube_commentcomment_diff',
                        primary_keys=['comment_id','source_id']
                    )
                
                time.sleep(5)
            
            log_record.set_delete_count(self.delete_count)
            print(f'Youtube 刪除資料筆數 : {self.delete_count}')
            log_record.set_insert_count(self.insert_count)
            print(f'Youtube 新增資料筆數 : {self.insert_count}')
            after_count=pd.read_sql_query(f'select count(1) as N from {table_name}',engine)['N'].iloc[0]  
            log_record.set_after_count(after_count)
            log_record.success = 1
            
            print(f'Youtube 爬蟲後筆數 : {after_count}')
            print('收集成功!')
        except Exception as e:
            log_record.raise_error(repr(e))
            print('任務失敗')
            raise e        
        finally:
            log_record.insert_to_log_record()
            conn.close()
            engine.dispose()

def collect_all_youtube():
    process=YoutubeCrawler()
    process.etl_process()