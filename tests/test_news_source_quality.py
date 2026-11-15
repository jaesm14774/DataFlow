"""Offline guards for observed publisher layouts and incorrect article fields."""
import datetime as dt
import ast
import importlib.util
import json
import os
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest
import requests
from bs4 import BeautifulSoup
import common.config as config

config.user_agent_path = str(Path(__file__).resolve().parents[1] / 'dags/common/user_agent.csv')
from news_ch.news_crawler.page_utils import article_metadata, published_time, scoped_text, validate_article
from news_ch.news_crawler import Anue, LibertyTimes, SETN, TVBS, ETtoday, EInfo, PanSci, Reporter, TheNewsLens, United


def response(html, url='https://example.test'):
    r = requests.Response()
    r.status_code = 200
    r._content = html.encode('utf-8')
    # Reproduces TVBS's wrong/missing HTTP charset while the HTML declares utf-8.
    r.encoding = 'ISO-8859-1'
    r.url = url
    return r


def crawler(cls):
    baseline = os.environ.get('NEWS_AUDIT_BASELINE')
    if baseline and cls in (Anue, LibertyTimes, SETN, TVBS, ETtoday, United):
        path = Path(baseline) / {'Anue':'anue','LibertyTimes':'libertytimes','SETN':'setn','TVBS':'tvbs','ETtoday':'ettoday','United':'united'}[cls.__name__]
        spec = importlib.util.spec_from_file_location('baseline_' + cls.__name__, path.with_suffix('.py'))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return getattr(module, cls.__name__)()
    return cls()


def parse(cls, html, url, monkeypatch, keyword='', tim=''):
    # For the regular source layouts the publisher sends UTF-8.
    r = response(html, url)
    if cls is not TVBS:
        r.encoding = 'utf-8'
    monkeypatch.setattr(requests, 'get', lambda *args, **kwargs: r)
    return crawler(cls).get_article_info(url, tim, '', keyword, '分類').iloc[0]


def test_anue_ignores_modal_h1_and_matches_article(monkeypatch):
    url = 'https://news.cnyes.com/news/id/6630550?exp=a'
    article = {'@type':'NewsArticle','url':url.split('?')[0], 'headline':'正確新聞標題',
               'datePublished':'2026-10-14T14:22:51Z','articleBody':'正確報導正文',
               'author':[{'name':'記者甲'}],'articleSection':'wd_stock','keywords':'國際,科技'}
    html = '<h1>...</h1><main id="article-container"><h1>正確新聞標題</h1><p>正確報導正文</p></main>'
    html += '<script type="application/ld+json">'+json.dumps(article,ensure_ascii=False)+'</script>'
    row = parse(Anue, html, url, monkeypatch)
    assert row['title'] == '正確新聞標題'
    assert row['created_at'] == '2026-10-14 22:22:51'
    assert row['category'] == 'wd_stock'


@pytest.mark.parametrize('wrapper', ['<article class="article"><div class="text">{}</div></article>',
                                     '<div class="whitecon article"><div class="text">{}</div></div>',
                                     '<div class="whitecon"><div class="text">{}</div></div>'])
def test_ltn_vertical_ignores_empty_template(wrapper, monkeypatch):
    html = '<h1>健康新聞</h1><meta property="article:published_time" content="2026-10-14T13:17:00Z">'
    html += '<div class="whitecon" data-desc="內文"><div class="text"></div></div>'
    html += wrapper.format('<p>這是實際正文。</p><p>請繼續往下閱讀...</p><p>這是第二段正文。</p>')
    row = parse(LibertyTimes,html,'https://health.ltn.com.tw/article/breakingnews/5606206',monkeypatch)
    assert '這是實際正文。' in row['content']
    assert '這是第二段正文。' in row['content']
    assert '請繼續往下閱讀' not in row['content']
    assert row['created_at'] == '2026-10-14 21:17:00'


@pytest.mark.parametrize('title,date', [('h1','<div class="page-date">2026/10/14 22:32</div>'),
                                      ('div class="newsTitle"','<time class="pageDate">2026/10/14 22:32</time>')])
def test_setn_vertical_uses_corroborated_wall_time(title,date,monkeypatch):
    end = title.split()[0]
    html = f'<{title}>正確子站標題</{end}>'+date+'<div id="ckuse"><p>子站真正正文。</p></div>'
    kw = '__setn_relative_age__=2分鐘前|observed_at=2026-10-14 22:34:00'
    row = parse(SETN,html,'https://health.setn.com/news/1922716',monkeypatch,kw)
    assert row['title']=='正確子站標題'
    assert row['created_at']=='2026-10-14 22:32:00'
    assert row['content']=='子站真正正文。'
    with pytest.raises(ValueError):
        parse(SETN,html,'https://health.setn.com/news/1922716',monkeypatch,'')


def test_tvbs_utf8_and_suffix_are_preserved(monkeypatch):
    data = {'@type':'NewsArticle','headline':'新聞開頭TVBS與末尾新聞│TVBS新聞網',
            'datePublished':'2026-10-14T12:00:00Z','articleBody':'正確中文正文','author':{'name':'作者'}}
    html = '<meta charset="utf-8"><script type="application/ld+json">'+json.dumps(data,ensure_ascii=False)+'</script>'
    row = parse(TVBS,html,'https://news.tvbs.com.tw/life/1234567',monkeypatch)
    assert row['title']=='新聞開頭TVBS與末尾新聞'
    assert row['content']=='正確中文正文'


def test_ettoday_stops_repeated_pages_and_sets_timeout(monkeypatch):
    publisher_day=dt.datetime.now(dt.timezone(dt.timedelta(hours=8)))
    html = f'<div class="part_list_2"><h3><span class="date">{publisher_day:%Y/%m/%d} 01:00</span><em class="tag">科技</em><a href="/news/{publisher_day:%Y%m%d}/1.htm">文章</a></h3></div>'
    r = response(html);r.encoding='utf-8'
    calls=[]
    monkeypatch.setattr(requests,'get',lambda *args,**kwargs:r)
    def post(*args,**kwargs):
        calls.append(kwargs)
        if len(calls)>2:
            raise RuntimeError('unbounded repeated pagination')
        return r
    monkeypatch.setattr(requests,'post',post)
    value=crawler(ETtoday).get_article_url_from()
    assert len(value)==1 and len(calls)==1
    assert calls[0]['timeout']==30


@pytest.mark.parametrize('html,fallback,expected',[
    ('<div class="logo_esg">ETtoday<span>ET ESG 永續雲</span><button>GO</button></div>','生活','ET ESG 永續雲'),
    ('<div class="star_logo">ETtoday星光雲</div>','影劇','ETtoday星光雲'),
    ('<div class="logo_site">ETtoday<a>選單一</a><a>選單二</a><button>GO</button></div>','生活','生活'),
    ('<meta property="article:section" content="永續"><div class="logo_esg">整段導覽</div>','生活','永續'),
])
def test_ettoday_category_excludes_navigation(html,fallback,expected):
    assert ETtoday._article_category(BeautifulSoup(html,'lxml'),fallback)==expected


def test_metadata_excludes_recommended_article():
    nodes=[{'@type':'NewsArticle','url':'https://site/a/2','headline':'錯誤推薦'},
           {'@type':'NewsArticle','url':'https://site/a/1','headline':'正確'}]
    soup=BeautifulSoup('<script type="application/ld+json">'+json.dumps(nodes)+'</script>','lxml')
    assert article_metadata(soup,'https://site/a/1?utm=ad')['headline']=='正確'
    assert article_metadata(soup,'https://site/a/3')=={}


def test_udn_literal_redirect_uses_actual_article_identity_and_body(monkeypatch):
    stub='https://udn.com/news/story/12806/9814351'
    original='https://udn.com/news/story/6839/9813826'
    landing='<meta property="og:title" content="文章"><meta property="og:description" content="只有摘要"><section class="article-content__wrapper"><p><script>window.location.href="'+original+'";</script></p></section>'
    html='<h1 class="article-content__title">文章</h1><meta property="article:published_time" content="2026-10-14T14:58:00+08:00"><div class="article-content__paragraph"><section><p>這是完整正文第一段。</p><p>這是完整正文最後一段。</p></section></div>'
    pages={stub:response(landing,stub),original:response(html,original)}
    calls=[]
    def get(url,**kwargs):
        calls.append(url)
        return pages[url]
    monkeypatch.setattr(requests,'get',get)
    row=crawler(United).get_article_info(stub,'2026-10-15 07:07:24','','','').iloc[0]
    assert calls==[stub,original]
    assert row['article_url']==original and row['article_id']=='6839/9813826'
    assert row['created_at']=='2026-10-14 14:58:00'
    assert row['content']=='這是完整正文第一段。\n這是完整正文最後一段。'


@pytest.mark.parametrize('script',['','<script>window.location.href="https://other.test/news/story/1/2";</script>'])
def test_udn_never_uses_description_as_article_body(monkeypatch,script):
    stub='https://udn.com/news/story/12806/9814351'
    r=response('<meta property="og:description" content="只有摘要">'+script,stub)
    monkeypatch.setattr(requests,'get',lambda *args,**kwargs:r)
    with pytest.raises(ValueError,match='no recognized public body'):
        United().get_article_info(stub,'2026-10-15 07:07:24','','','')


def test_rich_div_body_is_not_reduced_to_headings():
    root=BeautifulSoup('<article><div>第一段</div><h3>小標</h3><div>-----廣告，請繼續往下閱讀-----</div><div>第二段</div><figure>圖片说明</figure></article>','lxml').article
    assert scoped_text(root)=='第一段\n小標\n第二段'


def test_new_sources_use_scoped_body():
    soup=BeautifulSoup('<meta property="og:title" content="正確 - 報導者 The Reporter"><div id="article-body"><div class="article-page__ContentBlock"><div>完整報導正文</div></div><div>推薦捐款</div></div>','lxml')
    title,body,_,_=Reporter().parse_page(soup)
    assert title=='正確' and body=='完整報導正文'
    soup=BeautifulSoup('<section id="load_post_1"><h1>科學新聞</h1><div class="post-content-container"><p>真正正文</p></div></section><section id="load_post_2"><div class="post-content-container">別篇文章</div></section>','lxml')
    assert PanSci().parse_page(soup)[1]=='真正正文'


def test_reporter_keys_fit_existing_storage_without_losing_url_identity():
    source=Reporter()
    path='/a/2026-local-elections-ai-generated-contents-negatively-affect-elections'
    key=source.article_id(path)
    assert len(path.rsplit('/',1)[-1])>50
    assert len(key)<=50
    assert source.article_id(path+'/')==key
    assert source.article_id(path+'-updated')!=key
    assert PanSci().article_id('/archives/382368')=='382368'


def test_pansci_preserves_all_article_categories():
    soup=BeautifulSoup('<section id="load_post_1"><a class="single_top_cat">來自 YT</a><a class="single_top_cat">科學儀器</a><a class="single_top_cat">科技能源</a><h1>晶片文章</h1><div class="post-content-container">實際正文</div></section>','lxml')
    assert PanSci().parse_page(soup)[3]=='來自 YT;科學儀器;科技能源'


@pytest.mark.parametrize('redirect,canonical', [
    ('https://pansci.asia/archives/222',''),
    ('https://pansci.asia/archives/111','https://pansci.asia/archives/222'),
])
def test_rss_does_not_attach_other_article_to_original_key(monkeypatch,redirect,canonical):
    html=f'<link rel="canonical" href="{canonical}"><section id="load_post_1"><h1>別篇標題</h1><div class="post-content-container">別篇正文</div></section>'
    r=response(html,redirect)
    monkeypatch.setattr(requests,'get',lambda *args,**kwargs:r)
    with pytest.raises(ValueError,match='different|does not match'):
        PanSci().get_article_info('https://pansci.asia/archives/111','2026-10-14 10:00:00','','','')


@pytest.mark.parametrize('title,content,time,reason', [
    ('...', '正文', '2026-10-14 10:00:00','placeholder_title'),
    ('正確', '', '2026-10-14 10:00:00','empty_body'),
    ('亂碼\x91','正文','2026-10-14 10:00:00','invalid_text_encoding'),
    ('正確','正文','2026-10-15 10:00:00','future_published_time'),
    ('正確','正文','','invalid_published_time'),
    ('正確','正文','2026-10-14 10:00:00',None),
])
def test_article_validation(title,content,time,reason):
    frame=pd.DataFrame([{'title':title,'content':content,'created_at':time}])
    assert validate_article(frame,dt.datetime(2026,10,14,11))==reason


@pytest.mark.parametrize('metadata,expected',[
    ('<meta property="article:published_time" content="2026-10-14T10:26:00Z">','2026-10-14 18:26:00'),
    ('','2026-10-14 00:00:00'),
])
def test_tnl_publication_metadata_preserves_precision(metadata,expected,monkeypatch):
    html=metadata+'<h1>正確新聞</h1><section class="article-body"><p>真正正文。</p></section>'
    row=parse(TheNewsLens,html,'https://www.thenewslens.com/article/270568',monkeypatch,tim='2026-10-14')
    assert row['created_at']==expected


def test_batch_rejections_reconcile_and_staging_is_clean(monkeypatch):
    """Execute the production function with only the external DB boundary mocked."""
    path=Path(__file__).resolve().parents[1]/'dags/news_ch/all_news.py'
    tree=ast.parse(path.read_text(encoding='utf-8'))
    functions=[]
    for node in tree.body:
        if isinstance(node,ast.FunctionDef) and node.name in {
            '_source_row_counts','_source_removed_counts','_non_blank_news_text','collect_news'
        }:
            node.decorator_list=[]
            functions.append(node)
    connection,cursor,engine,record=Mock(),Mock(),Mock(),Mock()
    now=dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).replace(tzinfo=None)-dt.timedelta(hours=1)
    values=[('正確新聞','完整正文',now.isoformat()),('亂碼\x91','正文',now.isoformat()),
            ('...', '正文',now.isoformat()),('空內文','',now.isoformat()),
            ('無時間','正文',''),('未來','正文','2036-01-01 01:00:00')]
    urls=pd.DataFrame([{'source':'測試','article_url':str(i),'category':'科技','created_at':'','img':'','keyword':''} for i in range(len(values))])
    def get_info(article_url,**kwargs):
        title,content,stamp=values[int(article_url)]
        return pd.DataFrame([{'source':'測試','article_id':article_url,'title':title,
                            'content':content,'created_at':stamp,'category':'科技'}])
    staged={}
    monkeypatch.setattr(pd,'read_sql_query',lambda *args,**kwargs:urls.copy())
    monkeypatch.setattr(pd.DataFrame,'to_sql',lambda self,name,*args,**kwargs:staged.update({name:self.copy()}))
    namespace={'pd':pd,'datetime':dt,'validate_article':validate_article,'get_info':get_info,
               'taipei_now':lambda:dt.datetime.now()+dt.timedelta(hours=8),
               '_ensure_runtime':lambda:(connection,cursor,engine),'_default_log_record':record,
               '_date_only_news_sources':frozenset(),'task_dict':{1:Mock(_source='測試')},
               'clean_time_format':lambda value:published_time(value),'clean_text':lambda value,**kwargs:value}
    exec(compile(ast.Module(body=functions,type_ignores=[]),str(path),'exec'),namespace)
    receipt=namespace['collect_news']()
    source=receipt['sources'][0]
    assert (source['parsed_count'],source['valid_time_count'],source['accepted_count'])==(6,4,1)
    assert source['invalid_time_quarantined']==2
    assert source['article_quality_rejected']==2
    assert source['content_blank_or_whitespace_rejected']==1
    assert receipt['post_time_filter_rejections_reconciled'] is True
    assert source['accepted_categories']=={'科技':1}
    assert list(staged['news_temp']['title'])==['正確新聞']
    assert '_quality_rejected' not in staged['news_temp'].columns


def test_article_url_refresh_uses_dml_and_serialized_dag(monkeypatch):
    """Avoid exclusive metadata locks and races on the shared URL staging table."""
    root=Path(__file__).resolve().parents[1]
    module_path=root/'dags/news_ch/all_news.py'
    tree=ast.parse(module_path.read_text(encoding='utf-8'))
    function=next(node for node in tree.body
                  if isinstance(node,ast.FunctionDef) and node.name=='collect_article_url')
    function.decorator_list=[]

    connection,cursor,engine,record=Mock(),Mock(),Mock(),Mock()
    crawler_instance=Mock(_source='測試來源')
    crawler_instance.get_article_url_from.return_value=pd.DataFrame([{
        'source':'測試來源','article_url':'https://example.test/news/1',
        'category':'財經','created_at':'2026-10-15 20:00:00','img':'','keyword':'',
    }])
    staged={}
    monkeypatch.setattr(pd.DataFrame,'to_sql',
                        lambda frame,name,*args,**kwargs:staged.update({name:frame.copy()}))
    namespace={
        'pd':pd,'np':pytest.importorskip('numpy'),'is_valid_url':lambda value:value.startswith('https://'),
        'task_dict':{1:crawler_instance},
        '_ensure_runtime':lambda:(connection,cursor,engine),
        '_default_log_record':record,
    }
    exec(compile(ast.Module(body=[function],type_ignores=[]),str(module_path),'exec'),namespace)
    namespace['collect_article_url']()

    cursor.execute.assert_called_once_with('delete from article_url')
    connection.commit.assert_called_once_with()
    assert len(staged['article_url'])==1

    dag_path=root/'dags/main_news_ch_collect.py'
    dag_tree=ast.parse(dag_path.read_text(encoding='utf-8'))
    dag_call=next(item.context_expr for node in ast.walk(dag_tree)
                  if isinstance(node,ast.With)
                  for item in node.items
                  if isinstance(item.context_expr,ast.Call)
                  and getattr(item.context_expr.func,'id',None)=='DAG')
    active_runs=next(keyword.value for keyword in dag_call.keywords
                      if keyword.arg=='max_active_runs')
    assert ast.literal_eval(active_runs)==1


@pytest.mark.parametrize('url', [
    'https://www.setn.com/news/1922700',
    'https://star.setn.com/news/1922683',
    'https://health.setn.com/news/1922710',   # health vertical, seen in the live /viewall audit
    'https://fuhouse.setn.com/news/1922707',  # housing vertical, seen in the live /viewall audit
])
def test_setn_listing_keeps_every_publisher_vertical_it_really_links_to(url):
    import re
    from news_ch.news_crawler.page_utils import validate_publisher_url
    from news_ch.news_crawler.setn import SETN_ARTICLE_HOSTS
    assert validate_publisher_url(url, allowed_hosts=SETN_ARTICLE_HOSTS,
                                  article_path=re.compile(r'/(?:star/)?news/\d+', re.I)) == url


@pytest.mark.parametrize('url', [
    'https://evil.example/news/1922700',
    'https://health.setn.com.evil.example/news/1922710',
    'https://health.setn.com/about',
    'http://health.setn.com/news/1922710',
    'https://health.setn.com:8443/news/1922710',
])
def test_setn_listing_still_rejects_foreign_hosts_and_non_article_paths(url):
    import re
    from news_ch.news_crawler.page_utils import validate_publisher_url
    from news_ch.news_crawler.setn import SETN_ARTICLE_HOSTS
    with pytest.raises(ValueError):
        validate_publisher_url(url, allowed_hosts=SETN_ARTICLE_HOSTS,
                               article_path=re.compile(r'/(?:star/)?news/\d+', re.I))
