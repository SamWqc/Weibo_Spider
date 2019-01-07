# -*- coding:utf-8 -*-
import sys

sys.path.append("..")
import re
import time
import requests
from bs4 import BeautifulSoup
from tools.Date_Process import time_process
from tools.Emoji_Process import filter_emoji
from tools.Mysql_Process import mysqlHelper
from tools.Number_Process import num_process
from search_spider.hour_fenge import hour_fenge
from tools.Mysql_Process import get_db

url_template = 'https://s.weibo.com/weibo?q={}&typeall=1&suball=1&timescope=custom:{}:{}&Refer=g&page={}'  # 要访问的微博搜索接口URL

"""抓取关键词某一页的数据"""


def fetch_weibo_data(keyword, start_time, end_time, page_id):
    resp = requests.get(url_template.format(keyword, start_time, end_time, page_id))
    soup = BeautifulSoup(resp.text, 'lxml')
    all_contents = soup.select('.card-wrap')

    wb_count = 0
    mblog = []  # 保存处理过的微博
    for card in all_contents:
        if (card.get('mid') != None):  # 如果微博ID不为空则开始抓取
            wb_username = card.select_one('.txt').get('nick-name')  # 微博用户名
            href = card.select_one('.from').select_one('a').get('href')
            re_href = re.compile('.*com/(.*)/.*')
            wb_userid = re_href.findall(href)[0]  # 微博用户ID
            wb_content = card.select_one('.txt').text.strip()  # 微博内容
            wb_create = card.select_one('.from').select_one('a').text.strip()  # 微博创建时间
            wb_url = 'https:' + str(card.select_one('.from').select_one('a').get('href'))  # 微博来源URL
            wb_id = str(card.select_one('.from').select_one('a').get('href')).split('/')[-1].split('?')[0]  # 微博ID
            wb_createtime = time_process(wb_create)
            wb_forward = str(card.select_one('.card-act').select('li')[1].text)  # 微博转发数
            wb_forwardnum = num_process(wb_forward)
            wb_comment = str(card.select_one('.card-act').select('li')[2].text)  # 微博评论数
            wb_commentnum = num_process(wb_comment)
            wb_like = str(card.select_one('.card-act').select_one('em').text)  # 微博点赞数

            if (wb_like == ''):  # 点赞数的处理
                wb_likenum = '0'
            else:
                wb_likenum = wb_like

            blog = {'wb_id': wb_id,  # 生成一条微博记录的列表
                    'wb_username': wb_username,
                    'wb_userid': wb_userid,
                    'wb_content': wb_content,
                    'wb_createtime': wb_createtime,
                    'wb_forwardnum': wb_forwardnum,
                    'wb_commentnum': wb_commentnum,
                    'wb_likenum': wb_likenum,
                    'wb_url': wb_url
                    }
            mblog.append(blog)
            wb_count = wb_count + 1  # 表示此页的微博数

    print("--------- 正在爬取第%s页 --------- " % page_id + "当前页微博数：" + str(wb_count))
    return mblog


"""抓取关键词多页的数据"""


def fetch_pages(keyword, start_time, end_time):
    resp = requests.get(url_template.format(keyword, start_time, end_time, '1'))
    soup = BeautifulSoup(resp.text, 'lxml')
    if (str(soup.select_one('.card-wrap').select_one('p').text).startswith('抱歉')):  # 此次搜索条件的判断，如果没有相关搜索结果！退出...
        print("此次搜索条件无相关搜索结果！\n请重新选择条件筛选...")
        return
    try:
        page_num = len(soup.select_one('.m-page').select('li'))  # 获取此时间单位内的搜索页面的总数量，
        # print(page_num)
        page_num = int(page_num)
        print(start_time + ' 到 ' + end_time + " 时间单位内搜索结果页面总数为：%d" % page_num)
    except Exception as err:
        page_num = 1

    mblogs = []  # 此次时间单位内的搜索全部结果先临时用列表保存，后存入数据库
    for page_id in range(page_num):
        page_id = page_id + 1
        try:
            mblogs.extend(fetch_weibo_data(keyword, start_time, end_time, page_id))  # 每页调用fetch_data函数进行微博信息的抓取
        except Exception as e:
            print(e)

    # 保存到mysql数据库
    mh = mysqlHelper(get_db()[0], get_db()[1], get_db()[2], get_db()[3], get_db()[4], int(get_db()[5]))
    sql = "insert into keyword_weibo(wb_id,wb_username,wb_userid,wb_content,wb_createtime,wb_forwardnum,wb_commentnum,wb_likenum,wb_url,keyword) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    mh.open();
    for i in range(len(mblogs)):
        mh.cud(sql, (
            mblogs[i]['wb_id'], mblogs[i]['wb_username'], mblogs[i]['wb_userid'], filter_emoji(mblogs[i]['wb_content']),
            mblogs[i]['wb_createtime'], mblogs[i]['wb_forwardnum'], mblogs[i]['wb_commentnum'], mblogs[i]['wb_likenum'],
            mblogs[i]['wb_url'], keyword))
    mh.tijiao();
    mh.close()


if __name__ == '__main__':
    keyword = input("请输入要搜索的关键字：")
    start_time = input("请输入要查询的开始时间：")
    end_time = input("请输入要查询的结束时间：")

    time_start_jishi = time.time()
    hour_all = hour_fenge(start_time, end_time)
    i = 0
    while i < len(hour_all):
        fetch_pages(keyword, hour_all[i][0], hour_all[i][1])
        print(hour_all[i][0] + ' 到 ' + hour_all[i][1] + ' 时间单位内的数据爬取完成！\n')
        i = i + 1
    time_end_jishi = time.time()

    print('本次操作数据全部爬取成功，爬取用时秒数:', (time_end_jishi - time_start_jishi))
