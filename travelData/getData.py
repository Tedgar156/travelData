from queue import Queue
import threading
import re
import time
import requests
from json import loads
import pymysql
from multiprocessing.dummy import Pool
from bs4 import BeautifulSoup
from bs4 import element

headers = {
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.221 Safari/537.36 SE 2.X MetaSr 1.0'
    }

def getAllUrl():
    #获取网页
    url = 'http://travel.qunar.com/place/'
    response = requests.get(url=url,headers=headers)
    soup = BeautifulSoup(response.text,'html.parser')
    # #获取数据
    # r  = '<li class=".*?"><a href="(.*?)" class="link" target="_blank">(.*?)</a></li>'
    # r = re.compile(r)
    # text = re.findall(r,response.text)
    # print(text)
    # return text
    texts = []
    if soup.find('div',class_='contbox current') != None:
        list = soup.find('div',class_='contbox current').find_all('a',class_='link')
        for li in list:
            text = []
            text.append(li.attrs['href'])
            text.append(li.text)
            texts.append(text)
    return texts
#处理请求到的网页返回数据，拿到景点的简单介绍
def extractAttract(city,content,queue):
    soup = BeautifulSoup(content, 'html.parser')
    if soup.find('ul', class_='list_item clrfix') != None:
        list = soup.find('ul', class_='list_item clrfix').find_all('li')
        # 数据处理
        for li in list:
            # 需要放入队列中的信息，方便获取景点的详细信息
            infor = {}
            # 需要放入数据库中的信息
            mysql = {}
            if li.find('a', class_='titlink') != None:
                alist = li.find('a', class_='titlink')
            if alist.find('span', class_='cn_tit') != None:
                attraction = alist.find('span', class_='cn_tit').text
            else:
                attraction = ''
            if alist.attrs['href'] != None:
                url = alist.attrs['href']
            else:
                url = ''
            mysql['attraction'] = attraction
            infor['attract'] = attraction
            infor['url'] = url
            if li.find('span', class_='cur_star') != None:
                star = li.find('span', class_='cur_star').attrs['style'].split(':')[1][:-1]
            else:
                star = ''
            mysql['star'] = star
            if li.find('span', class_='ranking_sum').text != '':
                ranks = li.find('span', class_='ranking_sum')
                rank = ranks.text
                rank += ranks.find('span').text
            else:
                rank = ''
            mysql['rank'] = rank
            if li.find('div',class_='desbox'):
                briefIntru = li.find('div',class_='desbox').text
            else:
                briefIntru = ''
            mysql['briefintru'] = briefIntru
            #将景点的简略介绍放入数据库中
            addBriefsceneMysql(city,mysql)
            #将需要获取景点的详细信息，放入队列中
            queue.put(infor)
def addBriefscene(texts,queue):
    if texts[0] != '':
        # 处理 url 获取一个地方的全部景点
        url = texts[0] + '-jingdian'
        response = requests.get(url=url, headers=headers)
        extractAttract(texts[1], response.text, queue)
        for num in range(2, 50):
            tempulr = url + '-1-' + str(num)
            time.sleep(1)
            response = requests.get(url=tempulr, headers=headers, allow_redirects=False)
            if response.status_code == 200:
                extractAttract(texts[1], response.text, queue)
            else:
                break
# 将简略的景点信息放入数据库中
def addBriefsceneMysql(city,infor):
    # 连接 mysql
    db = pymysql.connect("localhost", "root", "123456", "data")
    cursor = db.cursor()
    # 数据库的相关操作
    sql = "insert into briefscene(city,attract,star,briefintru,rank) values (%s,%s,%s,%s,%s)"
    #执行数据库语句
    cursor.execute(sql, (city, infor['attraction'], infor['star'], infor['briefintru'], infor['rank']))
    #提交数据库
    db.commit()
    print('addBriefscene done')
    db.close()
# 将详细的景点信息放入数据库中
def addDetailsceneMysql(detail):
    # 连接 mysql
    db = pymysql.connect("localhost", "root", "123456", "data")
    cursor = db.cursor()
    sql = "insert into detailscene(attract,overview,ticket,season,traffic,tip,address,phone,hours,website,scene) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cursor.execute(sql, (detail['attract'], detail['overview'], detail['ticket'], detail['season'], detail['traffic'], detail['tip'],detail['address'], detail['phone'], detail['hours'], detail['website'], detail['scene']))
    db.commit()
    print('addDetailsceneMysql done')
    db.close()
# 一个景点的图片
def addImgsceneMysql(img):
    # 连接 mysql 并且将写入格式改为 utf8mb4 。四个字节形式
    db = pymysql.connect("localhost", "root", "123456", "data",charset='utf8mb4')
    cursor = db.cursor()
    sql = "insert into imgscene(attract,intro,url,smallimage,iconimage,middleimage,bigmiddleimage,bigimage) values (%s,%s,%s,%s,%s,%s,%s,%s)"
    cursor.execute(sql, (img['attract'], img['intro'], img['url'], img['smallImageURL'], img['iconImageURL'], img['middleImageURL'],img['bigMiddleImageURL'], img['bigImageURL']))
    db.commit()
    print('addImgsceneMysql done')
    db.close()
# 这个类继承 threading.Thread 类即为多线程类，需要执行的操作即为 run 里面。拿到放入队列中的数据进行获取景点的详细信息操作。
class addDetailscene(threading.Thread):
    def __init__(self,queue):
        threading.Thread.__init__(self)
        self.queue = queue
    def run(self):
        time.sleep(1)
        while not self.queue.empty():
            city = self.queue.get()
            url_queue.task_done()
            if city['url'] != '':
                addDetailscene3(city)
# 处理景点详细信息数据
def extractDetail(conten,attract):
    # 保存需要放入数据库中的数据
    infor = {}
    soup = BeautifulSoup(conten,'lxml')
    detail = soup.find('div',id='detail_box')
    if isinstance(detail,element.Tag):
        overview = detail.find('div',class_='e_db_content_box').text.replace('\n','')
    else:
        overview = ''
    if detail.find('div',class_='e_summary_list clrfix') != None:
        span = detail.find('div',class_='e_summary_list clrfix').find_all('dl')
        for i in span:
            d = i.find('dt').text
            if d == '地址:':
                address = i.find('span').text
            else:
                address = ''
            if d == '电话:':
                phone = i.find('span').text
            else:
                phone = ''
            if d == '开放时间:':
                hours = i.find('span').text
            else:
                hours = ''
    else:
        address = ''
        phone = ''
        hours = ''
    if detail.find('dd',class_='m_desc_isurl') != None:
        website = detail.find('dd',class_='m_desc_isurl').text
    else:
        website = ''
    if detail.find('div', class_='e_db_content_box e_db_content_dont_indent') != None:
        scene = detail.find('div', class_='e_db_content_box e_db_content_dont_indent').text.replace('\n','')
    else:
        scene = ''
    if detail.find('div', class_='e_ticket_info') != None:
        ticket = detail.find('div', class_='e_ticket_info').text.replace('\n','')
    else:
        ticket = ''
    if detail.find('div', id='lysj') != None:
        season = detail.find('div', id='lysj').find('div',class_='e_db_content_box e_db_content_dont_indent').text.replace('\n','')
    else:
        season = ''
    if detail.find('div', class_='b_detail_section b_detail_traffic') != None:
        traffic = detail.find('div', class_='b_detail_section b_detail_traffic').find('div',class_='e_db_content_box e_db_content_dont_indent').text.replace('\n','')
    else:
        traffic = ''
    if detail.find('div', class_='b_detail_section b_detail_tips') != None:
        tip = detail.find('div', class_='b_detail_section b_detail_tips').find('div',class_='e_db_content_box e_db_content_dont_indent').text.replace('\n','')
    else:
        tip = ''
    infor['attract'] = attract
    infor['overview'] = overview
    infor['ticket'] = ticket
    infor['season'] = season
    infor['traffic'] = traffic
    infor['tip'] = tip
    infor['address'] = address
    infor['phone'] = phone
    infor['hours'] = hours
    infor['website'] = website
    infor['scene'] = scene
    # 将景点的详细信息放入数据库中
    addDetailsceneMysql(infor)
#将景点的图片信息处理
def extractImg(city):
    # 获取景点信息的图片
    id = re.findall(r"\d{5,7}", city['url'])
    time.sleep(1)
    url = "http://travel.qunar.com/place/api/poi/image?offset=0&limit=100&poiId={}".format(id[0])
    response = requests.get(url=url, headers=headers)
    content = loads(response.content)
    for data in content['data']:
        info = {}
        info['attract'] = city['attract']
        info['intro'] = data['intro']
        info['url'] = data['url']
        info['smallImageURL'] = data['smallImageURL']
        info['iconImageURL'] = data['iconImageURL']
        info['middleImageURL'] = data['middleImageURL']
        info['bigMiddleImageURL'] = data['bigMiddleImageURL']
        info['bigImageURL'] = data['bigImageURL']
        addImgsceneMysql(info)
#获取景点的图片信息
def addDetailscene3(city):
    time.sleep(1)
    #获取景点详细信息
    response = requests.get(url=city['url'],headers=headers)
    extractDetail(response.content,city['attract'])
    extractImg(city)
'''
获取数据思想：
1. 先自己在浏览器上搜索网站，查看有没有自己想要的数据。确定数据来源 url
2. 用 python 模拟请求网页，获取返回数据。
3. 用正则 re 或者用 beautifulsoup 分支形式拿到自己想要的数据即可。
-----------------------------------------------------------------
查看一个 url 获取到的数据
 在浏览器上进入开发者模式，按 F12 。
 在 Network 中刷新网页就会看到请求这一个网页时请求的数据
 一般 Js、css 可以不用管，就直接查看本网页的请求就行了。很多网站都有反爬，所以需要使用 headers。
'''
'''
这篇文章我是先 getAllUrl() 拿到所有城市的 url。
然后启动线程池 pool。加快速度去获取一个城市中的景点信息，并将景点的 url 放入队列中。
再次启动多线程，拿到放入队列中的景点 url，从景点的 url 中获取景点的详细信息。
'''
if __name__ == '__main__':
    #初始化队列和线程池
    url_queue = Queue()
    pool = Pool(10)
    #获取主页中查找到的 Url
    texts = getAllUrl()
    # 使用线程池获取详细景点的 url，和景点的简单介绍
    for i in texts:
        pool.apply_async(addBriefscene,args=(i,url_queue))
    #等十秒钟给加入队列中
    time.sleep(10)
    #启动多线程去执行任务队
    for i in range(5):
        th = addDetailscene(url_queue)
        th.setDaemon(True)
        th.start()
        th.join()
    url_queue.join()
    print('finish')