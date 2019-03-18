#coding:utf-8
# import requests
# headers = {
#     'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.221 Safari/537.36 SE 2.X MetaSr 1.0'
#     }
#
# url = 'http://travel.qunar.com/p-cs297397-guiping'
# for num in range(2,50):
#     tempulr = url + '-jingdian-1-' + str(num)
#     response = requests.get(url=tempulr,headers=headers,allow_redirects=False)
#     print(response.status_code)
import pymysql
db = pymysql.connect("localhost", "root", "123456", "data")
cursor = db.cursor()
city = '北京'
infor = {'attraction': "天安门广场Tian'anmen Square", 'url': 'http://travel.qunar.com/p-oi711995-tiananmenguangchang', 'star': '94', 'rank': '北京景点排名第33', 'briefintru': '共和国的标志，早起看一场庄严肃穆的升旗仪式，瞻仰人民英雄纪念碑。'}
sql = "insert into briefscene(city,attract,star,briefintru,rank) values (%s,%s,%s,%s,%s)"
cursor.execute(sql,(city, infor['attraction'], infor['star'], infor['briefintru'], infor['rank']))
db.commit()
db.close()
