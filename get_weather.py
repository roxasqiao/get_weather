'''
Created on 2019年8月27日

@author: MR.Tree
'''


from urllib import request                                                              
from bs4 import BeautifulSoup     
import re
from datetime import datetime
from datetime import timedelta
import pyhdb

def get_pro_url():
    province_grop={
#                    '北京':'beijing',
#                    '上海':'shanghai',
#                    '天津':'tianjin',
#                    '重庆':'chongqing',
                   '湖北':'hubei',
                   '河北':'hebei',
                   '河南':'henan',
                   '山东':'shandong',
                   '山西':'shanxi',
                   '陕西':'shaanxi',
                   '江苏':'jiangsu',
                   '湖南':'hunan',
                   '安徽':'anhui',
                   '浙江':'zhejiang',
                   '江西':'jiangxi',
                   '福建':'fujian',
#                    '台湾':'taiwan',
#                    '香港':'xianggang',
#                    '澳门':'aomen',
                   '广东':'guangdong',
                   '广西':'guangxi',
                   '海南':'hainan',
                   '云南':'yunnan',
                   '四川':'sichuan',
                   '西藏':'xizang',
                   '新疆':'xinjiang',
                   '青海':'qinghai',
                   '甘肃':'gansu',
                   '宁夏':'ningxia',
                   '内蒙古':'neimenggu',
                   '黑龙江':'heilongjiang',
                   '吉林':'jilin'
                   }
    for each in province_grop.keys():
        pro_url='http://www.weather.com.cn/'+province_grop[each]+'/index.shtml'
        print('\n'+each+' -->'+pro_url)
        wea_group=get_city_url(each,pro_url)


def get_city_url(pro_name,pro_url):
#     url='http://www.weather.com.cn/guangdong/index.shtml'
    url=pro_url
    response=request.urlopen(url)
    html=response.read().decode('utf-8')
#     print(html)
    pattern='<dt>\n.*?\n</dt>'
    data=re.findall(pattern, html)
    for each in data:
        beg_1=each.find('href="')+6
        end_1=each.find('shtml')+5
        beg_2=each.find('">')+2
        end_2=each.find('</a>')
        city_url_7day=each[beg_1:end_1] #七天数据
        city_name=each[beg_2:end_2]
#         print('\n'+city_name+'：'+city_url_7day)
        get_city_weather(pro_name,city_name,city_url_7day)
        
def get_city_weather(pro_name,city_name,city_url_7day):
#     url='http://www.weather.com.cn/weather/101120201.shtml'
    predays=7 #预测7天数据
    url=city_url_7day
    now = datetime.now()
    response=request.urlopen(url,timeout=30)
    html=response.read().decode('utf-8')
    soup=BeautifulSoup(html,'html.parser') #解析器Python标准库
    content=soup.find(name='ul', attrs={'class': 't clearfix'})
    
    pattern1='<h1>.*?</h1>'
    pattern2='<p class="wea".*?</p>'
    pattern3='<span>.*?</span>'
    pattern4='/<i>.*?</i>'
    pattern5='<i>.*?[级]</i>'
    date=re.findall(pattern1, str(content))
    wea=re.findall(pattern2, str(content))
    tem_low=re.findall(pattern3, str(content))
    tem_high=re.findall(pattern4, str(content))
    win=re.findall(pattern5, str(content))
    
    wea_group=[]
    for i in range(predays):
        aDay = timedelta(days=i)
        cadate=str(now+aDay)[0:10]
        zdate=date[i][4:-5]
        zwea=wea[i].split('">')[1].split('</p>')[0]
        ztem=tem_low[i][6:-7]+'/'+tem_high[i][4:-4]
        zwin=win[i][3:-4].replace('&lt;','')
        lis=pro_name+','+city_name+','+cadate+','+zdate+','+zwea+','+ztem+','+zwin
#         print(lis)
        wea_group.append(lis)
    hana_connection(wea_group)
    save_txt(wea_group)
def special_weather():
    print('\n-----special-----')
    get_city_weather('北京','北京','http://www.weather.com.cn/weather/101010100.shtml')
    get_city_weather('上海','上海','http://www.weather.com.cn/weather/101020100.shtml')
    get_city_weather('天津','天津','http://www.weather.com.cn/weather/101030100.shtml')
    get_city_weather('重庆','重庆','http://www.weather.com.cn/weather/101040100.shtml')
    get_city_weather('台湾','台北','http://www.weather.com.cn/weather/101340101.shtml')
    get_city_weather('香港','香港','http://www.weather.com.cn/weather/101320101.shtml')
    get_city_weather('澳门','澳门','http://www.weather.com.cn/weather/101330101.shtml')

def hana_connection(wea_group):
        conn = pyhdb.connect(host = ip,
                                 port = prort,
                                 user = "",
                                 password = "")
        cursor = conn.cursor()
#         sql1="drop table config.CITY_WEATHER"
#         sql2="create column table config.CITY_WEATHER(CITY NVARCHAR(25),DAY_ID NVARCHAR(10),DAY_DES nvarchar(25),ZWEA nvarchar(225),ZTEM nvarchar(25),ZWIN nvarchar(25))"
        sql3="insert into config.CITY_WEATHER(PROVINCE,CITY, DAY_ID,DAY_DES,ZWEA,ZTEM,ZWIN) values(%s,%s,%s,%s,%s,%s,%s)"
        try:
#             cursor.execute(sql1)
#             cursor.execute(sql2)
            i=len(wea_group)
            for num in range(i):
                co1=wea_group[num].split(',')[0]
                co2=wea_group[num].split(',')[1]
                co3=wea_group[num].split(',')[2]
                co4=wea_group[num].split(',')[3]
                co5=wea_group[num].split(',')[4]
                co6=wea_group[num].split(',')[5]
                co7=wea_group[num].split(',')[6]
                par=co1,co2,co3,co4,co5,co6,co7
                print(par)
                cursor.execute(sql3,par)
            conn.commit()
#             print( '----sql执行成功----')
        except Exception as e:
            print("----sql异常-->"+str(e))
            conn.rollback()
        finally:
            # 最终关闭数据库连接
            cursor.close()     
 
def save_txt(wea_group):
        i=len(wea_group)
#         print('总条数：',i)
        for t in wea_group:
            my_file=open('E:\\weather_date.txt','a')
            my_file.write('\n'+t)
#             print(t)
        my_file.close()
        
if __name__ == '__main__':   
    special_weather()
    get_pro_url()
    


