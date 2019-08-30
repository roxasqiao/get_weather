'''
Created on 2019年8月29日

@author: MR.Tree
'''
import pyhdb
import pymysql
from datetime import datetime

def hana_connection(wea_group):
        conn = pyhdb.connect(host = 'ip',
                                 port = 'prort',
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
            conn.close()    
            
def mysql_connection(wea_group):
    conn=pymysql.connect("127.0.0.1","user","pwd","spider")
    #使用cursor()方法创建一个游标对象cursor
    cursor=conn.cursor()
    sql='insert into spider.city_weather(PROVINCE,CITY, DAY_ID,DAY_DES,ZWEA,ZTEM,ZWIN) values (%s,%s,%s,%s,%s,%s,%s)'
    sql2='insert into spider.job_logs values (%s,%s,%s);'
    sql3='delete from spider.city_weather where CITY=%s and DAY_ID=%s'
    try:
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
            par_d=co2,co3
            print(par)
            cursor.execute(sql3,par_d)
            cursor.execute(sql,par)
        conn.commit()
        print( '----sql执行成功----')
    except Exception as e:
        print("----sql异常-->"+str(e))
        conn.rollback()
        #错误日志
        par2=str(datetime.now()),'get_weather',str(e)
#         print(par2)
        cursor.execute(sql2,par2)
        conn.commit()
    finally:
            conn.close()    
    
    
    



