'''
Created on 2019年8月29日

@author: MR.Tree
'''
def save_txt(wea_group):
        i=len(wea_group)
        print('总条数：',i)
        for t in wea_group:
            my_file=open('E:\\weather_date01.txt','a')
            my_file.write('\n'+t)
        print('---写入完成---')
        my_file.close()