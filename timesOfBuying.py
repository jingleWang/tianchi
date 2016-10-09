import pymysql as db
import time
import datetime
import os

start_time = time.time()
print("start")
#连接数据库
try:
    conn = db.connect(host="xxbird.cn",user="tianchi",passwd="123456",db="tianchi")  
except:
    print("无法连接数据库!")
    end_time = time.time()
    print("start from %s"%(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(start_time))))
    print("quit at %s"%(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(end_time))))
    exit()

try:
    with conn.cursor() as cursor:
        # 新建数据表
        time.clock()
        try:
            sql = "create table timesOfBuying(user_id varchar(20) primary key,times int)"
            cursor.execute(sql)
            conn.commit()
            print("成功创建表！","(",str(time.clock()),"s)")
        except:
            print("无法新建表！","(",str(time.clock()),"s)")
            exit()

        #获得所有用户
        try:
            time.clock()
            sql = "select distinct user_id from user_item limit"
            cursor.execute(sql)
            conn.commit()
            users=cursor.fetchall()
            print("获得"+str(len(users))+"个用户成功！","(",str(time.clock()),"s)")
        except:
            print("获取用户失败！","(",str(time.clock()),")")
            exit()

        
        
        #统计每个用户购买次数并写入数据库
        print("开始统计每个用户的购买次数：")
        for user in users:
            t0 = time.clock()
            sql = "select count(*) from user_item where type=4 and user_id='%s'" % user[0]
            print(sql)
            cursor.execute(sql)
            conn.commit()
            count = cursor.fetchone()
            sql = "insert into timesOfBuying(user_id,times) values('%s',%d)"%(user[0],count[0])
            cursor.execute(sql)
            conn.commit()
            print('user:%s count:%s'%(user[0],count[0]),'(use %f s)'%(time.clock()-t0))
        
finally:
    end_time = time.time()
    print("start from %s"%(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(start_time))))
    print("quit at %s"%(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(end_time))))
    conn.close()