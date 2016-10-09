import pymysql as db
import time
import datetime
import os

start_time = time.time()
print("start")
#connect the mysql
try:
    conn = db.connect(host="xxbird.cn",user="tianchi",passwd="123456",db="tianchi")  
except:
    print("Fail to connect the mysql!")
    end_time = time.time()
    print("start from %s"%(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(start_time))))
    print("quit at %s"%(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(end_time))))
    exit()

try:
    with conn.cursor() as cursor:
        # create new table
        time.clock()
        try:
            sql = "create table timesOfBuying(user_id varchar(20) primary key,times int)"
            cursor.execute(sql)
            conn.commit()
            print("Success to create new table","(",str(time.clock()),"s)")
        except:
            print("Fail to creat table","(",str(time.clock()),"s)")
            exit()

        #fetch all users
        try:
            time.clock()
            sql = "select distinct user_id from user_item limit"
            cursor.execute(sql)
            conn.commit()
            users=cursor.fetchall()
            print("Success to get "+str(len(users))+" users！","(",str(time.clock()),"s)")
        except:
            print("Fail to fetch users！","(",str(time.clock()),")")
            exit()

        
        
        #get the times of Buying of all users and insert into mysql
        print("start getting the times of buying")
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