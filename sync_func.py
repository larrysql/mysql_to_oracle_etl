#coding=utf8
import ConfigParser
from utils import *
import pandas as pd
from sqlalchemy.types import String
import sys

init_file = 'conf/init.conf'
def sync_full_data(schema):
    cf = ConfigParser.ConfigParser()
    cf.read(init_file)
    #读取mysql数据库配置信息,创建数据库engine
    section = 'my_' + schema
    host = cf.get(section,'host')
    username = cf.get(section,'username')
    password = cf.get(section,'password')
    dbname = cf.get(section,'dbname')
    port = cf.get(section,'port')
    my_engine = conn_db('mysql',host,username,password,dbname,port)
    #df = pd.read_sql('select * from usertree limit 2',my_engine)
    #读取oracle数据库配置信息,创建数据库engine
    section = 'ora_' + schema
    host = cf.get(section,'host')
    username = cf.get(section,'username')
    password = cf.get(section,'password')
    dbname = cf.get(section,'dbname')
    port = cf.get(section,'port')
    ora_engine = conn_db('oracle',host,username,password,dbname,port)
    #df = pd.read_sql('select * from orders where rownum=1',ora_engine)
    #wr_log('info',df)
    #获取需要同步的表
    section = schema + '_tables'
    full_tables = cf.get(section,'full_tables')
    l_full_tables = full_tables.split(',')
    wr_log('debug',l_full_tables) 
    #开始同步数据
    for full_table in l_full_tables:
        sql = 'select * from ' + schema + '.' + full_table
        try:
            wr_log('info','开始读取mysql中的表' + schema + '.' + full_table)
            df=pd.read_sql(sql,my_engine)
            wr_log('info','结束读取mysql中的表' + schema + '.' + full_table)
            cols = df.dtypes[df.dtypes=='object'].index
            type_mapping = {col : String(2048) for col in cols }
            wr_log('info','表' + schema + '.' + full_table + '开始数据同步')
            df.to_sql(full_table,ora_engine,if_exists='replace',index=False,chunksize=5000,dtype=type_mapping)
            wr_log('info','表' + schema + '.' + full_table + '结束数据同步')
        except Exception,e: 
            content = '表' + schema + '.' + full_table + "的数据同步失败:" + str(e)
            msg = funcName + ':' + content
            #print msg
            wr_log('warning',msg)

def sync_range_data(schema,tran_day):
    funcName = sys._getframe().f_code.co_name
    cf = ConfigParser.ConfigParser()
    cf.read(init_file)
    #读取mysql数据库配置信息,创建数据库engine
    section = 'my_' + schema
    host = cf.get(section,'host')
    username = cf.get(section,'username')
    password = cf.get(section,'password')
    dbname = cf.get(section,'dbname')
    port = cf.get(section,'port')
    my_engine = conn_db('mysql',host,username,password,dbname,port)
    #读取oracle数据库配置信息,创建数据库engine
    section = 'ora_' + schema
    host = cf.get(section,'host')
    username = cf.get(section,'username')
    password = cf.get(section,'password')
    dbname = cf.get(section,'dbname')
    port = cf.get(section,'port')
    ora_engine = conn_db('oracle',host,username,password,dbname,port)
    #wr_log('info',df)
    #获取需要同步的表
    section = schema + '_tables'
    day_tables = cf.get(section,'day_tables')
    l_day_tables = day_tables.split(',')
    wr_log('debug','---------------开始同步' + schema + '的表--------------------') 
    wr_log('debug',l_day_tables)
    #开始数据同步
    for table in l_day_tables:
        l_table = table.split(':') 
        table_name,column,range_type = l_table[0],l_table[1],l_table[2]
        if range_type == 'range':
            next_day = (datetime.datetime.strptime(tran_day, "%Y%m%d").date() + datetime.timedelta(days=1)).strftime("%Y%m%d")
            sql = 'select * from ' + table_name + ' where ' + column + '>=\'' + tran_day + '\' and ' + column + '<\'' + next_day + '\''
        elif range_type == 'name':
            sql = 'select * from ' + table_name + '_' + tran_day 
        else:
            msg = '不支持的范围类型' + table_name + ':' + range_type
            wr_log('warning',msg) 
            continue
        try:
            wr_log('info','表' + schema + '.' + table_name + '开始读取mysql中' + tran_day + '的数据...')
            print sql
            df=pd.read_sql(sql,my_engine)
            cols = df.dtypes[df.dtypes=='object'].index
            type_mapping = {col : String(1024) for col in cols }
            wr_log('info','表' + schema + '.' + table_name + '开始将' + tran_day +'的数据写入到oracle中...')
            df.to_sql(table_name,ora_engine,if_exists='append',index=False,chunksize=5000,dtype=type_mapping)
            row_cnt = df.shape[0]
            wr_log('info','表' + schema + '.' + table_name + '的' + tran_day + '数据同步成功,共同步数据' + str(row_cnt) + '条')
        except Exception,e:
            content = '表' + schema + '.' + table_name + "的" + tran_day +"数据同步失败:" + str(e)
            msg = funcName + ':' + content
            #print msg
            wr_log('warning',msg)
    wr_log('debug','---------------结束同步' + schema + '的表--------------------')

def sync_single_range_tab(schema,table_name,column,range_type,tran_day):
    funcName = sys._getframe().f_code.co_name
    cf = ConfigParser.ConfigParser()
    cf.read(init_file)
    #读取mysql数据库配置信息,创建数据库engine
    section = 'my_' + schema
    host = cf.get(section,'host')
    username = cf.get(section,'username')
    password = cf.get(section,'password')
    dbname = cf.get(section,'dbname')
    port = cf.get(section,'port')
    my_engine = conn_db('mysql',host,username,password,dbname,port)
    #读取oracle数据库配置信息,创建数据库engine
    section = 'ora_' + schema
    host = cf.get(section,'host')
    username = cf.get(section,'username')
    password = cf.get(section,'password')
    dbname = cf.get(section,'dbname')
    port = cf.get(section,'port')
    ora_engine = conn_db('oracle',host,username,password,dbname,port)
    #wr_log('info',df)
    #获取需要同步的表
    wr_log('debug','---------------开始同步' + schema + '的表--------------------') 
    wr_log('debug',table_name + '|' + column + '|' + range_type + '|' + tran_day)
    #开始数据同步
    if range_type == 'range':
        next_day = (datetime.datetime.strptime(tran_day, "%Y%m%d").date() + datetime.timedelta(days=1)).strftime("%Y%m%d")
        sql = 'select * from ' + table_name + ' where ' + column + '>=\'' + tran_day + '\' and ' + column + '<\'' + next_day + '\''
    elif range_type == 'name':
        sql = 'select * from ' + table_name + '_' + tran_day 
    else:
        msg = '不支持的范围类型' + table_name + ':' + range_type
        wr_log('warning',msg) 
        sys.exit(0)
    try:
        wr_log('info','表' + schema + '.' + table_name + '开始读取mysql中' + tran_day + '的数据...')
        print sql
        df=pd.read_sql(sql,my_engine)
        #cols = df.dtypes[df.dtypes=='object'].index
        #type_mapping = {col : String(1024) for col in cols }
        wr_log('info','表' + schema + '.' + table_name + '开始将' + tran_day +'的数据写入到oracle中...')
        #df.to_sql(table_name,ora_engine,if_exists='append',index=False,chunksize=5000,dtype=type_mapping)
        df.to_sql(table_name,ora_engine,if_exists='append',index=False,chunksize=5000)
        row_cnt = df.shape[0]
        wr_log('info','表' + schema + '.' + table_name + '的' + tran_day + '数据同步成功,共同步数据' + str(row_cnt) + '条')
    except Exception,e:
        content = '表' + schema + '.' + table_name + "的" + tran_day +"数据同步失败:" + str(e)
        msg = funcName + ':' + content
        #print msg
        wr_log('warning',msg)
    wr_log('debug','---------------结束同步' + schema + '的表--------------------')
