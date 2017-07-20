#coding=utf-8
import ConfigParser
from sqlalchemy import create_engine
import os,sys,datetime
import logging

def wr_log(lv,msg):
    cf = ConfigParser.ConfigParser()
    if os.path.exists('conf/init.conf'):
        confile = 'conf/init.conf'
    elif os.path.exists('../conf/init.conf'):
        confile = '../conf/init.conf'
    else:
        print '未找到配置文件!'
        sys.exit(0)
    cf.read(confile)
    logfile = cf.get("log", "logfile")
    logging.basicConfig(level=logging.DEBUG,
                    #format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    format='%(asctime)s %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename=logfile,
                    filemode='a')
    if lv == 'info':
        logging.info(msg)
    elif lv == 'debug':
        logging.debug(msg)
    elif lv == 'warning':
        logging.warning(msg)
    else:
        print '没有此日志级别!'

def conn_db(db_type,host,username,password,dbname,port,charset='utf8'):
    funcName = sys._getframe().f_code.co_name
    try:
        if db_type == 'mysql':
            conn_str = 'mysql://' + username + ':' + password + '@' + host + ':' + port + '/' + dbname + '?charset=' + charset
            engine = create_engine(conn_str)
        elif db_type == 'oracle':
            conn_str = 'oracle+cx_oracle://' + username + ':' + password + '@' + host + ':' + port + '/' + dbname
            engine = create_engine(conn_str)
        else:
            wr_log('warning','不支持该数据库类型')
            sys.exit(0)
        return engine
    except Exception,e:
        content = ":数据库连接失败!请检查" + str(e)
        #print content
        msg = funcName + ':' + content
        wr_log('warning',msg)

def now_time():
    return datetime.datetime.now().strftime("%Y%m%d %H:%M:%S")
