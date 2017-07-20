#coding=utf8
import ConfigParser
from utils import *
from sync_func import *
import pandas as pd
from sqlalchemy.types import String
import sys
import datetime

if __name__ == "__main__":
    if len(sys.argv) ==1:
        tran_day = (datetime.date.today()-datetime.timedelta(1)).strftime('%Y%m%d')
    else:
        tran_day = sys.argv[1] 
    print '开始同步' + tran_day
    #schema = sys.argv[1]
    #table_name = sys.argv[2]
    #column = sys.argv[3]
    #range_type = sys.argv[4]
    #tran_day = sys.argv[5]
    #sync_single_range_tab(schema,table_name,column,range_type,tran_day)
    #sync_single_range_tab('passport','orders','times','range','20170525')
    sync_range_data('hgame',tran_day)
    sync_range_data('passport',tran_day)
    sync_full_data('hgame')
    sync_full_data('passport')
