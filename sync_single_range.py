#coding=utf8
import ConfigParser
from utils import *
from sync_func import *
import pandas as pd
from sqlalchemy.types import String
import sys

if __name__ == "__main__":
    schema = sys.argv[1]
    table_name = sys.argv[2]
    column = sys.argv[3]
    range_type = sys.argv[4]
    tran_day = sys.argv[5]
    sync_single_range_tab(schema,table_name,column,range_type,tran_day)
    #sync_single_range_tab('passport','userlog','times','range','20170716')
    #sync_range_data('hgame','20170606')
    #sync_range_data('passport','20170606')
    #sync_full_data('hgame')
    #sync_full_data('passport')
