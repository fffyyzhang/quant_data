import os,sys,json,re,random
from datetime import datetime
import pandas as pd

from utils.config import DIR_DATA
from utils.handler_kline import HandlerTushareBar
from apis.tushare_api_wrapper import get_all_concept_info, get_ths_daily

# 使用统一封装的函数
def get_all_stock_info():
    """获取所有概念板块信息"""
    return get_all_concept_info()

def get_hist_bar(**kwargs):
    """获取概念板块历史数据"""
    return get_ths_daily(**kwargs)


# class ConceptHandler():
#     def __init__(self):
#         pass

#def get_all_data(start_date, end_date, refresh=False):

# end_date = datetime.now().strftime('%Y%m%d')
# handler_concept_daily = HandlerTushareBar(
#     data_dir=os.path.join(DIR_DATA, 'ths_concepts'),
#     api_limit=3000,
#     fnc_info=get_all_stock_info,
#     fnc_data=get_hist_bar
# )

# handler_concept_daily.get_all_data(start_date='20150101', end_date=end_date, refresh=True)