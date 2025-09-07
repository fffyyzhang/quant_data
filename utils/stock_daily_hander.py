import os,sys,json,re,random
from datetime import datetime
import pandas as pd

from utils.config import DIR_DATA
from utils.handler_kline import HandlerTushareBar
from apis.tushare_api_wrapper import get_all_stock_info, get_ths_daily, get_daily


# 使用统一封装的函数，避免重复的重试装饰器代码
def get_one_by_code(**kwargs):
    """日期约束获取单一标的数据"""
    return get_ths_daily(**kwargs)


def get_data_all_market(**kwargs):
    """获取全市场数据"""
    return get_daily(**kwargs)


def download_all():
    end_date = datetime.now().strftime('%Y%m%d')
    handler_concept_daily = HandlerTushareBar(
        data_dir=os.path.join(DIR_DATA, 'stock_daily'),
        api_limit=3000,
        fnc_info=get_all_stock_info,
        fnc_data=get_one_by_code,
        func_get_by_date=get_data_all_market
    )
    handler_concept_daily.get_all_data(start_date='20150101', end_date=end_date, refresh=True)


#只更新当日的数据，使用全市场api快速更新全量数据，避免对于code循环，提高速度
def fast_update():
    end_date = datetime.now().strftime('%Y%m%d')
    handler_concept_daily = HandlerTushareBar(
        data_dir=os.path.join(DIR_DATA, 'stock_daily'),
        api_limit=3000,
        fnc_info=get_all_stock_info,
        fnc_data=get_one_by_code,
        func_get_by_date=get_data_all_market
    )
    handler_concept_daily.fast_update(days=20)

    


if __name__ == '__main__':
    #fast_update()
    download_all()