import os,sys,json,re,random
from datetime import datetime
import pandas as pd

from utils.config import *
from handler_kline import HandlerTushareBar
from tenacity import retry, stop_after_attempt, wait_exponential


def get_all_stock_info():
    df_stock_info = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    return df_stock_info[['ts_code','name']]


#日期约束获取单一标的数据
@retry(
    stop=stop_after_attempt(3),  # 最多重试3次
    wait=wait_exponential(multiplier=1, min=2, max=10),  # 指数退避：2秒->4秒->8秒，最大10秒
    reraise=True  # 失败时重新抛出原始异常
)
def get_one_by_code(**kwargs):
    return pro.ths_daily(**kwargs)

#获取全市场数据
@retry(
    stop=stop_after_attempt(3),  # 最多重试3次
    wait=wait_exponential(multiplier=1, min=2, max=10),  # 指数退避：2秒->4秒->8秒，最大10秒
    reraise=True  # 失败时重新抛出原始异常
)
def get_data_all_market(**kwargs):
    return pro.daily(**kwargs)


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
    handler_concept_daily.fast_update(days=5)

    


if __name__ == '__main__':
    fast_update()