import os,sys,re,json
import tushare as ts
from tenacity import retry, stop_after_attempt, wait_exponential
from datetime import datetime, timedelta

from utils.handler_kline import HandlerTushareBar
from config import DIR_DATA

_TS_TOKEN = os.getenv('TS_TOKEN')
if not _TS_TOKEN:
    raise RuntimeError("环境变量 TS_TOKEN 未设置，请先在系统中导出 TS_TOKEN 再运行程序")
ts.set_token(_TS_TOKEN)
pro = ts.pro_api()



def get_all_stock_info():
    df_stock_info = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    return df_stock_info[['ts_code','name']]

@retry(
    stop=stop_after_attempt(3),  # 最多重试3次
    wait=wait_exponential(multiplier=1, min=2, max=10),  # 指数退避：2秒->4秒->8秒，最大10秒
    reraise=True  # 失败时重新抛出原始异常
)
def get_hist_bar(**kwargs):
    return ts.pro_bar(**kwargs)


def get_all_etf_info():
    df_etf_info = pro.etf_basic(list_status='L', fields='ts_code,extname,index_code,index_name,exchange,mgr_name,list_date')
    return df_etf_info[['ts_code','extname']].rename(columns={'extname':'name'})


def get_all_stock_info():
    df_stock_info = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    return df_stock_info[['ts_code','name']]


#下载/更新股票日线数据
def process_stock_daily():

    end_date = (datetime.now() + timedelta(days=1)).strftime('%Y%m%d')
    
    handler_stock_daily = HandlerTushareBar(
        data_dir=os.path.join(DIR_DATA, 'stock_daily'),
        fq='hfq', 
        time_freq='D',
        api_limit=8000,
        fnc_info=get_all_stock_info,
        fnc_data=get_hist_bar
    )
    handler_stock_daily.get_all_data(start_date='20150101', end_date=end_date, refresh=True)


#下载/更新股票ETF数据
def process_etf_daily():
    end_date = (datetime.now() + timedelta(days=1)).strftime('%Y%m%d')
    
    handler_etf_daily = HandlerTushareBar(
        data_dir=os.path.join(DIR_DATA, 'etf_daily'),
        fq='hfq', 
        time_freq='D',
        api_limit=8000,
        allinfo_func=get_all_etf_info,
        data_func=get_hist_bar
    )
    handler_etf_daily.get_all_data(start_date='20150101', end_date=end_date, refresh=True)


#下载所有同花顺板块的数据
# def process_ths_concepts():
#     end_date = datetime.now().strftime('%Y%m%d')
#     handler_concept_daily = HandlerTushareBar(
#         data_dir=os.path.join(DIR_DATA, 'stock_daily'),
#         fq='hfq', 
#         time_freq='D',
#         api_limit=8000,
#         fnc_info=get_all_stock_info,
#         fnc_data=get_hist_bar
#     )
#     handler_concept_daily.get_all_data(start_date='20150101', end_date=end_date, refresh=True)


if __name__=='__main__':
    #process_etf_daily()
    #process_stock_daily()
    process_ths_concepts()