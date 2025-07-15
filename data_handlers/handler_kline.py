import tushare as ts
import pandas as pd
import os,re,sys,time
from tenacity import retry, stop_after_attempt, wait_exponential
import logging
from config import DIR_DATA

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ts.set_token('b9b26e60eb1a7ffb3578ae28d43d113c63be23d63b13a9feea0340f4')  # 替换为实际Token
pro = ts.pro_api()

import warnings
warnings.filterwarnings("ignore")


@retry(
    stop=stop_after_attempt(3),  # 最多重试3次
    wait=wait_exponential(multiplier=1, min=2, max=10),  # 指数退避：2秒->4秒->8秒，最大10秒
    reraise=True  # 失败时重新抛出原始异常
)
def get_hist_bar(**kwargs):
    return ts.pro_bar(**kwargs)

@retry(
    stop=stop_after_attempt(3),  # 最多重试3次
    wait=wait_exponential(multiplier=1, min=2, max=10),  # 指数退避：2秒->4秒->8秒，最大10秒
    reraise=True  # 失败时重新抛出原始异常
)
def get_etf_daily(**kwargs):
    return pro.fund_daily(**kwargs)



def get_trade_dates(start_date, end_date):
    trade_cal_df = pro.trade_cal(exchange='', start_date=start_date, end_date=end_date, fields='cal_date,is_open')
    return list(reversed(trade_cal_df[trade_cal_df['is_open'] == 1]['cal_date'].tolist()))  


def get_all_stock_info():
    df_stock_info = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    return df_stock_info[['ts_code','name']]


def get_all_etf_info():
    df_etf_info = pro.etf_basic(list_status='L', fields='ts_code,extname,index_code,index_name,exchange,mgr_name,list_date')
    return df_etf_info[['ts_code','extname']].rename(columns={'extname':'name'})



class HandlerTushareBar:
    '''
        获取A股所有股票的复权K线数据，封装了通用的分批请求、断点续传、数据增量保存功能
        
        参数：
        file: 保存数据的文件路径
        fq: 复权类型，默认'hfq'
        time_freq: 时间频率，默认'5min'
        limit: 每次请求最多获取8000条数据
        allinfo_func: 获取所有股票信息的函数，默认get_all_stock_info,必须返回ts_code和name两个字段
    '''
    def __init__(self,
                 file, 
                 fq=None, 
                 time_freq=None,
                 api_limit=None,
                 allinfo_func=None,
                 data_func=None
                 ):

        self.file = file
        self.fq= fq
        self.time_freq = time_freq
        self.api_limit=api_limit # 每次请求最多获取8000条数据
        self.fnc_info=allinfo_func #必须返回ts_code和name两个字段
        self.fnc_data=data_func #必须返回DataFrame

    def get_batch_size(self):
        """
            根据每次调用api的返回数量限制，计算每次能够fetch多少个日期的数据
        """
        if not self.time_freq or self.time_freq == 'D':
            return self.api_limit
        else:   
            match_obj=re.match(r'(\d+)', self.time_freq)
            if match_obj:
                return self.api_limit // int(match_obj.group(1))
            else:
                print(f"频率 {self.time_freq} 不合法")
                return self.api_limit
                

    def get_all_data(self, **kwargs):
        """
        获取A股所有股票的复权K线数据
        """
        start_date=kwargs.get('start_date')
        end_date=kwargs.get('end_date')
        refresh=kwargs.get('refresh',False)
        
        trade_dates = get_trade_dates(start_date, end_date)
        print(f"共找到 {len(trade_dates)} 个交易日，开始获取数据...")
        batch_size=self.get_batch_size()

        #tushare复权数据没有按照日期获取全量数据的功能，只能先按照股票列表循环
        stock_info = self.fnc_info()
        for ts_code, stock_name in stock_info.values:
            for start_idx in range(0, len(trade_dates), batch_size):
                end_idx = min(start_idx + batch_size-1, len(trade_dates)-1)
                start_date, end_date=trade_dates[start_idx], trade_dates[end_idx]
                print(f"正在获取{stock_name} 从{start_date} 到 {end_date} 的数据...")
                
                other_kwargs={}
                if self.time_freq:
                    other_kwargs['freq']=self.time_freq
                if self.fq:
                    other_kwargs['adj']=self.fq
                
                try:    
                    df = self.fnc_data(
                            ts_code=ts_code, 
                            start_date=start_date,
                            end_date=end_date,
                            **other_kwargs
                        )

                    if df is not None and not df.empty: # 如果数据不为空，则保存到文件
                        df['stock_name'] = stock_name
                        print(f"  获取到 {len(df)} 条数据")
                        df.to_csv(self.file, mode='a', header=not os.path.exists(self.file), index=False)
                    else:
                        print(f"  {ts_code} 无数据")
                    time.sleep(0.2)
                except Exception as e:
                    print(f"  获取 {ts_code} 数据失败: {str(e)}")
                    continue
            






if __name__ == "__main__":
    # handler_1d = HandlerTushareBar(file=os.path.join(DIR_DATA,'1d_hfq.csv'), fq='hfq', time_freq='D')
    # handler_1d.get_all_data(start_date='20150101', end_date='20250710', refresh=True)
    
    # f_obj=get_all_stock_info
    # print(f_obj())

    handler_etf_daily = HandlerTushareBar(
        file=os.path.join(DIR_DATA,'etf_daily.csv'),
        api_limit=2000,
        allinfo_func=get_all_etf_info,
        data_func=get_etf_daily
        )

    handler_etf_daily.get_all_data(start_date='20150101', end_date='20250710', refresh=True)


    # df_stock_info=get_all_stock_info()
    # d=1