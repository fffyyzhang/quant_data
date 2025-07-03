import tushare as ts
import pandas as pd
import time
import os
from tenacity import retry, stop_after_attempt, wait_exponential
import logging

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ts.set_token('b9b26e60eb1a7ffb3578ae28d43d113c63be23d63b13a9feea0340f4')  # 替换为实际Token
pro = ts.pro_api()


@retry(
    stop=stop_after_attempt(3),  # 最多重试3次
    wait=wait_exponential(multiplier=1, min=2, max=10),  # 指数退避：2秒->4秒->8秒，最大10秒
    reraise=True  # 失败时重新抛出原始异常
)
def get_hist_data(**kwargs):
    return ts.pro_bar(**kwargs)



class Handler:
    def __init__(self, file, fq='hfq', time_freq='5min'):
        #self.file='/data/data_liy/quant/raw/5m_hfq.csv'
        self.file = file
        self.fq= fq
        self.time_freq = time_freq

    def get_all_data(self, start_date, end_date, refresh=False):
        """
        获取A股所有股票的5分钟后复权K线数据
        """
        stock_list = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
        print(f"共找到 {len(stock_list)} 只股票，开始获取5分钟K线数据...")
        
        #with open(self.file, 'w') as fout:
        # 遍历所有股票代码
        for idx, stock in stock_list.iterrows():
            ts_code = stock['ts_code']
            stock_name = stock['name']
            
            try:
                print(f"正在获取 {ts_code} ({stock_name}) 的5分钟K线数据... [{idx+1}/{len(stock_list)}]")

                # 使用带tenacity重试的pro_bar函数
                df = get_hist_data(ts_code=ts_code, 
                                   adj=self.fq,
                                   freq=self.time_freq,
                                   start_date=start_date,
                                   end_date=end_date    
                                   )
                
                if df is not None and not df.empty:
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
    handler_5m = Handler(file='/data/data_liy/quant/raw/5m_hfq.csv', fq='hfq', time_freq='5min')
    handler_5m.get_all_data(start_date='20250701', end_date='20250703', refresh=True)