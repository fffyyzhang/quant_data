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
def pro_bar(**kwargs):
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
        data_dir: 保存数据的文件夹路径
        fq: 复权类型，默认'hfq'
        time_freq: 时间频率，默认'5min'
        api_limit: 每次请求最多获取数据条数限制
        fnc_info: 获取所有股票信息的函数，默认get_all_stock_info,必须返回ts_code和name两个字段
        fnc_data: 获取数据的函数，必须返回DataFrame
    '''
    def __init__(self,
                 data_dir, 
                 fq=None, 
                 time_freq=None,
                 api_limit=None,
                 fnc_info=None,
                 fnc_data=None,
                 asset=None
                 ):

        vars(self).update({k: v for k, v in locals().items() if k != 'self'})
        os.makedirs(self.data_dir, exist_ok=True)  # 确保数据目录存在
        self.no_data_list = [] # 记录无数据的ts_code

    def get_batch_size(self):
        """
            换算成每次最多可以fetch多少个整天的数据，不超过api limit
        """
        if not self.time_freq or self.time_freq == 'D':
            return self.api_limit
        elif match_obj:=re.match(r'(\d+)min', self.time_freq): # 分钟频率
            f=int(match_obj.group(1))
            return self.api_limit*f // 270 # 270是每天的总交易分钟数
        else: 
            print(f"频率 {self.time_freq} 不合法")
            return self.api_limit
    
    
    def save_no_data_list(self, start_date, end_date):
        """
        将无数据的ts_code列表保存到文件
        """
        if not self.no_data_list:
            print("没有发现无数据的标的")
            return
            
        no_data_file = os.path.join(self.data_dir, 'no_data_symbols.txt')
        with open(no_data_file, 'w', encoding='utf-8') as f:
            f.write(f"# 无数据标的列表 (时间段: {start_date} ~ {end_date})\n")
            f.write(f"# 生成时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# 总计: {len(self.no_data_list)} 个标的\n\n")
            
            for item in self.no_data_list:
                f.write(f"{item['ts_code']}\t{item['name']}\t{item['reason']}\n")
        
        print(f"无数据标的列表已保存到: {no_data_file}")
        print(f"共计 {len(self.no_data_list)} 个标的无数据")
                

    def get_all_data(self, **kwargs):
        """
        获取A股所有股票的复权K线数据，每个ts_code保存为单独的文件
        """
        start_date=kwargs.get('start_date')
        end_date=kwargs.get('end_date')
        refresh=kwargs.get('refresh',False)
        
        # 重置无数据列表
        self.no_data_list = []
        
        trade_dates = get_trade_dates(start_date, end_date)
        print(f"共找到 {len(trade_dates)} 个交易日，开始获取数据...")
        print(f"数据将保存到目录: {self.data_dir}")
        batch_size=self.get_batch_size()

        #tushare复权数据没有按照日期获取全量数据的功能，只能先按照股票列表循环
        stock_info = self.fnc_info()
        for i, (ts_code, stock_name) in enumerate(stock_info.values):
            file_path = os.path.join(self.data_dir, f"{ts_code}.csv")
            has_data = False  # 标记该ts_code是否有任何数据
            
            # 如果refresh=True，删除已存在的文件
            if refresh and os.path.exists(file_path):
                os.remove(file_path)
                print(f"删除已存在文件: {file_path}")
            
            # 切分时间片分批获取数据
            for start_idx in range(0, len(trade_dates), batch_size):
                end_idx = min(start_idx + batch_size-1, len(trade_dates)-1)
                start_date_batch, end_date_batch = trade_dates[start_idx], trade_dates[end_idx]
                print(f"正在获取{i+1}/{len(stock_info)} {stock_name}({ts_code}) 从{start_date_batch} 到 {end_date_batch} 的数据...")
                
                other_kwargs={}
                if self.time_freq:
                    other_kwargs['freq']=self.time_freq
                if self.fq:
                    other_kwargs['adj']=self.fq
                if self.asset:
                    other_kwargs['asset']=self.asset
                
                try:    
                    df = self.fnc_data(
                            ts_code=ts_code, 
                            start_date=start_date_batch,
                            end_date=end_date_batch,
                            **other_kwargs
                        )

                    if df is not None and not df.empty: # 如果数据不为空，则保存到文件
                        df['stock_name'] = stock_name
                        df['ts_code'] = ts_code
                        df.rename(columns={'vol':'volume'}, inplace=True)
                        print(f"  获取到 {len(df)} 条数据，保存到 {file_path}")
                        
                        # 检查文件是否存在，决定是否写入header
                        write_header = not os.path.exists(file_path)
                        df.to_csv(file_path, mode='a', header=write_header, index=False)
                        has_data = True
                    else:
                        print(f"  {ts_code} 本批次无数据")
                    time.sleep(0.2)
                except Exception as e:
                    print(f"  获取 {ts_code} 数据失败: {str(e)}")
                    continue
            
            # 如果整个时间段都没有数据，记录到无数据列表
            if not has_data:
                self.no_data_list.append({
                    'ts_code': ts_code,
                    'name': stock_name,
                    'reason': '全时间段无数据'
                })
                print(f"警告: {stock_name}({ts_code}) 在整个时间段内都无数据")
        
        # 保存无数据列表
        self.save_no_data_list(start_date, end_date)






if __name__ == "__main__":
    # 股票日线数据示例
    # handler_stock_daily = HandlerTushareBar(
    #     data_dir=os.path.join(DIR_DATA, 'stock_daily'),
    #     fq='hfq', 
    #     time_freq='D',
    #     api_limit=8000,
    #     allinfo_func=get_all_stock_info,
    #     data_func=get_hist_bar
    # )
    # handler_stock_daily.get_all_data(start_date='20150101', end_date='20250710', refresh=True)
    
    # ETF日线数据示例
    # handler_etf_daily = HandlerTushareBar(
    #     data_dir=os.path.join(DIR_DATA, 'etf_daily'),
    #     api_limit=2000,
    #     fnc_info=get_all_etf_info,
    #     fnc_data=get_etf_daily
    # )

    # handler_etf_daily.get_all_data(start_date='20140101', end_date='20250720', refresh=True)


    #ETF分钟线数据示例
    handler_etf_5min = HandlerTushareBar(
        data_dir=os.path.join(DIR_DATA, 'etf_5min'),
        api_limit=8000,
        fnc_info=get_all_etf_info,
        fnc_data=pro_bar,
        time_freq='5min',
        fq='hfq',
        asset='FD'
    )
    #print(handler_etf_5min.get_batch_size())

    handler_etf_5min.get_all_data(start_date='20140101', end_date='20250803', refresh=True)