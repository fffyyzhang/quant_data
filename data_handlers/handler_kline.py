import tushare as ts
import pandas as pd
import os,re,sys,time
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential
import logging
from config import DIR_DATA

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_TS_TOKEN = os.getenv('TS_TOKEN')
if not _TS_TOKEN:
    raise RuntimeError("环境变量 TS_TOKEN 未设置，请先在系统中导出 TS_TOKEN 再运行程序")
ts.set_token(_TS_TOKEN)
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

#获取复权因子
@retry(
    stop=stop_after_attempt(3),  # 最多重试3次
    wait=wait_exponential(multiplier=1, min=2, max=10),  # 指数退避：2秒->4秒->8秒，最大10秒
    reraise=True  # 失败时重新抛出原始异常
)
def get_adj_factor(**kwargs):
    return pro.fund_adj(**kwargs)







def get_trade_dates(start_date, end_date):
    trade_cal_df = pro.trade_cal(exchange='', start_date=start_date, end_date=end_date, fields='cal_date,is_open')
    return list(reversed(trade_cal_df[trade_cal_df['is_open'] == 1]['cal_date'].tolist()))  


def get_all_stock_info():
    df_stock_info = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    return df_stock_info[['ts_code','name']]


def get_all_etf_info():
    df_etf_info = pro.etf_basic(list_status='L', fields='ts_code,extname,index_code,index_name,exchange,mgr_name,list_date')
    return df_etf_info[['ts_code','extname']].rename(columns={'extname':'name'})



def _next_day(date_str: str) -> str:
    if not date_str:
        return date_str
    s = str(date_str).replace('-', '')
    d = datetime.strptime(s, '%Y%m%d')
    return (d + timedelta(days=1)).strftime('%Y%m%d')


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
        asset: 资产类型，默认None,适配pro_bar的参数
        force_adj: 是否强制获取复权因子，默认False
    ''' 
    def __init__(self,
                 data_dir, 
                 fq=None, 
                 time_freq=None,
                 api_limit=None,
                 fnc_info=None,
                 fnc_data=None,
                 fnc_adj=None, #获取复权因子的函数，默认None,适配pro_bar的参数
                 asset=None,
                 force_adj=False #是否强制获取复权因子，默认False
                 ):
        if force_adj and not fnc_adj:
            raise ValueError("force_adj为True时，必须提供fnc_adj函数")
        vars(self).update({k: v for k, v in locals().items() if k != 'self'})
        os.makedirs(self.data_dir, exist_ok=True)  # 确保数据目录存在
        self.no_data_list = [] # 记录无数据的ts_code
        self.adj_error_list = [] # 记录复权错误的ts_code

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
    
    def save_adj_error_list(self, start_date, end_date):
        """
        将复权错误的ts_code列表保存到文件
        """
        if not self.adj_error_list:
            print("没有发现复权错误的标的")
            return
            
        adj_error_file = os.path.join(self.data_dir, 'adj_error_symbols.txt')
        with open(adj_error_file, 'w', encoding='utf-8') as f:
            f.write(f"# 复权错误标的列表 (时间段: {start_date} ~ {end_date})\n")
            f.write(f"# 生成时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# 总计: {len(self.adj_error_list)} 个标的\n\n")
            
            for item in self.adj_error_list:
                f.write(f"{item['ts_code']}\t{item['name']}\t{item['reason']}\n")
        
        print(f"复权错误标的列表已保存到: {adj_error_file}")
        print(f"共计 {len(self.adj_error_list)} 个标的复权错误")
                

    def get_all_data(self, **kwargs):
        """
        获取A股所有股票的复权K线数据，每个ts_code保存为单独的文件
        """
        start_date=kwargs.get('start_date')
        end_date=kwargs.get('end_date') or time.strftime('%Y%m%d')
        refresh=kwargs.get('refresh',False)
        
        # 重置无数据列表和复权错误列表
        self.no_data_list = []
        self.adj_error_list = []
        
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
            
            # 增量：若文件已存在且未 refresh，则仅请求最后日期之后的区间
            last_date = None
            symbol_trade_dates = trade_dates
            if (not refresh) and os.path.exists(file_path):
                try:
                    df_dates = pd.read_csv(file_path, usecols=['trade_date'])
                    if df_dates is not None and not df_dates.empty:
                        last_date = str(df_dates['trade_date'].max()).replace('-', '')
                        symbol_trade_dates = [d for d in trade_dates if d >= last_date]
                        if len(symbol_trade_dates) <= 1:
                            print(f"{stock_name}({ts_code}) 已是最新，无需更新")
                            continue
                        else:
                            print(f"增量更新 {stock_name}({ts_code}) 从 {symbol_trade_dates[0]} 起，共 {len(symbol_trade_dates)} 个交易日")
                except Exception as e:
                    print(f"读取历史最大 trade_date 失败，将按全量区间处理: {e}")

            # 切分时间片分批获取数据（针对该标的的实际日期范围）
            for start_idx in range(0, len(symbol_trade_dates), batch_size):
                end_idx = min(start_idx + batch_size, len(symbol_trade_dates)-1)
                start_date_batch, end_date_inclusive = symbol_trade_dates[start_idx], symbol_trade_dates[end_idx]
                end_date_exclusive = _next_day(end_date_inclusive)
                print(f"正在获取{i+1}/{len(stock_info)} {stock_name}({ts_code}) [{start_date_batch} , {end_date_exclusive} )的数据...")
                
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
                            end_date=end_date_exclusive,
                            **other_kwargs
                        )
                    

                    if df is not None and not df.empty: # 如果数据不为空，则保存到文件
                        # 若为增量模式，过滤掉历史已存在日期（含 last_date 当日）
                        if last_date is not None and 'trade_date' in df.columns:
                            df = df[df['trade_date'].astype(str).str.replace('-', '') > last_date]
                            if df.empty:
                                print(f"  本批次全为历史数据，跳过保存")
                                time.sleep(0.1)
                                continue
                        df['stock_name'] = stock_name
                        df['ts_code'] = ts_code
                        df.rename(columns={'vol':'volume'}, inplace=True)
                        
                        # 强制复权处理
                        if self.force_adj :
                            
                            # 获取复权因子
                            adj_df = self.fnc_adj(
                                ts_code=ts_code,
                                start_date=start_date_batch,
                                end_date=end_date_exclusive
                            )
                            
                            if adj_df is not None and not adj_df.empty:
                                # 保存原始close为close_raw
                                df['close_raw'] = df['close']
                                
                                # 合并复权因子数据
                                df = df.merge(adj_df[['trade_date', 'adj_factor']], on='trade_date', how='left')
                                # 为确保“最近”插补逻辑按时间顺序执行，按日期升序排序
                                df.sort_values('trade_date', inplace=True)
                                df.reset_index(drop=True, inplace=True)

                                # 检查是否有缺失的复权因子；若有，记录并用最近有效值插补
                                if df['adj_factor'].isna().any():
                                    missing_dates = df[df['adj_factor'].isna()]['trade_date'].tolist()
                                    print(f"  错误: {ts_code} 在以下日期缺失复权因子: {missing_dates}")
                                    self.adj_error_list.append({
                                        'ts_code': ts_code,
                                        'name': stock_name,
                                        'reason': f'缺失复权因子，日期: {missing_dates}'
                                    })

                                    # 使用最近有效值（前后均可）的插补方式；两端也进行补全
                                    df['adj_factor'] = pd.to_numeric(df['adj_factor'], errors='coerce')
                                    df['adj_factor'] = df['adj_factor'].interpolate(method='nearest', limit_direction='both')

                                # 若插补后仍存在缺失（例如整个区间都没有有效因子），则跳过该批次
                                if df['adj_factor'].isna().any():
                                    remaining_missing = df[df['adj_factor'].isna()]['trade_date'].tolist()
                                    print(f"  错误: {ts_code} 插补后仍缺失复权因子，日期: {remaining_missing}，跳过该批次")
                                    continue

                                # 应用复权因子计算新的close价格
                                df['close'] = df['close'] * df['adj_factor']
                                print(f"  已应用复权因子到 {ts_code} 的close价格（含最近值插补处理）")
                            else:
                                print(f"  错误: 无法获取 {ts_code} 的复权因子数据")
                                self.adj_error_list.append({
                                    'ts_code': ts_code,
                                    'name': stock_name,
                                    'reason': '无法获取复权因子数据'
                                })
                                continue
                                                     
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
        
        # 保存无数据列表和复权错误列表
        self.save_no_data_list(start_date, end_date)
        self.save_adj_error_list(start_date, end_date)



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
    handler_etf_daily = HandlerTushareBar(
        data_dir=os.path.join(DIR_DATA, 'etf_daily'),
        api_limit=2000,
        fnc_info=get_all_etf_info,
        fnc_data=get_etf_daily,
        force_adj=True,
        fnc_adj=get_adj_factor
    )

    handler_etf_daily.get_all_data(start_date='20140101', end_date=None, refresh=True) #end_date=None表示获取最新数据


    #ETF分钟线数据示例
    # handler_etf_5min = HandlerTushareBar(
    #     data_dir=os.path.join(DIR_DATA, 'etf_30min'),
    #     api_limit=8000,
    #     fnc_info=get_all_etf_info,
    #     fnc_data=pro_bar,
    #     time_freq='30min',
    #     fq='hfq',
    #     asset='FD'
    # )
    #print(handler_etf_5min.get_batch_size())

    #handler_etf_5min.get_all_data(start_date='20140101', end_date='20250803', refresh=True)