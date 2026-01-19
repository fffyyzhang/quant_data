#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
二进制K线数据下载器 - 使用numpy格式存储，节省空间
"""

import os
import time
import warnings
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

# 抑制tushare库的FutureWarning警告
warnings.filterwarnings('ignore', category=FutureWarning, module='tushare')

from apis.tushare_api_wrapper import *
from config import DIR_DATA
from utils.common_utils import next_day


class BinaryDownloader:
    """
    二进制K线数据下载器
    
    数据结构：
    - 每个ts_code一个文件夹: data_dir/ts_code/
    - K线数据: data.npz (压缩格式，包含time[int64],open,high,low,close,volume,amount[float32]数组)
    - 元信息: meta.txt (股票名称等)
    
    time字段格式: 20240101 这样的int64整数，可转换为任意日期格式
    
    参数:
        data_dir: 数据保存根目录
        time_freq: 时间频率 (None/'D'/1min/5min等)
        api_limit: API单次请求限制条数
        fnc_info: 获取所有标的信息的函数
        fnc_data: 获取K线数据的函数
        func_get_by_date: 按日期获取全市场数据的函数(用于fast_update)
        asset: 资产类型 (pro_bar参数)
    """
    
    def __init__(self,
                 data_dir,
                 time_freq=None,
                 api_limit=8000,
                 fnc_info=None,
                 fnc_data=None,
                 func_get_by_date=None,
                 asset=None):
        
        # 将所有参数赋值给实例变量
        vars(self).update({k: v for k, v in locals().items() if k != 'self'})
        os.makedirs(self.data_dir, exist_ok=True)
        
        # 错误日志
        self.error_log = []
        
    def _get_batch_size(self):
        """计算每批次获取多少天的数据"""
        if not self.time_freq or self.time_freq == 'D':
            return self.api_limit
        elif 'min' in self.time_freq:
            freq_min = int(self.time_freq.replace('min', ''))
            return self.api_limit * freq_min // 270  # 每天270分钟交易时间
        return self.api_limit
    
    def _get_symbol_dir(self, ts_code):
        """获取单个标的的数据目录"""
        symbol_dir = os.path.join(self.data_dir, ts_code)
        os.makedirs(symbol_dir, exist_ok=True)
        return symbol_dir
    
    def _load_existing_data(self, ts_code):
        """加载已有数据 (time,open,high,low,close,volume,amount)"""
        symbol_dir = self._get_symbol_dir(ts_code)
        data_file = os.path.join(symbol_dir, 'data.npz')
        if not os.path.exists(data_file):
            return None
        npz_data = np.load(data_file)
        if len(npz_data['time']) == 0:
            return None
        # 构造结构化数组以保持兼容性
        structured_array = np.empty(len(npz_data['time']), dtype=[
            ('time', 'i8'),
            ('open', 'f4'), ('high', 'f4'), ('low', 'f4'), ('close', 'f4'),
            ('volume', 'f4'), ('amount', 'f4')
        ])
        structured_array['time'] = npz_data['time']
        for col in ['open', 'high', 'low', 'close', 'volume', 'amount']:
            structured_array[col] = npz_data[col]
        return structured_array
    
    def _get_time_from_data(self, time_value):
        """
        从time字段提取日期字符串
        
        参数:
            time_value: 时间整数值
        
        返回:
            str: 日期字符串 (YYYYMMDD格式)
        
        示例:
            - 日线: 20240101 -> '20240101'
            - 分钟线: 202401011530 -> '20240101'
        """
        time_str = str(int(time_value))
        # 如果是分钟级数据，截取前8位
        if self.time_freq and 'min' in self.time_freq and len(time_str) > 8:
            return time_str[:8]
        return time_str
    
    def _save_data(self, ts_code, stock_name, data_array):
        """保存数据到二进制文件 (time[int64], ohlc+va[float32])"""
        symbol_dir = self._get_symbol_dir(ts_code)
        
        # 提取各列数据
        time_data = data_array[:, 0].astype(np.int64)
        open_data = data_array[:, 1].astype(np.float32)
        high_data = data_array[:, 2].astype(np.float32)
        low_data = data_array[:, 3].astype(np.float32)
        close_data = data_array[:, 4].astype(np.float32)
        volume_data = data_array[:, 5].astype(np.float32)
        amount_data = data_array[:, 6].astype(np.float32)
        
        # 使用压缩格式保存
        np.savez_compressed(
            os.path.join(symbol_dir, 'data.npz'),
            time=time_data,
            open=open_data,
            high=high_data,
            low=low_data,
            close=close_data,
            volume=volume_data,
            amount=amount_data
        )
        
        # 保存元信息
        with open(os.path.join(symbol_dir, 'meta.txt'), 'w', encoding='utf-8') as f:
            f.write(f"ts_code: {ts_code}\n")
            f.write(f"name: {stock_name}\n")
            f.write(f"update_time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"rows: {len(data_array)}\n")
    
    def _df_to_array(self, df):
        """
        将DataFrame(),转换array数组 (time,open,high,low,close,volume,amount)
        
        时间格式：
        - 日线: 20240101 (YYYYMMDD)
        - 分钟线: 202401011530 (YYYYMMDDHHMM)
        
        注意：调用此方法前，已统一为trade_date字段
        """
        # 处理时间字段（已经统一命名为trade_date）
        time_col = df['trade_date'].astype(str).str.replace('-', '').str.replace(':', '').str.replace(' ', '').astype(np.int64).values.reshape(-1, 1)
        
        # 价格+成交量成交额
        columns = ['open', 'high', 'low', 'close', 'volume', 'amount']
        arr = df[columns].values
        
        # 合并: time + ohlc + volume + amount
        return np.hstack([time_col, arr])
    
    def _fetch_data_batch(self, ts_code, start_date, end_date):
        """获取一批数据"""
        kwargs = {}
        if self.time_freq:
            kwargs['freq'] = self.time_freq
        if self.asset:
            kwargs['asset'] = self.asset
            
        df = self.fnc_data(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            **kwargs
        )
        
        return df
    
    def download_single(self, ts_code, stock_name, start_date, end_date, refresh=False):
        """
        下载单个标的的数据
        
        参数:
            ts_code: 标的代码
            stock_name: 标的名称
            start_date: 开始日期
            end_date: 结束日期
            refresh: 是否刷新(删除已有数据重新下载)
        """
        symbol_dir = self._get_symbol_dir(ts_code)
        
        # 如果refresh=True，删除已有数据
        if refresh:
            data_file = os.path.join(symbol_dir, 'data.npz')
            if os.path.exists(data_file):
                os.remove(data_file)
        
        # 加载已有数据(增量模式)
        existing_data = self._load_existing_data(ts_code)
        
        # 获取交易日期列表
        trade_dates = get_trade_dates(start_date, end_date)
        
        # 如果是增量更新，只获取新增日期
        if existing_data is not None and not refresh:
            # 取最后一条记录的时间字段并转换为日期字符串
            last_date = self._get_time_from_data(existing_data['time'][-1])
            trade_dates = [d for d in trade_dates if d > last_date]
            if len(trade_dates) == 0:
                print(f"{stock_name}({ts_code}) 已是最新")
                return True
            print(f"增量更新 {stock_name}({ts_code}): {len(trade_dates)}个交易日")
        else:
            print(f"全量下载 {stock_name}({ts_code}): {len(trade_dates)}个交易日")
        
        # 分批获取数据
        batch_size = self._get_batch_size()
        df_list = []
        
        # 按日期数量分批，而不是按索引步长
        num_batches = (len(trade_dates) + batch_size - 1) // batch_size
        for batch_idx in range(num_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(trade_dates))
            batch_start = trade_dates[start_idx]
            batch_end = next_day(trade_dates[end_idx - 1]) if end_idx > start_idx else next_day(trade_dates[start_idx])
            
            print(f"  批次 [{batch_start}, {batch_end})")
            
            df = self._fetch_data_batch(ts_code, batch_start, batch_end)
            
            if df is not None and not df.empty:
                df.rename(columns={'vol': 'volume'}, inplace=True)
                df_list.append(df)
            
            #time.sleep(0.2)
        
        if not df_list:
            self.error_log.append({
                'ts_code': ts_code,
                'name': stock_name,
                'reason': '全时间段无数据'
            })
            print(f"  警告: {stock_name}({ts_code}) 无数据")
            return False
        
        # 合并所有批次数据
        df_all = pd.concat(df_list, ignore_index=True)
        
        # 统一时间字段名（分钟级可能叫trade_time）
        time_col = 'trade_time' if 'trade_time' in df_all.columns else 'trade_date'
        if time_col == 'trade_time':
            df_all.rename(columns={'trade_time': 'trade_date'}, inplace=True)
        
        # 处理重复列名（如果有的话）
        # 先删除重复的列，只保留第一个
        df_all = df_all.loc[:, ~df_all.columns.duplicated(keep='first')]
        
        df_all.sort_values('trade_date', inplace=True)
        df_all.reset_index(drop=True, inplace=True)
        
        # 转换为numpy数组 (time,open,high,low,close,volume,amount)
        data_array = self._df_to_array(df_all)
        
        # 如果是增量模式，合并旧数据
        if existing_data is not None and not refresh:
            # 将结构化数组转为普通数组
            old_array = np.column_stack([existing_data[field] for field in existing_data.dtype.names])
            data_array = np.vstack([old_array, data_array])
        
        # 保存数据
        self._save_data(ts_code, stock_name, data_array)
        print(f"  保存成功: {len(data_array)}条记录")
        
        return True
    
    def get_all(self, start_date, end_date=None, refresh=False):
        """
        下载所有标的的数据
        
        参数:
            start_date: 开始日期
            end_date: 结束日期 (None表示今天)
            refresh: 是否刷新所有数据
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        print(f"开始下载数据: {start_date} ~ {end_date}")
        print(f"数据保存目录: {self.data_dir}")
        
        # 重置错误日志
        self.error_log = []
        
        # 获取所有标的信息
        stock_info = self.fnc_info()
        total = len(stock_info)
        
        success_count = 0
        for idx, (ts_code, stock_name) in enumerate(stock_info.values):
            print(f"\n[{idx+1}/{total}] {stock_name}({ts_code})")
            
            try:
                result = self.download_single(ts_code, stock_name, start_date, end_date, refresh)
                if result:
                    success_count += 1
            except Exception as e:
                self.error_log.append({
                    'ts_code': ts_code,
                    'name': stock_name,
                    'reason': str(e)
                })
                print(f"  错误: {e}")
        
        # 保存错误日志
        self._save_error_log(start_date, end_date)
        
        print(f"\n下载完成: {success_count}/{total} 成功")
        
    
    def read_data(self, ts_code, as_dataframe=True):
        """
        读取标的数据
        
        参数:
            ts_code: 标的代码
            as_dataframe: 是否返回DataFrame (否则返回numpy结构化数组)
        
        返回:
            DataFrame或numpy数组，列为: time, open, high, low, close, volume, amount
        """
        data = self._load_existing_data(ts_code)
        if data is None:
            return None
        
        if as_dataframe:
            df = pd.DataFrame({
                'time': data['time'],
                'open': data['open'],
                'high': data['high'],
                'low': data['low'],
                'close': data['close'],
                'volume': data['volume'],
                'amount': data['amount']
            })
            # 可选：转换时间格式
            # 根据time字段长度判断是日线还是分钟线
            time_str = str(data['time'][0])
            if len(time_str) == 8:
                # 日线：YYYYMMDD
                df['date'] = pd.to_datetime(df['time'].astype(str), format='%Y%m%d')
            elif len(time_str) == 12:
                # 分钟线：YYYYMMDDHHMM
                df['date'] = pd.to_datetime(df['time'].astype(str), format='%Y%m%d%H%M')
            elif len(time_str) == 14:
                # 分钟线(带秒)：YYYYMMDDHHMMSS
                df['date'] = pd.to_datetime(df['time'].astype(str), format='%Y%m%d%H%M%S')
            else:
                # 尝试通用解析
                df['date'] = pd.to_datetime(df['time'].astype(str), errors='coerce')
            
            return df
        else:
            return data
    
    def _save_error_log(self, start_date, end_date):
        """保存错误日志"""
        if not self.error_log:
            print("没有错误记录")
            return
        
        log_file = os.path.join(self.data_dir, 'download_errors.txt')
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write(f"# 下载错误日志 ({start_date} ~ {end_date})\n")
            f.write(f"# 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# 总计: {len(self.error_log)} 个错误\n\n")
            
            for item in self.error_log:
                f.write(f"{item['ts_code']}\t{item['name']}\t{item['reason']}\n")
        
        print(f"\n错误日志已保存: {log_file}")
        print(f"共 {len(self.error_log)} 个错误")


if __name__ == '__main__':
    # 测试日线单条数据
    downloader = BinaryDownloader(
        data_dir=os.path.join(DIR_DATA, 'stock_binary/1min'),
        fnc_info=get_all_stock_info,
        fnc_data=get_pro_bar,
        time_freq='1min',
    )
    
    downloader.get_all_data(start_date='20250101', end_date='20251220', refresh=True)
    
    
    # downloader.download_single('000001.SZ', '平安银行', '20150101', '20251026',refresh=True)
    
    
    # 测试30min单条数据 
    # downloader = BinaryDownloader(
    #     data_dir=os.path.join(DIR_DATA, 'stock_binary/1min'),
    #     fnc_info=get_all_stock_info,
    #     fnc_data=get_pro_bar,
    #     time_freq='1min',
    # )
    #downloader.download_single('000001.SZ', '平安银行', '20250101', '20251030',refresh=True)
    
    df=downloader.read_data('000001.SZ', as_dataframe=True)
    d=1
