"""
通用数据下载器，支持4种模式：
1. by_code + distributed: 按标的遍历，分散式存储（每个symbol一个文件）
2. by_code + centralized: 按标的遍历，集中式存储（所有数据一个文件）
3. by_date + distributed: 按日期遍历，分散式存储（每个symbol一个文件）
4. by_date + centralized: 按日期遍历，集中式存储（所有数据一个文件）
"""
import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Callable, Optional, List
from apis.tushare_api_wrapper import get_trade_dates, get_adj_factor


class CommonDownloader:
    """通用数据下载器类"""
    
    def __init__(
        self,
        data_dir: str,
        data_name: str,
        func_get_symbols: Optional[Callable[[], List[str]]] = None,
        func_get_by_code: Optional[Callable] = None,
        func_get_by_date: Optional[Callable] = None,
        date_field: str = 'trade_date',
        centralized: bool = False
    ):
        for k, v in locals().items():
            if k != 'self':
                setattr(self, k, v)
        
        self.logger = logging.getLogger(f"{__name__}.{data_name}")
        os.makedirs(self.data_dir, exist_ok=True)

    
    def _get_file_path(self, symbol: Optional[str] = None) -> str:
        """获取文件路径"""
        filename = "data.csv" if symbol is None else f"{symbol}.csv"
        return os.path.join(self.data_dir, filename)
    
    def _save_csv(self, df: pd.DataFrame, file_path: str, mode: str = 'w'):
        """保存CSV文件"""
        write_header = mode == 'w' or not os.path.exists(file_path)
        df.to_csv(file_path, mode=mode, header=write_header, index=False, encoding='utf-8-sig')
    
    def _get_latest_date(self, file_path: str) -> Optional[str]:
        """获取文件中的最新日期"""
        if not os.path.exists(file_path):
            return None
        df = pd.read_csv(file_path, usecols=[self.date_field])
        return str(df[self.date_field].max()).replace('-', '')
    
    # ============ 模式1: by_code + distributed ============
    
    def get_by_code_distributed(
            self,
            start_date: str,
            end_date: Optional[str] = None,
            refresh: bool = False, 
            **kwargs
            ):
        
        """按标的遍历 + 分散式存储"""
        end_date = end_date or datetime.now().strftime('%Y%m%d')
        symbols_list = self.func_get_symbols()
        self.logger.info(f'开始下载（按标的+分散式）: symbols={len(symbols_list)}, start={start_date}, end={end_date}')
        
        total_records = 0
        for i, symbol in enumerate(symbols_list, 1):
            self.logger.info(f'[{i}/{len(symbols_list)}] 处理: {symbol}')
            file_path = self._get_file_path(symbol)
            
            # 确定起始日期
            actual_start = start_date
            if not refresh and os.path.exists(file_path):
                latest_date = self._get_latest_date(file_path)
                if latest_date and latest_date >= start_date:
                    actual_start = latest_date
            
            # 删除旧文件（refresh模式）
            if refresh and os.path.exists(file_path):
                os.remove(file_path)
            
            # 获取数据
            df = self.func_get_by_code(ts_code=symbol, start_date=actual_start, end_date=end_date, **kwargs)
            
            if df is not None and not df.empty:
                # 过滤增量数据
                if not refresh and actual_start != start_date:
                    df = df[df[self.date_field].astype(str).str.replace('-', '') > actual_start]
                
                if not df.empty:
                    mode = 'w' if refresh or not os.path.exists(file_path) else 'a'
                    self._save_csv(df, file_path, mode)
                    total_records += len(df)
                    self.logger.info(f'  保存: {len(df)}条')
        
        self.logger.info(f'完成: total_records={total_records}')
        return {'total_records': total_records, 'total_symbols': len(symbols_list)}
    
    # ============ 模式2: by_code + centralized ============
    
    def get_by_code_centralized(
            self, 
            start_date: str, 
            end_date: Optional[str] = None, 
            refresh: bool = False,
            **kwargs
        ):
        """按标的遍历 + 集中式存储"""
        end_date = end_date or datetime.now().strftime('%Y%m%d')
        symbols_list = self.func_get_symbols()
        file_path = self._get_file_path()
        
        self.logger.info(f'开始下载（按标的+集中式存储）: symbols={len(symbols_list)}, start={start_date}, end={end_date}')
        
        # 删除旧文件（refresh模式）
        if refresh and os.path.exists(file_path):
            os.remove(file_path)
        
        # 确定起始日期
        actual_start = start_date
        if not refresh:
            latest_date = self._get_latest_date(file_path)
            if latest_date and latest_date >= start_date:
                actual_start = latest_date
        
        all_data = []
        for i, symbol in enumerate(symbols_list, 1):
            self.logger.info(f'[{i}/{len(symbols_list)}] 处理: {symbol}')
            
            df = self.func_get_by_code(ts_code=symbol, start_date=actual_start, end_date=end_date, **kwargs)
            
            if df is not None and not df.empty:
                # 过滤增量数据
                if not refresh and actual_start != start_date:
                    df = df[df[self.date_field].astype(str).str.replace('-', '') > actual_start]
                
                if not df.empty:
                    all_data.append(df)
                    self.logger.info(f'  获取: {len(df)}条')
        
        # 保存所有数据
        if all_data:
            merged_df = pd.concat(all_data, ignore_index=True)
            mode = 'w' if refresh or not os.path.exists(file_path) else 'a'
            self._save_csv(merged_df, file_path, mode)
            self.logger.info(f'完成: total_records={len(merged_df)}')
            return {'total_records': len(merged_df), 'total_symbols': len(symbols_list)}
        else:
            self.logger.info('完成: 无新数据')
            return {'total_records': 0, 'total_symbols': len(symbols_list)}
    
    #liy：这种通常就只用作更新吧，不然文件IO太多了，速度太慢
    def get_by_date_distributed(
            self, 
            start_date: str, 
            end_date: Optional[str] = None, 
            refresh: bool = False, 
            **kwargs
            ):
        """按日期遍历 + 分散式存储"""
        trade_dates = get_trade_dates(start_date, end_date)
        self.logger.info(f'开始下载（按日期+分散式）: dates={len(trade_dates)}, start={start_date}, end={end_date}')
        
        # 下载所有数据
        all_data = []
        for date_idx, date in enumerate(trade_dates, 1):
            self.logger.info(f'[{date_idx}/{len(trade_dates)}] 处理: {date}')
            daily_data = self.func_get_by_date(trade_date=date, **kwargs)
            if daily_data is not None and not daily_data.empty:
                all_data.append(daily_data)
        
        if not all_data:
            self.logger.info('完成: 无数据')
            return {'total_records': 0, 'updated_files': 0, 'total_dates': len(trade_dates)}
        
        # 合并所有数据并按symbol分组保存
        merged_data = pd.concat(all_data, ignore_index=True)
        total_records = 0
        updated_files = set()
        
        for symbol, group_df in merged_data.groupby('ts_code'):
            file_path = self._get_file_path(symbol)
            
            # 删除旧文件（refresh模式）
            if refresh and os.path.exists(file_path):
                os.remove(file_path)
            
            # 增量更新：过滤已存在的数据
            if not refresh and os.path.exists(file_path):
                latest_date = self._get_latest_date(file_path)
                if latest_date:
                    group_df = group_df[group_df[self.date_field].astype(str).str.replace('-', '') > latest_date]
                    if group_df.empty:
                        continue
            
            # 保存数据
            mode = 'w' if refresh or not os.path.exists(file_path) else 'a'
            self._save_csv(group_df, file_path, mode)
            updated_files.add(symbol)
            total_records += len(group_df)
        
        self.logger.info(f'完成: total_records={total_records}, updated_files={len(updated_files)}')
        return {'total_records': total_records, 'updated_files': len(updated_files), 'total_dates': len(trade_dates)}
    
    # ============ 模式4: by_date + centralized ============
    
    def get_by_date_centralized(
            self, 
            start_date: str,
            end_date: Optional[str] = None,
            refresh: bool = False, 
            **kwargs
            ):
        """按日期遍历 + 集中式存储"""
        trade_dates = get_trade_dates(start_date, end_date)
        file_path = self._get_file_path()
        
        self.logger.info(f'开始下载（按日期+集中式）: dates={len(trade_dates)}, start={start_date}, end={end_date}')
        
        # 删除旧文件（refresh模式）
        if refresh and os.path.exists(file_path):
            os.remove(file_path)
        
        # 过滤已存在的日期
        if not refresh:
            latest_date = self._get_latest_date(file_path)
            if latest_date:
                trade_dates = [d for d in trade_dates if d > latest_date]
                self.logger.info(f'过滤已存在日期: 剩余{len(trade_dates)}个交易日 (最新={latest_date})')
        
        all_data = []
        for date_idx, date in enumerate(trade_dates, 1):
            self.logger.info(f'[{date_idx}/{len(trade_dates)}] 处理: {date}')
            
            daily_data = self.func_get_by_date(trade_date=date, **kwargs)
            
            if daily_data is not None and not daily_data.empty:
                all_data.append(daily_data)
                self.logger.info(f'  获取: {len(daily_data)}条')
        
        # 保存所有数据
        if all_data:
            merged_df = pd.concat(all_data, ignore_index=True)
            mode = 'w' if refresh or not os.path.exists(file_path) else 'a'
            self._save_csv(merged_df, file_path, mode)
            self.logger.info(f'完成: total_records={len(merged_df)}')
            return {'total_records': len(merged_df), 'total_dates': len(trade_dates)}
        else:
            self.logger.info('完成: 无新数据')
            return {'total_records': 0, 'total_dates': len(trade_dates)}

    # ============ 统一接口 ============

    def get_all(
        self,
        start_date: str,
        end_date: Optional[str] = None,
        refresh: bool = False,
        mode: str = 'by_code',
        **kwargs
    ):
        """
        统一接口，根据参数选择调用对应的下载方法
        
        参数:
            start_date: 开始日期
            end_date: 结束日期，默认为今天
            refresh: 是否全量刷新
            mode: 获取模式，'by_code'（按标的遍历）或 'by_date'（按日期遍历）
            **kwargs: 传递给数据获取函数的额外参数
        """
        if mode == 'by_code':
            if self.centralized:
                return self.get_by_code_centralized(start_date, end_date, refresh, **kwargs)
            else:
                return self.get_by_code_distributed(start_date, end_date, refresh, **kwargs)
        elif mode == 'by_date':
            if self.centralized:
                return self.get_by_date_centralized(start_date, end_date, refresh, **kwargs)
            else:
                return self.get_by_date_distributed(start_date, end_date, refresh, **kwargs)
        else:
            raise ValueError(f"不支持的mode参数: {mode}，必须是'by_code'或'by_date'")

    def fast_update(self, days: int = 30, **kwargs):
        """
        快速更新模式：基于日期的全市场API更新最近N天的数据
        
        参数:
            days: 获取最近几天的数据
            **kwargs: 传递给数据获取函数的额外参数
        """
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=2*days)).strftime('%Y%m%d')
        trade_dates = get_trade_dates(start_date, end_date)[:days]
        
        if not trade_dates:
            self.logger.warning('未获取到交易日')
            return {'updated_files': 0, 'total_records': 0}
        
        if self.centralized:
            return self.get_by_date_centralized(trade_dates[0], trade_dates[-1], refresh=False, **kwargs)
        else:
            return self.get_by_date_distributed(trade_dates[0], trade_dates[-1], refresh=False, **kwargs)


if __name__ == '__main__':
    """测试代码"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    dir_data = '/data/data_liy/quant'
    downloader = CommonDownloader(
        data_dir=os.path.join(dir_data, 'adj_factor'),
        data_name='adj_factor',
        func_get_by_date=get_adj_factor,
        date_field='trade_date',
        centralized=True
    )
    
    # 使用统一接口
    result = downloader.get_all(
        start_date='20140101',
        end_date=datetime.now().strftime('%Y%m%d'),
        refresh=False,
        mode='by_date'
    )
    
    print(f"\n下载完成:")
    print(f"  总记录数: {result['total_records']}")
    print(f"  交易日数: {result['total_dates']}")
