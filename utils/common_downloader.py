"""
通用数据下载器，支持两种模式：
1. fast_update: 基于日期的全市场API快速更新
2. get_all: 基于ts_code遍历的全量/增量更新
"""
import os
import json
import time
import pandas as pd
from datetime import datetime, timedelta
from typing import Callable, Optional, Dict, Any, List
from utils.config import DIR_DATA
from utils.tushare_api_wrapper import get_trade_dates


class CommonDownloader:
    """通用数据下载器类"""
    
    def __init__(
        self,
        data_dir: str,
        data_name: str,
        func_get_symbols: Optional[Callable[[], pd.DataFrame]] = None,  # 获取标的列表的函数
        func_get_by_code: Optional[Callable] = None,  # 基于ts_code获取数据的函数
        func_get_by_date: Optional[Callable] = None,  # 基于日期获取全市场数据的函数
        primary_key: str = 'ts_code',  # 主键字段名
        date_field: str = 'trade_date',  # 日期字段名
        additional_fields: Optional[List[str]] = None,  # 额外需要的字段
    ):
        self.data_dir = data_dir
        self.data_name = data_name
        self.func_get_symbols = func_get_symbols
        self.func_get_by_code = func_get_by_code
        self.func_get_by_date = func_get_by_date
        self.primary_key = primary_key
        self.date_field = date_field
        self.additional_fields = additional_fields or []
        
        # 确保数据目录存在
        os.makedirs(self.data_dir, exist_ok=True)
        
        # 日志文件
        self.log_file = os.path.join(self.data_dir, 'download_log.jsonl')
        
        # 错误统计
        self.error_stats = {
            'no_data': [],
            'api_error': [],
            'save_error': []
        }
    
    def _log(self, level: str, message: str, **extra_fields):
        """结构化日志输出（JSON Lines格式）"""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'level': level,
            'data_type': self.data_name,
            'message': message,
            **extra_fields
        }
        
        # 控制台输出
        print(f"[{level}] {message}")
        
        # 文件输出
        try:
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
        except Exception as e:
            print(f"写入日志失败: {e}")
    
    def _get_file_path(self, identifier: str) -> str:
        """获取数据文件路径"""
        return os.path.join(self.data_dir, f"{identifier}.csv")
    
    def _save_data(self, df: pd.DataFrame, identifier: str, mode: str = 'replace'):
        """
        保存数据到CSV文件
        
        参数:
            df: 要保存的数据
            identifier: 标识符（通常是ts_code）
            mode: 保存模式，'replace'或'append'
        """
        file_path = self._get_file_path(identifier)
        
        if mode == 'replace':
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
        elif mode == 'append':
            write_header = not os.path.exists(file_path)
            df.to_csv(file_path, mode='a', header=write_header, index=False, encoding='utf-8-sig')
        
        self._log('INFO', f'数据保存成功', 
                 identifier=identifier, 
                 file_path=file_path, 
                 records=len(df),
                 mode=mode)
    
    def _get_latest_date(self, identifier: str) -> Optional[str]:
        """获取文件中的最新日期"""
        file_path = self._get_file_path(identifier)
        
        if not os.path.exists(file_path):
            return None
        
        try:
            df = pd.read_csv(file_path, usecols=[self.date_field])
            if df.empty:
                return None
            
            # 确保日期格式一致并获取最大值
            latest_date = str(df[self.date_field].max())
            return latest_date.replace('-', '')  # 统一为YYYYMMDD格式
            
        except Exception as e:
            self._log('WARNING', f'读取历史日期失败: {str(e)}',
                     identifier=identifier,
                     file_path=file_path)
            return None
    
    def fast_update(self, days: int = 5) -> Dict[str, Any]:
        """
        快速更新模式：基于日期的全市场API更新
        
        参数:
            days: 获取最近几天的数据
        
        返回:
            更新统计信息
        """
        if not self.func_get_by_date:
            raise ValueError("func_get_by_date函数未设置，无法执行快速更新")
        
        self._log('INFO', f'开始快速更新模式', days=days)
        
        # 获取最近N个交易日
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y%m%d')  # 预留更多天数
        trade_dates = get_trade_dates(start_date, end_date)[:days]
        
        self._log('INFO', f'获取到交易日', dates=trade_dates)
        
        updated_files = 0
        total_records = 0
        
        # 按日期获取数据
        all_market_data = []
        for date in trade_dates:
            try:
                self._log('INFO', f'获取全市场数据', date=date)
                daily_data = self.func_get_by_date(trade_date=date)
                
                if daily_data is not None and not daily_data.empty:
                    all_market_data.append(daily_data)
                    self._log('INFO', f'获取到数据', date=date, records=len(daily_data))
                else:
                    self._log('WARNING', f'无数据', date=date)
                    
            except Exception as e:
                self._log('ERROR', f'获取数据失败: {str(e)}', 
                         date=date,
                         error_type='api_error',
                         exception=str(e))
                self.error_stats['api_error'].append({
                    'date': date,
                    'error': str(e)
                })
        
        if not all_market_data:
            self._log('WARNING', '未获取到任何市场数据')
            return {'updated_files': 0, 'total_records': 0}
        
        # 合并所有数据
        market_data = pd.concat(all_market_data, ignore_index=True)
        
        # 按标的分组并更新文件
        grouped = market_data.groupby(self.primary_key)
        
        for identifier, group_df in grouped:
            try:
                latest_date = self._get_latest_date(identifier)
                
                if latest_date:
                    # 过滤出新数据
                    group_df = group_df[group_df[self.date_field].astype(str).str.replace('-', '') > latest_date]
                
                if not group_df.empty:
                    try:
                        self._save_data(group_df, identifier, mode='append')
                        updated_files += 1
                        total_records += len(group_df)
                    except Exception as e:
                        self._log('ERROR', f'保存数据失败: {str(e)}',
                                 identifier=identifier,
                                 error_type='save_error')
                        self.error_stats['save_error'].append({
                            'identifier': identifier,
                            'error': str(e)
                        })
                        
            except Exception as e:
                self._log('ERROR', f'处理标的数据失败: {str(e)}',
                         identifier=identifier,
                         error_type='save_error')
        
        self._log('INFO', f'快速更新完成',
                 updated_files=updated_files,
                 total_records=total_records)
        
        return {
            'updated_files': updated_files,
            'total_records': total_records,
            'error_stats': self.error_stats
        }
    
    def get_all(self, start_date: str, end_date: Optional[str] = None, 
                refresh: bool = False, **kwargs) -> Dict[str, Any]:
        """
        获取所有数据：基于ts_code遍历模式
        
        参数:
            start_date: 开始日期
            end_date: 结束日期，默认为今天
            refresh: 是否全量刷新
            **kwargs: 传递给数据获取函数的额外参数
        
        返回:
            下载统计信息
        """
        if not self.func_get_symbols or not self.func_get_by_code:
            raise ValueError("func_get_symbols和func_get_by_code函数必须设置")
        
        end_date = end_date or datetime.now().strftime('%Y%m%d')
        
        self._log('INFO', f'开始批量数据获取',
                 start_date=start_date,
                 end_date=end_date,
                 refresh=refresh)
        
        # 重置错误统计
        self.error_stats = {'no_data': [], 'api_error': [], 'save_error': []}
        
        # 获取所有标的列表
        try:
            symbols_df = self.func_get_symbols()
            self._log('INFO', f'获取标的列表', total_symbols=len(symbols_df))
        except Exception as e:
            self._log('ERROR', f'获取标的列表失败: {str(e)}')
            raise
        
        success_count = 0
        total_records = 0
        
        # 遍历每个标的
        for i, (_, symbol) in enumerate(symbols_df.iterrows()):
            identifier = symbol[self.primary_key]
            symbol_name = symbol.get('name', identifier)
            
            self._log('INFO', f'处理标的 {i+1}/{len(symbols_df)}',
                     identifier=identifier,
                     name=symbol_name)
            
            try:
                file_path = self._get_file_path(identifier)
                
                # 确定数据获取的日期范围
                actual_start_date = start_date
                if not refresh and os.path.exists(file_path):
                    latest_date = self._get_latest_date(identifier)
                    if latest_date and latest_date >= start_date:
                        actual_start_date = latest_date
                        self._log('INFO', f'增量更新',
                                 identifier=identifier,
                                 from_date=actual_start_date)
                elif refresh and os.path.exists(file_path):
                    os.remove(file_path)
                    self._log('INFO', f'删除旧文件进行全量更新', identifier=identifier)
                
                # 调用数据获取函数
                df = self.func_get_by_code(
                    **{self.primary_key: identifier},
                    start_date=actual_start_date,
                    end_date=end_date,
                    **kwargs
                )
                
                if df is not None and not df.empty:
                    # 过滤增量数据
                    if not refresh and actual_start_date != start_date:
                        df = df[df[self.date_field].astype(str).str.replace('-', '') > actual_start_date]
                    
                    if not df.empty:
                        # 添加标识字段
                        if 'name' not in df.columns:
                            df['name'] = symbol_name
                        
                        try:
                            self._save_data(df, identifier, mode='replace' if refresh else 'append')
                            success_count += 1
                            total_records += len(df)
                        except Exception as e:
                            self._log('ERROR', f'保存数据失败: {str(e)}',
                                     identifier=identifier,
                                     error_type='save_error')
                            self.error_stats['save_error'].append({
                                'identifier': identifier,
                                'name': symbol_name,
                                'error': str(e)
                            })
                    else:
                        self._log('INFO', f'无新增数据', identifier=identifier)
                else:
                    self._log('WARNING', f'无数据', identifier=identifier)
                    self.error_stats['no_data'].append({
                        'identifier': identifier,
                        'name': symbol_name,
                        'reason': '无数据返回'
                    })
                        
            except Exception as e:
                self._log('ERROR', f'处理标的失败: {str(e)}',
                         identifier=identifier,
                         error_type='api_error',
                         exception=str(e))
                self.error_stats['api_error'].append({
                    'identifier': identifier,
                    'name': symbol_name,
                    'error': str(e)
                })
        
        # 保存错误统计
        self._save_error_stats()
        
        self._log('INFO', f'批量数据获取完成',
                 success_count=success_count,
                 total_records=total_records,
                 total_symbols=len(symbols_df))
        
        return {
            'success_count': success_count,
            'total_records': total_records,
            'total_symbols': len(symbols_df),
            'error_stats': self.error_stats
        }
    
    def _save_error_stats(self):
        """保存错误统计信息"""
        for error_type, error_list in self.error_stats.items():
            if error_list:
                error_file = os.path.join(self.data_dir, f'{error_type}_list.txt')
                with open(error_file, 'w', encoding='utf-8') as f:
                    f.write(f"# {self.data_name} - {error_type} 错误列表\n")
                    f.write(f"# 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"# 总计: {len(error_list)} 个\n\n")
                    
                    for item in error_list:
                        f.write(f"{item.get('identifier', 'N/A')}\t{item.get('name', 'N/A')}\t{item.get('error', item.get('reason', 'N/A'))}\n")
                
                self._log('INFO', f'错误统计保存完成',
                         error_type=error_type,
                         count=len(error_list),
                         file=error_file)
