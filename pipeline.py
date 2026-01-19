#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据下载管道 - 统一管理各种数据的增量和全量下载
"""

import os
from datetime import datetime
from config import DIR_DATA
from utils.kline_downloader import DownloaderTushareBar
from utils.common_downloader import CommonDownloader
from utils.binary_downloader import BinaryDownloader
from apis.tushare_api_wrapper import *

# 数据配置
DATA_CONFIGS = {
    'stock_daily': {
        'name': '股票日线数据',
        'downloader_class': DownloaderTushareBar,
        'class_params': {
            'data_dir': os.path.join(DIR_DATA, 'stock_daily'),
            'api_limit': 3000,
            'fnc_info': get_all_stock_info,
            'fnc_data': get_pro_bar,
            'func_get_by_date': get_daily,
            'fq':'hfq'
        },
        'download_params': {
            'refresh': True,
            'start_date': '20140101',
        },
        'support_fast_update': True
    },
    'etf_daily': {
        'name': 'ETF日线数据',
        'downloader_class': DownloaderTushareBar,
        'class_params': {
            'data_dir': os.path.join(DIR_DATA, 'etf_daily'),
            'api_limit': 2000,
            'fnc_info': get_all_etf_info,
            'fnc_data': get_etf_daily
        },
        'download_params': {
            'refresh': False
        },
        'support_fast_update': True
    },
    'concept_daily': {
        'name': '概念板块日线数据',
        'downloader_class': DownloaderTushareBar,
        'class_params': {
            'data_dir': os.path.join(DIR_DATA, 'ths_concepts'),
            'api_limit': 3000,
            'fnc_info': get_all_concept_info,
            'fnc_data': get_ths_daily
        },
        'download_params': {
            'refresh': False
        },
        'support_fast_update': False
    },
    'stock_1min_binary': {
        'name': '股票1分钟数据(二进制)',
        'downloader_class': BinaryDownloader,
        'class_params': {
            'data_dir': os.path.join(DIR_DATA, 'stock_1min_binary'),
            'time_freq': '1min',
            'api_limit': 8000,
            'fnc_info': get_all_stock_info,
            'fnc_data': get_pro_bar,
            'fq': 'hfq',
            'asset': 'E'
        },
        'download_params': {
            'refresh': False
        },
        'support_fast_update': False
    },
    'stock_5min_binary': {
        'name': '股票5分钟数据(二进制)',
        'downloader_class': BinaryDownloader,
        'class_params': {
            'data_dir': os.path.join(DIR_DATA, 'stock_5min_binary'),
            'time_freq': '5min',
            'api_limit': 8000,
            'fnc_info': get_all_stock_info,
            'fnc_data': get_pro_bar,
            'fq': 'hfq',
            'asset': 'E'
        },
        'download_params': {
            'refresh': False
        },
        'support_fast_update': False
    },
    'stock_15min_binary': {
        'name': '股票15分钟数据(二进制)',
        'downloader_class': BinaryDownloader,
        'class_params': {
            'data_dir': os.path.join(DIR_DATA, 'stock_15min_binary'),
            'time_freq': '15min',
            'api_limit': 8000,
            'fnc_info': get_all_stock_info,
            'fnc_data': get_pro_bar,
            'fq': 'hfq',
            'asset': 'E'
        },
        'download_params': {
            'refresh': False
        },
        'support_fast_update': False
    },
    'stock_30min_binary': {
        'name': '股票30分钟数据(二进制)',
        'downloader_class': BinaryDownloader,
        'class_params': {
            'data_dir': os.path.join(DIR_DATA, 'stock_30min_binary'),
            'time_freq': '30min',
            'api_limit': 8000,
            'fnc_info': get_all_stock_info,
            'fnc_data': get_pro_bar,
            'asset': 'E'
        },
        'download_params': {
            'refresh': True,
            'start_date': '20200101',
        },
        'support_fast_update': False
    },
    'concept_components': {
        'name': '板块成分数据',
        'downloader_class': CommonDownloader,
        'class_params': {
            'data_dir': os.path.join(DIR_DATA, 'concept_components'),
            'data_name': '板块成分',
            'func_get_symbols': get_all_concept_info,
            'func_get_by_code': get_concept_components,
            'primary_key': 'ts_code',
            'date_field': None,
            'additional_fields': []
        },
        'download_params': {
            'refresh': False
        },
        'support_fast_update': False
    },
    'adj_factor_stock': {
        'name': '股票复权因子',
        'downloader_class': CommonDownloader,
        'class_params': {
            'data_dir': os.path.join(DIR_DATA, 'adj_factor_stock'),
            'data_name': '复权因子',
            'func_get_by_date': get_adj_factor,
            'date_field': 'trade_date',
            'centralized': True
        },
        'download_params': {
            'refresh': False
        },
        'support_fast_update': True
    },
    'adj_factor_etf': {
        'name': 'ETF复权因子',
        'downloader_class': CommonDownloader,
        'class_params': {
            'data_dir': os.path.join(DIR_DATA, 'adj_factor_etf'),
            'data_name': '复权因子',
            'func_get_by_date': get_fund_adj,
            'date_field': 'trade_date',
            'centralized': True
        },
        'download_params': {
            'mode': 'by_date',
            'start_date': '20150101'
        },
        'support_fast_update': True
    },
    'daily_basic': {
        'name': '个股每日指标(流通市值等)',
        'downloader_class': CommonDownloader,
        'class_params': {
            'data_dir': os.path.join(DIR_DATA, 'daily_basic'),
            'data_name': '每日指标',
            'func_get_by_date': get_daily_basic,
            'date_field': 'trade_date',
            'centralized': True
        },
        'download_params': {
            'refresh': False,
            'start_date': '20150101',
            'mode': 'by_date'
        },
        'support_fast_update': True
    },
    'ths_hot_concept': {
        'name': '同花顺热点概念板块',
        'downloader_class': CommonDownloader,
        'class_params': {
            'data_dir': os.path.join(DIR_DATA, 'ths_hot_concept'),
            'data_name': '热点概念板块',
            'func_get_by_date': get_ths_hot_concept,
            'date_field': 'trade_date',
            'centralized': True
        },
        'download_params': {
            'start_date': '20230830',
            'mode': 'by_date'
        },
        'support_fast_update': True
    },
    'ths_hot_stocks': {
        'name': '同花顺热股',
        'downloader_class': CommonDownloader,
        'class_params': {
            'data_dir': os.path.join(DIR_DATA, 'ths_hot_stocks'),
            'data_name': '热股',
            'func_get_by_date': get_ths_hot_stocks,
            'date_field': 'trade_date',
            'centralized': True
        },
        'download_params': {
            'start_date': '20150101',
            'mode': 'by_date'
        },
        'support_fast_update': True
    },

}


def pipeline(update_plan):
    """
    数据下载管道函数
    
    参数:
        update_plan: dict, 格式为 {'data_type': 'mode'}
                    mode 可以是 'fast', 'full', 'incremental'
    """
    print("开始执行数据更新...")
    results = {}
    
    for data_type, mode in update_plan.items():
        config = DATA_CONFIGS.get(data_type)

        print(f"\n{'='*50}")
        print(f"处理 {config['name']} - 模式: {mode}")
        
        # 创建处理器
        class_params = config.get('class_params', {})
        downloader = config['downloader_class'](**class_params)
        
        # 获取下载参数，默认 refresh=False
        download_params = config.get('download_params', {'refresh': False}).copy()
        
        if mode == 'fast' and config.get('support_fast_update'):
            # 快速更新
            downloader.fast_update(days=30)
        elif mode == 'get_all':
            if not download_params.get('end_date'):
                download_params['end_date'] = datetime.now().strftime('%Y%m%d')
            # 全量下载
            downloader.get_all(**download_params)
        else:
            print(f"不支持的模式或数据类型: {data_type} - {mode}")
            results[data_type] = False
            continue
        
        print(f"{config['name']} 完成!")
        results[data_type] = True

    
    # 显示结果
    print(f"\n{'='*50}")
    print("更新结果:")
    success_count = 0
    for data_type, success in results.items():
        config = DATA_CONFIGS.get(data_type, {})
        status = "成功" if success else "失败"
        print(f"  {config.get('name', data_type)}: {status}")
        if success:
            success_count += 1

    
    print(f"总结: {success_count}/{len(results)} 个数据类型成功")
    return results



if __name__ == '__main__':
    # 定义每个数据类型的更新模式
    update_plan = {
        #'adj_factor_stock': 'get_all',
        #'adj_factor_etf': 'get_all',
        'stock_daily': 'get_all',          # 股票数据快速更新
        #'etf_daily': 'get_all',    # ETF日线数据全量下载
        #'concept_daily': 'get_all', # 概念数据增量更新  
        #'concept_components': 'get_all',    # 概念成分股全量更新
        #'stock_1m': 'get_all',              # 股票1分钟K线数据(2024-2025年)
        #'stock_30min_binary': 'get_all',     # 股票30分钟数据(二进制)
        #'daily_basic': 'get_all',  #基本信息，市值，市盈率，市净率，股息率等
        #"ths_hot_concept": "get_all",
        #"ths_hot_stocks": "get_all",
    }
        
    pipeline(update_plan)

