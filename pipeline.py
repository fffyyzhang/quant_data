#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据下载管道 - 统一管理各种数据的增量和全量下载
"""

import os
from datetime import datetime
from utils.config import DIR_DATA
#from utils.concept_handler import ConceptHandler
from utils.concept_component_handler import ConceptComponentHandler
from utils.handler_kline import HandlerTushareBar
from apis.tushare_api_wrapper import *

# 数据配置
DATA_CONFIGS = {
    'stock_daily': {
        'name': '股票日线数据',
        'handler_class': HandlerTushareBar,
        'handler_params': {
            'data_dir': os.path.join(DIR_DATA, 'stock_daily'),
            'api_limit': 3000,
            'fnc_info': get_all_stock_info,
            'fnc_data': get_pro_bar,
            'func_get_by_date': get_daily
        },
        'support_fast_update': True
    },
    'concept_daily': {
        'name': '概念板块日线数据',
        'handler_class': HandlerTushareBar,
        'handler_params': {
            'data_dir': os.path.join(DIR_DATA, 'ths_concepts'),
            'api_limit': 3000,
            'fnc_info': get_all_concept_info,
            'fnc_data': get_ths_daily
        },
        'support_fast_update': False
    },
    'concept_components': {
        'name': '概念板块成分股数据',
        'handler_class': ConceptComponentHandler,
        'handler_params': {},
        'custom_method': 'process_all_data',
        'support_fast_update': False
    }
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
        
        try:
            # 创建处理器
            handler = config['handler_class'](**config['handler_params'])
            
            if mode == 'fast' and config.get('support_fast_update'):
                # 快速更新
                handler.fast_update(days=20)
            elif mode == 'get_all':
                # 全量下载
                if hasattr(handler, 'get_all_data'):
                    end_date = datetime.now().strftime('%Y%m%d')
                    handler.get_all_data(start_date='20150101', end_date=end_date, refresh=True)
                elif config.get('custom_method'):
                    # 概念成分股等特殊数据
                    getattr(handler, config['custom_method'])()
            elif mode == 'incremental':
                # 增量下载
                if hasattr(handler, 'get_all_data'):
                    end_date = datetime.now().strftime('%Y%m%d')
                    handler.get_all_data(start_date='20150101', end_date=end_date, refresh=False)
                elif config.get('custom_method'):
                    # 概念成分股等特殊数据
                    getattr(handler, config['custom_method'])()
            else:
                print(f"不支持的模式或数据类型: {data_type} - {mode}")
                results[data_type] = False
                continue
            
            print(f"{config['name']} 完成!")
            results[data_type] = True
            
        except Exception as e:
            print(f"{config['name']} 失败: {e}")
            results[data_type] = False
    
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
        'stock_daily': 'get_all',          # 股票数据快速更新
        # 'concept_daily': 'get_all', # 概念数据增量更新  
        # 'concept_components': 'get_all'    # 概念成分股全量更新
    }
        
    pipeline(update_plan)

