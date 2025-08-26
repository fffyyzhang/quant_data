#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试fast_update功能的示例脚本
"""

import sys
import os
sys.path.append(os.path.dirname(os.getcwd()))

from utils.handler_kline import HandlerTushareBar
from utils.config import DIR_DATA, pro

def test_fast_update():
    """测试快速更新功能"""
    
    # 创建处理器实例，传入按日期获取数据的函数
    handler = HandlerTushareBar(
        data_dir=os.path.join(DIR_DATA, 'stock_daily'),
        func_get_by_date=pro.daily  # 传入按日期获取全市场数据的函数
    )
    
    # 执行快速更新
    print("开始快速更新...")
    handler.fast_update(days=5)
    print("快速更新完成!")

def test_fast_update_etf():
    """测试ETF快速更新功能"""
    
    # ETF使用不同的数据获取函数
    def get_etf_by_date(trade_date):
        """获取ETF当日数据的函数示例"""
        return pro.fund_daily(trade_date=trade_date)
    
    handler = HandlerTushareBar(
        data_dir=os.path.join(DIR_DATA, 'etf_daily'),
        func_get_by_date=get_etf_by_date  # 传入ETF的数据获取函数
    )
    
    # 执行快速更新
    print("开始ETF快速更新...")
    handler.fast_update(days=5)
    print("ETF快速更新完成!")

if __name__ == "__main__":
    # 测试股票快速更新
    test_fast_update()
    
    # 取消注释以测试ETF快速更新
    # test_fast_update_etf()
