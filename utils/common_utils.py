#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
公共工具函数
"""

from datetime import datetime, timedelta


def next_day(date_str: str) -> str:
    """
    获取下一天的日期
    
    参数:
        date_str: 日期字符串，支持 YYYYMMDD 或 YYYY-MM-DD 格式
    
    返回:
        str: 下一天的日期字符串 (YYYYMMDD格式)
    
    示例:
        next_day('20240101') -> '20240102'
        next_day('2024-01-01') -> '20240102'
    """
    if not date_str:
        return date_str
    s = str(date_str).replace('-', '')
    d = datetime.strptime(s, '%Y%m%d')
    return (d + timedelta(days=1)).strftime('%Y%m%d')


