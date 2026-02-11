#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tushare API 统一封装
避免在各个文件中重复写重试装饰器代码
"""

import os
import pandas as pd
import tushare as ts
from tenacity import retry, stop_after_attempt, wait_exponential


# 初始化 Tushare
_TS_TOKEN = os.getenv('TS_TOKEN')
if not _TS_TOKEN:
    raise RuntimeError("环境变量 TS_TOKEN 未设置，请先在系统中导出 TS_TOKEN 再运行程序")

ts.set_token(_TS_TOKEN)
pro = ts.pro_api()

# 公用重试装饰器
def with_retry(func):
    """为函数添加重试机制的装饰器"""
    return retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=20),
        reraise=True
    )(func)

# ===== 项目中用到的 Tushare API 封装 =====

@with_retry
def get_index_daily(**kwargs):
    """获取指数日线"""
    return pro.index_daily(**kwargs)

@with_retry
def get_stock_basic(**kwargs):
    """获取股票基础信息"""
    return pro.stock_basic(**kwargs)

@with_retry
def get_trade_cal(**kwargs):
    """获取交易日历"""
    return pro.trade_cal(**kwargs)

@with_retry
def get_daily(**kwargs):
    """获取日线行情"""
    return pro.daily(**kwargs)

@with_retry
def get_pro_bar(**kwargs):
    """获取复权行情数据"""
    return ts.pro_bar(**kwargs)

@with_retry
def get_ths_daily(**kwargs):
    """获取同花顺概念板块日线行情"""
    return pro.ths_daily(**kwargs)

@with_retry
def get_adj_factor(**kwargs):
    """获取复权因子"""
    return pro.adj_factor(**kwargs)

@with_retry
def get_fund_adj(**kwargs):
    """获取基金复权因子"""
    return pro.fund_adj(**kwargs)

@with_retry
def get_ths_index(**kwargs):
    """获取同花顺概念和行业指数"""
    return pro.ths_index(**kwargs)

@with_retry
def get_ths_member(**kwargs):
    """获取同花顺概念板块成分"""
    return pro.ths_member(**kwargs)

@with_retry
def get_ths_hot_concept(**kwargs):
    """获取同花顺热点概念板块"""
    return  pro.ths_hot(market='概念板块', **kwargs)

@with_retry
def get_ths_hot_stocks(**kwargs):
    """获取同花顺热股,数据从20230820开始"""
    return  pro.ths_hot(market='热股', **kwargs)

@with_retry
def get_fund_basic(**kwargs):
    """获取基金基础信息"""
    return pro.fund_basic(**kwargs)

@with_retry
def get_fund_daily(**kwargs):
    """获取基金日线行情"""
    return pro.fund_daily(**kwargs)

@with_retry
def get_daily_basic(**kwargs):
    """获取个股每日基本信息行情，如流通市值、市盈率、市净率、股息率等"""
    return pro.daily_basic(**kwargs)

# ===== 便捷函数 =====

def get_all_stock_info():
    """获取所有A股股票基础信息"""
    df_stock_info = get_stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    return df_stock_info[['ts_code', 'name']]


def get_all_index_info():
    """获取所有指数基础信息"""
    #df_index_info = get_index_basic(market='CSI')
    data=[
        {'ts_code': '000985.CSI', 'name': '中证全指'},
        {'ts_code': '000001.SH', 'name': '上证指数'},
        {'ts_code': '399006.SZ', 'name': '创业板指'},
        {'ts_code': '000688.SH', 'name': '科创50'},
    ]
    df_index_info = pd.DataFrame(data)
    
    return df_index_info


def get_all_concept_info():
    """获取所有概念板块信息"""
    df1 = get_ths_index(exchange="A", type='I')
    df2 = get_ths_index(exchange="A", type='N')
    df = pd.concat([df1, df2])
    return df[['ts_code', 'name']]

def get_trade_dates(start_date, end_date):
    """获取指定时间段的交易日期列表"""
    trade_cal_df = get_trade_cal(exchange='', start_date=start_date, end_date=end_date, fields='cal_date,is_open')
    return list(reversed(trade_cal_df[trade_cal_df['is_open'] == 1]['cal_date'].tolist()))



def get_all_etf_info():
    """获取所有ETF基础信息"""
    df = get_fund_basic(market='E')
    return df[['ts_code', 'name']].copy()

def get_etf_daily(**kwargs):
    """获取ETF日线数据"""
    return get_fund_daily(**kwargs)

def get_concept_components(**kwargs):
    """获取概念板块成分（包装函数，忽略日期参数）"""
    # 只使用 ts_code 参数，忽略日期相关参数
    ts_code = kwargs.get('ts_code')
    if not ts_code:
        return None
    return get_ths_member(ts_code=ts_code)





if __name__ == '__main__':
    # def test_get_pro_bar():
    #     """测试 get_pro_bar 函数"""
    #     # 取一只股票，取最近5天的日线
    #     df = get_pro_bar(ts_code='000001.SZ', asset='E', freq='D', start_date='20240101', end_date='20240110')
    #     print(df.head())
    #     assert df is not None and not df.empty, "get_pro_bar 返回结果为空"
    #     print("get_pro_bar 测试通过")
        
    # test_get_pro_bar()
    # #print(get_all_concept_info())

    #print(get_etf_daily(ts_code='159241.SZ', start_date='20150105', end_date='20250825',fq='hfq'))
    #print(get_)
    #print(get_ths_hot_stocks(trade_date='20250815'))
    
    df=get_all_stock_info()
    #df=get_all_concept_info()
    d=1