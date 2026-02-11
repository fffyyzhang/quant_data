#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据后处理脚本
"""

import os
import pandas as pd
from config import DIR_DATA

DIR_DERIVED = DIR_DATA.replace('/raw', '/derived')


def process_ths_hot_concept():
    """处理同花顺热点概念数据：去重并排序"""
    input_path = os.path.join(DIR_DATA, 'ths_hot_concept/data.csv')
    output_dir = os.path.join(DIR_DERIVED, 'ths_hot_concept')
    output_path = os.path.join(output_dir, 'data.csv')
    
    os.makedirs(output_dir, exist_ok=True)
    
    df = pd.read_csv(input_path)
    
    # 过滤掉hot为空的记录
    df = df.dropna(subset=['hot', 'trade_date', 'ts_code'])
    
    # groupby trade_date, ts_code，取hot最大的记录
    df = df.loc[df.groupby(['trade_date', 'ts_code'])['hot'].idxmax()]
    
    # 按trade_date和rank排序
    df = df.sort_values(['trade_date', 'rank'],ascending=[False,True])
    
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"处理完成，输出: {output_path}, 共{len(df)}条")


def process_ths_hot_stocks():
    """处理同花顺热点股票数据：去重并排序"""
    input_path = os.path.join(DIR_DATA, 'ths_hot_stocks/data.csv')
    output_dir = os.path.join(DIR_DERIVED, 'ths_hot_stock')
    output_path = os.path.join(output_dir, 'data.csv')
    
    os.makedirs(output_dir, exist_ok=True)
    
    df = pd.read_csv(input_path)
    
    # 过滤掉hot为空的记录
    df = df.dropna(subset=['hot', 'trade_date', 'ts_code'])
    
    # groupby trade_date, ts_code，取hot最大的记录
    df = df.loc[df.groupby(['trade_date', 'ts_code'])['hot'].idxmax()]
    
    # 按trade_date和rank排序
    df = df.sort_values(['trade_date', 'rank'],ascending=[False,True])
    
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"处理完成，输出: {output_path}, 共{len(df)}条")


def process_adj_factors():
    # 读取数据
    input_file = '/data/data_liy/quant/adj_factor/data.csv'
    output_file = '/data/data_liy/quant/adj_factor/data_concise.csv'

    print(f'读取数据: {input_file}')
    df = pd.read_csv(input_file)

    # 按ts_code分组，按trade_date排序
    df = df.sort_values(['ts_code', 'trade_date']).reset_index(drop=True)

    # 按ts_code分组，使用shift比较adj_factor是否变化
    df['prev_adj_factor'] = df.groupby('ts_code')['adj_factor'].shift(1)
    # 保留第一行（prev_adj_factor为NaN）或adj_factor变化的那一行
    result_df = df[(df['prev_adj_factor'].isna()) | (df['adj_factor'] != df['prev_adj_factor'])].drop(columns=['prev_adj_factor'])

    # 保存
    print(f'保存简化数据: {output_file}')
    result_df.to_csv(output_file, index=False)

    print(f'原始数据: {len(df)} 行')
    print(f'简化后: {len(result_df)} 行')
    print(f'压缩率: {len(result_df)/len(df)*100:.2f}%')




if __name__ == '__main__':
    process_ths_hot_concept()
    process_ths_hot_stocks()
