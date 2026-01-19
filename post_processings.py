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


if __name__ == '__main__':
    process_ths_hot_concept()
