import os,sys,json,re,random
from datetime import datetime
import pandas as pd

from config import *
from handler_kline import HandlerTushareBar
from tenacity import retry, stop_after_attempt, wait_exponential

def get_all_stock_info():
    df1 = pro.ths_index(exchange="A",type='I')
    df2 = pro.ths_index(exchange="A",type='N')
    df = pd.concat([df1,df2])
    return df[['ts_code','name']]

@retry(
    stop=stop_after_attempt(3),  # 最多重试3次
    wait=wait_exponential(multiplier=1, min=2, max=10),  # 指数退避：2秒->4秒->8秒，最大10秒
    reraise=True  # 失败时重新抛出原始异常
)
def get_hist_bar(**kwargs):
    return pro.ths_daily(**kwargs)


# class ConceptHandler():
#     def __init__(self):
#         pass

#def get_all_data(start_date, end_date, refresh=False):

end_date = datetime.now().strftime('%Y%m%d')
handler_concept_daily = HandlerTushareBar(
    data_dir=os.path.join(DIR_DATA, 'ths_concepts'),
    api_limit=3000,
    fnc_info=get_all_stock_info,
    fnc_data=get_hist_bar
)

handler_concept_daily.get_all_data(start_date='20150101', end_date=end_date, refresh=True)