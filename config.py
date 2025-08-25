import os
import subprocess
import tushare as ts

try:
    host = subprocess.getoutput('host $(hostname)')
    if 'mac' in host.lower():
        DIR_DATA='/Users/liyuan/data/quant/raw'
    else:
        DIR_DATA='/data/data_liy/quant/raw'
except Exception:
    DIR_DATA='/Users/liyuan/data/quant/raw'

print(DIR_DATA)

_TS_TOKEN = os.getenv('TS_TOKEN')
if not _TS_TOKEN:
    raise RuntimeError("环境变量 TS_TOKEN 未设置，请先在系统中导出 TS_TOKEN 再运行程序")
ts.set_token(_TS_TOKEN)
pro = ts.pro_api()