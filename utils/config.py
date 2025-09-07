import os
import subprocess

try:
    host = subprocess.getoutput('host $(hostname)')
    if 'mac' in host.lower():
        DIR_DATA='/Users/liyuan/data/quant/raw'
    else:
        DIR_DATA='/data/data_liy/quant/raw'
except Exception:
    DIR_DATA='/Users/liyuan/data/quant/raw'

print(DIR_DATA)

# 注意：pro 对象已移到 apis.tushare_api_wrapper 中统一管理
# 如果需要使用，请从 apis.tushare_api_wrapper import pro