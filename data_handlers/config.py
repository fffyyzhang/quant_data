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