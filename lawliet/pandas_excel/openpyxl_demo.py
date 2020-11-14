# @Time    : 2020/11/12 6:58 下午
# @Author  : luoh
# @Email   : luohenghlx@163.com
# @File    : openpyxl_demo.py
# @Software: PyCharm
# @Description:


import pandas as pd
import numpy as np
from datetime import datetime, timedelta,time
import openpyxl


wb = openpyxl.load_workbook('pandas_excel_test.xlsx')
sht = wb.active
data = [121,124,135,615]
sht.append(data)
wb.save('pandas_excel_test_pyxl.xlsx')