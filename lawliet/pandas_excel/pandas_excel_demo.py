# @Time    : 2020/11/12 6:35 下午
# @Author  : luoh
# @Email   : luohenghlx@163.com
# @File    : pandas_excel_demo.py
# @Software: PyCharm
# @Description: pandas excel demo

import pandas as pd
import numpy as np
from datetime import datetime, timedelta,time
from openpyxl import load_workbook


excel = pd.read_excel("pandas_excel_test.xlsx")
frame = pd.DataFrame(excel)

print("frame: ")
print(frame)


t = datetime.now().date() - timedelta(days=1)

print("write..... ")
writer = pd.ExcelWriter("pandas_excel_test" + (u'_%d%02d%02d.xlsx' % (t.year, t.month, t.day)))
wb = writer.book
frame.to_excel(writer, sheet_name=u'测试页签', encoding='utf8', header=True, index=False, startcol=0, startrow=0)
writer.save()
print("finish..... ")
