from datafactory import Databasefactory2 as dbf
import numpy as np
import pandas as pd

from datetime import datetime, timedelta

db = dbf.DataBaseFactory2()

table_name = 'testExcel'
data = db.querysql(
    '''
    select * from account_agent limit 100,300;
    ''')

column = db.get_column_name()
print(data)

data_df = pd.DataFrame(np.array(data))

data_df.columns = column

print(type(data_df))
print(data_df)

t = datetime.now().date() - timedelta(days=1)
writer = pd.ExcelWriter(table_name + (u'_%d%02d%02d.xlsx' % (t.year, t.month, t.day)))
wb = writer.book

# 3.设置格式
fmt = wb.add_format({"font_name": u"微软雅黑"})
percent_fmt = wb.add_format({'num_format': '0.00%'})
amt_fmt = wb.add_format({'num_format': '#,##0'})
border_format = wb.add_format({'border': 1})
note_fmt = wb.add_format(
    {'bold': False, 'font_size': 16, 'font_name': u'微软雅黑', 'font_color': 'red', 'bg_color': 'green', 'align': 'left',
     'valign': 'vcenter'})
date_fmt = wb.add_format({'bold': False, 'font_name': u'微软雅黑', 'num_format': 'yyyy-mm-dd'})

header_fmt = wb.add_format(
    {'bold': True, 'font_size': 14, 'font_name': u'微软雅黑', 'num_format': 'yyyy-mm-dd', 'bg_color': '#9FC3D1',
     'valign': 'vcenter', 'align': 'center'})
highlight_fmt = wb.add_format({'bg_color': '#FFD7E2', 'num_format': '0.00%'})

# 4.写入excel
l_end = len(data_df.index) + 3
data_df.to_excel(writer, sheet_name=u'测试页签', encoding='utf_8_sig', header=False, index=False, startcol=0, startrow=3,
                 float_format="%0.1f")
worksheet1 = writer.sheets[u'测试页签']
## 写表格头部
for col_num, value in enumerate(data_df.columns.values):
    worksheet1.write(2, col_num, value, header_fmt)

# 5.生效单元格格式
# 增加个表格说明
worksheet1.merge_range('B1:D2', u'测试情况统计表', note_fmt)
worksheet1.merge_range('G1:H2', '代理数据', note_fmt)
# 设置列宽
worksheet1.set_column('A:E', 20, fmt)
# 有条件设定表格格式：金额列
worksheet1.conditional_format('G3:F%d' % l_end, {'type': 'cell', 'criteria': '>=', 'value': 1, 'format': amt_fmt})
# 有条件设定表格格式：百分比
worksheet1.conditional_format('F3:G%d' % l_end, {'type': 'cell', 'criteria': '<=', 'value': 0.1, 'format': percent_fmt})
# 有条件设定表格格式：高亮百分比
# worksheet1.conditional_format('E3:E%d' % l_end, {'type': 'cell', 'criteria': '>', 'value': 0.1, 'format': highlight_fmt})
# 加边框
worksheet1.conditional_format('A1:AV%d' % l_end, {'type': 'no_blanks', 'format': border_format})
# 设置日期格式
worksheet1.conditional_format('R3:R%d' % l_end, {'type': 'no_blanks', 'format': date_fmt})
worksheet1.conditional_format('U3:U%d' % l_end, {'type': 'no_blanks', 'format': date_fmt})

writer.save()
print("finish......")
