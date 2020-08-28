from datafactory import Databasefactory as dbf
import numpy as np
import pandas as pd
from datafactory import newdev_db_config as db_config
from datetime import datetime, timedelta


db = dbf.DataBaseFactory(db_config)

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


## 分组
gr = data_df.groupby('agent_type_code')
print(type(gr))


## 获取昨天
t = datetime.now().date() - timedelta(days=1)
writer = pd.ExcelWriter(table_name + (u'_%d%02d%02d.xlsx' % (t.year, t.month, t.day)))

wb = writer.book
header_fmt = wb.add_format(
    {'bold': True, 'font_size': 12, 'font_name': u'微软雅黑', 'num_format': 'yyyy-mm-dd',
     'valign': 'vcenter', 'align': 'center'})

data_df.to_excel(writer, sheet_name=u'summary', encoding='utf_8_sig', header=True, index=False, startcol=0, startrow=0,
                 float_format="%0.1f")
worksheet1 = writer.sheets[u'summary']
for col_num, value in enumerate(data_df.columns.values):
    worksheet1.write(0, col_num, value, header_fmt)

for name,each_sheet in gr:
    this_Df = pd.DataFrame(each_sheet)
    this_Df.to_excel(writer, sheet_name=name, encoding='utf_8_sig', header=True, index=False, startcol=0, startrow=0,
                     float_format="%0.1f")
    worksheet1 = writer.sheets[name]
    for col_num, value in enumerate(this_Df.columns.values):
        worksheet1.write(0, col_num, value, header_fmt)


writer.save()
print("finish......")
