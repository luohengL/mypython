# @Time    : 2020/11/6 3:32 下午
# @Author  : luoh
# @Email   : luohenghlx@163.com
# @File    : RP-2256.py
# @Software: PyCharm
# @Description: PR-2256 Excel 导出

from datafactory import Databasefactory as dbf
import numpy as np
import pandas as pd
from datafactory import pro_db_config as db_config
from datetime import datetime, timedelta


db = dbf.DataBaseFactory(db_config)

table_name = 'pa_policy_2'
data = db.querysql(
    '''
    SELECT  pp.product_code,
DATE_ADD(pp.create_time,INTERVAL 7 HOUR)'Order Time', 
DATE_ADD(pm.create_time,INTERVAL 7 HOUR)'payment Time' ,
pp.fuse_policy_code 'Fuse Code',
pp.company_policy_code 'Insurance Policy Number',
pgp.`name` 'Insured Name',
acc.partner_id 'Partner Fuse ID',
acc.`name` 'Partner Name',
DATE_ADD(pp.effective_time,INTERVAL 7 HOUR) 'Effective Date', 
DATE_ADD(pp.expire_time,INTERVAL 7 HOUR) 'Expired Date', 
pp.policy_amount 'Premium Amount',
IFNULL(pp.admin_fee,0) 'Admin Fee',
(pp.policy_amount-pp.policy_discount_amount) 'Discount' ,
sfe.premium_mark_up 'Premium on Policy',
sf.admin_fee_mark_up 'Admin Fee on Policy',
sfe.discount_on_policy 'Discount on Policy',
(CASE pp.policy_status WHEN 101 THEN 'Quote' WHEN 102 THEN 'Pending' WHEN 103 THEN 'Inactive' WHEN 104 THEN 'Active' WHEN 105 THEN 'Expired' WHEN 106 THEN 'Cancel' WHEN 107 THEN 'Rejected' WHEN 108 THEN 'Invalid' WHEN 109 THEN 'FORCE CANCEL' ELSE '' END) AS 'Policy Status',
(CASE pp.pay_status WHEN 101 THEN 'UNPAID' WHEN 102 THEN 'PAID' WHEN 103 THEN 'REFUND' ELSE '' END) as 'Payment Status',
(CASE pp.operation_status WHEN 101 THEN 'UNSUBMIT' WHEN 102 THEN 'PENGDING' WHEN 103 THEN 'APPROVED' WHEN 104 THEN 'RESUBMIT' WHEN 105 THEN 'DECLINE' WHEN 106 THEN 'BLACKLIST' WHEN 107 THEN 'CANCEL' WHEN 108 THEN 'CONFIRMED' WHEN 109 THEN 'Agent Agree' ELSE '' END) as 'Operation Status' 
from policy_general pp LEFT JOIN policy ppp on pp.main_policy_code=ppp.main_policy_code 
LEFT JOIN pay_info pi on pp.fuse_policy_code=pi.fuse_policy_code 
LEFT JOIN settlement_flow_ext sfe ON sfe.fuse_policy_code=pp.fuse_policy_code  
LEFT JOIN settlement_flow sf on sf.fuse_policy_code=pp.fuse_policy_code
LEFT JOIN payment pm on pm.main_policy_code=pp.main_policy_code 
LEFT JOIN account acc on pp.create_account=acc.mobile 
LEFT JOIN account_agent aa on aa.mobile=pp.create_account 
LEFT JOIN policy_rm_info pri on pp.fuse_policy_code=pri.fuse_policy_code 
LEFT JOIN agent_type att on aa.agent_type_code=att.agent_type_code 
LEFT JOIN product pd on pp.product_code = pd.product_code 
LEFT JOIN product_category pc on pd.category_code=pc.category_code
LEFT JOIN policy_general_person pgp on pgp.person_code=pp.insured_person_code
where pp.policy_status not in (-1,101) and aa.test_account in (0,4) and  pp.product_code in  (
'c31b76f317d34a8cb7eac1a0b41d7977',
'b69d5b3f9fdd4691a681f2bd1dee0837',
'5c8d6be54b8a45d3b85b2cfc773d821e',
'2c50cacd965644ca9024171df581b607',
'e4e23384617b4ee0b0c096e13badd644',
'PDT0000000000588',
'f751dfe8bfb54ce2b460812a357b0c86',
'f511925b850948c5bdf1d6c75cea2633');
    ''')

column = db.get_column_name()
print(data)

data_df = pd.DataFrame(np.array(data))

data_df.columns = column

print(type(data_df))
print(data_df)


## 分组
gr = data_df.groupby('product_code')
print(type(gr))

print("write to excel ......")
## 获取昨天
t = datetime.now().date() - timedelta(days=1)
writer = pd.ExcelWriter(table_name + (u'_%d%02d%02d.xlsx' % (t.year, t.month, t.day)))

wb = writer.book
header_fmt = wb.add_format(
    {'bold': True, 'font_size': 12, 'font_name': u'微软雅黑', 'num_format': 'yyyy-mm-dd',
     'valign': 'vcenter', 'align': 'center'})


for product_code,each_sheet in gr:
    this_Df = pd.DataFrame(each_sheet)
    this_Df = this_Df.drop(["product_code"],axis=1)
    sheet_name = product_code[0:31]
    this_Df.to_excel(writer, sheet_name=sheet_name, encoding='utf_8_sig', header=True, index=False, startcol=0, startrow=0,
                     float_format="%0.1f")
    worksheet1 = writer.sheets[sheet_name]
    for col_num, value in enumerate(this_Df.columns.values):
        worksheet1.write(0, col_num, value, header_fmt)


writer.save()
print("finish......")
