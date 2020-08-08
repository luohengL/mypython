from datafactory import Databasefactory as dbf
import numpy as np
import pandas as pd

db = dbf.DataBaseFactory()

table_name = 'rm_policy'
data = db.querysql(
    '''
    SELECT pat.partner_id `partner id`,pat.`name`  `partner name` ,pat.agent_type_name `partner type`,if(po.tag is null,pat.tag,po.tag) tag,pat.typeD category,ifnull(po.count4,0) count4,ifnull(po.gwp4,0) gwp4,ifnull(po.count5,0) count5,ifnull(po.gwp5,0) gwp5,ifnull(po.count6,0) count6,ifnull(po.gwp6,0) gwp6,ifnull(po.totalCount,0) totalCount,ifnull(po.totalGwp,0) totalGwp from (SELECT aa.mobile,a.`name`,a.partner_id,att.agent_type_name ,'1' as type ,'MV' as typeD,ra.tag from account_agent aa LEFT JOIN account a on aa.mobile=a.mobile  LEFT JOIN agent_type att on aa.agent_type_code=att.agent_type_code LEFT JOIN rm_advance_agent_waiting ra on aa.mobile=ra.mobile  where aa.agent_type_code IN ('200004','224045') and  test_account in (0,4) union
SELECT aa.mobile,a.`name`,a.partner_id,att.agent_type_name ,'2' as type ,'Health' as typeD ,ra.tag from account_agent aa LEFT JOIN account a on aa.mobile=a.mobile  LEFT JOIN agent_type att on aa.agent_type_code=att.agent_type_code  LEFT JOIN rm_advance_agent_waiting ra on aa.mobile=ra.mobile where aa.agent_type_code IN ('200004','224045') and  test_account in (0,4) union
SELECT aa.mobile,a.`name`,a.partner_id,att.agent_type_name ,'3' as type ,'Other' as typeD ,ra.tag from account_agent aa LEFT JOIN account a on aa.mobile=a.mobile  LEFT JOIN agent_type att on aa.agent_type_code=att.agent_type_code  LEFT JOIN rm_advance_agent_waiting ra on aa.mobile=ra.mobile where aa.agent_type_code IN ('200004','224045') and  test_account in (0,4)
) as pat  LEFT JOIN (
SELECT ppp.account_id,ppp.tag,ppp.type,sum(if(left(create_time,7)='2020-04',1,0)) count4,SUM(if(left(create_time,7)='2020-04',gwp,0)) as gwp4,sum(if(left(create_time,7)='2020-05',1,0)) count5,SUM(if(left(create_time,7)='2020-05',gwp,0)) as gwp5,sum(if(left(create_time,7)='2020-06',1,0)) count6,SUM(if(left(create_time,7)='2020-06',gwp,0)) as gwp6,count(*) totalCount,SUM(gwp) as totalGwp from (SELECT pp.create_account account_id,pp.fuse_policy_code,pri.tag,'auto' policySource,DATE_ADD(pp.create_time,INTERVAL 7 HOUR) create_time ,pp.gross_policy_amount gwp,CASE WHEN pc.category_type IN (3,4) THEN '1' WHEN pc.category_type=9 THEN '2' ELSE '3' END type FROM policy_auto pp LEFT JOIN account_agent aa ON pp.create_account=aa.mobile LEFT JOIN product pd ON pp.product_code=pd.product_code LEFT JOIN product_category pc ON pd.category_code=pc.category_code LEFT JOIN policy_rm_info pri on pp.fuse_policy_code=pri.fuse_policy_code WHERE DATE_ADD(pp.create_time,INTERVAL 7 HOUR)>='2020-04-01 00:00:00' AND DATE_ADD(pp.create_time,INTERVAL 7 HOUR)<='2020-06-31 23:59:59' AND aa.agent_type_code IN ('200004','224045') and pp.policy_status not in (-1,101,106,109) UNION all
SELECT pp.create_account account_id,pp.fuse_policy_code,pri.tag,'general' policySource,DATE_ADD(pp.create_time,INTERVAL 7 HOUR) create_time,pp.gross_policy_amount gwp,CASE WHEN pc.category_type IN (3,4) THEN '1' WHEN pc.category_type=9 THEN '2' ELSE '3' END type FROM policy_general pp LEFT JOIN account_agent aa ON pp.create_account=aa.mobile LEFT JOIN product pd ON pp.product_code=pd.product_code LEFT JOIN product_category pc ON pd.category_code=pc.category_code LEFT JOIN policy_rm_info pri on pp.fuse_policy_code=pri.fuse_policy_code WHERE DATE_ADD(pp.create_time,INTERVAL 7 HOUR)>='2020-04-01 00:00:00' AND DATE_ADD(pp.create_time,INTERVAL 7 HOUR)<='2020-06-31 23:59:59' AND aa.agent_type_code IN ('200004','224045') and pp.policy_status not in (-1,101,106,109) UNION all 
SELECT pp.agent_mobile account_id ,pp.fuse_policy_code,pri.tag,'backdoor' policySource,DATE_ADD(pp.create_time,INTERVAL 7 HOUR) create_time,(pp.sub_total_premium-pp.admin_fee) gwp,CASE WHEN pp.policy_type IN ('Car','Moto') THEN '1' WHEN pp.policy_type ='Health' THEN '2' ELSE '3' END type from policy_back_door pp LEFT JOIN account_agent aa ON pp.agent_mobile=aa.mobile LEFT JOIN policy_rm_info pri on pp.fuse_policy_code=pri.fuse_policy_code where DATE_ADD(pp.create_time,INTERVAL 7 HOUR)>='2020-04-01 00:00:00' AND DATE_ADD(pp.create_time,INTERVAL 7 HOUR)<='2020-06-31 23:59:59' AND aa.agent_type_code IN ('200004','224045') and pp.policy_status not in (-1,101,106,109) UNION all
SELECT pp.create_account account_id ,pp.fuse_policy_code,pri.tag,'other' policySource,DATE_ADD(pp.create_time,INTERVAL 7 HOUR) create_time,pp.pay_amount gwp,CASE WHEN pp.external_table_name IN ('motoMultiyear','carMultiyear','autoSingleYear') THEN '1' WHEN pp.external_table_name ='healthInsurance' THEN '2' ELSE '3' END type from external_policy_common pp LEFT JOIN account_agent aa ON pp.create_account=aa.mobile LEFT JOIN policy_rm_info pri on pp.fuse_policy_code=pri.fuse_policy_code where DATE_ADD(pp.create_time,INTERVAL 7 HOUR)>='2020-04-01 00:00:00' AND DATE_ADD(pp.create_time,INTERVAL 7 HOUR)<='2020-06-31 23:59:59' AND aa.agent_type_code IN ('200004','224045') and pp.policy_status not in (-1,101,106,109) and pp.pay_amount is not null 
) as ppp  GROUP BY ppp.account_id,ppp.type
) as po on (pat.mobile=po.account_id and pat.type=po.type) ORDER BY pat.mobile,pat.type;
    ''')

column = db.get_column_name()
print(data)

data_df = pd.DataFrame(np.array(data))

data_df.columns = column

print(type(data_df))
print(data_df)

writer = pd.ExcelWriter(table_name + '.xlsx')
wb = writer.book

# 3.设置格式
fmt = wb.add_format({"font_name": u"微软雅黑"})
percent_fmt = wb.add_format({'num_format': '0.00%'})
amt_fmt = wb.add_format({'num_format': '#,##0'})
border_format = wb.add_format({'border': 1})
note_fmt = wb.add_format(
    {'bold': True, 'font_name': u'微软雅黑', 'font_color': 'red', 'align': 'left', 'valign': 'vcenter'})
date_fmt = wb.add_format({'bold': False, 'font_name': u'微软雅黑', 'num_format': 'yyyy-mm-dd'})

date_fmt1 = wb.add_format(
    {'bold': True, 'font_size': 10, 'font_name': u'微软雅黑', 'num_format': 'yyyy-mm-dd', 'bg_color': '#9FC3D1',
     'valign': 'vcenter', 'align': 'center'})
highlight_fmt = wb.add_format({'bg_color': '#FFD7E2', 'num_format': '0.00%'})

# 4.写入excel
l_end = len(data_df.index) + 2
data_df.to_excel(writer, sheet_name=u'测试页签', encoding='utf8', header=False, index=False, startcol=0, startrow=2)
worksheet1 = writer.sheets[u'测试页签']
for col_num, value in enumerate(data_df.columns.values):
    worksheet1.write(1, col_num, value, date_fmt1)

    # data_df.to_excel(writer, float_format='%.5f')

    # 5.生效单元格格式
    # 增加个表格说明
    worksheet1.merge_range('A1:B1', u'测试情况统计表', note_fmt)
    # 设置列宽
    worksheet1.set_column('A:E', 15, fmt)
    # 有条件设定表格格式：金额列
    worksheet1.conditional_format('H2:E%d' % l_end, {'type': 'cell', 'criteria': '>=', 'value': 1, 'format': amt_fmt})
    # 有条件设定表格格式：百分比
    worksheet1.conditional_format('E3:E%d' % l_end,
                                  {'type': 'cell', 'criteria': '<=', 'value': 0.1, 'format': percent_fmt})
    # 有条件设定表格格式：高亮百分比
    # worksheet1.conditional_format('E3:E%d' % l_end, {'type': 'cell', 'criteria': '>', 'value': 0.1, 'format': highlight_fmt})
    # 加边框
    worksheet1.conditional_format('A1:E%d' % l_end, {'type': 'no_blanks', 'format': border_format})
    # 设置日期格式
#  worksheet1.conditional_format('A3:A62', {'type': 'no_blanks', 'format': date_fmt})

writer.save()
print("finish......")
