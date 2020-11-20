# @Time    : 2020/11/14 5:22 下午
# @Author  : luoh
# @Email   : luohenghlx@163.com
# @File    : product_report_No5.py
# @Software: PyCharm
# @Description: 保险公司产品保单统计

import numpy as np
import pandas as pd
from datafactory import Databasefactory as dbf
from datafactory import pro_db_config as db_config
from datetime import datetime, timedelta
from mongo_data_factory import MongoDataBaseFactory as mdbf
from mongo_data_factory import pro_db_config as mdb_config

## 获取数据库连接
db = dbf.DataBaseFactory(db_config)
mdbF = mdbf.MongoDataBaseFactory(mdb_config)
mgdb = mdbF.getdb()

##sql

policy_sql = """
-- auto
SELECT * from (
SELECT pp.fuse_policy_code,att.agent_type_name,DATE_ADD(pp.create_time,INTERVAL 7 HOUR) 'order_time',DATE_FORMAT(DATE_ADD(pp.create_time,INTERVAL 7 HOUR),'%Y-%m') 'order_time_month'  ,'Offline' as 'policy_source',ic.company_name 'insurance_company_name',(CASE pc.category_type WHEN 1 THEN 'General' WHEN 2 THEN 'Travel' WHEN 3 THEN 'Car' WHEN 4 THEN 'Moto' WHEN 6 THEN 'Life' WHEN 7 THEN 'PA' WHEN 8 THEN 'Property' WHEN 9 THEN 'Health' WHEN 11 THEN 'Marine Cargo' WHEN 13 THEN 'SME' WHEN 14 THEN 'MOVEABLE ALL RISK' WHEN 12 THEN 'ROP' WHEN 15 THEN 'VIP' ELSE '' END) as 'policy_type'  ,(pp.policy_amount-pp.admin_fee-pp.service_fee)  'gwp'
from policy_auto pp
LEFT JOIN policy p on p.main_policy_code=pp.main_policy_code
LEFT JOIN product pd on pd.product_code=pp.product_code
LEFT JOIN product_category pc on pd.category_code=pc.category_code  
LEFT JOIN insurance_company ic on  pd.insurance_company_code=ic.company_code
LEFT JOIN account_agent aa on pp.create_account=aa.mobile
LEFT JOIN agent_type att on att.agent_type_code=aa.agent_type_code
where  aa.test_account in (0,4) and policy_status not in (-1,101) and (p.pay_time_type !=2 OR (p.pay_time_type=2 and pp.pay_status=102))  and  DATE_ADD(pp.create_time,INTERVAL 7 HOUR)>'2020-08-01 00:00:00' and DATE_ADD(pp.create_time,INTERVAL 7 HOUR)<'2020-11-01 00:00:00'
union
SELECT pp.fuse_policy_code,att.agent_type_name,DATE_ADD(pp.create_time,INTERVAL 7 HOUR) 'order_time',DATE_FORMAT(DATE_ADD(pp.create_time,INTERVAL 7 HOUR),'%Y-%m') 'order_time_month'  ,'Offline' as 'policy_source',ic.company_name 'insurance_company_name',(CASE pc.category_type WHEN 1 THEN 'General' WHEN 2 THEN 'Travel' WHEN 3 THEN 'Car' WHEN 4 THEN 'Moto' WHEN 6 THEN 'Life' WHEN 7 THEN 'PA' WHEN 8 THEN 'Property' WHEN 9 THEN 'Health' WHEN 11 THEN 'Marine Cargo' WHEN 13 THEN 'SME' WHEN 14 THEN 'MOVEABLE ALL RISK' WHEN 12 THEN 'ROP' WHEN 15 THEN 'VIP' ELSE '' END) as 'policy_type'  ,(pp.policy_amount-pp.admin_fee-pp.service_fee)  'gwp'
from policy_general pp
LEFT JOIN policy p on p.main_policy_code=pp.main_policy_code
LEFT JOIN product pd on pd.product_code=pp.product_code
LEFT JOIN product_category pc on pd.category_code=pc.category_code  
LEFT JOIN insurance_company ic on  pd.insurance_company_code=ic.company_code
LEFT JOIN account_agent aa on pp.create_account=aa.mobile
LEFT JOIN agent_type att on att.agent_type_code=aa.agent_type_code
where  aa.test_account in (0,4) and policy_status not in (-1,101) and (p.pay_time_type !=2 OR (p.pay_time_type=2 and pp.pay_status=102))  and  DATE_ADD(pp.create_time,INTERVAL 7 HOUR)>'2020-08-01 00:00:00' and DATE_ADD(pp.create_time,INTERVAL 7 HOUR)<'2020-11-01 00:00:00'
union
-- bacdkoor
SELECT pp.fuse_policy_code,att.agent_type_name,DATE_ADD(pp.order_date,INTERVAL 7 HOUR) 'order_time',DATE_FORMAT(DATE_ADD(pp.order_date,INTERVAL 7 HOUR),'%Y-%m') 'order_time_month'  ,'Backdoor' as 'policy_source',pp.insurance_company_name 'insurance_company_name',pp.policy_type as 'policy_type'  ,IF(pp.is_co_as=1,(ifnull( `pp`.`basic_premium`, 0 ) + ifnull( `pp`.`rider_total_premium`, 0 )) * ( ifnull( t.`company_percentage`, 0 ) / 100 ),ifnull( `pp`.`basic_premium`, 0 ) + ifnull( `pp`.`rider_total_premium`, 0 ))  'gwp'
from policy_back_door pp
LEFT JOIN account_agent aa on pp.agent_mobile=aa.mobile
LEFT JOIN agent_type att on att.agent_type_code=aa.agent_type_code
LEFT JOIN (
	SELECT sum(`pbdca`.`company_percentage` ) AS `company_percentage`,`pbdca`.`fuse_policy_code` AS `fuse_policy_code` FROM `policy_back_door_co_as` `pbdca` 
	WHERE ( `pbdca`.`is_fuse` = 1 ) GROUP BY `pbdca`.`fuse_policy_code`
) t on pp.fuse_policy_code=t.fuse_policy_code
where  aa.test_account in (0,4) and pp.policy_status not in (-1,101) and  DATE_ADD(pp.order_date,INTERVAL 7 HOUR)>'2020-08-01 00:00:00' and DATE_ADD(pp.order_date,INTERVAL 7 HOUR)<'2020-11-01 00:00:00'

union
-- other
SELECT pp.fuse_policy_code,att.agent_type_name,DATE_ADD(ci.create_time,INTERVAL 7 HOUR) 'order_time',DATE_FORMAT(DATE_ADD(ci.create_time,INTERVAL 7 HOUR),'%Y-%m') 'order_time_month'  ,'Others' as 'policy_source',pp.insurance_company_order 'insurance_company_name',pp.product_name as 'policy_type'  ,IFNULL(pp.pay_amount,0)-IFNULL(pp.admin_fee,0)   'gwp'
from external_policy_common pp
LEFT JOIN  (SELECT fuse_policy_code,send_time 'create_time' from  covernote_information where type=2 GROUP BY fuse_policy_code ) ci on ci.fuse_policy_code=pp.fuse_policy_code 
LEFT JOIN account_agent aa on pp.create_account=aa.mobile
LEFT JOIN agent_type att on att.agent_type_code=aa.agent_type_code
where  aa.test_account in (0,4) and pp.policy_status not in (-1,101) and  DATE_ADD(ci.create_time,INTERVAL 7 HOUR)>'2020-08-01 00:00:00' and DATE_ADD(ci.create_time,INTERVAL 7 HOUR)<'2020-11-01 00:00:00') t where t.agent_type_name in (
"CEKPREMI - AUTO",
"Agent Type ARMS",
"Agent Type ARMS ADMIN",
"CEKPREMI-AGENT-TAJ",
"Agent Type TAJ Admin",
"Agent Type TAJ Owner",
"Agent Type CK7",
"Elizabet Admin 04",
"Elizabet Owner",
"GSP Admin 05",
"GSP Owner",
"GSP Admin 04",
"GSP Admin 03",
"GSP Admin 02",
"GSP Admin 01",
"Elizabet Admin 03",
"Elizabet Admin 02",
"Elizabet Admin 01",
"SBS OWNER",
"SBS ADMIN MAGI, ASOKA & KB",
"SBS ADMIN SIMASNET",
"SBS ADMIN MAG",
"SBS ADMIN PANFIC & HARTA",
"SBS ADMIN ACA",
"SBS ADMIN ADIRA",
"TJM OWNER",
"TJM ADMIN - MAGI & SIMASNET",
"TJM ADMIN - MAG",
"TJM ADMIN - PANFIC",
"TJM ADMIN - ACA",
"TJM ADMIN - ADIRA",
"CEKPREMI-AGENT-TJIPTA MARLINA",
"CEKPREMI-AGENT-AGUS RIYANTO",
"CEKPREMI-AGENT-A",
"CEKPREMI-AGENT-B",
"CEKPREMI-AGENT-C",
"CEKPREMI-AGENT-D",
"CEKPREMI-AGENT-E",
"Dutamas",
"Dutamas Admin ACA",
"Dutamas Admin AXA",
"Dutamas Admin MAGI & SIMAS",
"Dutamas Admin MAG & FPG",
"Dutamas Admin Panfic& Sompo",
"Dutamas Admin Adira & Harta",
"Dutamas Downline",
"Rasdi",
"Asia Pacific Owner",
"Asia Pacific Admin MAGI & SIMASNET",
"Asia Pacific Admin MAG & FPG",
"Asia Pacific Admin AXA",
"Asia Pacific Admin Harta & Panfic",
"Asia Pacific Admin ACA",
"Asia Pacific Admin ADIRA",
"Lbc Pasific Owner",
"Lbc Pasific Admin MAGI & Simasnet",
"Lbc Pasific Admin MAG & FPG",
"Lbc Pasific Admin TOB & SOMPO",
"Lbc Pasific Admin Harta, Panfic & ACA",
"Lbc Pasific Admin Adira",
"Premi Type H Corporate",
"Premi Type A'",
"Premi Type B'",
"Premi Type C'",
"Premi Type D'",
"Premi Type E'",
"CMP Type A",
"CMP Type B",
"Farajasa Owner",
"Farajasa Admin MAGI & SIMASNET",
"Farajasa Admin MAG & KSK",
"Farajasa Admin Adira",
"Farajasa Admin ACA & Panfic",
"DMA Owner",
"DMA Admin",
"CEKPREMI-AGENT-E Advance Commission",
"CEKPREMI-AGENT-DJATI",
"CEKPREMI-AGENT-D Advance Commission",
"Buana Sejahtera Wisata",
"CEKPREMI-AGENT-DE",
"CEKPREMI-AGENT-NURLELA",
"CEKPREMI-AGENT-ANGGI ROZA",
"Lim Kuan Siong (A)",
"Lim Kuan Siong (B)",
"PREMIQ (A)",
"PREMIQ (B)",
"CEKPREMI-AGENT-FEBRIYANTO LIMAS",
"Mustika",
"Sub Agent Mustika - A",
"Sub Agent Mustika - B",
"DEDDY JANUAR",
"Member CK7",
"CEKPREMI-AGENT-HUSIN",
"Type Partner Penampungan Wuling",
"Type Partner OM Wuling",
"Partner Type BM Wuling",
"Partner Type SPV Wuling",
"Partner Type Sales Wuling",
"Partner Type Office Wuling",
"Partner Type  BNI Multifinance",
"Sinergi Platinum",
"Sinergi Partner",
"Firman Burham",
"Dealership",
"Sinergi Partner C",
"Sinergi Partner B",
"Sinergi Partner A",
"Makmur Enterprise",
"LIM A",
"LIM B",
"LIM Type A",
"LIM Type B",
"LIM Type C",
"PREMIQ Topline",
"PREMIQ Downline",
"CEKPREMI-AGENT-D' (SEMARANG)",
"CEKPREMI-AGENT-JCS",
"Agent Type TOP",
"ENDAH",
"DIRECTOR HONDA LAMPUNG",
"OFFICE HONDA LAMPUNG",
"SM / CCO Mngr HONDA LAMPUNG",
"SPV / SPV CCO HONDA LAMPUNG",
"Sales / CCO HONDA LAMPUNG",
"BK Medan",
"Mitra Sinergi Bisnis",
"Sub Agen Mitra Sinergi Bisnis",
"Owner Mitra Sinergi Bisnis",
"Admin Mitra Sinergi Bisnis",
"Nadia Yunita",
"Nadia Yunita B",
"Kendali Type E",
"Kendali Type A",
"SP_A",
"Premi Dealership",
"Health Retail - Partner",
"Health Retail - Partner Manager",
"Health Retail - Partner Director",
"B2C-MANAGER-AGENT",
"CEKPREMI-AGENT-P",
"CEKPREMI-AGENT-Q",
"TAJ-Automate",
"testing PO",
"Nadia Yunita C");
"""

backdoor_cancel_sql = """
    SELECT pp.fuse_policy_code, DATE_FORMAT(DATE_ADD(pbdr1.cancel_date,INTERVAL 7 HOUR),'%Y-%m') 'cancel_time', DATE_FORMAT(DATE_ADD(pp.force_cancel_time,INTERVAL 7 HOUR),'%Y-%m') 'force_cancel_time' from policy_back_door pp LEFT JOIN  policy_back_door_review pbdr1 ON ( pp.fuse_policy_code = pbdr1.fuse_policy_code AND  (pbdr1.review_opinion = 'Cancel' or (pbdr1.type = 2 AND pbdr1.review_opinion = 'Approved' AND pbdr1.cancel_policy = 1)))
where pbdr1.cancel_date is not null or  pp.force_cancel_time is not null ;
"""

offline_reject ="""
SELECT fuse_code fuse_policy_code,DATE_FORMAT(DATE_ADD(app_time,INTERVAL 7 HOUR),'%Y-%m') reject_time from policy_approve_record where approve_type in (105,106);
"""
backdoor_reject="""
SELECT fuse_policy_code,DATE_FORMAT(DATE_ADD(operate_time,INTERVAL 7 HOUR),'%Y-%m') reject_time from policy_back_door_review where confirm_opinion='Reject';
"""

## mongo query
offline_cancel_data = mgdb.policy.find({"$or":[{"cancelTime":{ "$exists": "true" }},{"forceCancelTime":{ "$exists": "true" }}]},
                              {"_id": 0, "fusePolicyCode": 1, "cancelTime": 1,"forceCancelTime":1});

other_reject_info_data = mgdb.operateLog.find({"operateType":14,"details":{"$regex":"Action: no process"}},{"_id":0,"serchId":1,"operateTime":1});

def get_policy_info():
    print("query data......")
    data = db.querysql(policy_sql)
    column = db.get_column_name()
    data_df = pd.DataFrame(np.array(data))
    data_df.columns = column
    return data_df


def get_reject_time():
    ## offline
    offline_reject_info = db.querysql(offline_reject)
    column = db.get_column_name()
    ## backdoor
    backdoor_reject_info = db.querysql(backdoor_reject)
    ## other
    other_reject_info = [(u['serchId'],datetime.strftime(datetime.fromtimestamp(int(u['operateTime'])/1000+25200),'%Y-%m')) for u in other_reject_info_data]
    all_reject_info = backdoor_reject_info + offline_reject_info + other_reject_info

    data_df = pd.DataFrame(np.array(all_reject_info))
    data_df.columns = column
    return data_df


def get_cancel_time():
    print("query cancel time")
    ## offline

    offline_cancel_info = [(u['fusePolicyCode'],datetime.strftime(datetime.fromtimestamp(int(u['cancelTime'])/1000+25200),'%Y-%m') if 'cancelTime' in u else None,datetime.strftime(datetime.fromtimestamp(int(u['forceCancelTime'])/1000+25200),'%Y-%m') if 'forceCancelTime' in u else None ) for u in offline_cancel_data]

    ## backdoor
    backdoor_cancel_info = db.querysql(backdoor_cancel_sql)

    column = db.get_column_name()

    all_cancel_info = offline_cancel_info+backdoor_cancel_info
    data_df = pd.DataFrame(np.array(all_cancel_info))
    data_df.columns = column
    return data_df


def data_generate():
    basic_info = get_policy_info()
    reject_time_info = get_reject_time()
    cancel_time_info = get_cancel_time()

    #数据合并
    temp1 = pd.merge(basic_info,reject_time_info,on="fuse_policy_code",how='left')
    policy_info= pd.merge(temp1,cancel_time_info,on="fuse_policy_code",how='left')




    # time_filter = lambda x: x['order_time_month']=='2020-10'

    time_filter = lambda x: x['order_time_month']!=x['reject_time'] and x['order_time_month']!=x['cancel_time'] and x['order_time_month']!=x['force_cancel_time']

    policy_info=policy_info[policy_info.apply(time_filter, axis=1)]


    ## 分组聚合
    ## 保险公司名字转小写
    insurance_group = policy_info

    policy_count=[]
    sub_total = {"policy_source":'GRAND TOTAL',"policy_type" :'GRAND TOTAL'}
    sub_total["total_count"] = policy_info.__len__()
    sub_total["total_gwp"] = policy_info['gwp'].sum()

    policy_group = policy_info.groupby(["policy_source", "policy_type"])

    for name, policy_sheet in policy_group:
        this_type = pd.DataFrame(policy_sheet)
        each_month = this_type.groupby(["order_time_month"])
        this_policy = { "policy_source":name[0],"policy_type" :name[1]}
        for month_name, each_month_sheet in each_month:
            this_month = pd.DataFrame(each_month_sheet)
            this_policy[month_name+"_count"]=this_month.__len__()
            this_policy[month_name+"_gwp"]=this_month['gwp'].sum()
            sub_total[month_name + "_count"] = (sub_total[month_name + "_count"] if (month_name + "_count") in sub_total else 0) +this_month.__len__()
            sub_total[month_name + "_gwp"] = (sub_total[month_name + "_gwp"] if (month_name + "_gwp") in sub_total else 0) +this_month['gwp'].sum()

        this_policy["total_count"] = this_type.__len__()
        this_policy["total_gwp"] = this_type['gwp'].sum()
        policy_count.append(this_policy)

    policy_count.append(sub_total)

    policy_info = pd.DataFrame(policy_count)

    order = ['policy_source', 'policy_type', '2020-08_count', '2020-08_gwp','2020-09_count', '2020-09_gwp','2020-10_count', '2020-10_gwp', 'total_count', 'total_gwp']
    policy_info = policy_info[order]

    policy_info = policy_info.fillna(0)
    print(policy_info)
    return policy_info


def write_to_excel(policy_info):
    ## 导出到excel
    print("writing......")
    t = datetime.now().date() - timedelta(days=1)
    writer = pd.ExcelWriter("product_report_No2" + (u'_%d%02d%02d.xlsx' % (t.year, t.month, t.day)))

    wb = writer.book

    # 3.设置格式
    header_fmt = wb.add_format(
        {'bold': True, 'font_size': 13,'font_color': 'white', 'font_name': u'微软雅黑','valign': 'vcenter', 'bg_color': '#808080', 'align': 'center'})
    total_line_fmt = wb.add_format({ 'bg_color': '#A9A9A9'})
    merge_fmt = wb.add_format({ 'bold': True, 'font_size': 12,'bg_color': '#A9A9A9', 'align': 'center'})

    sheet_name = u'product_report_No2'
    policy_info.to_excel(writer, sheet_name=sheet_name, encoding='utf8', header=True, index=False, startcol=0, startrow=0)
    worksheet1 = writer.sheets[sheet_name]
    worksheet1.set_column('A:K', 20)
    for col_num, value in enumerate(policy_info.columns.values):
        worksheet1.write(0, col_num, value, header_fmt)

    for index, row in policy_info.iterrows():
        if(row['policy_source']=="GRAND TOTAL"):
            for col_num, value in enumerate(row):
                worksheet1.write(index + 1, col_num, value if not pd.isna(value) else '', header_fmt)

    # worksheet1.merge_range(first_row=1,last_row=1,first_col=1,last_col=2,data="merge",cell_format=merge_fmt)
    writer.save()

    print("write done !!!")




write_to_excel(data_generate())
## 关闭连接
mdbF.shoutdown()
db.shoutdown()

print("finish..........")
