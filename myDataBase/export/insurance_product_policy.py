# @Time    : 2020/11/14 5:22 下午
# @Author  : luoh
# @Email   : luohenghlx@163.com
# @File    : insurance_product_policy.py
# @Software: PyCharm
# @Description:

import numpy as np
import pandas as pd
from datafactory import Databasefactory as dbf
from datafactory import pro_db_config as db_config
from datetime import datetime, timedelta
from mongo_data_factory import MongoDataBaseFactory as mdbf
from mongo_data_factory import pro_db_config as mdb_config


policy_sql = """
-- auto
SELECT * from (
SELECT pp.fuse_policy_code,att.agent_type_name,DATE_ADD(pp.create_time,INTERVAL 7 HOUR) 'order_time',DATE_FORMAT(DATE_ADD(pp.create_time,INTERVAL 7 HOUR),'%Y-%m') 'order_time_month'  ,'Offline' as 'policy_source',ic.company_name 'insurance_company_name',(CASE pc.category_type WHEN 1 THEN 'General' WHEN 2 THEN 'Travel' WHEN 3 THEN 'Car' WHEN 4 THEN 'Moto' WHEN 6 THEN 'Life' WHEN 7 THEN 'PA' WHEN 8 THEN 'Property' WHEN 9 THEN 'Health' WHEN 11 THEN 'Marine Cargo' WHEN 13 THEN 'SME' WHEN 14 THEN 'MOVEABLE ALL RISK' WHEN 12 THEN 'ROP' WHEN 15 THEN 'VIP' ELSE '' END) as 'policy type'  ,(pp.policy_amount-pp.admin_fee-pp.service_fee)  'gwp'
from policy_auto pp
LEFT JOIN policy p on p.main_policy_code=pp.main_policy_code
LEFT JOIN product pd on pd.product_code=pp.product_code
LEFT JOIN product_category pc on pd.category_code=pc.category_code  
LEFT JOIN insurance_company ic on  pd.insurance_company_code=ic.company_code
LEFT JOIN account_agent aa on pp.create_account=aa.mobile
LEFT JOIN agent_type att on att.agent_type_code=aa.agent_type_code
where  aa.test_account in (0,4) and policy_status not in (-1,101) and (p.pay_time_type !=2 OR (p.pay_time_type=2 and pp.pay_status=102))  and  DATE_ADD(pp.create_time,INTERVAL 7 HOUR)>'2020-08-01 00:00:00' and DATE_ADD(pp.create_time,INTERVAL 7 HOUR)<'2020-11-01 00:00:00'
union

-- bacdkoor
SELECT pp.fuse_policy_code,att.agent_type_name,DATE_ADD(pp.order_date,INTERVAL 7 HOUR) 'order_time',DATE_FORMAT(DATE_ADD(pp.order_date,INTERVAL 7 HOUR),'%Y-%m') 'order_time_month'  ,'Backdoor' as 'policy_source',pp.insurance_company_name 'insurance_company_name',pp.policy_type as 'policy type'  ,IF(pp.is_co_as=1,(ifnull( `pp`.`basic_premium`, 0 ) + ifnull( `pp`.`rider_total_premium`, 0 )) * ( ifnull( t.`company_percentage`, 0 ) / 100 ),ifnull( `pp`.`basic_premium`, 0 ) + ifnull( `pp`.`rider_total_premium`, 0 ))  'gwp'
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
SELECT pp.fuse_policy_code,att.agent_type_name,DATE_ADD(ci.create_time,INTERVAL 7 HOUR) 'order_time',DATE_FORMAT(DATE_ADD(ci.create_time,INTERVAL 7 HOUR),'%Y-%m') 'order_time_month'  ,'Others' as 'policy_source',pp.insurance_company_order 'insurance_company_name',pp.product_name as 'policy type'  ,IFNULL(pp.pay_amount,0)-IFNULL(pp.admin_fee,0)   'gwp'
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


offline_reject ="""
SELECT fuse_code fuse_policy_code,app_time reject_time from policy_approve_record where approve_type in (105,106);
"""
backdoor_reject="""
SELECT fuse_policy_code,operate_time reject_time from policy_back_door_review where confirm_opinion='Reject';
"""
other_reject = """
db.operateLog.find({"operateType":14,"details":{$regex:/Action: no process/}});
"""

def get_policy_info():
    print("query data......")
    db = dbf.DataBaseFactory(db_config)
    data = db.querysql(policy_sql)
    column = db.get_column_name()
    data_df = pd.DataFrame(np.array(data))
    data_df.columns = column
    return data_df


def get_cancel_time():
    db = dbf.DataBaseFactory(db_config)
    offline_reject_info = db.querysql(offline_reject)
    print(type(offline_reject_info))
    print(offline_reject_info)
    column = db.get_column_name()
    backdoor_reject_info = db.querysql(backdoor_reject)
    print(backdoor_reject_info)
    all_reject_info=backdoor_reject_info+offline_reject_info
    print(all_reject_info)

    db = mdbf.MongoDataBaseFactory(mdb_config).getdb()
    data = db.operateLog.find({"operateType":14,"details":{"$regex":"Action: no process"}});
    data_list = [u for u in data]
    print(data_list)
    print("get cancel time")


policy_info = get_policy_info()
cancel_time_info = get_cancel_time()
print(policy_info)
