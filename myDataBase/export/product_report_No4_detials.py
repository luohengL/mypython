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

closing_sql = """
SELECT *  from (
-- auto
SELECT pp.fuse_policy_code,pp.associated_policy_code,ic.company_name 'insurance_company_name',acc.partner_id,'Offline' as 'policy_source',pri.tag,att.agent_type_name 'partner_type',
(CASE pc.category_type WHEN 1 THEN 'General' WHEN 2 THEN 'Travel' WHEN 3 THEN 'Car' WHEN 4 THEN 'Moto' WHEN 6 THEN 'Life' WHEN 7 THEN 'PA' WHEN 8 THEN 'Property' WHEN 9 THEN 'Health' WHEN 11 THEN 'Marine Cargo' WHEN 13 THEN 'SME' WHEN 14 THEN 'MOVEABLE ALL RISK' WHEN 12 THEN 'ROP' WHEN 15 THEN 'VIP' ELSE '' END) as 'policy type' ,
(CASE pp.policy_status WHEN 101 THEN 'Quote' WHEN 102 THEN 'Pending' WHEN 103 THEN 'Inactive' WHEN 104 THEN 'Active' WHEN 105 THEN 'Expired' WHEN 106 THEN 'Cancel' WHEN 107 THEN 'Rejected' WHEN 108 THEN 'Invalid' WHEN 109 THEN 'FORCE CANCEL' ELSE '' END) AS policy_status ,
(CASE pp.operation_status WHEN 101 THEN 'UNSUBMIT' WHEN 102 THEN 'PENGDING' WHEN 103 THEN 'APPROVED' WHEN 104 THEN 'RESUBMIT' WHEN 105 THEN 'DECLINE' WHEN 106 THEN 'BLACKLIST' WHEN 107 THEN 'CANCEL' WHEN 108 THEN 'CONFIRMED' WHEN 109 THEN 'Agent Agree' ELSE '' END) as operateStatus,
(CASE pp.pay_status WHEN 101 THEN 'UNPAID' WHEN 102 THEN 'PAID' WHEN 103 THEN 'REFUND' ELSE '' END) as 'payment_status',
DATE_ADD(pp.create_time,INTERVAL 7 HOUR) 'order_time',DATE_ADD(pm.actual_pay_time,INTERVAL 7 HOUR)'actual payment date' , DATE_ADD(pm.create_time,INTERVAL 7 HOUR)  'confirm payment date' 
from policy_auto pp
LEFT JOIN policy p on p.main_policy_code=pp.main_policy_code
LEFT JOIN policy_auto_person pap on pap.person_code=pp.person_code
LEFT JOIN policy_rm_info pri on pri.fuse_policy_code=pp.fuse_policy_code
LEFT JOIN payment pm on pm.main_policy_code=pp.main_policy_code
LEFT JOIN product pd on pd.product_code=pp.product_code
LEFT JOIN product_category pc on pd.category_code=pc.category_code  
LEFT JOIN insurance_company ic on  pd.insurance_company_code=ic.company_code
LEFT JOIN account_agent aa on pp.create_account=aa.mobile
LEFT JOIN account acc on pp.create_account=acc.mobile
LEFT JOIN rm_advance_agent_waiting raaw on raaw.mobile=pp.create_account
LEFT JOIN agent_type att on att.agent_type_code=aa.agent_type_code
where  aa.test_account in (0,4) and policy_status not in (-1,101) and (p.pay_time_type !=2 OR (p.pay_time_type=2 and pp.pay_status=102))  and  DATE_FORMAT(DATE_ADD(pp.create_time,INTERVAL 7 HOUR),'%Y-%m') ='2020-10'
union
-- general
SELECT 
 pp.fuse_policy_code,pp.associated_policy_code,ic.company_name 'insurance_company_name',acc.partner_id,'Offline' as 'policy_source',pri.tag,att.agent_type_name 'partner_type',
(CASE pc.category_type WHEN 1 THEN 'General' WHEN 2 THEN 'Travel' WHEN 3 THEN 'Car' WHEN 4 THEN 'Moto' WHEN 6 THEN 'Life' WHEN 7 THEN 'PA' WHEN 8 THEN 'Property' WHEN 9 THEN 'Health' WHEN 11 THEN 'Marine Cargo' WHEN 13 THEN 'SME' WHEN 14 THEN 'MOVEABLE ALL RISK' WHEN 12 THEN 'ROP' WHEN 15 THEN 'VIP' ELSE '' END) as 'policy type' ,
(CASE pp.policy_status WHEN 101 THEN 'Quote' WHEN 102 THEN 'Pending' WHEN 103 THEN 'Inactive' WHEN 104 THEN 'Active' WHEN 105 THEN 'Expired' WHEN 106 THEN 'Cancel' WHEN 107 THEN 'Rejected' WHEN 108 THEN 'Invalid' WHEN 109 THEN 'FORCE CANCEL' ELSE '' END) AS policy_status ,
(CASE pp.operation_status WHEN 101 THEN 'UNSUBMIT' WHEN 102 THEN 'PENGDING' WHEN 103 THEN 'APPROVED' WHEN 104 THEN 'RESUBMIT' WHEN 105 THEN 'DECLINE' WHEN 106 THEN 'BLACKLIST' WHEN 107 THEN 'CANCEL' WHEN 108 THEN 'CONFIRMED' WHEN 109 THEN 'Agent Agree' ELSE '' END) as operateStatus,
(CASE pp.pay_status WHEN 101 THEN 'UNPAID' WHEN 102 THEN 'PAID' WHEN 103 THEN 'REFUND' ELSE '' END) as 'payment_status',
DATE_ADD(pp.create_time,INTERVAL 7 HOUR) 'order_time',DATE_ADD(pm.actual_pay_time,INTERVAL 7 HOUR)'actual payment date' , DATE_ADD(pm.create_time,INTERVAL 7 HOUR)  'confirm payment date' 
from policy_general pp
LEFT JOIN policy p on p.main_policy_code=pp.main_policy_code
LEFT JOIN policy_general_person pgp on pgp.person_code=pp.insured_person_code
LEFT JOIN policy_rm_info pri on pri.fuse_policy_code=pp.fuse_policy_code
LEFT JOIN payment pm on pm.main_policy_code=pp.main_policy_code
LEFT JOIN product pd on pd.product_code=pp.product_code
LEFT JOIN product_category pc on pd.category_code=pc.category_code  
LEFT JOIN insurance_company ic on  pd.insurance_company_code=ic.company_code
LEFT JOIN account_agent aa on pp.create_account=aa.mobile
LEFT JOIN account acc on pp.create_account=acc.mobile
LEFT JOIN rm_advance_agent_waiting raaw on raaw.mobile=pp.create_account
LEFT JOIN agent_type att on att.agent_type_code=aa.agent_type_code
where  aa.test_account in (0,4) and policy_status not in (-1,101) and (p.pay_time_type !=2 OR (p.pay_time_type=2 and pp.pay_status=102))   and  DATE_FORMAT(DATE_ADD(pp.create_time,INTERVAL 7 HOUR),'%Y-%m') ='2020-10'
union 
-- bacdkoor
SELECT 
pp.fuse_policy_code,pp.npp as  associated_policy_code,pp.insurance_company_name 'insurance_company_name',acc.partner_id,'Backdoor' as 'policy_source',pri.tag,att.agent_type_name 'partner_type',
pp.policy_type as 'policy type' ,
(CASE pp.policy_status WHEN 101 THEN 'Quote' WHEN 102 THEN 'Pending' WHEN 103 THEN 'Inactive' WHEN 104 THEN 'Active' WHEN 105 THEN 'Expired' WHEN 106 THEN 'Cancel' WHEN 107 THEN 'Rejected' WHEN 108 THEN 'Invalid' WHEN 109 THEN 'FORCE CANCEL' ELSE '' END) AS policy_status ,
(CASE pp.operate_status WHEN 101 THEN 'UNSUBMIT' WHEN 102 THEN 'PENGDING' WHEN 103 THEN 'APPROVED' WHEN 104 THEN 'RESUBMIT' WHEN 105 THEN 'DECLINE' WHEN 106 THEN 'BLACKLIST' WHEN 107 THEN 'CANCEL' WHEN 108 THEN 'CONFIRMED' WHEN 109 THEN 'Agent Agree' ELSE '' END) as operateStatus,
(CASE pp.pay_status WHEN 101 THEN 'UNPAID' WHEN 102 THEN 'PAID' WHEN 103 THEN 'REFUND' ELSE '' END) as 'payment_status',
DATE_ADD(pp.create_time,INTERVAL 7 HOUR) 'order_time',DATE_ADD(pm.actual_pay_time,INTERVAL 7 HOUR)'actual payment date' , DATE_ADD(pm.create_time,INTERVAL 7 HOUR)  'confirm payment date' 

from policy_back_door pp
LEFT JOIN payment pm on pm.main_policy_code=pp.fuse_policy_code
LEFT JOIN account_agent aa on pp.agent_mobile=aa.mobile
LEFT JOIN policy_rm_info pri on pri.fuse_policy_code=pp.fuse_policy_code
LEFT JOIN account acc on pp.agent_mobile=acc.mobile
LEFT JOIN rm_advance_agent_waiting raaw on raaw.mobile=pp.agent_mobile
LEFT JOIN agent_type att on att.agent_type_code=aa.agent_type_code
LEFT JOIN (
	SELECT sum(`pbdca`.`company_percentage` ) AS `company_percentage`,`pbdca`.`fuse_policy_code` AS `fuse_policy_code` FROM `policy_back_door_co_as` `pbdca` 
	WHERE ( `pbdca`.`is_fuse` = 1 ) GROUP BY `pbdca`.`fuse_policy_code`
) t on pp.fuse_policy_code=t.fuse_policy_code
where  aa.test_account in (0,4) and pp.policy_status not in (-1,101) and  DATE_FORMAT(DATE_ADD(pp.order_date,INTERVAL 7 HOUR),'%Y-%m')='2020-10'

union
-- other
SELECT 

pp.fuse_policy_code,null as  associated_policy_code,pp.insurance_company_order 'insurance_company_name',acc.partner_id,'Backdoor' as 'policy_source',pri.tag,att.agent_type_name 'partner_type',
pp.product_name as 'policy type' ,
(CASE pp.policy_status WHEN 101 THEN 'Quote' WHEN 102 THEN 'Pending' WHEN 103 THEN 'Inactive' WHEN 104 THEN 'Active' WHEN 105 THEN 'Expired' WHEN 106 THEN 'Cancel' WHEN 107 THEN 'Rejected' WHEN 108 THEN 'Invalid' WHEN 109 THEN 'FORCE CANCEL' ELSE '' END) AS policy_status ,
(CASE pp.operate_status WHEN 101 THEN 'Unsubmit' WHEN 102 THEN 'Submit' WHEN 103 THEN 'Got QS' WHEN 104 THEN 'Accepted' WHEN 105 THEN 'Rejected' WHEN 106 THEN 'Change' WHEN 107 THEN 'Wait CN' WHEN 108 THEN 'Got CN' WHEN 109 THEN 'Approved' WHEN 110 THEN 'Declined' WHEN 111 THEN 'Got CN' WHEN 112 THEN 'Got CN' ELSE '' END) as operateStatus ,
(CASE pp.pay_status WHEN 101 THEN 'UNPAID' WHEN 102 THEN 'PAID' WHEN 103 THEN 'REFUND' ELSE '' END) as 'payment_status',
DATE_ADD(pp.create_time,INTERVAL 7 HOUR) 'order_time',DATE_ADD(pm.actual_pay_time,INTERVAL 7 HOUR)'actual payment date' , DATE_ADD(pm.create_time,INTERVAL 7 HOUR)  'confirm payment date' 

from external_policy_common pp
LEFT JOIN policy_rm_info pri on pri.fuse_policy_code=pp.fuse_policy_code
LEFT JOIN payment pm on pm.main_policy_code=pp.fuse_policy_code
LEFT JOIN  (SELECT fuse_policy_code,send_time 'create_time' from  covernote_information where type=2 GROUP BY fuse_policy_code ) ci on ci.fuse_policy_code=pp.fuse_policy_code 
LEFT JOIN account_agent aa on pp.create_account=aa.mobile
LEFT JOIN account acc on pp.create_account=acc.mobile
LEFT JOIN rm_advance_agent_waiting raaw on raaw.mobile=pp.create_account
LEFT JOIN agent_type att on att.agent_type_code=aa.agent_type_code
where  aa.test_account in (0,4) and pp.policy_status not in (-1,101) and DATE_FORMAT(DATE_ADD(ci.create_time,INTERVAL 7 HOUR),'%Y-%m')='2020-10' 

) t where  t.partner_type in (
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
"Nadia Yunita C") ;

"""


paid_sql = """
SELECT * from (

-- auto
SELECT DATE_ADD(pp.create_time,INTERVAL 7 HOUR) 'order_time',pap.`name` 'insured_name',acc.`name` 'partner_name' ,att.agent_type_name 'partner_type' ,acc.partner_id,pp.fuse_policy_code,pp.company_policy_code 'insurance_number' ,(pp.policy_amount-pp.admin_fee-pp.service_fee)  ' premium ' ,
(CASE pp.pay_status WHEN 101 THEN 'UNPAID' WHEN 102 THEN 'PAID' WHEN 103 THEN 'REFUND' ELSE '' END) as 'payment_status',
(CASE pp.policy_status WHEN 101 THEN 'Quote' WHEN 102 THEN 'Pending' WHEN 103 THEN 'Inactive' WHEN 104 THEN 'Active' WHEN 105 THEN 'Expired' WHEN 106 THEN 'Cancel' WHEN 107 THEN 'Rejected' WHEN 108 THEN 'Invalid' WHEN 109 THEN 'FORCE CANCEL' ELSE '' END) AS policy_status ,
(CASE pp.operation_status WHEN 101 THEN 'UNSUBMIT' WHEN 102 THEN 'PENGDING' WHEN 103 THEN 'APPROVED' WHEN 104 THEN 'RESUBMIT' WHEN 105 THEN 'DECLINE' WHEN 106 THEN 'BLACKLIST' WHEN 107 THEN 'CANCEL' WHEN 108 THEN 'CONFIRMED' WHEN 109 THEN 'Agent Agree' ELSE '' END) as operateStatus,
ic.company_name 'insurance_company_name',(CASE pc.category_type WHEN 1 THEN 'General' WHEN 2 THEN 'Travel' WHEN 3 THEN 'Car' WHEN 4 THEN 'Moto' WHEN 6 THEN 'Life' WHEN 7 THEN 'PA' WHEN 8 THEN 'Property' WHEN 9 THEN 'Health' WHEN 11 THEN 'Marine Cargo' WHEN 13 THEN 'SME' WHEN 14 THEN 'MOVEABLE ALL RISK' WHEN 12 THEN 'ROP' WHEN 15 THEN 'VIP' ELSE '' END) as 'policy type' ,'Offline' as 'policy_source',
DATE_ADD(pm.actual_pay_time,INTERVAL 7 HOUR)'actual payment date' , DATE_ADD(pm.create_time,INTERVAL 7 HOUR)  'confirm payment date' 
from policy_auto pp
LEFT JOIN policy p on p.main_policy_code=pp.main_policy_code
LEFT JOIN policy_auto_person pap on pap.person_code=pp.person_code
LEFT JOIN payment pm on pm.main_policy_code=pp.main_policy_code
LEFT JOIN product pd on pd.product_code=pp.product_code
LEFT JOIN product_category pc on pd.category_code=pc.category_code  
LEFT JOIN insurance_company ic on  pd.insurance_company_code=ic.company_code
LEFT JOIN account_agent aa on pp.create_account=aa.mobile
LEFT JOIN account acc on pp.create_account=acc.mobile
LEFT JOIN rm_advance_agent_waiting raaw on raaw.mobile=pp.create_account
LEFT JOIN agent_type att on att.agent_type_code=aa.agent_type_code
where  aa.test_account in (0,4) and policy_status not in (-1,101) and (p.pay_time_type !=2 OR (p.pay_time_type=2 and pp.pay_status=102))  and  DATE_FORMAT(DATE_ADD(pm.actual_pay_time,INTERVAL 7 HOUR),'%Y-%m') ='2020-10'
union
-- general
SELECT 
DATE_ADD(pp.create_time,INTERVAL 7 HOUR) 'order_time',pgp.`name` 'insured_name',acc.`name` 'partner_name' ,att.agent_type_name 'partner_type' ,acc.partner_id,pp.fuse_policy_code,pp.company_policy_code 'insurance_number' ,(pp.policy_amount-pp.admin_fee-pp.service_fee)  ' premium ' ,
(CASE pp.pay_status WHEN 101 THEN 'UNPAID' WHEN 102 THEN 'PAID' WHEN 103 THEN 'REFUND' ELSE '' END) as 'payment_status',
(CASE pp.policy_status WHEN 101 THEN 'Quote' WHEN 102 THEN 'Pending' WHEN 103 THEN 'Inactive' WHEN 104 THEN 'Active' WHEN 105 THEN 'Expired' WHEN 106 THEN 'Cancel' WHEN 107 THEN 'Rejected' WHEN 108 THEN 'Invalid' WHEN 109 THEN 'FORCE CANCEL' ELSE '' END) AS policy_status ,
(CASE pp.operation_status WHEN 101 THEN 'UNSUBMIT' WHEN 102 THEN 'PENGDING' WHEN 103 THEN 'APPROVED' WHEN 104 THEN 'RESUBMIT' WHEN 105 THEN 'DECLINE' WHEN 106 THEN 'BLACKLIST' WHEN 107 THEN 'CANCEL' WHEN 108 THEN 'CONFIRMED' WHEN 109 THEN 'Agent Agree' ELSE '' END) as operateStatus,
ic.company_name 'insurance_company_name',(CASE pc.category_type WHEN 1 THEN 'General' WHEN 2 THEN 'Travel' WHEN 3 THEN 'Car' WHEN 4 THEN 'Moto' WHEN 6 THEN 'Life' WHEN 7 THEN 'PA' WHEN 8 THEN 'Property' WHEN 9 THEN 'Health' WHEN 11 THEN 'Marine Cargo' WHEN 13 THEN 'SME' WHEN 14 THEN 'MOVEABLE ALL RISK' WHEN 12 THEN 'ROP' WHEN 15 THEN 'VIP' ELSE '' END) as 'policy type' ,'Offline' as 'policy_source',
DATE_ADD(pm.actual_pay_time,INTERVAL 7 HOUR)'actual payment date' , DATE_ADD(pm.create_time,INTERVAL 7 HOUR)  'confirm payment date' 
from policy_general pp
LEFT JOIN policy p on p.main_policy_code=pp.main_policy_code
LEFT JOIN policy_general_person pgp on pgp.person_code=pp.insured_person_code
LEFT JOIN payment pm on pm.main_policy_code=pp.main_policy_code
LEFT JOIN product pd on pd.product_code=pp.product_code
LEFT JOIN product_category pc on pd.category_code=pc.category_code  
LEFT JOIN insurance_company ic on  pd.insurance_company_code=ic.company_code
LEFT JOIN account_agent aa on pp.create_account=aa.mobile
LEFT JOIN account acc on pp.create_account=acc.mobile
LEFT JOIN rm_advance_agent_waiting raaw on raaw.mobile=pp.create_account
LEFT JOIN agent_type att on att.agent_type_code=aa.agent_type_code
where  aa.test_account in (0,4) and policy_status not in (-1,101) and (p.pay_time_type !=2 OR (p.pay_time_type=2 and pp.pay_status=102))   and  DATE_FORMAT(DATE_ADD(pm.actual_pay_time,INTERVAL 7 HOUR),'%Y-%m') ='2020-10'
union 
-- bacdkoor
SELECT 
DATE_ADD(pp.order_date,INTERVAL 7 HOUR) 'order_time',pp.insured_person_name 'insured_name',acc.`name` 'partner_name' ,att.agent_type_name 'partner_type' ,acc.partner_id,pp.fuse_policy_code,pp.company_policy_code 'insurance_number' ,IF(pp.is_co_as=1,(ifnull( `pp`.`basic_premium`, 0 ) + ifnull( `pp`.`rider_total_premium`, 0 )) * ( ifnull( t.`company_percentage`, 0 ) / 100 ),ifnull( `pp`.`basic_premium`, 0 ) + ifnull( `pp`.`rider_total_premium`, 0 ))  ' premium ' ,
(CASE pp.pay_status WHEN 101 THEN 'UNPAID' WHEN 102 THEN 'PAID' WHEN 103 THEN 'REFUND' ELSE '' END) as 'payment_status',
(CASE pp.policy_status WHEN 101 THEN 'Quote' WHEN 102 THEN 'Pending' WHEN 103 THEN 'Inactive' WHEN 104 THEN 'Active' WHEN 105 THEN 'Expired' WHEN 106 THEN 'Cancel' WHEN 107 THEN 'Rejected' WHEN 108 THEN 'Invalid' WHEN 109 THEN 'FORCE CANCEL' ELSE '' END) AS policy_status ,
(CASE pp.operate_status WHEN 101 THEN 'UNSUBMIT' WHEN 102 THEN 'PENGDING' WHEN 103 THEN 'APPROVED' WHEN 104 THEN 'RESUBMIT' WHEN 105 THEN 'DECLINE' WHEN 106 THEN 'BLACKLIST' WHEN 107 THEN 'CANCEL' WHEN 108 THEN 'CONFIRMED' WHEN 109 THEN 'Agent Agree' ELSE '' END) as operateStatus,
pp.insurance_company_name  'insurance_company_name',pp.policy_type as 'policy type' ,'Backdoor' as 'policy_source',
DATE_ADD(pm.actual_pay_time,INTERVAL 7 HOUR)'actual payment date' , DATE_ADD(pm.create_time,INTERVAL 7 HOUR)  'confirm payment date' 

from policy_back_door pp
LEFT JOIN payment pm on pm.main_policy_code=pp.fuse_policy_code
LEFT JOIN account_agent aa on pp.agent_mobile=aa.mobile
LEFT JOIN account acc on pp.agent_mobile=acc.mobile
LEFT JOIN rm_advance_agent_waiting raaw on raaw.mobile=pp.agent_mobile
LEFT JOIN agent_type att on att.agent_type_code=aa.agent_type_code
LEFT JOIN (
	SELECT sum(`pbdca`.`company_percentage` ) AS `company_percentage`,`pbdca`.`fuse_policy_code` AS `fuse_policy_code` FROM `policy_back_door_co_as` `pbdca` 
	WHERE ( `pbdca`.`is_fuse` = 1 ) GROUP BY `pbdca`.`fuse_policy_code`
) t on pp.fuse_policy_code=t.fuse_policy_code
where  aa.test_account in (0,4) and pp.policy_status not in (-1,101) and  DATE_FORMAT(DATE_ADD(pm.actual_pay_time,INTERVAL 7 HOUR),'%Y-%m')='2020-10'

union
-- other
SELECT 
DATE_ADD(ci.create_time,INTERVAL 7 HOUR) 'order_time',pp.insured_name 'insured_name',acc.`name` 'partner_name' ,att.agent_type_name 'partner_type' ,acc.partner_id,pp.fuse_policy_code,pp.company_policy_code 'insurance_number' ,IFNULL(pp.pay_amount,0)-IFNULL(pp.admin_fee,0)  ' premium ' ,
(CASE pp.pay_status WHEN 101 THEN 'UNPAID' WHEN 102 THEN 'PAID' WHEN 103 THEN 'REFUND' ELSE '' END) as 'payment_status',
(CASE pp.policy_status WHEN 101 THEN 'Quote' WHEN 102 THEN 'Pending' WHEN 103 THEN 'Inactive' WHEN 104 THEN 'Active' WHEN 105 THEN 'Expired' WHEN 106 THEN 'Cancel' WHEN 107 THEN 'Rejected' WHEN 108 THEN 'Invalid' WHEN 109 THEN 'FORCE CANCEL' ELSE '' END) AS policy_status ,
(CASE pp.operate_status WHEN 101 THEN 'Unsubmit' WHEN 102 THEN 'Submit' WHEN 103 THEN 'Got QS' WHEN 104 THEN 'Accepted' WHEN 105 THEN 'Rejected' WHEN 106 THEN 'Change' WHEN 107 THEN 'Wait CN' WHEN 108 THEN 'Got CN' WHEN 109 THEN 'Approved' WHEN 110 THEN 'Declined' WHEN 111 THEN 'Got CN' WHEN 112 THEN 'Got CN' ELSE '' END) as operateStatus ,
pp.insurance_company_order  'insurance_company_name',pp.product_name as 'policy type' ,'Others' as 'policy_source',
DATE_ADD(pm.actual_pay_time,INTERVAL 7 HOUR)'actual payment date' , DATE_ADD(pm.create_time,INTERVAL 7 HOUR)  'confirm payment date' 
from external_policy_common pp
LEFT JOIN payment pm on pm.main_policy_code=pp.fuse_policy_code
LEFT JOIN  (SELECT fuse_policy_code,send_time 'create_time' from  covernote_information where type=2 GROUP BY fuse_policy_code ) ci on ci.fuse_policy_code=pp.fuse_policy_code 
LEFT JOIN account_agent aa on pp.create_account=aa.mobile
LEFT JOIN account acc on pp.create_account=acc.mobile
LEFT JOIN rm_advance_agent_waiting raaw on raaw.mobile=pp.create_account
LEFT JOIN agent_type att on att.agent_type_code=aa.agent_type_code
where  aa.test_account in (0,4) and pp.policy_status not in (-1,101) and DATE_FORMAT(DATE_ADD(pm.actual_pay_time,INTERVAL 7 HOUR),'%Y-%m')='2020-10' 

) t where t.partner_id in ('a436bb69',
'7704D255',
'9D7ED67C',
'98BCDE34',
'a43d4f37',
'99965CEC',
'9C53AF4A',
'7975CE3B') and t.partner_type in (
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


cancel_sql = """
SELECT * from (
-- auto
SELECT DATE_ADD(pp.create_time,INTERVAL 7 HOUR) 'order_time',pap.`name` 'insured_name',acc.`name` 'partner_name' ,att.agent_type_name 'partner_type' ,acc.partner_id,pp.fuse_policy_code,pp.company_policy_code 'insurance_number' ,(pp.policy_amount-pp.admin_fee-pp.service_fee)  ' premium ' ,
(CASE pp.pay_status WHEN 101 THEN 'UNPAID' WHEN 102 THEN 'PAID' WHEN 103 THEN 'REFUND' ELSE '' END) as 'payment_status',
(CASE pp.policy_status WHEN 101 THEN 'Quote' WHEN 102 THEN 'Pending' WHEN 103 THEN 'Inactive' WHEN 104 THEN 'Active' WHEN 105 THEN 'Expired' WHEN 106 THEN 'Cancel' WHEN 107 THEN 'Rejected' WHEN 108 THEN 'Invalid' WHEN 109 THEN 'FORCE CANCEL' ELSE '' END) AS policy_status ,
(CASE pp.operation_status WHEN 101 THEN 'UNSUBMIT' WHEN 102 THEN 'PENGDING' WHEN 103 THEN 'APPROVED' WHEN 104 THEN 'RESUBMIT' WHEN 105 THEN 'DECLINE' WHEN 106 THEN 'BLACKLIST' WHEN 107 THEN 'CANCEL' WHEN 108 THEN 'CONFIRMED' WHEN 109 THEN 'Agent Agree' ELSE '' END) as operateStatus,
ic.company_name 'insurance_company_name',(CASE pc.category_type WHEN 1 THEN 'General' WHEN 2 THEN 'Travel' WHEN 3 THEN 'Car' WHEN 4 THEN 'Moto' WHEN 6 THEN 'Life' WHEN 7 THEN 'PA' WHEN 8 THEN 'Property' WHEN 9 THEN 'Health' WHEN 11 THEN 'Marine Cargo' WHEN 13 THEN 'SME' WHEN 14 THEN 'MOVEABLE ALL RISK' WHEN 12 THEN 'ROP' WHEN 15 THEN 'VIP' ELSE '' END) as 'policy type' ,'Offline' as 'policy_source',
 DATE_ADD(pm.actual_pay_time,INTERVAL 7 HOUR)'actual payment date' , DATE_ADD(pm.create_time,INTERVAL 7 HOUR)  'confirm payment date' 
from policy_auto pp
LEFT JOIN policy p on p.main_policy_code=pp.main_policy_code
LEFT JOIN policy_auto_person pap on pap.person_code=pp.person_code
LEFT JOIN payment pm on pm.main_policy_code=pp.main_policy_code
LEFT JOIN product pd on pd.product_code=pp.product_code
LEFT JOIN product_category pc on pd.category_code=pc.category_code  
LEFT JOIN insurance_company ic on  pd.insurance_company_code=ic.company_code
LEFT JOIN account_agent aa on pp.create_account=aa.mobile
LEFT JOIN account acc on pp.create_account=acc.mobile
LEFT JOIN rm_advance_agent_waiting raaw on raaw.mobile=pp.create_account
LEFT JOIN agent_type att on att.agent_type_code=aa.agent_type_code
where  aa.test_account in (0,4) and policy_status not in (-1,101) and (p.pay_time_type !=2 OR (p.pay_time_type=2 and pp.pay_status=102))  and   pp.fuse_policy_code in %s
union
-- general
SELECT 
DATE_ADD(pp.create_time,INTERVAL 7 HOUR) 'order_time',pgp.`name` 'insured_name',acc.`name` 'partner_name' ,att.agent_type_name 'partner_type' ,acc.partner_id,pp.fuse_policy_code,pp.company_policy_code 'insurance_number' ,(pp.policy_amount-pp.admin_fee-pp.service_fee)  ' premium ' ,
(CASE pp.pay_status WHEN 101 THEN 'UNPAID' WHEN 102 THEN 'PAID' WHEN 103 THEN 'REFUND' ELSE '' END) as 'payment_status',
(CASE pp.policy_status WHEN 101 THEN 'Quote' WHEN 102 THEN 'Pending' WHEN 103 THEN 'Inactive' WHEN 104 THEN 'Active' WHEN 105 THEN 'Expired' WHEN 106 THEN 'Cancel' WHEN 107 THEN 'Rejected' WHEN 108 THEN 'Invalid' WHEN 109 THEN 'FORCE CANCEL' ELSE '' END) AS policy_status ,
(CASE pp.operation_status WHEN 101 THEN 'UNSUBMIT' WHEN 102 THEN 'PENGDING' WHEN 103 THEN 'APPROVED' WHEN 104 THEN 'RESUBMIT' WHEN 105 THEN 'DECLINE' WHEN 106 THEN 'BLACKLIST' WHEN 107 THEN 'CANCEL' WHEN 108 THEN 'CONFIRMED' WHEN 109 THEN 'Agent Agree' ELSE '' END) as operateStatus,
ic.company_name 'insurance_company_name',(CASE pc.category_type WHEN 1 THEN 'General' WHEN 2 THEN 'Travel' WHEN 3 THEN 'Car' WHEN 4 THEN 'Moto' WHEN 6 THEN 'Life' WHEN 7 THEN 'PA' WHEN 8 THEN 'Property' WHEN 9 THEN 'Health' WHEN 11 THEN 'Marine Cargo' WHEN 13 THEN 'SME' WHEN 14 THEN 'MOVEABLE ALL RISK' WHEN 12 THEN 'ROP' WHEN 15 THEN 'VIP' ELSE '' END) as 'policy type' ,'Offline' as 'policy_source',
DATE_ADD(pm.actual_pay_time,INTERVAL 7 HOUR)'actual payment date' , DATE_ADD(pm.create_time,INTERVAL 7 HOUR)  'confirm payment date' 
from policy_general pp
LEFT JOIN policy p on p.main_policy_code=pp.main_policy_code
LEFT JOIN policy_general_person pgp on pgp.person_code=pp.insured_person_code
LEFT JOIN payment pm on pm.main_policy_code=pp.main_policy_code
LEFT JOIN product pd on pd.product_code=pp.product_code
LEFT JOIN product_category pc on pd.category_code=pc.category_code  
LEFT JOIN insurance_company ic on  pd.insurance_company_code=ic.company_code
LEFT JOIN account_agent aa on pp.create_account=aa.mobile
LEFT JOIN account acc on pp.create_account=acc.mobile
LEFT JOIN rm_advance_agent_waiting raaw on raaw.mobile=pp.create_account
LEFT JOIN agent_type att on att.agent_type_code=aa.agent_type_code
where  aa.test_account in (0,4) and policy_status not in (-1,101) and (p.pay_time_type !=2 OR (p.pay_time_type=2 and pp.pay_status=102))   and  pp.fuse_policy_code in %s
union 
-- bacdkoor
SELECT 
DATE_ADD(pp.order_date,INTERVAL 7 HOUR) 'order_time',pp.insured_person_name 'insured_name',acc.`name` 'partner_name' ,att.agent_type_name 'partner_type' ,acc.partner_id,pp.fuse_policy_code,pp.company_policy_code 'insurance_number' ,IF(pp.is_co_as=1,(ifnull( `pp`.`basic_premium`, 0 ) + ifnull( `pp`.`rider_total_premium`, 0 )) * ( ifnull( t.`company_percentage`, 0 ) / 100 ),ifnull( `pp`.`basic_premium`, 0 ) + ifnull( `pp`.`rider_total_premium`, 0 ))  ' premium ' ,
(CASE pp.pay_status WHEN 101 THEN 'UNPAID' WHEN 102 THEN 'PAID' WHEN 103 THEN 'REFUND' ELSE '' END) as 'payment_status',
(CASE pp.policy_status WHEN 101 THEN 'Quote' WHEN 102 THEN 'Pending' WHEN 103 THEN 'Inactive' WHEN 104 THEN 'Active' WHEN 105 THEN 'Expired' WHEN 106 THEN 'Cancel' WHEN 107 THEN 'Rejected' WHEN 108 THEN 'Invalid' WHEN 109 THEN 'FORCE CANCEL' ELSE '' END) AS policy_status ,
(CASE pp.operate_status WHEN 101 THEN 'UNSUBMIT' WHEN 102 THEN 'PENGDING' WHEN 103 THEN 'APPROVED' WHEN 104 THEN 'RESUBMIT' WHEN 105 THEN 'DECLINE' WHEN 106 THEN 'BLACKLIST' WHEN 107 THEN 'CANCEL' WHEN 108 THEN 'CONFIRMED' WHEN 109 THEN 'Agent Agree' ELSE '' END) as operateStatus,
pp.insurance_company_name  'insurance_company_name',pp.policy_type as 'policy type' ,'Backdoor' as 'policy_source',
DATE_ADD(pm.actual_pay_time,INTERVAL 7 HOUR)'actual payment date' , DATE_ADD(pm.create_time,INTERVAL 7 HOUR)  'confirm payment date' 

from policy_back_door pp
LEFT JOIN payment pm on pm.main_policy_code=pp.fuse_policy_code
LEFT JOIN account_agent aa on pp.agent_mobile=aa.mobile
LEFT JOIN account acc on pp.agent_mobile=acc.mobile
LEFT JOIN rm_advance_agent_waiting raaw on raaw.mobile=pp.agent_mobile
LEFT JOIN agent_type att on att.agent_type_code=aa.agent_type_code
LEFT JOIN (
	SELECT sum(`pbdca`.`company_percentage` ) AS `company_percentage`,`pbdca`.`fuse_policy_code` AS `fuse_policy_code` FROM `policy_back_door_co_as` `pbdca` 
	WHERE ( `pbdca`.`is_fuse` = 1 ) GROUP BY `pbdca`.`fuse_policy_code`
) t on pp.fuse_policy_code=t.fuse_policy_code
where  aa.test_account in (0,4) and pp.policy_status not in (-1,101) and   pp.fuse_policy_code in %s

union
-- other
SELECT 
DATE_ADD(ci.create_time,INTERVAL 7 HOUR) 'order_time',pp.insured_name 'insured_name',acc.`name` 'partner_name' ,att.agent_type_name 'partner_type' ,acc.partner_id,pp.fuse_policy_code,pp.company_policy_code 'insurance_number' ,IFNULL(pp.pay_amount,0)-IFNULL(pp.admin_fee,0)  ' premium ' ,
(CASE pp.pay_status WHEN 101 THEN 'UNPAID' WHEN 102 THEN 'PAID' WHEN 103 THEN 'REFUND' ELSE '' END) as 'payment_status',
(CASE pp.policy_status WHEN 101 THEN 'Quote' WHEN 102 THEN 'Pending' WHEN 103 THEN 'Inactive' WHEN 104 THEN 'Active' WHEN 105 THEN 'Expired' WHEN 106 THEN 'Cancel' WHEN 107 THEN 'Rejected' WHEN 108 THEN 'Invalid' WHEN 109 THEN 'FORCE CANCEL' ELSE '' END) AS policy_status ,
(CASE pp.operate_status WHEN 101 THEN 'Unsubmit' WHEN 102 THEN 'Submit' WHEN 103 THEN 'Got QS' WHEN 104 THEN 'Accepted' WHEN 105 THEN 'Rejected' WHEN 106 THEN 'Change' WHEN 107 THEN 'Wait CN' WHEN 108 THEN 'Got CN' WHEN 109 THEN 'Approved' WHEN 110 THEN 'Declined' WHEN 111 THEN 'Got CN' WHEN 112 THEN 'Got CN' ELSE '' END) as operateStatus ,
pp.insurance_company_order  'insurance_company_name',pp.product_name as 'policy type' ,'Others' as 'policy_source',
DATE_ADD(pm.actual_pay_time,INTERVAL 7 HOUR)'actual payment date' , DATE_ADD(pm.create_time,INTERVAL 7 HOUR)  'confirm payment date'  
from external_policy_common pp
LEFT JOIN payment pm on pm.main_policy_code=pp.fuse_policy_code
LEFT JOIN  (SELECT fuse_policy_code,send_time 'create_time' from  covernote_information where type=2 GROUP BY fuse_policy_code ) ci on ci.fuse_policy_code=pp.fuse_policy_code 
LEFT JOIN account_agent aa on pp.create_account=aa.mobile
LEFT JOIN account acc on pp.create_account=acc.mobile
LEFT JOIN rm_advance_agent_waiting raaw on raaw.mobile=pp.create_account
LEFT JOIN agent_type att on att.agent_type_code=aa.agent_type_code
where  aa.test_account in (0,4) and pp.policy_status not in (-1,101) and  pp.fuse_policy_code in %s

) t where  t.partner_id in ('a436bb69',
'7704D255',
'9D7ED67C',
'98BCDE34',
'a43d4f37',
'99965CEC',
'9C53AF4A',
'7975CE3B') and  t.partner_type in (
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

def get_closing_info():
    print("query data......")
    data = db.querysql(closing_sql)
    column = db.get_column_name()
    data_df = pd.DataFrame(np.array(data))
    data_df.columns = column
    return data_df

def get_paid_info():
    print("query data......")
    data = db.querysql(paid_sql)
    column = db.get_column_name()
    data_df = pd.DataFrame(np.array(data))
    data_df.columns = column
    return data_df

def get_cancel_info(cancel_policy_code):
    print("query data......")
    data = db.querysql(cancel_sql,(cancel_policy_code,cancel_policy_code,cancel_policy_code,cancel_policy_code,))
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


def cancel_data_generate():
    ## cancel 时间
    reject_time_info = get_reject_time()
    cancel_time_info = get_cancel_time()
    cancel_base_info = pd.merge(reject_time_info, cancel_time_info, on="fuse_policy_code", how='outer')

    time_filter = lambda x:  x['reject_time'] == '2020-10' or  x['cancel_time'] == '2020-10' or x['force_cancel_time'] == '2020-10'

    cancel_base_info = cancel_base_info[cancel_base_info.apply(time_filter, axis=1)]

    cancel_policy_code = cancel_base_info['fuse_policy_code'].tolist()
    print(cancel_policy_code)
    policy_info = get_cancel_info(cancel_policy_code)

    print(policy_info)
    return policy_info


def write_to_excel(policy_info,excel_name):
    ## 导出到excel
    print("writing......")
    t = datetime.now().date() - timedelta(days=1)
    writer = pd.ExcelWriter(excel_name + (u'_%d%02d%02d.xlsx' % (t.year, t.month, t.day)))

    wb = writer.book

    # 3.设置格式
    header_fmt = wb.add_format(
        {'bold': True, 'font_size': 13,'font_color': 'white', 'font_name': u'微软雅黑','valign': 'vcenter', 'bg_color': '#787878', 'align': 'center'})
    total_line_fmt = wb.add_format({ 'bg_color': '#A9A9A9'})
    merge_fmt = wb.add_format({ 'bold': True, 'font_size': 12,'bg_color': '#A9A9A9', 'align': 'center'})

    sheet_name = excel_name
    policy_info.to_excel(writer, sheet_name=sheet_name, encoding='utf8', header=True, index=False, startcol=0, startrow=0)
    worksheet1 = writer.sheets[sheet_name]
    worksheet1.set_column('A:K', 20)
    for col_num, value in enumerate(policy_info.columns.values):
        worksheet1.write(0, col_num, value, header_fmt)


    # worksheet1.merge_range(first_row=1,last_row=1,first_col=1,last_col=2,data="merge",cell_format=merge_fmt)
    writer.save()

    print("write done !!!")




write_to_excel(get_closing_info(),'No4_closing_details')
write_to_excel(get_paid_info(),'No4_paid_details')
write_to_excel(cancel_data_generate(),'No4__cancel_details')
## 关闭连接
mdbF.shoutdown()
db.shoutdown()

print("finish..........")
