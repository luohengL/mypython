# @Time    : 2020/11/14 5:22 下午
# @Author  : luoh
# @Email   : luohenghlx@163.com
# @File    : closing_policy_check.py
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
codes_sql = """
SELECT *  from (
-- auto
SELECT pp.fuse_policy_code,pp.associated_policy_code,ic.company_name 'insurance_company_name',acc.partner_id,att.agent_type_name 'partner_type' ,'Offline' as 'policy_source',
(CASE pc.category_type WHEN 1 THEN 'General' WHEN 2 THEN 'Travel' WHEN 3 THEN 'Car' WHEN 4 THEN 'Moto' WHEN 6 THEN 'Life' WHEN 7 THEN 'PA' WHEN 8 THEN 'Property' WHEN 9 THEN 'Health' WHEN 11 THEN 'Marine Cargo' WHEN 13 THEN 'SME' WHEN 14 THEN 'MOVEABLE ALL RISK' WHEN 12 THEN 'ROP' WHEN 15 THEN 'VIP' ELSE '' END) as 'policy type' ,
pri.tag,
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
where  pp.fuse_policy_code in %s or pp.associated_policy_code in  %s 
union
-- general
SELECT 
 pp.fuse_policy_code,pp.associated_policy_code,ic.company_name 'insurance_company_name',acc.partner_id,att.agent_type_name 'partner_type' ,'Offline' as 'policy_source',
(CASE pc.category_type WHEN 1 THEN 'General' WHEN 2 THEN 'Travel' WHEN 3 THEN 'Car' WHEN 4 THEN 'Moto' WHEN 6 THEN 'Life' WHEN 7 THEN 'PA' WHEN 8 THEN 'Property' WHEN 9 THEN 'Health' WHEN 11 THEN 'Marine Cargo' WHEN 13 THEN 'SME' WHEN 14 THEN 'MOVEABLE ALL RISK' WHEN 12 THEN 'ROP' WHEN 15 THEN 'VIP' ELSE '' END) as 'policy type' ,
pri.tag,
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
where  pp.fuse_policy_code in %s or pp.associated_policy_code in  %s 
union 
-- bacdkoor
SELECT 
pp.fuse_policy_code,pp.npp as  associated_policy_code,pp.insurance_company_name 'insurance_company_name',acc.partner_id,att.agent_type_name 'partner_type' ,'Backdoor' as 'policy_source',
pp.policy_type as 'policy type' ,pri.tag,
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
where  pp.fuse_policy_code in %s or pp.npp in  %s 
union
-- other
SELECT 

pp.fuse_policy_code,null as  associated_policy_code,pp.insurance_company_order 'insurance_company_name',acc.partner_id,att.agent_type_name 'partner_type' ,'Others' as 'policy_source',
pp.product_name as 'policy type' ,pri.tag,
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
where  pp.fuse_policy_code in %s 
) t 
"""



cancel_sql = """

SELECT *  from (
-- auto
SELECT pp.fuse_policy_code,pp.associated_policy_code,ic.company_name 'insurance_company_name',acc.partner_id,att.agent_type_name 'partner_type' ,'Offline' as 'policy_source',pri.tag,
(CASE pc.category_type WHEN 1 THEN 'General' WHEN 2 THEN 'Travel' WHEN 3 THEN 'Car' WHEN 4 THEN 'Moto' WHEN 6 THEN 'Life' WHEN 7 THEN 'PA' WHEN 8 THEN 'Property' WHEN 9 THEN 'Health' WHEN 11 THEN 'Marine Cargo' WHEN 13 THEN 'SME' WHEN 14 THEN 'MOVEABLE ALL RISK' WHEN 12 THEN 'ROP' WHEN 15 THEN 'VIP' ELSE '' END) as 'policy type' ,
(CASE pp.policy_status WHEN 101 THEN 'Quote' WHEN 102 THEN 'Pending' WHEN 103 THEN 'Inactive' WHEN 104 THEN 'Active' WHEN 105 THEN 'Expired' WHEN 106 THEN 'Cancel' WHEN 107 THEN 'Rejected' WHEN 108 THEN 'Invalid' WHEN 109 THEN 'FORCE CANCEL' ELSE '' END) AS policy_status ,
(CASE pp.operation_status WHEN 101 THEN 'UNSUBMIT' WHEN 102 THEN 'PENGDING' WHEN 103 THEN 'APPROVED' WHEN 104 THEN 'RESUBMIT' WHEN 105 THEN 'DECLINE' WHEN 106 THEN 'BLACKLIST' WHEN 107 THEN 'CANCEL' WHEN 108 THEN 'CONFIRMED' WHEN 109 THEN 'Agent Agree' ELSE '' END) as operateStatus,
(CASE pp.pay_status WHEN 101 THEN 'UNPAID' WHEN 102 THEN 'PAID' WHEN 103 THEN 'REFUND' ELSE '' END) as 'payment_status',
DATE_ADD(pp.create_time,INTERVAL 7 HOUR) 'order_time',DATE_ADD(pm.actual_pay_time,INTERVAL 7 HOUR)'actual payment date' , DATE_ADD(pm.create_time,INTERVAL 7 HOUR)  'confirm payment date' 
from policy_auto pp
LEFT JOIN policy p on p.main_policy_code=pp.main_policy_code
LEFT JOIN policy_auto_person pap on pap.person_code=pp.person_code
LEFT JOIN policy_endorsement AS pe on pp.fuse_policy_code = pe.new_fuse_policy_code
LEFT JOIN policy_rm_info pri on pri.fuse_policy_code=pp.fuse_policy_code
LEFT JOIN payment pm on pm.main_policy_code=pp.main_policy_code
LEFT JOIN product pd on pd.product_code=pp.product_code
LEFT JOIN product_category pc on pd.category_code=pc.category_code  
LEFT JOIN insurance_company ic on  pd.insurance_company_code=ic.company_code
LEFT JOIN account_agent aa on pp.create_account=aa.mobile
LEFT JOIN account acc on pp.create_account=acc.mobile
LEFT JOIN rm_advance_agent_waiting raaw on raaw.mobile=pp.create_account
LEFT JOIN agent_type att on att.agent_type_code=aa.agent_type_code
where  aa.test_account in (0,4) and pp.policy_status  in (106,107,109) and pe.id is null  and (p.pay_time_type !=2 OR (p.pay_time_type=2 and pp.pay_status=102))  and pp.fuse_policy_code in %s
union
-- general
SELECT 
 pp.fuse_policy_code,pp.associated_policy_code,ic.company_name 'insurance_company_name',acc.partner_id,att.agent_type_name 'partner_type' ,'Offline' as 'policy_source',pri.tag,
(CASE pc.category_type WHEN 1 THEN 'General' WHEN 2 THEN 'Travel' WHEN 3 THEN 'Car' WHEN 4 THEN 'Moto' WHEN 6 THEN 'Life' WHEN 7 THEN 'PA' WHEN 8 THEN 'Property' WHEN 9 THEN 'Health' WHEN 11 THEN 'Marine Cargo' WHEN 13 THEN 'SME' WHEN 14 THEN 'MOVEABLE ALL RISK' WHEN 12 THEN 'ROP' WHEN 15 THEN 'VIP' ELSE '' END) as 'policy type' ,
(CASE pp.policy_status WHEN 101 THEN 'Quote' WHEN 102 THEN 'Pending' WHEN 103 THEN 'Inactive' WHEN 104 THEN 'Active' WHEN 105 THEN 'Expired' WHEN 106 THEN 'Cancel' WHEN 107 THEN 'Rejected' WHEN 108 THEN 'Invalid' WHEN 109 THEN 'FORCE CANCEL' ELSE '' END) AS policy_status ,
(CASE pp.operation_status WHEN 101 THEN 'UNSUBMIT' WHEN 102 THEN 'PENGDING' WHEN 103 THEN 'APPROVED' WHEN 104 THEN 'RESUBMIT' WHEN 105 THEN 'DECLINE' WHEN 106 THEN 'BLACKLIST' WHEN 107 THEN 'CANCEL' WHEN 108 THEN 'CONFIRMED' WHEN 109 THEN 'Agent Agree' ELSE '' END) as operateStatus,
(CASE pp.pay_status WHEN 101 THEN 'UNPAID' WHEN 102 THEN 'PAID' WHEN 103 THEN 'REFUND' ELSE '' END) as 'payment_status',
DATE_ADD(pp.create_time,INTERVAL 7 HOUR) 'order_time',DATE_ADD(pm.actual_pay_time,INTERVAL 7 HOUR)'actual payment date' , DATE_ADD(pm.create_time,INTERVAL 7 HOUR)  'confirm payment date' 
from policy_general pp
LEFT JOIN policy p on p.main_policy_code=pp.main_policy_code
LEFT JOIN policy_general_person pgp on pgp.person_code=pp.insured_person_code
LEFT JOIN policy_endorsement AS pe on pp.fuse_policy_code = pe.new_fuse_policy_code
LEFT JOIN policy_rm_info pri on pri.fuse_policy_code=pp.fuse_policy_code
LEFT JOIN payment pm on pm.main_policy_code=pp.main_policy_code
LEFT JOIN product pd on pd.product_code=pp.product_code
LEFT JOIN product_category pc on pd.category_code=pc.category_code  
LEFT JOIN insurance_company ic on  pd.insurance_company_code=ic.company_code
LEFT JOIN account_agent aa on pp.create_account=aa.mobile
LEFT JOIN account acc on pp.create_account=acc.mobile
LEFT JOIN rm_advance_agent_waiting raaw on raaw.mobile=pp.create_account
LEFT JOIN agent_type att on att.agent_type_code=aa.agent_type_code
where  aa.test_account in (0,4) and pp.policy_status  in (106,107,109) and pe.id is null and (p.pay_time_type !=2 OR (p.pay_time_type=2 and pp.pay_status=102))   and  pp.fuse_policy_code in %s
union 
-- bacdkoor
SELECT 
pp.fuse_policy_code,pp.npp as  associated_policy_code,pp.insurance_company_name 'insurance_company_name',acc.partner_id,att.agent_type_name 'partner_type' ,'Backdoor' as 'policy_source',pri.tag,
pp.policy_type as 'policy type' ,
(CASE pp.policy_status WHEN 101 THEN 'Quote' WHEN 102 THEN 'Pending' WHEN 103 THEN 'Inactive' WHEN 104 THEN 'Active' WHEN 105 THEN 'Expired' WHEN 106 THEN 'Cancel' WHEN 107 THEN 'Rejected' WHEN 108 THEN 'Invalid' WHEN 109 THEN 'FORCE CANCEL' ELSE '' END) AS policy_status ,
(CASE pp.operate_status WHEN 101 THEN 'UNSUBMIT' WHEN 102 THEN 'PENGDING' WHEN 103 THEN 'APPROVED' WHEN 104 THEN 'RESUBMIT' WHEN 105 THEN 'DECLINE' WHEN 106 THEN 'BLACKLIST' WHEN 107 THEN 'CANCEL' WHEN 108 THEN 'CONFIRMED' WHEN 109 THEN 'Agent Agree' ELSE '' END) as operateStatus,
(CASE pp.pay_status WHEN 101 THEN 'UNPAID' WHEN 102 THEN 'PAID' WHEN 103 THEN 'REFUND' ELSE '' END) as 'payment_status',
DATE_ADD(pp.create_time,INTERVAL 7 HOUR) 'order_time',DATE_ADD(pm.actual_pay_time,INTERVAL 7 HOUR)'actual payment date' , DATE_ADD(pm.create_time,INTERVAL 7 HOUR)  'confirm payment date' 

from policy_back_door pp
LEFT JOIN payment pm on pm.main_policy_code=pp.fuse_policy_code
LEFT JOIN endorsement_record er on er.fuse_policy_code = pp.fuse_policy_code
LEFT JOIN account_agent aa on pp.agent_mobile=aa.mobile
LEFT JOIN policy_rm_info pri on pri.fuse_policy_code=pp.fuse_policy_code
LEFT JOIN account acc on pp.agent_mobile=acc.mobile
LEFT JOIN rm_advance_agent_waiting raaw on raaw.mobile=pp.agent_mobile
LEFT JOIN agent_type att on att.agent_type_code=aa.agent_type_code
LEFT JOIN (
	SELECT sum(`pbdca`.`company_percentage` ) AS `company_percentage`,`pbdca`.`fuse_policy_code` AS `fuse_policy_code` FROM `policy_back_door_co_as` `pbdca` 
	WHERE ( `pbdca`.`is_fuse` = 1 ) GROUP BY `pbdca`.`fuse_policy_code`
) t on pp.fuse_policy_code=t.fuse_policy_code
where  aa.test_account in (0,4) and pp.policy_status  in (106,107,109)and  er.id is null and  pp.fuse_policy_code in %s

union
-- other
SELECT 

pp.fuse_policy_code,null as  associated_policy_code,pp.insurance_company_order 'insurance_company_name',acc.partner_id,att.agent_type_name 'partner_type' ,'Backdoor' as 'policy_source',pri.tag,
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
where  aa.test_account in (0,4) and pp.policy_status  in (106,107,109) and pp.fuse_policy_code in %s

) t 
"""

backdoor_cancel_sql = """
    SELECT pp.fuse_policy_code, DATE_ADD(pbdr1.cancel_date,INTERVAL 7 HOUR) 'cancel_time', DATE_ADD(pp.force_cancel_time,INTERVAL 7 HOUR) 'force_cancel_time' from policy_back_door pp LEFT JOIN  policy_back_door_review pbdr1 ON ( pp.fuse_policy_code = pbdr1.fuse_policy_code AND  (pbdr1.review_opinion = 'Cancel' or (pbdr1.type = 2 AND pbdr1.review_opinion = 'Approved' AND pbdr1.cancel_policy = 1)))
where pbdr1.cancel_date is not null or  pp.force_cancel_time is not null ;
"""

offline_reject ="""
SELECT fuse_code fuse_policy_code,DATE_ADD(app_time,INTERVAL 7 HOUR) reject_time from policy_approve_record where approve_type in (105,106);
"""


backdoor_reject="""
SELECT fuse_policy_code,DATE_ADD(operate_time,INTERVAL 7 HOUR) reject_time from policy_back_door_review where confirm_opinion='Reject';
"""

## mongo query
offline_cancel_data = mgdb.policy.find({"$or":[{"cancelTime":{ "$exists": "true" }},{"forceCancelTime":{ "$exists": "true" }}]},
                              {"_id": 0, "fusePolicyCode": 1, "cancelTime": 1,"forceCancelTime":1});

other_reject_info_data = mgdb.operateLog.find({"operateType":14,"details":{"$regex":"Action: no process"}},{"_id":0,"serchId":1,"operateTime":1});


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
    other_reject_info = [(u['serchId'],datetime.strftime(datetime.fromtimestamp(int(u['operateTime'])/1000+25200),'%Y-%m-%d %H:%M:%S')) for u in other_reject_info_data]
    all_reject_info = backdoor_reject_info + offline_reject_info + other_reject_info

    data_df = pd.DataFrame(np.array(all_reject_info))
    data_df.columns = column
    return data_df


def get_cancel_time():
    print("query cancel time")
    ## offline

    offline_cancel_info = [(u['fusePolicyCode'],datetime.strftime(datetime.fromtimestamp(int(u['cancelTime'])/1000+25200),'%Y-%m-%d %H:%M:%S') if 'cancelTime' in u else None,datetime.strftime(datetime.fromtimestamp(int(u['forceCancelTime'])/1000+25200),'%Y-%m-%d %H:%M:%S') if 'forceCancelTime' in u else None ) for u in offline_cancel_data]

    ## backdoor
    backdoor_cancel_info = db.querysql(backdoor_cancel_sql)

    column = db.get_column_name()

    all_cancel_info = offline_cancel_info+backdoor_cancel_info
    data_df = pd.DataFrame(np.array(all_cancel_info))
    data_df.columns = column
    return data_df




def merge_cancel_info():
    ## cancel 时间
    reject_time_info = get_reject_time()
    cancel_time_info = get_cancel_time()
    cancel_base_info = pd.merge(reject_time_info, cancel_time_info, on="fuse_policy_code", how='outer')
    cancel_policy_codes = cancel_base_info['fuse_policy_code'].tolist()
    cancel_policy_info = get_cancel_info(cancel_policy_codes)
    cancel_policy_info = pd.merge(cancel_policy_info,cancel_base_info,on='fuse_policy_code',how='left')

    cancel_policy_info['cancel_date']=cancel_policy_info.apply(lambda x : x['cancel_time'] if x['policy_status']=='Cancel' else x['force_cancel_time'] if x['policy_status']=='FORCE CANCEL' else x['reject_time'] if x['policy_status']=='Rejected' else '',axis=1)
    cancel_policy_info.loc[cancel_policy_info['cancel_date'].isnull(),'cancel_date']=cancel_policy_info.apply(lambda x:  x['cancel_time'] if not pd.isna(x['cancel_time']) else x['force_cancel_time'] if not pd.isna(x['force_cancel_time']) else x['reject_time'] if not pd.isna(x['reject_time']) else 'asdf',axis=1 )

    cancel_policy_info['cancel_date'] = pd.to_datetime(cancel_policy_info['cancel_date'])
    cancel_policy_info.set_index(pd.to_datetime(cancel_policy_info['cancel_date']),inplace=True)
    print(cancel_policy_info.index)
    # cancel_policy_info = cancel_policy_info.loc['2020']
    cancel_policy_info = cancel_policy_info.drop(
        ['actual payment date', 'confirm payment date','cancel_time','force_cancel_time','reject_time'], axis=1)
    return cancel_policy_info

all_cancel_policy = merge_cancel_info()
sys_cancel_policy = all_cancel_policy.loc['2020-01-01 00:00:00':'2020-10-31 23:59:59']
all_cancel_policy.reindex
sys_cancel_policy.reindex
print("after index")
print(all_cancel_policy.index)
print(sys_cancel_policy.index)
def get_closing_info():
    print("query data......")
    data = db.querysql(cancel_sql)
    column = db.get_column_name()
    data_df = pd.DataFrame(np.array(data))
    data_df.columns = column
    return data_df

## 获取模板
def get_source():
    df_resource = pd.read_excel('cancel_template.xlsx')
    return df_resource

def get_insurance_mapping():
    print("get_insurance_mapping......")
    mapping = pd.read_excel('../../insuance_mappping.xlsx')
    mapping.drop(mapping.columns[-1], axis=1, inplace=True)
    mapping.item_id = (mapping['insurance_company_name']).astype(str)
    mapping.item_category = (mapping['insurance_company_name_mapped']).astype(str)
    item_dict = mapping.set_index('insurance_company_name')['insurance_company_name_mapped'].to_dict()
    print(item_dict)
    return item_dict

insurance_mapping = get_insurance_mapping()




def write_to_excel(policy_info,excel_name):
    ## 导出到excel
    print("writing......")
    t = datetime.now().date() - timedelta(days=1)
    writer = pd.ExcelWriter(excel_name + (u'_%d%02d%02d.xlsx' % (t.year, t.month, t.day)))

    wb = writer.book

    # 3.设置格式
    header_fmt = wb.add_format(
        {'bold': True, 'font_size': 13,'font_color': 'white', 'font_name': u'微软雅黑','valign': 'vcenter', 'bg_color': '#787878', 'align': 'center'})
    sheet_name = excel_name
    policy_info.to_excel(writer, sheet_name=sheet_name, encoding='utf8', header=True, index=False, startcol=0, startrow=0)
    worksheet1 = writer.sheets[sheet_name]
    worksheet1.set_column('A:K', 20)
    for col_num, value in enumerate(policy_info.columns.values):
        worksheet1.write(0, col_num, value, header_fmt)


    writer.save()

    print("write done !!!")



def get_policy_info_by_codes(policy_codes,associated_codes):
    print("query data......")
    data = db.querysql(codes_sql, (policy_codes,associated_codes, policy_codes,associated_codes, policy_codes,associated_codes, policy_codes,))
    column = db.get_column_name()
    data_df = pd.DataFrame(np.array(data))
    data_df.columns = column
    return data_df



def generate_info():
    df_resource = get_source()
    ## 处理数字类型
    df_resource['Associated Number'] =df_resource['Associated Number'].apply(lambda x: str(int(x)) if not pd.isna(x) else x)
    print(df_resource['Associated Number'].head(20))
    ## 获取对应code
    policy_codes=df_resource['Fuse Code'].tolist()
    associated_codes=df_resource['Associated Number'].tolist()
    policy_codes = [elem for elem in policy_codes if not pd.isna(elem)]
    associated_codes = [elem for elem in associated_codes if not pd.isna(elem)]
    print(policy_codes)
    print(len(policy_codes))
    print(associated_codes)
    print(len(associated_codes))
    ## 查询保单信息
    policy_info_by_codes = get_policy_info_by_codes(policy_codes,associated_codes)


    cancel_info=sys_cancel_policy[['fuse_policy_code','cancel_date']]
    policy_info_by_codes = pd.merge(policy_info_by_codes,cancel_info,on='fuse_policy_code',how='left')
    policy_info_by_codes = policy_info_by_codes.drop( ['actual payment date', 'confirm payment date'], axis=1)

    ## 标记原始数据顺序
    df_resource.insert(0, 'No', range(1, len(df_resource) + 1))

    policy_info_by_fuse_codes = df_resource.loc[df_resource['Fuse Code'].notnull(),:]
    policy_info_by_asso_codes =df_resource.loc[df_resource['Fuse Code'].isnull(),:]

    print(len(policy_info_by_fuse_codes))
    print(policy_info_by_fuse_codes.head())
    print(len(policy_info_by_asso_codes))
    print(policy_info_by_asso_codes.head())


    policy_info_by_fuse_codes['fuse_policy_code_map'] = policy_info_by_fuse_codes['Fuse Code'].str.lower()
    policy_info_by_asso_codes['associated_policy_code_map'] = policy_info_by_asso_codes['Associated Number'].str.lower()

    policy_info_by_codes['fuse_policy_code_map'] = policy_info_by_codes['fuse_policy_code'].str.lower()
    policy_info_by_codes['associated_policy_code_map'] = policy_info_by_codes['associated_policy_code'].str.lower()

    policy_info_by_fuse_codes = pd.merge(policy_info_by_fuse_codes, policy_info_by_codes,on='fuse_policy_code_map', how='left')
    policy_info_by_asso_codes = pd.merge(policy_info_by_asso_codes, policy_info_by_codes,on='associated_policy_code_map', how='left')

    print(policy_info_by_fuse_codes.head(10))
    print(policy_info_by_asso_codes.head(10))
    policy_info_by_fuse_codes_total = policy_info_by_fuse_codes.append(policy_info_by_asso_codes)

    policy_info_by_fuse_codes_total = policy_info_by_fuse_codes_total.drop(['Fuse Code', 'Associated Number'], axis=1)

    policy_info_by_fuse_codes_total = policy_info_by_fuse_codes_total.drop_duplicates(['No','fuse_policy_code', 'associated_policy_code'], keep="first")

    policy_info_by_codes = pd.merge(df_resource, policy_info_by_fuse_codes_total, on='No', how='left')

    print(policy_info_by_codes)
    return policy_info_by_codes

def get_unique(policy_info_by_codes):
    policy_info_by_codes_one = policy_info_by_codes.drop_duplicates(["No"], keep="first")
    policy_info_by_codes_one['Associated Number'] = policy_info_by_codes_one.apply(
        lambda x: x['Associated Number'] if not pd.isna(x['Associated Number']) else x['associated_policy_code'],
        axis=1)
    policy_info_by_codes_one['Fuse Code'] = policy_info_by_codes_one.apply(
        lambda x: x['Fuse Code'] if not pd.isna(x['Fuse Code']) else x['fuse_policy_code'], axis=1)
    policy_info_by_codes_one = policy_info_by_codes_one.drop(
        ['fuse_policy_code', 'associated_policy_code', 'fuse_policy_code_map', 'associated_policy_code_map','order_time'],
        axis=1)
    policy_info_by_codes_one['insurance_company_name_mapped']=policy_info_by_codes_one['insurance_company_name'].str.lower().map(insurance_mapping)
    policy_info_by_codes_one.rename(
        columns={'Fuse Code': 'fuse_policy_code', 'Associated Number': 'associated_policy_code'},inplace=True)

    return policy_info_by_codes_one

def get_duplicate(policy_info_by_codes):
    policy_info_by_codes_no = policy_info_by_codes.drop_duplicates(["No"], keep=False)
    policy_info_by_codes_one = policy_info_by_codes.drop_duplicates(["No"], keep="first")
    policy_info_by_codes_duplicate = policy_info_by_codes_one.append(policy_info_by_codes_no).drop_duplicates(keep=False)
    policy_info_by_codes_duplicate = policy_info_by_codes.loc[policy_info_by_codes['No'].isin(policy_info_by_codes_duplicate['No'].tolist())]
    policy_info_by_codes_duplicate = policy_info_by_codes_duplicate.drop(
        ['fuse_policy_code_map', 'associated_policy_code_map','order_time'],
        axis=1)
    policy_info_by_codes_duplicate['insurance_company_name_mapped'] = policy_info_by_codes_duplicate[
        'insurance_company_name'].str.lower().map(insurance_mapping)
    policy_info_by_codes_duplicate.rename(columns={'Fuse Code':'fuse_policy_code','Associated Number':'associated_policy_code','fuse_policy_code': 'fuse_policy_code in sys', 'associated_policy_code': 'associated_policy_code in sys'},inplace=True)
    return policy_info_by_codes_duplicate

def generate_sys_info(policy_info_by_codes):
    policy_info_by_codes = policy_info_by_codes.drop(
        ['No', 'Fuse Code', 'Associated Number', 'fuse_policy_code_map', 'associated_policy_code_map', 'order_time'],
        axis=1)
    sys_policy=sys_cancel_policy
    sys_policy = sys_policy.drop(
        ['order_time'],
        axis=1)
    sys_policy=sys_policy.append(policy_info_by_codes)
    sys_policy=sys_policy.append(policy_info_by_codes)
    sys_policy = sys_policy.drop_duplicates(subset=['fuse_policy_code', 'associated_policy_code'], keep=False)

    sys_policy['insurance_company_name_mapped'] = sys_policy[
        'insurance_company_name'].str.lower().map(insurance_mapping)
    return sys_policy

policy_info_by_codes = generate_info()
#write_to_excel(policy_info_by_codes,'policy_info_by_codes')
policy_unique = get_unique(policy_info_by_codes)
write_to_excel(policy_unique,'cancel_policy')
policy_duplicate = get_duplicate(policy_info_by_codes)
write_to_excel(policy_duplicate,'cancel_policy_duplicate')
data = generate_sys_info(policy_info_by_codes)
write_to_excel(data,'cancel_policy_not_in_id')
## 关闭连接
mdbF.shoutdown()
db.shoutdown()

print("finish..........")