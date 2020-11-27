# @Time    : 2020/11/26 12:21 下午
# @Author  : luoh
# @Email   : luohenghlx@163.com
# @File    : cancel_policy_generate_sql.py
# @Software: PyCharm
# @Description:

import numpy as np
import pandas as pd
from datafactory import Databasefactory as dbf
from datafactory import pro_db_config as db_config
from datetime import datetime, timedelta
from mongo_data_factory import MongoDataBaseFactory as mdbf
from mongo_data_factory import pro_db_config as mdb_config
import datetime
import time

## 获取数据库连接
db = dbf.DataBaseFactory(db_config)
mdbF = mdbf.MongoDataBaseFactory(mdb_config)
mgdb = mdbF.getdb()

offset=28800
add_time=43200



policy_source_sql="""
SELECT fuse_policy_code,main_policy_code,'policy_auto' as policy_source   from policy_auto where fuse_policy_code in %s
union 
SELECT fuse_policy_code,main_policy_code , 'policy_general' as policy_source from policy_general where fuse_policy_code in %s
union 
SELECT fuse_policy_code,null as main_policy_code , 'policy_back_door' as policy_source from policy_back_door where fuse_policy_code in %s
union 
SELECT fuse_policy_code,null as main_policy_code , 'external_policy_common' as policy_source from external_policy_common where fuse_policy_code in %s
"""



def get_policy_source(policy_code):
    print("query data......")
    data = db.querysql(policy_source_sql,(policy_code,policy_code,policy_code,policy_code,))
    column = db.get_column_name()
    data_df = pd.DataFrame(np.array(data))
    data_df.columns = column
    return data_df


source_df = pd.read_excel("unpaid_policy_update.xlsx")



def general_sql():

    ## 获取 policy source
    policy_code=source_df['Fuse code'].tolist()
    policy_source_df = get_policy_source(policy_code)
    policy_df = pd.merge(source_df,policy_source_df,left_on='Fuse code',right_on='fuse_policy_code',how='left')


    offline_cancel_sql_lambda = lambda x :  "update " + x['policy_source'] + " set policy_status=106,operation_status=107  where fuse_policy_code='" + x['Fuse code'] + "';" if ( (x['policy_source']=='policy_auto' or x['policy_source']=='policy_general') and (not pd.isna(x['cancel_time']))) else ""
    policy_df['offline_cancel_sql'] = policy_df.apply(offline_cancel_sql_lambda, axis=1)

    backdoor_cancel_sql_lambda = lambda x: "update policy_back_door set policy_status=106,operate_status=107  where fuse_policy_code='" + x['Fuse code'] + "';" if (x['policy_source']=='policy_back_door' and not pd.isna(x['cancel_time'])) else ""
    policy_df['backdoor_cancel_sql'] = policy_df.apply(backdoor_cancel_sql_lambda, axis=1)

    main_cancel_sql_lambda = lambda x :  "update policy set cancel_time='"+str(x['cancel_time'])[0:10]+" 12:00:00' where main_policy_code='" + x['main_policy_code'] + "';" if ( (x['policy_source']=='policy_auto' or x['policy_source']=='policy_general') and (not pd.isna(x['cancel_time']))) else ""
    policy_df['main_cancel_sql'] = policy_df.apply(main_cancel_sql_lambda, axis=1)

    time.mktime(datetime.datetime.now().timetuple()) * 1000
    mongo_cancel_sql_lambda = lambda x : "db.policy.update({'mainPolicyCode':'" +x['main_policy_code'] +"''},{$set:{"+\
                                         "cancelTime: '"+str((time.mktime((x['cancel_time'].timetuple()))+add_time+offset)* 1000)+"',"\
                                         "policyStatus: 106 ,"\
                                         "operationStatus: 107"\
                                         "}});" if ( (x['policy_source']=='policy_auto' or x['policy_source']=='policy_general') and (not pd.isna(x['cancel_time']))) else ""
    policy_df['mongo_cancel_sql'] = policy_df.apply(mongo_cancel_sql_lambda, axis=1)


    return policy_df





def write_excel(excel_dateframe):
    writer = pd.ExcelWriter("cancel_sql.xlsx")
    wb = writer.book
    excel_dateframe.to_excel(writer, sheet_name=u'cancel_sql', encoding='utf8', header=True, index=False, startcol=0,
                     startrow=0)
    writer.save()




write_excel(general_sql())

## 关闭连接
mdbF.shoutdown()
db.shoutdown()

print("finish..........")