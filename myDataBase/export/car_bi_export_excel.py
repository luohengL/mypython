import numpy as np
import pandas as pd
from datafactory import Databasefactory as dbf
from datafactory import pro_db_config as db_config

from datetime import datetime, timedelta

export_path = 'excel/'
def get_data():
    print("query data......")
    db = dbf.DataBaseFactory(db_config)
    data = db.querysql(
        '''
        SELECT insured_name insurance_company ,car_type ,aging,tsi_2019,tsi_2020,tsi_total,tsi_2019/tsi_total tsi_ratio_2019,tsi_2020/tsi_total tsi_ratio_2020, pc_2019 policy_count_2019,pc_2020 policy_count_2020 ,pc_total policy_count_total,pc_2019/pc_total  policy_count_ratio_2019 ,pc_2020/pc_total policy_count_ratio_2020  from ( 
SELECT insured_name,car_type, ifnull(aging,0) aging,count(*) pc_total,sum(price) tsi_total,sum(if(LEFT(order_time,4)=2019,1,0)) pc_2019,sum(if(LEFT(order_time,4)=2019,price,0)) tsi_2019 ,sum(if(LEFT(order_time,4)=2020,1,0)) pc_2020,sum(if(LEFT(order_time,4)=2020,price,0)) tsi_2020  from ( SELECT  (LEFT(order_time,4)-year) AS aging ,tt.* from  (SELECT 
 DATE_ADD(pa.create_time,INTERVAL 7 hour) order_time ,pa.fuse_policy_code AS fuse_policy_code,pa.effective_time+INTERVAL 7 HOUR AS start_date,pa.expire_time+INTERVAL 7 HOUR AS end_date,ic.company_name AS insured_name,
pav.plate_no AS plate_number,pav.brand AS brand,pav.type AS type,pav.series AS series,pav.year AS year,pav.price+pav.modified_info AS price,
CASE WHEN pc.category_type=3 THEN 'car' WHEN pc.category_type=4 THEN 'moto' END AS car_type,
'auto' as mytype
FROM policy_auto AS pa
LEFT JOIN product AS p on p.product_code = pa.product_code
LEFT JOIN insurance_company AS ic on ic.company_code=p.insurance_company_code
LEFT JOIN account_agent AS aa on aa.mobile=pa.create_account
LEFT JOIN policy_auto_vehicle AS pav on pav.vehicle_code=pa.vehicle_code
LEFT JOIN policy_auto_person AS pap on  pap.person_code=pa.person_code
LEFT JOIN product_category AS pc on pc.category_code=pa.product_category_code
WHERE  aa.test_account  IN (0,4)
AND pa.policy_status not in (-1,101)
AND DATE_ADD(pa.create_time,INTERVAL 7 hour)>='2019-01-01' 
AND DATE_ADD(pa.create_time,INTERVAL 7 hour)<='2020-08-20'
AND pc.category_type in (3,4)
UNION ALL
SELECT DATE_ADD(IFNULL(pbd.ops_oder_time,pbd.order_date),INTERVAL 7 hour) order_time,
pbd.fuse_policy_code AS fuse_policy_code,pbd.effective_date+INTERVAL 7 HOUR AS start_date,pbd.expired_date+INTERVAL 7 HOUR AS end_date,pbd.insurance_company_name AS insured_name,
pbda.plate_no AS plate_number,pbda.brand AS brand,pbda.type AS type,pbda.series AS series,pbda.year AS year,pbd.tsi AS price,
CASE WHEN pbda.category_name='Car' THEN 'car' WHEN pbda.category_name='Moto' THEN 'moto' END AS car_type,
'back_door' as mytype
FROM policy_back_door AS pbd
LEFT JOIN account_agent AS aa on aa.mobile=pbd.agent_mobile
LEFT JOIN policy_back_door_auto AS pbda on pbda.fuse_policy_code=pbd.fuse_policy_code
WHERE  aa.test_account  IN (0,4)
AND pbd.policy_status not in (-1,101)
AND DATE_ADD(IFNULL(pbd.ops_oder_time,pbd.order_date),INTERVAL 7 hour)>='2019-01-01' 
AND DATE_ADD(IFNULL(pbd.ops_oder_time,pbd.order_date),INTERVAL 7 hour)<='2020-08-20'
AND pbda.category_name in ('Car','Moto')
UNION ALL
SELECT DATE_ADD(IFNULL(epc.ops_oder_time,epc.create_time),INTERVAL 7 hour) order_time,
epc.fuse_policy_code AS fuse_policy_code,epc.effective_time+INTERVAL 7 HOUR AS start_date,epc.expire_time+INTERVAL 7 HOUR AS end_date,epc.insurance_company AS insured_name,
eam.plate_no AS plate_number,eam.brand AS brand,eam.type AS type,eam.series AS series,eam.year AS year,eam.price+eam.modified_info AS price,
CASE WHEN epc.product_name='Car Multiyears' THEN 'car' WHEN epc.product_name='Motor Multiyears' THEN 'moto' END AS car_type,
'multi_other' AS mytype
FROM external_policy_common AS epc
LEFT JOIN account_agent AS aa on aa.mobile=epc.create_account
LEFT JOIN external_auto_multiyear AS eam on eam.fuse_policy_code=epc.fuse_policy_code
WHERE aa.test_account  IN (0,4)
AND epc.product_name IN ("Motor Multiyears", "Car Multiyears")
AND epc.policy_status not in (-1,101)
AND DATE_ADD(IFNULL(epc.ops_oder_time,epc.create_time),INTERVAL 7 hour)>='2019-01-01' 
AND DATE_ADD(IFNULL(epc.ops_oder_time,epc.create_time),INTERVAL 7 hour)<='2020-08-20'
UNION ALL
SELECT DATE_ADD(IFNULL(epc.ops_oder_time,epc.create_time),INTERVAL 7 hour) order_time,
epc.fuse_policy_code AS fuse_policy_code,epc.effective_time+INTERVAL 7 HOUR AS start_date,epc.expire_time+INTERVAL 7 HOUR AS end_date,epc.insurance_company AS insured_name,
eam.plate_no AS plate_number,eam.brand AS brand,eam.type AS type,eam.series AS series,eam.year AS year,eam.price+eam.modified_info AS price,
'car' AS car_type,
'single_other' AS mytype
FROM external_policy_common AS epc
LEFT JOIN account_agent AS aa on aa.mobile=epc.create_account
LEFT JOIN external_auto_single_year AS eam on eam.fuse_policy_code=epc.fuse_policy_code
WHERE aa.test_account  IN (0,4)
AND epc.product_name IN ("Car single year")
AND epc.policy_status not in (-1,101)
AND DATE_ADD(IFNULL(epc.ops_oder_time,epc.create_time),INTERVAL 7 hour)>='2019-01-01' 
AND DATE_ADD(IFNULL(epc.ops_oder_time,epc.create_time),INTERVAL 7 hour)<='2020-08-20') tt where insured_name!='' and insured_name is not null) tt2 GROUP BY tt2.insured_name,car_type,aging) tt3;
        ''')

    column = db.get_column_name()
    data_df = pd.DataFrame(np.array(data))
    data_df.columns = column
    return data_df

def write_to_excel(data_df):
    print("write to excel......")
    ## 获取昨天
    t = datetime.now() - timedelta(days=1)
    table_name = export_path+'Car_BI_Summary' + (
                u'_%d%02d%02d_%02d%02d%02d' % (t.year, t.month, t.day, t.hour, t.minute, t.second))
    writer = pd.ExcelWriter(table_name + '.xlsx')
    wb = writer.book
    # 3.设置格式
    header_fmt = wb.add_format(
        {'bold': True, 'font_size': 12, 'font_name': u'微软雅黑', 'num_format': 'yyyy-mm-dd',
         'valign': 'vcenter', 'align': 'center'})

    data_df.to_excel(writer, sheet_name=u'Summary', encoding='utf8', header=True, index=False, startcol=0, startrow=0)

    worksheet1 = writer.sheets[u'Summary']
    for col_num, value in enumerate(data_df.columns.values):
        worksheet1.write(0, col_num, value, header_fmt)

    gr_data = data_df.groupby('insurance_company')
    cc=0
    for name, each_sheet in gr_data:
        this_Df = pd.DataFrame(each_sheet)
        sheet_name= name[0:28]+''+str(cc)
        this_Df.to_excel(writer, sheet_name=sheet_name, encoding='utf_8_sig', header=True, index=False, startcol=0,
                         startrow=0,
                         float_format="%0.1f")
        worksheet1 = writer.sheets[sheet_name]
        for col_num, value in enumerate(this_Df.columns.values):
            worksheet1.write(0, col_num, value, header_fmt)
        cc= cc+1
    writer.save()



data_df=get_data();
print(data_df)
write_to_excel(data_df);
print("finish......")
