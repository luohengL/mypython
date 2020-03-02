import databasefactory as dbf
import numpy as np
import pandas as pd


db = dbf.DataBaseFactory()


data = db.querysql(
    'select aa.name,role.role_name as roleName,role.role_descrption as des from account aa left  join  role on aa.role_id= role.id limit 0,10')

column = db.get_column_name()
print(data)

data_df = pd.DataFrame(np.array(data))

data_df.columns = column

print(type(data_df))
print(data_df)

writer = pd.ExcelWriter('my.xlsx')
data_df.to_excel(writer, float_format='%.5f')
writer.save()
