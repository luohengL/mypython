import databasefactory as dbf
import numpy as np

db = dbf.DataBaseFactory()

result = db.querysql('select * from account limit 0,10')

print(db.cursor.description)
print(result)

result2 = db.querysql('select * from role limit 0,10')
print(db.cursor.description)
print(result2)

result3 = db.querysql(
    'select aa.name,role.role_name as roleName,role.role_descrption as des from account aa left  join  role on aa.role_id= role.id limit 0,10')
print(db.get_column_name())
print(result3)

account_darr = np.array(result3)

print(account_darr)
