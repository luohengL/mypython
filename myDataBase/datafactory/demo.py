from datafactory import Databasefactory as dbf
import numpy as np

db = dbf.DataBaseFactory()

print("excute.....")
result = db.querysql('select * from account limit 0,10')

print(db.cursor.description)
print("result")
print(result)

result2 = db.querysql('select * from account_agent limit 0,10')
print(db.cursor.description)
print(result2)

result3 = db.querysql(
    'SELECT aa.`name` ,ag.mobile,ag.agent_type_code from account_agent ag LEFT JOIN account aa on aa.mobile=ag.mobile LIMIT 0,10')
print(db.get_column_name())
print(result3)

account_darr = np.array(result3)

print(account_darr)
