import pandas as pd
import pymysql
pymysql.install_as_MySQLdb() #引入mysqldb不然会出错
from sqlalchemy import create_engine#引擎
yconnect = create_engine('mysql+mysqldb://root:root@localhost:3306/bbc?charset=utf8mb4')  #建立连接
import os
file_root=r'E:\2-数据源\14-企微流失用户'
df=pd.DataFrame()
for r,d,f in os.walk(file_root):
    for s in f:
        df_xinzeng=pd.read_excel(f'{r}/{s}',header=[4])
        df=pd.concat([df,df_xinzeng])

unionid=df['unionid']
nickname=df['客户名称']
liushi_status=df['流失状态']
customer_service=df['所属客服']
customer_service_team=df['客服所属部门']
add_time=df['添加时间']

df_qiweiusers_lost=pd.DataFrame({'unionid':unionid,'nickname':nickname,'liushi_status':liushi_status,'customer_service':customer_service,'customer_service_team':customer_service_team,'add_time':add_time})
df_qiweiusers_lost=df_qiweiusers_lost[df_qiweiusers_lost['unionid'].notnull()==True]
df_qiweiusers_lost=df_qiweiusers_lost.drop_duplicates(['unionid','customer_service'])

pd.io.sql.to_sql(df_qiweiusers_lost,'qiweiusers_lost', con=yconnect, schema='bbc',if_exists='append', index=False)