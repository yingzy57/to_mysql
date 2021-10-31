import pandas as pd
import pymysql
pymysql.install_as_MySQLdb() #引入mysqldb不然会出错
from sqlalchemy import create_engine#引擎
yconnect = create_engine('mysql+mysqldb://root:root@localhost:3306/bbc?charset=utf8mb4')  #建立连接
import os
import re

file_root=r'E:\2-数据源\12-句子导出有赞订单表\未归因'
df=pd.DataFrame()
for r,d,f in os.walk(file_root):
    for s in f:
        df_xinzeng=pd.read_excel(f'{r}/{s}')
        df=pd.concat([df,df_xinzeng])
df['创建时间']=pd.to_datetime(df['创建时间'])
df['创建日期']=df['创建时间'].dt.date
#得到数据源dataframe
order_id=df['订单id']
order_status=df['订单状态']
order_type=df['订单类型']
createtime=df['创建时间']
createdate=df['创建日期']
address=df['收货地址省市']
mobile=df['买家手机号']
youzan_nickname=df['买家有赞昵称']
pay_amount=df['买家付款金额']
refund_apply_time=df['退款时间']
refund_amount=df['退款金额']
goods_name=df['商品名称']


#群成员订单（没有按照商品名称罗列，暂不处理商品表）
df_qiweiqunorder_fail=pd.DataFrame({'order_id':order_id,'order_status':order_status,'order_type':order_type,'createtime':createtime,'createdate':createdate,'address':address,'mobile':mobile,'youzan_nickname':youzan_nickname,'pay_amount':pay_amount,'refund_apply_time':refund_apply_time,'refund_amount':refund_amount,'goods_name':goods_name})

pd.io.sql.to_sql(df_qiweiqunorder_fail,'qiweipayorder_fail', con=yconnect, schema='bbc',if_exists='append', index=False)
