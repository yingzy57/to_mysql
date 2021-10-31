
import pandas as pd
import os
import pymysql
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
yconnect = create_engine('mysql+mysqldb://root:root@localhost:3306/bbc?charset=utf8mb4')  #建立连接

file_root=r'E:\2-数据源\9-退款表\待上传'
df=pd.DataFrame()
for r,d,f in os.walk(file_root):
    for s in f:
        df_xinzeng=pd.read_csv(f'{r}/{s}',low_memory=False)
        df=pd.concat([df,df_xinzeng])
#得到数据源dataframe
after_sales_id=df['售后编号']
belong_store=df['归属店铺']
after_sales_mode=df['售后方式']
order_id=df['订单编号']
goods_name=df['商品名称']
goods_code=df['商品编码']
goods_sku_code=df['商品SKU编码']
logistics_company=df['发货物流公司']
logistics_id=df['发货物流单号']
pay_time=pd.to_datetime(df['付款时间'])
transaction_completion_time=pd.to_datetime(df['交易完成时间'])
amount=df['订单金额']
refund_amount=df['退款金额']
apply_time=df['申请时间']
refund_finish_time=pd.to_datetime(df['退款完成时间'])
after_sales_status=df['售后状态']
reason=df['申请原因']
ship_status=df['发货状态']
return_logistics_id=df['退货物流单号']
refund_number=df['申请数量']

df_refund=pd.DataFrame({'after_sales_id':after_sales_id,'belong_store':belong_store,'after_sales_mode':after_sales_mode,\
                        'order_id':order_id,'goods_name':goods_name,'goods_code':goods_code,'goods_sku_code':goods_sku_code,\
                        'logistics_company':logistics_company,'logistics_id':logistics_id,'pay_time':pay_time,\
                        'transaction_completion_time':transaction_completion_time,'amount':amount,'refund_amount':refund_amount,\
                        'apply_time':apply_time,'refund_finish_time':refund_finish_time,'after_sales_status':after_sales_status,\
                        'reason':reason,'ship_status':ship_status,'return_logistics_id':return_logistics_id,'refund_number':refund_number})
#从数据库删除与即将上传的数据的重复记录
df_refund_index=pd.read_sql_query('select after_sales_id from refund',yconnect)#从数据库取出已上传的ID索引列
df_jiaoji=pd.merge(df_refund,df_refund_index,on='after_sales_id')
del_index=list(df_jiaoji['after_sales_id'].values)
if len(del_index)!=0:
    yconnect.execute('delete from refund where after_sales_id in ({})'.format(','.join(map(str,del_index))))

#新值写入数据库
pd.io.sql.to_sql(df_refund,'refund',con=yconnect,schema='bbc',if_exists='append', index=False)