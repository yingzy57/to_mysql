import pandas as pd
import pymysql
pymysql.install_as_MySQLdb() #引入mysqldb不然会出错
from sqlalchemy import create_engine#引擎
yconnect = create_engine('mysql+mysqldb://root:root@localhost:3306/bbc?charset=utf8mb4')  #建立连接
import os
import re

file_root=r'E:\2-数据源\12-句子导出有赞订单表\归因'
df=pd.DataFrame()
for r,d,f in os.walk(file_root):
    for s in f:
        df_xinzeng=pd.read_excel(f'{r}/{s}')
        df=pd.concat([df,df_xinzeng])
df['创建时间']=pd.to_datetime(df['创建时间'])
df['创建日期']=df['创建时间'].dt.date
#得到数据源dataframe
group_id=df['微信群id']
group_name=df['微信群名称']
group_owner_wechatid=df['群主微信id']
group_owner_wechatnum=df['群主微信号']
unionid=df['客户unionId']
wxuserid=df['客户wxUserId']
wechatid=df['客户wxid']
nickname=df['客户昵称']
avater=df['客户头像']
belong_wechat_id=df['所属微信id']
belong_wechat_num=df['所属微信号']
belong_wechat_nickname=df['所属微信昵称']
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

def getname(a):
    #如果有标点符号连接在姓名和手机尾号中间，则捕获文本部分
    if len(re.findall('[- + .]',a))>0:
         return re.findall('(\w+)+',a)[0]
    #如果姓名直接连接=手机尾号，则捕获非数字部分（\D）
    if len(re.findall('[- + .]',a))==0:
         return re.findall('(?:\D)+',a)[0]
#缺失值会报错，先填充
df['所属微信昵称']=df['所属微信昵称'].fillna('未知')
#按行处理，根据昵称得到姓名
sales_name=df.apply(lambda x : getname(x['所属微信昵称']),axis=1)


#群成员订单（没有按照商品名称罗列，暂不处理商品表）
df_qiweiqunorder=pd.DataFrame({'group_id':group_id,'group_name':group_name,'group_owner_wechatid':group_owner_wechatid,'group_owner_wechatnum':group_owner_wechatnum,'unionid':unionid,'wxuserid':wxuserid,'wechatid':wechatid,'nickname':nickname,'avater':avater,'belong_wechat_id':belong_wechat_id,'belong_wechat_num':belong_wechat_num,'belong_wechat_nickname':belong_wechat_nickname,'sales_name':sales_name,'order_id':order_id,'order_status':order_status,'order_type':order_type,'createtime':createtime,'createdate':createdate,'address':address,'mobile':mobile,'youzan_nickname':youzan_nickname,'pay_amount':pay_amount,'refund_apply_time':refund_apply_time,'refund_amount':refund_amount,'goods_name':goods_name})
df_qiweipayorder=df_qiweiqunorder.drop_duplicates(['order_id'],keep='first')
pd.io.sql.to_sql(df_qiweipayorder,'qiweipayorder', con=yconnect, schema='bbc',if_exists='append', index=False)
pd.io.sql.to_sql(df_qiweiqunorder,'qiweiqunorder', con=yconnect, schema='bbc',if_exists='append', index=False)