import pandas as pd
import pymysql
pymysql.install_as_MySQLdb() #引入mysqldb不然会出错
from sqlalchemy import create_engine#引擎
yconnect = create_engine('mysql+mysqldb://root:root@localhost:3306/bbc?charset=utf8mb4')  #建立连接
import hashlib
import os
def md5(x):
    md5_val = hashlib.md5(x.encode('utf8')).hexdigest()
    return md5_val
file_root=r'E:\2-数据源\8-有赞订单维度业绩结算表'
df=pd.DataFrame()
for r,d,f in os.walk(file_root):
    for s in f:
        df_xinzeng=pd.read_excel(f'{r}/{s}',dtype={'客户手机号':str})
        df=pd.concat([df,df_xinzeng])
#得到数据源dataframe
createtime=df['下单时间']
belong_wechat_nickname=df['分销员昵称']
sales_mobile=df['分销员手机号']
nickname=df['客户昵称']
buyer_mobile=df['客户手机号']
store_information=df['门店信息']
team=df['所属分组']
dept=df['所属团队']
order_id=df['订单号']
amount=df['成交金额']
commission_type=df['订单佣金类型']
commission=df['订单佣金']
settlement_status=df['结算状态']
settlement_time=df['结算时间']
invite_nickname=df['邀请方昵称']
invite_mobile=df['邀请方手机号']
invite_reward_type=df['邀请奖励类型']
invite_reward_amount=df['邀请奖励金额']
order_from=df['订单来源']
order_status=df['订单状态']
commission_tax_amount=df['佣金个税金额']
commission_tax_payer=df['佣金个税承担方']
invite_reward_tax_amount=df['邀请奖励个税金额']
invite_reward_tax_payer=df['邀请奖励个税承担方']
df_payorderjs=pd.DataFrame({'createtime':createtime,'belong_wechat_nickname':belong_wechat_nickname,\
                            'sales_mobile':sales_mobile,'nickname':nickname,'buyer_mobile':buyer_mobile,\
                            'store_information':store_information,'team':team,'dept':dept,'order_id':order_id,\
                            'amount':amount,'commission_type':commission_type,'commission':commission,\
                            'settlement_status':settlement_status,'settlement_time':settlement_time,\
                            'invite_nickname':invite_nickname,'invite_mobile':invite_mobile,\
                            'invite_reward_type':invite_reward_type,'invite_reward_amount':invite_reward_amount,\
                            'order_from':order_from,'order_status':order_status,'commission_tax_amount':commission_tax_amount,\
                            'commission_tax_payer':commission_tax_payer,'invite_reward_tax_amount':invite_reward_tax_amount,\
                            'invite_reward_tax_payer':invite_reward_tax_payer})
df_payorderjs.dropna(axis='index', how='any', subset=['buyer_mobile','sales_mobile'],inplace=True)#用户手机、销售手机，任一有空值，就不记录
df_payorderjs['buyer_mobile']=df_payorderjs['buyer_mobile'].apply(str)
df_payorderjs['buyer_mobile'] = df_payorderjs['buyer_mobile'].map(md5)
#从数据库删除与即将上传的数据的重复记录
df_payorderjs_index=pd.read_sql_query('select payorderjs_id,order_id,order_status from payorderjs',yconnect)
df_jiaoji=pd.merge(df_payorderjs,df_payorderjs_index,on=['order_id','order_status'])
del_index=list(df_jiaoji['payorderjs_id'].values)
if len(del_index)!=0:
    yconnect.execute('delete from payorderjs where payorderjs_id in ({})'.format(','.join(map(str,del_index))))

#新值写入数据库
pd.io.sql.to_sql(df_payorderjs,'payorderjs', con=yconnect, schema='bbc',if_exists='append', index=False)