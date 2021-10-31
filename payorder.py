
import pandas as pd
import os
import hashlib
import pymysql
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine

yconnect = create_engine('mysql+mysqldb://root:root@localhost:3306/bbc?charset=utf8mb4')  #建立连接

def md5(x):
    md5_val = hashlib.md5(x.encode('utf8')).hexdigest()
    return md5_val

file_root=r'E:\2-数据源\3-订单表\有赞订单表'
df=pd.DataFrame()
for r,d,f in os.walk(file_root):
    for s in f:
        df_xinzeng=pd.read_csv(f'{r}/{s}',dtype={'买家手机号':str},low_memory=False,usecols=any)
        df=pd.concat([df,df_xinzeng])
#得到数据源dataframe
order_id=df['订单号']
belong_store=df['归属店铺']
order_type=df['订单类型']
order_status=df['订单状态']
promotion_type=df['推广方式']
order_from=df['订单来源']
createtime=pd.to_datetime(df['订单创建时间'])
pay_time=pd.to_datetime(df['买家付款时间'])
pay_type=df['付款方式']
outer_orderid=df['外部订单号']
pay_id=df['支付流水号']
goods_amount=df['商品金额合计']
expressfee=df['运费']
store_discounts=df['店铺优惠合计']
marketing_benefits=df['支付营销优惠']
amount=df['应收订单金额']
pay_amount=df['订单实付金额']
store_discount_type=df['店铺优惠方式']
all_goods_name=df['全部商品名称']
goods_type_num=df['商品种类数']
delivery_type=df['订单配送方式']
scheduled_delivery_time=df['预约同城送达时间/预约自提时间']
consignee=df['收货人/提货人']
consignee_mobile=df['收货人手机号/提货人手机号']
province=df['收货人省份']
city=df['收货人城市']
district=df['收货人地区']
address=df['详细收货地址/提货地址']
note=df['买家备注']
order_star=df['订单星级']
buyer_name=df['买家姓名']
is_number=df['买家是否会员']
buyer_mobile=df['买家手机号']
order_net=df['下单网点']
order_refund_status=df['订单退款状态']
refund_amount=df['订单已退款金额']
seller_order_note=df['商家订单备注']
coupon_name=df['优惠券码名称']
check_in_time=pd.to_datetime(df['入住时间'])
group_order=df['团长订单']
df_order=pd.DataFrame({'order_id':order_id,'belong_store':belong_store,'order_type':order_type,'order_status':order_status,\
                       'promotion_type':promotion_type,'order_from':order_from,'createtime':createtime,\
                       'pay_time':pay_time,'pay_type':pay_type,'outer_orderid':outer_orderid,'pay_id':pay_id,\
                       'goods_amount':goods_amount,'expressfee':expressfee,'store_discounts':store_discounts,\
                       'marketing_benefits':marketing_benefits,'amount':amount,'pay_amount':pay_amount,\
                       'store_discount_type':store_discount_type,'all_goods_name':all_goods_name,'goods_type_num':goods_type_num,\
                       'delivery_type':delivery_type,'scheduled_delivery_time':scheduled_delivery_time,'consignee':consignee,\
                       'consignee_mobile':consignee_mobile,'province':province,'city':city,'district':district,\
                       'address':address,'note':note,'order_star':order_star,'buyer_name':buyer_name,\
                       'is_number':is_number,'buyer_mobile':buyer_mobile,'order_net':order_net,'order_refund_status':order_refund_status,\
                       'refund_amount':refund_amount,'seller_order_note':seller_order_note,'coupon_name':coupon_name,\
                       'check_in_time':check_in_time,'group_order':group_order})
df_order['order_day']=createtime.map(lambda x:x.strftime('%Y/%m/%d'))#新增一列不带时间的日期列
df_order['buyer_mobile']  = df_order['buyer_mobile'].replace('\t', '', regex=True).apply(str).apply(md5)
#从数据库删除与即将上传的数据的重复记录
df_order_index=pd.read_sql_query('select order_id from payorder',yconnect)#从数据库取出已上传的ID索引列
df_jiaoji=pd.merge(df_order,df_order_index,on='order_id')
del_index=list(df_jiaoji['order_id'].values)
if len(del_index)!=0:
    yconnect.execute('delete from payorder where order_id in ({})'.format(','.join(map(str,del_index))))

#新值写入数据库
pd.io.sql.to_sql(df_order,'payorder',con=yconnect,schema='bbc',if_exists='append', index=False)