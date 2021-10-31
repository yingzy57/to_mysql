
import pandas as pd
import os
import pymysql
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
yconnect = create_engine('mysql+mysqldb://root:root@localhost:3306/bbc?charset=utf8mb4')  #建立连接
#读取csv表
file_root=r'E:\2-数据源\3-订单表\有赞商品维度订单表\正在上传'
df=pd.DataFrame()
for r,d,f in os.walk(file_root):
    for s in f:
        df_xinzeng=pd.read_csv(f'{r}/{s}')
        df=pd.concat([df,df_xinzeng])
#得到数据源dataframe
order_id=df['订单号']
outer_orderid=df['外部订单号']
order_goods_status=df['订单商品状态']
transaction_success_time=df['交易成功时间']
goods_name=df['商品名称']
goods_type=df['商品类型']
goods_category=df['商品类目']
goods_specifications=df['商品规格']
specification_code=df['规格编码']
goods_code=df['商品编码']
goods_price=df['商品单价']
goods_cost_price=df['商品成本价']
goods_discount_mode=df['商品优惠方式']
price_after_discount=df['商品优惠后价格']
goods_quantity=df['商品数量']
goods_amount_subtotal=df['商品金额小计']
store_discount_share=df['店铺优惠（分摊）']
goods_pay_amount=df['商品实际成交金额']
goods_usepoints=df['商品抵用积分数']
goods_message=df['商品留言']
consignee=df['收货人/提货人']
consignee_mobile=df['收货人手机号/提货人手机号']
province=df['收货人省份']
city=df['收货人城市']
district=df['收货人地区']
address=df['详细收货地址/提货地址']
note=df['买家备注']
ship_status=df['商品发货状态']
ship_mode=df['商品发货方式']
logistics_company=df['商品发货物流公司']
logistics_id=df['商品发货物流单号']
ship_staff=df['发货员工']
ship_time=df['商品发货时间']
goods_refund_status=df['商品退款状态']
goods_refunded_amount=df['商品已退款金额']
seller_order_note=df['商家订单备注']
check_in_time=df['入住时间']
df_payordergoods=pd.DataFrame({'order_id':order_id,'outer_orderid':outer_orderid,'order_goods_status':order_goods_status,'transaction_success_time':transaction_success_time,'goods_name':goods_name,'goods_type':goods_type,'goods_category':goods_category,'goods_specifications':goods_specifications,'specification_code':specification_code,'goods_code':goods_code,'goods_price':goods_price,'goods_cost_price':goods_cost_price,'goods_discount_mode':goods_discount_mode,'price_after_discount':price_after_discount,'goods_quantity':goods_quantity,'goods_amount_subtotal':goods_amount_subtotal,'store_discount_share':store_discount_share,'goods_pay_amount':goods_pay_amount,'goods_usepoints':goods_usepoints,'goods_message':goods_message,'consignee':consignee,'consignee_mobile':consignee_mobile,'province':province,'city':city,'district':district,'address':address,'note':note,'ship_status':ship_status,'ship_mode':ship_mode,'logistics_company':logistics_company,'logistics_id':logistics_id,'ship_staff':ship_staff,'ship_time':ship_time,'goods_refund_status':goods_refund_status,'goods_refunded_amount':goods_refunded_amount,'seller_order_note':seller_order_note,'check_in_time':check_in_time})
year=df_payordergoods['order_id'].apply(lambda x:x[1:5])
month=df_payordergoods['order_id'].apply(lambda x:x[5:7])
day=df_payordergoods['order_id'].apply(lambda x:x[7:9])
df_payordergoods['order_day']=year+"/"+month+"/"+day#添加订单日期列
#写入数据库
pd.io.sql.to_sql(df_payordergoods,'payordergoods', con=yconnect, schema='bbc',if_exists='append', index=False)