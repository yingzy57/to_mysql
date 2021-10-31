import pandas as pd
import os
import pymysql
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine

file_root=r'E:\2-数据源\2-销售员表\property'
df1=pd.DataFrame()
df2=pd.DataFrame()
for r,d,f in os.walk(file_root):
    for s in f:
        if '微信号管理列表'in s:
            xinzeng=pd.read_excel(f'{r}/{s}')
            df1=pd.concat([df1,xinzeng])
        if '微信列表'in s:
            xinzeng=pd.read_excel(f'{r}/{s}')
            df2=pd.concat([df2,xinzeng])
#处理微信列表
#保管人员为空的删除
df2=df2.dropna(subset=['保管人员'])
#微信号为空的用前一列微信id填充
df2=df2.fillna(method='ffill',axis=1)
df_aike=df2[['微信名称','微信id','微信号','微信版本','微信手机号','设备名称','保管人员','备注']]
#处理粉丝登记列表
def zhuanhuan(a):
    if a==0:
        return '移动'
    if a==1:
        return '联通'
    if a==2:
        return '电信'
df1['电话卡类型']=df1.apply(lambda x : zhuanhuan(x['电话卡类型：移动：0，联通：1，电信：2']),axis=1)
df1=df1.drop('电话卡类型：移动：0，联通：1，电信：2',axis=1)
def zhuanhuan1(a):
    if a==0:
        return '否'
    if a==1:
        return '是'
df1['是否删除僵尸粉']=df1.apply(lambda x : zhuanhuan1(x['是否删除僵尸粉:0否,1是,默认:0']),axis=1)
df1=df1.drop('是否删除僵尸粉:0否,1是,默认:0',axis=1)
def zhuanhuan2(a):
    if a==0:
        return '未绑卡'
    if a==1:
        return '已绑卡'
df1['是否绑卡']=df1.apply(lambda x : zhuanhuan2(x['是否绑卡:0未绑卡,1已绑卡,默认:0']),axis=1)
def zhuanhuan3(a):
    if a==0:
        return '未使用'
    if a==1:
        return '使用中'
df1['使用状态']=df1.apply(lambda x : zhuanhuan3(x['使用状态:0未使用,1使用中,默认:0']),axis=1)
df1=df1.drop('使用状态:0未使用,1使用中,默认:0',axis=1)
df_maodong=df1[['组别','员工组名','工号','微信号','员工手机号','推广渠道','开通时间','开始使用时间','使用状态','绑定手机号','电话卡类型','资产编码','是否删除僵尸粉','支付密码','实名认证','是否绑卡']]
df_qitui=df1[['微信昵称','微信号','绑定手机号','姓名','组别','员工组名','工号','员工手机号','推广渠道','开通时间','开始使用时间','使用状态','电话卡类型','资产编码','是否删除僵尸粉','支付密码','实名认证','是否绑卡']]
df_qitui=df_qitui[(df_qitui['组别']=='企微社群组')|(df_qitui['组别']=='推广销售组')]
df_qitui=df_qitui.drop_duplicates(subset=['绑定手机号'])
#只取业务线组别
df_maodong=df_maodong[(df_maodong['组别']=='天猫社群组')|(df_maodong['组别']=='京东社群组')]
#粉丝登记列表去重
df_maodong=df_maodong.drop_duplicates(subset=['绑定手机号'])

df_aike['微信手机号']=df_aike['微信手机号'].apply(str)
df_maodong['绑定手机号']=df_maodong['绑定手机号'].apply(str)
mer=pd.merge(df_aike,df_maodong,left_on='微信手机号',right_on='绑定手机号',how='left')#第一次用手机号匹配
mer=mer.drop(['微信号_y','绑定手机号'],axis=1)
mer=mer.rename(columns={'微信号_x':'微信号'})
mer_isnull=mer[mer['推广渠道'].isnull()==True]
mer_notnull=mer[mer['推广渠道'].notnull()==True]
mer_isnull=mer_isnull[['微信名称','微信id','微信号','微信版本','微信手机号','设备名称','保管人员','备注']]

mer2=pd.merge(mer_isnull,df_maodong,left_on='微信号',right_on='微信号',how='left')#匹配不到的部分第二次用微信号匹配
mer2=mer2.drop(['绑定手机号'],axis=1)
maodong=pd.concat([mer_notnull,mer2])#合并第一次匹配的结果+第二次匹配的结果
maodong=maodong.drop_duplicates(subset=['微信号'])#两次匹配合并后用微信号去重
maodong=maodong.rename(columns={'微信名称':'微信昵称','微信手机号':'绑定手机号','保管人员':'姓名'})
con=pd.concat([maodong,df_qitui])
# 建立连接
yconnect = create_engine('mysql+mysqldb://root:root@localhost:3306/bbc?charset=utf8mb4')
#取出微信号上的粉丝数
fans_num=pd.read_sql_query('select belong_wechat_num as "微信号",sum_num as "粉丝数" from users_newlist where pass_date= (select max(pass_date) from users_newlist)',yconnect)
#把粉丝数和资产表合并
result=pd.merge(con,fans_num,on='微信号',how='left')
#根据粉丝数和微信昵称判断是否正常桃小白号
def taoxiaobai(result):
    if  '桃小白' in result['微信昵称']  and result['粉丝数'] >= 1000:
        return '正常桃小白号'
    elif '桃小白' not in result['微信昵称']:
        return '水军号'
    else:
        return '不正常桃小白号'
result['状态']=result.apply(taoxiaobai,axis=1)

with pd.ExcelWriter(r'E:\2-数据源\2-销售员表\property\整合资产表.xlsx') as writer:
    result.to_excel(writer,index=False)
belong_wechat_nickname=result['微信昵称']
belong_wechat_id=result['微信id']
belong_wechat_num=result['微信号']
wechat_version=result['微信版本']
sales_mobile=result['绑定手机号']
equipment=result['设备名称']
sales_name=result['姓名']
notes=result['备注']
team=result['组别']
group=result['员工组名']
job_number=result['工号']
job_mobile=result['员工手机号']
channel=result['推广渠道']
open_time=result['开通时间']
start_use_time=result['开始使用时间']
use_status=result['使用状态']
status=result['状态']
card_type=result['电话卡类型']
property_id=result['资产编码']
del_zombie=result['是否删除僵尸粉']
payment_password=result['支付密码']
real_name_authentication=result['实名认证']
card_bound=result['是否绑卡']
df_property=pd.DataFrame({'belong_wechat_nickname':belong_wechat_nickname,'belong_wechat_id':belong_wechat_id,'belong_wechat_num':belong_wechat_num,'wechat_version':wechat_version,'sales_mobile':sales_mobile,'equipment':equipment,'sales_name':sales_name,'notes':notes,'team':team,'group':group,'job_number':job_number,'job_mobile':job_mobile,'channel':channel,'open_time':open_time,'start_use_time':start_use_time,'use_status':use_status,'status':status,'card_type':card_type,'property_id':property_id,'del_zombie':del_zombie,'payment_password':payment_password,'real_name_authentication':real_name_authentication,'card_bound':card_bound})
#从数据库删除与即将上传的数据的重复记录
df_property_index=pd.read_sql_query('select sales_mobile from property',yconnect)#从数据库取出已上传的ID索引列
df_jiaoji=pd.merge(df_property,df_property_index,on='sales_mobile')
del_index=list(df_jiaoji['sales_mobile'].values)
if len(del_index)!=0:
    yconnect.execute('delete from property where sales_mobile in ({})'.format(','.join(map(str,del_index))))

pd.io.sql.to_sql(df_property,'property',con=yconnect,schema='bbc',if_exists='replace',index=False)