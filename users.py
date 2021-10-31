import pandas as pd
import pymysql
import os
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
import hashlib

# 与数据库建立连接
yconnect = create_engine('mysql+mysqldb://root:root@localhost:3306/bbc?charset=utf8mb4')


def md5(x):
    md5_val = hashlib.md5(x.encode('utf8')).hexdigest()
    return md5_val


# 读取excel数据
df=pd.DataFrame()
file_root=r'E:\2-数据源\1-用户表\艾克系统导出\待上传'
for r,d,f in os.walk(file_root):
    for s in f:
        xinzeng=pd.read_excel(f'{r}/{s}')
        df = pd.concat([df,xinzeng])

# 字典生成dataframe，上传之前的数据清洗
df_split = df['地区'].str.split(' ', expand=True)
df['省'] = df_split[0]
df['市'] = df_split[1]

nick_name = df['好友昵称']
types = df['类型']
mobile = df['手机号']
wechat_id = df['好友微信ID']
wechat_num = df['好友微信号']
avater = df['好友头像']
friend_status = df['好友状态']
allocation_account = df['分配账户']
allocation_account_dept = df['分配账户所属部门']
attribution = df['业绩归属']
attribution_dept = df['业绩归属账户所属部门']
bind = df['是否绑定']
wechat_notes = df['微信备注']
sex = df['性别']
location = df['地区']
province = df['省']
city = df['市']
belong_wechat = df['所属微信']
belong_wechat_id = df['所属微信ID']
belong_wechat_num = df['所属微信号']
add_from = df['添加来源']
come_time = pd.to_datetime(df['入库时间'])
apply_time = pd.to_datetime(df['申请时间'])
pass_time = pd.to_datetime(df['通过时间'])

df_users = pd.DataFrame(
    {'nick_name': nick_name, 'types': types, 'mobile': mobile, 'wechat_id': wechat_id, 'wechat_num': wechat_num, \
     'avater': avater, 'friend_status': friend_status, 'allocation_account': allocation_account, \
     'allocation_account_dept': allocation_account_dept, 'attribution': attribution, \
     'attribution_dept': attribution_dept, 'bind': bind, 'wechat_notes': wechat_notes, \
     'sex': sex, 'location': location, 'belong_wechat': belong_wechat, 'belong_wechat_id': belong_wechat_id, \
     'belong_wechat_num': belong_wechat_num, 'add_from': add_from, 'come_time': come_time, 'apply_time': apply_time, \
     'pass_time': pass_time, 'province': province, 'city': city})
df_users['wechat_id'] = df_users['wechat_id'].apply(str)
# 对微信号加密处理
df_users['wechat_num'] = df_users['wechat_num'].map(md5)
# 数据清洗
df_users = df_users.fillna({'pass_time': '2000/1/1', 'come_time': '2000/1/1'})
df_users['pass_date'] = df_users['pass_time'].map(lambda x: x.strftime('%Y/%m/%d'))  # 新增一列不带时间的日期列
df_users['come_date'] = df_users['come_time'].map(lambda x: x.strftime('%Y/%m/%d'))  # 新增一列不带时间的日期列
df_users.sort_values('pass_time', ascending=False)  # 降序
df_users.drop_duplicates(subset=['wechat_id', 'belong_wechat_num'], inplace=True)  # 去重
# 如果有重复记录则从数据库删除，没有则直接上传
df_users_index = pd.read_sql_query('select user_id,wechat_id,belong_wechat_num from users',
                                   yconnect)  # 从数据库取出已上传的微信ID索引列
df_jiaoji = pd.merge(df_users, df_users_index, on=['wechat_id', 'belong_wechat_num'])  # 取交集
del_index = list(df_jiaoji['user_id'].values)

if len(del_index) != 0:
    yconnect.execute("delete from users where user_id in ({})".format(
        ','.join(map(str, del_index))))

# 写入数据库
pd.io.sql.to_sql(df_users, 'users', con=yconnect, schema='bbc', if_exists='append', index=False)