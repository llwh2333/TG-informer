import csv
import sys
import os
import logging
import json
from dotenv import load_dotenv
from pathlib import Path
import sqlalchemy as db
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from models import Account, Channel, ChatUser, Message

""" 
本文件是在项目第一次执行时执行的文件，目标在数据库中建立相应的表字段结构
"""

logging.getLogger().setLevel(logging.INFO)


# 加载配置文件
dotenv_path = Path('../informer.env')
load_dotenv(dotenv_path=dotenv_path)

# 绑定一个数据库引擎
Session = None

# 实例化一个 session 对象
session = None

# 运行模式
SERVER_MODE = None

# 数据库引擎
engine = None

def init_add_account():
    """ 
    向数据库中添加配置文件中的傀儡账户信息
    """ 
    global session, SERVER_MODE, engine

    logging.info(f'{sys._getframe().f_code.co_name}: Adding bot account')

    # 傀儡账户列表
    BOT_ACCOUNTS = [
        Account(
            # 从配置文件获取默认的傀儡账户
            account_id=os.environ['TELEGRAM_ACCOUNT_ID'],
            account_api_id=os.environ['TELEGRAM_API_APP_ID'],
            account_api_hash=os.environ['TELEGRAM_API_HASH'],
            account_is_bot=False,
            account_is_verified=False,
            account_is_restricted=False,
            account_first_name=os.environ['TELEGRAM_ACCOUNT_FIRST_NAME'],
            account_last_name=os.environ['TELEGRAM_ACCOUNT_LAST_NAME'],
            account_user_name=os.environ['TELEGRAM_ACCOUNT_USER_NAME'],
            account_phone=os.environ['TELEGRAM_ACCOUNT_PHONE_NUMBER'], 
            account_is_enabled=True,
            account_tlogin=datetime.now(),
            account_tcreate=datetime.now(),
            account_tmodified=datetime.now()),
    ]

    # 向数据库中添加我们的账户信息
    for account in BOT_ACCOUNTS:
        session.add(account)

    session.commit()
    pass

def init_add_channels():
    """ 
    向数据库中根据配置文件获得 channel
    """ 

    global session, SERVER_MODE, engine

    # 获得数据库中的第一个傀儡账户的信息
    account = session.query(Account).first()

    CHANNELS = [
        {
            'channel_name': 'Informer monitoring',
            'channel_id': os.environ['TELEGRAM_NOTIFICATIONS_CHANNEL_ID'],
            'channel_url': os.environ['TELEGRAM_NOTIFICATIONS_CHANNEL_URL'],
            'channel_is_private': False if os.environ['TELEGRAM_NOTIFICATIONS_CHANNEL_IS_PRIVATE']=='0' else True
        },
    ]

    # 从 csv 文件中读取我们需要监控的频道
    with open(os.environ['TELEGRAM_CHANNEL_MONITOR_LIST']) as csv_file:

        # 读取文件中的每一行，并以 ‘，’为分隔符读取，返回一个可迭代对象
        csv_reader = csv.reader(csv_file, delimiter=',')

        # 记录监控数量
        line_count = 0
        for row in csv_reader:
            if line_count != 0:
                # print(f'Adding channel {row[0]} => {row[1]}') 
                CHANNELS.append({
                    'channel_name': row[0],
                     'channel_url': row[1]
                                })
            line_count += 1
     
    logging.info(f'Inserting {line_count} channels to database')

    # 将所有的频道添加到数据库中
    for channel in CHANNELS:
        logging.info(f"{sys._getframe().f_code.co_name}: Adding channel {channel['channel_name']} to database")

        channel_url = channel['channel_url'] if 'channel_url' in channel else None
        channel_id = channel['channel_id'] if 'channel_id' in channel else None
        channel_is_group = channel['channel_is_group'] if 'channel_is_group' in channel else None
        channel_is_private = channel['channel_is_private'] if 'channel_is_private' in channel else None

        # 添加到数据库中
        session.add(Channel(
            channel_name=channel['channel_name'],
            channel_url=channel_url,
            channel_id=channel_id,
            account_id=account.account_id,
            channel_tcreate=datetime.now(),
            channel_is_group=channel_is_group,
            channel_is_private=channel_is_private
        ))
    session.commit()
    pass

def init_db():
    """ 
    在数据库中建立 account、chatuser、channel、message
    """ 
    global session, SERVER_MODE, engine

    logging.info(f'{sys._getframe().f_code.co_name}: Initializing the database')
    Account.metadata.create_all(engine)
    ChatUser.metadata.create_all(engine)
    Channel.metadata.create_all(engine)
    Message.metadata.create_all(engine)

    # 关闭 session 对象
    session.close()

def init_data():
    """
    初始化数据库中我们需要监控的内容
    """

    global session, SERVER_MODE, engine
    
    # 建立一个 session 实例对象
    session = Session()

    #初始化数据
    init_add_account()
    init_add_channels(

    session.close()

def initialize_db():
    """ 
    根据配置文件中的内容进行初始化
    """ 

    global session,SERVER_MODE, engine, Session

    # 配置中的目标数据库名称
    DATABASE_NAME = os.environ['MYSQL_DATABASE']


    # 数据库的配置
    db_database = os.environ['MYSQL_DATABASE']
    db_user = os.environ['MYSQL_USER']
    db_password = os.environ['MYSQL_PASSWORD']
    db_ip_address = os.environ['MYSQL_IP_ADDRESS']
    db_port = os.environ['MYSQL_PORT']

    # 获取程序运行的模式
    SERVER_MODE = os.environ['ENV']

    # 连接数据使用的字符串
    MYSQL_CONNECTOR_STRING = f'mysql+mysqlconnector://{db_user}:{db_password}@{db_ip_address}:{db_port}/{db_database}?charset=utf8mb4&collation=utf8mb4_general_ci'

    # 与数据库连接，返回一个数据库引擎，同时开启引擎的日志功能
    engine = db.create_engine(MYSQL_CONNECTOR_STRING, echo=True) 

    # 利用 Session 进行数据库引擎的管理
    Session = sessionmaker(bind=engine)
    session = None
    session = Session()

    # 检查当前数据库中是否存在目标数据库，不存在就建立
    session.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME} CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_unicode_ci';")
    session.execute('commit')

    # 保证当前数据库支持表情符号的解析
    session.execute('SET NAMES "utf8mb4" COLLATE "utf8mb4_unicode_ci"')
    session.execute(f'ALTER DATABASE {DATABASE_NAME} CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;')
    session.execute('commit')

    # 初始化数据库
    init_db()

    #初始化数据库内容
    init_data()

if __name__ == '__main__':
    initialize_db()     
    # 数据库整体初始化
