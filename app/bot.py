#See:https://github.com/paulpierre/informer
import sys
import os
import logging
from dotenv import load_dotenv
from pathlib import Path
from informer import TGInformer

""" 
在第一次启动程序时，对于数据库进行初始化并根据配置填入
""" 
logging.getLogger().setLevel(logging.INFO)


env_file = 'informer.env' if os.path.isfile('informer.env') else '../informer.env'
logging.info(f'env_file: {env_file}')
  
dotenv_path = Path(env_file)
load_dotenv(dotenv_path=dotenv_path)

# 检查输入的变量是否足够
try:
    account_id = sys.argv[1]
except:
    raise Exception('informer.py <account_id> - account_id is a required param')

if not account_id:
    raise Exception('Account ID required')

if __name__ == '__main__':

    informer = TGInformer(
        db_database = os.environ['MYSQL_DATABASE'],
        db_user = os.environ['MYSQL_USER'],
        db_password = os.environ['MYSQL_PASSWORD'],
        db_ip_address = os.environ['MYSQL_IP_ADDRESS'],
        db_port = os.environ['MYSQL_PORT'],
        tg_account_id = os.environ['TELEGRAM_ACCOUNT_ID'],
        tg_notifications_channel_id = os.environ['TELEGRAM_NOTIFICATIONS_CHANNEL_ID'],
        es_ip=os.environ['ES_IP'],
        es_port=os.environ['ES_PORT'],
        es_message_index=os.environ['ES_MESSAGE_INDEX'],
        es_channel_index=os.environ['ES_CHANNEL_INDEX'],
        es_user_index=os.environ['ES_USER_INDEX'],
    )
    informer.init()

