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
    account_id = os.environ['TELEGRAM_ACCOUNT_ID']
except:
    raise Exception('informer.py <account_id> - account_id is a required param')

if not account_id:
    raise Exception('Account ID required')

if __name__ == '__main__':

    informer = TGInformer(
    )
    informer.init()

