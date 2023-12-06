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

if __name__ == '__main__':

    informer = TGInformer(
    )
    informer.init()

