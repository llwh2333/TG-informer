#See:https://github.com/paulpierre/informer
import sys
import os
import json
import re
import asyncio
import gspread
import logging
import sqlalchemy as db
from datetime import datetime, timedelta
from random import randrange
from telethon import utils
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError, InterfaceError, ProgrammingError
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import GetFullChatRequest
from telethon.tl.custom.dialog import Dialog
from telethon import TelegramClient, events,functions
from telethon.tl.types import PeerUser, PeerChat, PeerChannel,ChannelParticipant,ChannelParticipantAdmin
from telethon.errors.rpcerrorlist import FloodWaitError, ChannelPrivateError, UserAlreadyParticipantError
from telethon.tl.functions.channels import  JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest,ExportChatInviteRequest
from oauth2client.service_account import ServiceAccountCredentials
import threading
import json
from telethon.tl import types
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import ConnectionTimeout
from itertools import groupby
from urlextract import URLExtract
import signal
from telethon.errors import *
import hashlib
import yaml
from langdetect import detect, DetectorFactory, detect_langs
from geotext import GeoText
import jionlp as jio
import copy
import queue

""" 
监控程序主要部分
""" 

banner = """
---------------------------------------------------------
___________                               __          
\__    ___/__.__.                       _/  |_  ____  
  |    | <   |  |   ________________    \   __\/ ___\ 
  |    |  \___  |  /_______________/     |  | / /_/  >
  |____|  / ____|                        |__| \___  / 
          \/                                 /_____/  
---------------------------------------------------------
"""
version = '2.0.1'
update_time = '2023.12.06'

logFilename = './tgout.log'

logging.basicConfig(
                encoding='utf-8',
                level    = logging.ERROR,          # 定义输出到文件的log级别                                                
                format   = '%(asctime)s  %(filename)s : %(levelname)s  %(message)s',    # 定义输出log的格式
                datefmt  = '%Y-%m-%d %A %H:%M:%S',                                     # 时间
                filename = logFilename,                # log文件名
                filemode = 'w')                        # 写入模式“w”或“a”
# 只需要 error 级别以上的
logging.getLogger(__name__).setLevel(logging.INFO)

class TGInformer:
    def __init__(self):
        """
        初始化，并开始启动监控程序
        """
        # 检查配置文件
        if not os.path.exists("./informer.yaml"):
            logging.error(f"Unable to get the configuration file")
            sys.exit(0)

        # 加载配置文件
        with open('./informer.yaml','r',encoding='utf-8') as f:
            env = yaml.load(f.read() ,Loader=yaml.FullLoader)

        # 加载数据上传目标配置
        self.UPDATA_MODEL = env['INFO_UPDATA_TYPE']

        # 数据部分
        self.CHANNEL_META = []                              # 已加入 channel 的信息
        self.CHANNEL_ADD= set()                             # 新检测到，待上传的 channel
        self.UPDATA_MESSAGE = []                            # 等待上传的 message
        self.DIALOG_MESSAGE_BOT = {}                        # 用于过滤 BOT 消息
        self.ACTIVE_USER = {}                               # 统计活跃用户
        self.ADMINS_DATA = None                             # es 用户页中的管理员数据
        self.DUMP_MODEL = env['INFO_DUMP_LOCAL']            # 是否存储到本地中
        self.SKIP_FIRST = env['SKIP_FIRST_UPDATA']          # 启动时是否跳过加载用户

        # 连接
        self.CLIENT = None                                  # tg 客户端实例

        # TG 账号信息
        self.ACCOUNT = {
            'account_id' : env['TELEGRAM_ACCOUNT_ID'],
            'account_api_id':env['TELEGRAM_API_APP_ID'],
            'account_api_hash':env['TELEGRAM_API_HASH'],
            'account_first_name':env['TELEGRAM_ACCOUNT_FIRST_NAME'],
            'account_last_name':env['TELEGRAM_ACCOUNT_LAST_NAME'],
            'account_user_name':env['TELEGRAM_ACCOUNT_USER_NAME'],
            'account_phone':env['TELEGRAM_ACCOUNT_PHONE_NUMBER'], 
        }

        # 异步锁（后面看看有什么锁不在需要）
        self.LOCK_MSG = threading.Lock()                    # 本地消息存储异步锁
        self.LOCK_CHANNEL = threading.Lock()                # 本地频道存储异步锁
        self.LOCK_CHAT_USER = threading.Lock()              # 本地用户存储异步锁

        self.LOCK_FILTER_MSG = threading.Lock()             # 过滤消息异步锁

        self.LOCK_UPDATA_MSG = threading.Lock()             # es 消息累积存储异步锁
        self.LOCK_CHANNEL_ADD = threading.Lock()            # 等待添加更新频道异步锁
        self.LOCK_ACTIVE_USER = threading.Lock()            # 更新用户活跃信息异步锁

        # 线程池
        self.LOOP = asyncio.get_event_loop()

        # 时间参数
        self.CHANNEL_REFRESH_WAIT = 60 * 15                 # 日志频道数据显示刷新间隔
        self.MSG_TRANSFER_GAP = 10                          # msg 传输间隔

        # 开始运行
        logging.info(banner)
        logging.info('Version:'+version)

        self.Load_Config(env)

        # 存在该文件说明需要对于过去的文件进行更新
        if os.path.exists("./backup"):
            self.UPDATA_BACK()

        # 启动协程开始执行监听程序
        self.LOOP.run_until_complete(self.Bot_Interval())

    def Load_Config(self,Env:dict):
        """ 
        根据配置，加载采集数据的传输目标
        @param Env: 配置文件中的参数
        """ 
        if self.UPDATA_MODEL == '1':
            # ES 为传输目标进行加载
            self.ES_IP =Env['ES_IP']            # es 服务器 ip
            self.ES_PORT = Env['ES_PORT']       # es 服务器端口
            self.ES_MESSAGE_INDEX = Env['ES_MESSAGE_INDEX']     # es 消息页名称
            self.ES_CHANNEL_INDEX = Env['ES_CHANNEL_INDEX']     # es 频道页名称
            self.ES_USER_INDEX = Env['ES_USER_INDEX']           # es 用户页名称
            self.ES_ACCOUNT_INDEX = Env['ES_ACCOUNT_INDEX']     # es 账号页名称
            self.ES_MEDIO_INDEX = Env['ES_MEDIO_INDEX']         # es 媒体文件(该部分未完成)
            self.ES_ACTIVE_USER_INDEX = Env['ES_ACTIVE_USER_INDEX']     # es 用户的活跃情况

            self.ES_CONNECT = self.Es_Connect(Times=5)
            if self.ES_CONNECT == None:
                sys.exit(0)
        elif self.UPDATA_MODEL == '2':
            # 自定义的传输目标
            self.CUSTOM_CONNECT = self.Custom_Connect()
        elif self.UPDATA_MODEL == '0':
            # 没有传输目标，所以无需建立连接
            pass
        else:
            # 其它情况，也可以认为是没有传输目标，所以无需建立连接
            pass

    def Es_Connect(self,Times:int):
        """ 
        根据配置连接上 Es 服务器（无密码版本）
        最终返回建立好的连接或 none 值
        @param Times: 尝试连接次数上限
        @return: es 连接
        """ 
        es_connect = None
        try:
            if (self.ES_IP != '' and self.ES_PORT != ''):
                address = f"http://{self.ES_IP}:{self.ES_PORT}"
                es_connect = Elasticsearch([address])
            else:
                logging.error('You ip or port is None!!!!')
        except ConnectionError as e:
            logging.error(f'Received an error {e} \n Error raised by the HTTP connection!!! we will try againe.')
            es_connect = self.Es_Rebuilt(Times,address)
        except ConnectionTimeout as e:
            logging.error(f'Received an error {e} \n Connection timed out during an operation!!! we will try againe.')
            es_connect = self.Es_Rebuilt(Times,address)
        except Exception as e:
            logging.error(f"Received an error {e} exit!!")
        return es_connect
        
    def Es_Rebuilt(self,Times:int,Address:str):
        """ 
        尝试重新建立 ES 连接
        返回最终建立好的连接或 none 值
        @param Times: 重试次数上限
        @param Adress: es 地址
        @return: es 连接
        """  
        es_connect = None
        for i in range(0,Times):
            logging.info(f'Attempting times:{i+1}......')
            try:
                es_connect = Elasticsearch([Address])
                if es_connect :
                    break
                else:
                    logging.error(f'The {i+1}th times Connection is Wrong!!!')
            except Exception as e:
                logging.error(f'The {i+1}th times Connection is Wrong!!!')
        if es_connect == None:
            logging.error('ES connection rebuilt is wrong!!!!!!!!!')
        return es_connect

    def Custom_Connect(self):
        """ 
        用户自定的传输目标，需要用户自己去实现
        """
        pass

    def UPDATA_BACK(self):
        """ 
        TODO:根据本地的备份文件，重新上传到 es 中，进行数据的恢复
        """ 
        pass

    async def Bot_Interval(self):
        """ 
        根据当前的配置建立会话，并保存 session ，方便下一次登录，并调用监控函数开始监控
        """ 
        logging.info(f'Logging in with account # {self.ACCOUNT["account_phone"]} ... \n')

        # 用来存储会话文件的地址，方便下一次的会话连接
        session_file = self.ACCOUNT['account_phone']

        # 实例化一个 tg 端对象，初次登录会记录指定路径中，后续登录会直接使用以前的账户信息
        self.CLIENT = TelegramClient(session_file, self.ACCOUNT['account_api_id'], self.ACCOUNT['account_api_hash'])

        # 异步的启动这个实例化的 tg 客户端对象，其中手机号为配置文件中的手机号
        await self.CLIENT.start(phone=f'{self.ACCOUNT["account_phone"]}')

        # 检查当前用户是否已经授权使用 API
        if not await self.CLIENT.is_user_authorized():
            logging.info(f'Client is currently not logged in, please sign in! Sending request code to {self.ACCOUNT["account_phone"]}, please confirm on your mobile device')
            
            # 当发现没有授权时，向手机号发送验证码
            await self.CLIENT.send_code_request(self.ACCOUNT['account_phone'])
            self.TG_USER = await self.CLIENT.sign_in(self.ACCOUNT['account_phone'], input('Enter code: '))

        # 完成初始化监控频道
        await self.Init_Monitor_Channel()

        # 循环定时进行信息更新
        count = 0
        while True:
            count +=1
            logging.info(f'############## {count} Running bot interval ##############')
            await asyncio.sleep(self.CHANNEL_REFRESH_WAIT)
            # 检查并更新 channel
            await self.Channel_Flush()

    async def Channel_Flush(self):
        """ 
        定期(15min)检查是否存在更新的 channel 信息，同时对于其中的信息进行更新
        """ 
        count = 0
        update = False
        async for dialog in self.CLIENT.iter_dialogs():
            # 会话不能是用户间的对话
            if not dialog.is_user:
                channel_id = dialog.id 
                # 线程安全的加入会话信息
                with self.LOCK_CHANNEL_ADD:
                    for i in self.CHANNEL_META:
                        if i['channel id'] == channel_id :
                            channel_id = None
                            break
                    # 若当前会话并不在列表中
                    if channel_id != None:
                        # 获取会话的完整信息
                        try:
                            if dialog.is_channel:
                                channel_full = await self.CLIENT(GetFullChannelRequest(dialog.input_entity))
                            elif dialog.is_group:
                                channel_full = self.CLIENT(GetFullChatRequest(chat_id=dialog.id))
                        except ChannelPrivateError as e:
                            logging.error(f'The channel is private,we can\'t get channel full info: {dialog.id}')
                        except Exception as e:
                            logging.error(f"Fail to get the full entity:{dialog.id}")

                        # 将 channel 加入现在可监控的 channel 列表
                        about = None
                        try :
                            about = channel_full.full_chat.about
                        except AttributeError as e:
                            logging.error(f'Chat has no attribute full_chat')
                        except Exception as e:
                            logging.error(f"Get an error: {e}")
                        self.CHANNEL_META.append({
                            'channel id':dialog.id,
                            'channel name':dialog.name,
                            'channel about': about,
                            })

                    # 从添加列表中除移
                    id_name = self.Strip_Pre(dialog.id)
                    if not id_name in self.DIALOG_MESSAGE_BOT:
                        self.DIALOG_MESSAGE_BOT[id_name] = queue.Queue(10)
                    if id_name in self.CHANNEL_ADD:
                        self.CHANNEL_ADD.discard(id_name)

                e = await self.Get_Channel_Info_By_Dialog(dialog)
                await self.Dump_Channel_Info(e)

                # 用户的更新不再进行这种批量更新
                #await self.dump_channel_user_info(dialog)
                count +=1
        
        logging.info(f'########## Channel Count:{count}')
        logging.info(f'########## {sys._getframe().f_code.co_name}: Monitoring channel: {json.dumps(self.CHANNEL_META,ensure_ascii=False,indent=4)}')

    def Strip_Pre(self,Text:str|int)->str:
        """ 
        去除 channel id 的前缀
        @param text: 需要去除前缀的 channel id
        @return: 去除后的 channel id
        """ 
        text_str = str(Text)
        if text_str.startswith("-100"):
            text_str = text_str[4:]
        elif text_str.startswith("-"):
            text_str = text_str[1:]
        elif text_str.startswith("100"):
            text_str = text_str[3:]
        return text_str
    
    async def Init_Monitor_Channel(self):
        """ 
        初始化，并开始执行监听
        """ 
        logging.info('Init the monitor to channels')

        # 检测本地图片路径是否存在
        userpicture_path = r'./picture/user'
        channelpicture_path = r'./picture/channel'
        if not os.path.exists(userpicture_path):
            os.makedirs(userpicture_path)
            logging.info(f'Create the picture dir:{userpicture_path}')
        if not os.path.exists(channelpicture_path):
            os.makedirs(channelpicture_path)
            logging.info(f'Create the picture dir:{channelpicture_path}')
        photo_path = r'./picture/dialog'
        if not os.path.exists(photo_path):
            os.makedirs(photo_path)
            logging.info(f'Create the picture dir:{photo_path}')

        # 检测是否需要本地存储
        if self.DUMP_MODEL == '1':
            local_path = './local_store'
            if not os.path.exists(local_path):   
                message_path = './local_store/message'
                channel_path = './local_store/channel_info'
                user_path = './local_store/user_info'
                os.makedirs(message_path)
                os.makedirs(channel_path)
                os.makedirs(user_path)
                logging.info(f'Create the message dir:{message_path}')
                logging.info(f'Create the channel info dir:{channel_path}')
                logging.info(f'Create the user info dir:{user_path}')

        if self.SKIP_FIRST == '0':
            # 第一次加载 channel 信息
            await self.Channel_Load()
        else:
            await self.Channel_Load_Skip()

        # 处理接收到的 msg
        @self.CLIENT.on(events.NewMessage)
        async def Message_Event_Handler(event):
            # 只处理来自频道和群组的 msg
            message = event.raw_text
            if isinstance(event.message.to_id, PeerChannel):
                logging.info(f'############################################################## \n Get the channel message is ({message}) \n ##############################################################')
            elif isinstance(event.message.to_id, PeerChat):
                logging.info(f'############################################################## \n Get the chat message is ({message}) \n ##############################################################')
            else:
                # 两者均不是，跳过
                return
            # 通过协程存储当前的新 msg
            await self.Msg_Handler(event)

        # 处理群组出现的事件
        @self.CLIENT.on(events.ChatAction)
        async def Chat_Event_Handle(event):
            # 等待测试中
            #await self.Chat_Updata(event)
            pass

        # 处理用户出现更新的情况
        @self.CLIENT.on(events.UserUpdate)
        async def User_Event_Handle(event):
            # 等待测试中
            #await self.User_Updata(event)
            pass

        # 每隔 10s 上传一次 message
        while True:
            await self.Transfer_Msg()
            await asyncio.sleep(self.MSG_TRANSFER_GAP)

    async def Channel_Load(self):
        """ 
        在第一次启动程序时将遍历当前账户中的 channel 的所有信息，并传输到存储服务器上
        """ 
        count = 0                                           # 群组总数
        async for dialog in self.CLIENT.iter_dialogs():
            if not dialog.is_user:
                e = await self.Get_Channel_Info_By_Dialog(dialog)
                self.CHANNEL_META.append({
                    'channel id':dialog.id,
                    'channel name':dialog.name,
                    'channel about':e['channel_about'],
                    })

                # 添加对应频道消息过滤缓存队列
                id_name = self.Strip_Pre(dialog.id)
                if not id_name in self.DIALOG_MESSAGE_BOT:
                    self.DIALOG_MESSAGE_BOT[id_name] = queue.Queue(10)

                # 上传群组信息
                await self.Dump_Channel_Info(e)
                await self.Dump_Channel_User_Info(dialog,e)

        logging.info(f'{sys._getframe().f_code.co_name}:Channel Count:{count} ;Monitoring channel: {json.dumps(self.CHANNEL_META ,ensure_ascii=False,indent=4)}')

    async def Channel_Load_Skip(self):
        """ 
        在第一次启动程序时将遍历当前账户中的 channel 的所有信息（跳过对成员 user 信息的遍历），并传输到存储服务器上
        """ 
        count = 0                                           # 群组总数
        async for dialog in self.CLIENT.iter_dialogs():
            if not dialog.is_user:
                e = await self.Get_Channel_Info_By_Dialog(dialog)
                self.CHANNEL_META.append({
                    'channel id':dialog.id,
                    'channel name':dialog.name,
                    'channel about':e['channel_about'],
                    })

                # 添加对应频道消息过滤缓存队列
                id_name = self.Strip_Pre(dialog.id)
                if not id_name in self.DIALOG_MESSAGE_BOT:
                    self.DIALOG_MESSAGE_BOT[id_name] = queue.Queue(10)

                # 上传群组信息
                await self.Dump_Channel_Info(e)

        logging.info(f'{sys._getframe().f_code.co_name}:Channel Count:{count} ;Monitoring channel: {json.dumps(self.CHANNEL_META ,ensure_ascii=False,indent=4)}')

    async def Msg_Handler(self,Event:events):
        """ 
        对获得的 msg 进行处理
        """ 
        # 多媒体文件处理
        media = await self.Msg_Media(Event)

        # 分析 msg 的信息
        result = await self.Msg_Analysis(Event)

        # 提取 msg 的属性
        msg_info = await self.Get_Msg_Info(Event,media,result)

        # 过滤，检测当前的 msg 是否需要存储
        if self.Filter_Msg(msg_info):
            return 

        # 存储 msg 属性
        await self.Dump_Msg(msg_info)

    async def Msg_Analysis(self,Event:events)->dict:
        """ 
        对 msg 的内容进行分析，返回对 msg 内容的打标与信息提取
        目前若为多媒体文件或内容为空则返回 None
        @param Event: msg 事件
        @return: 标签内容
        """ 
        msg_content = Event.raw_text
        if msg_content is None or msg_content == "":
            return None

        tags = self.Tag_Msg(msg_content)

        vir_identity = self.Extract_Vir_Identity(msg_content)

        result = {
            'tag':tags,
            'virtual_identity':vir_identity,
        }
        return result

    def Tag_Msg(self,Text:str):
        """ 
        对 msg 内容进行打标，目前从：地区、语言类型方向打标
        如果打标结果为空则返回 None
        @param Text: msg 内容
        @return: 打标结果
        """ 
        tag = {
            'region':None,
            'language':None,
        }
        regiontag = self.Tag_Region(Text)
        language = self.Tag_Language(Text)
        if regiontag == None and language == None:
            return None
        tag['language'] = language
        tag['region'] = regiontag
        return tag

    def Tag_Region(self,Text:str):
        """ 
        对 msg 内容中涉及地区进行标记
        @param Text: 消息内容
        @return: 地区打标结果
        """ 
        result = []

        # 识别英文地区
        try:
            places = GeoText(Text)
            eng = places.cities
            if eng is not []:
                result.extend(eng)
        except Exception as e:
            logging.error(f"Detecting the text eng error:{e}.")

        # 识别中文谈到的地区
        try:
            chinese = jio.recognize_location(Text)
            if chinese['domestic'] is not None:
                for i in range(len(chinese['domestic'])):
                    if chinese['domestic'][i][0]['county'] is not None:
                        domestic = chinese['domestic'][i][0]['county']
                    elif chinese['domestic'][i][0]['city'] is not None:
                        domestic = chinese['domestic'][i][0]['city']
                    elif chinese['domestic'][i][0]['province'] is not None:
                        domestic = chinese['domestic'][i][0]['province']
                    result.append(domestic)
            if chinese['foreign'] is not None:
                for i in range(len(chinese['foreign'])):
                    if chinese['foreign'][i][0]['country'] == '中国':
                        continue
                    if chinese['foreign'][i][0]['city'] is not None:
                        foreign = chinese['foreign'][i][0]['city']
                    elif chinese['foreign'][i][0]['country'] is not None:
                        foreign = chinese['foreign'][i][0]['country']
                    result.append(foreign)
            if chinese['others'] is not None:
                for i in chinese['others'].keys():
                    result.append(i)
        except Exception as e:
            logging.error(f"Detecting the text zh-cn error:{e}.")

        if result == []:
            return None
        return result

    def Tag_Language(self,Text:str):
        """ 
        识别 msg 内容语言的类型
        @param Text: 消息内容
        @return: 识别语言类型结果
        """ 
        result = None
        try:
            result = detect_langs(Text)[0].lang
        except Exception as e:
            result = None
            logging.error(f'Fail to detect the language, reason is : {e}')
        return result

    def Extract_Vir_Identity(self,Text:str):
        """ 
        提取 msg 内容中涉及到的虚拟身份信息（手机号、身份证号、网址、qq号、微信号）
        如果没有任何虚拟身份提取到，返回 None
        @param Text: msg 内容
        @return: 提取结果
        """ 
        vir_data = {}

        email_accounts = self.Extract_Email(Text)
        phone_accounts = self.Extract_phone(Text)
        qq_accounts = self.Extract_QQ(Text)
        wechat_accounts = self.Extract_Wechat(Text)
        ids = self.Extract_Ids(Text)
        url_address = self.Extract_Url(Text)

        if email_accounts is not None:
            vir_data['e-mail'] = email_accounts
        if qq_accounts is not None:
            vir_data['qq'] = qq_accounts
        if phone_accounts is not None:
            vir_data['phone'] = phone_accounts
        if ids is not None:
            vir_data['id_card'] = ids
        if wechat_accounts is not None:
            vir_data['wechat'] = wechat_accounts
        if url_address is not None:
            vir_data['url'] = url_address
        
        if vir_data == {}:
            return None 
        return vir_data

    def Extract_Email(self,Text:str):
        """ 
        email 提取
        @param Text: 待提取的消息
        @return: 可能的邮箱
        """ 
        
        if Text=='':
            return None
        eng_texts = self.Replace_Chinese(Text)
        eng_texts = eng_texts.replace(' at ','@').replace(' dot ','.')
        sep = ',!?:; ，。！？《》、|\\/'
        eng_split_texts = [''.join(g) for k, g in groupby(eng_texts, sep.__contains__) if not k]

        email_pattern = r'^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\.[a-zA-Z_-]+)+$'

        emails = []
        for eng_text in eng_split_texts:
            result = re.match(email_pattern, eng_text, flags=0)
            if result:
                emails.append(result.str)
        if emails == []:
            return None
        return emails

    def Replace_Chinese(self,Text:str):
        """ 
        去除中文，替换位空格(默认非空字符串)
        @param Text: 待处理字符串
        @return: 处理后字符串
        """ 
        filtrate = re.compile(u'[\u4E00-\u9FA5]')
        text_without_chinese = filtrate.sub(r' ', Text)
        return text_without_chinese

    def Extract_phone(self,Text:str):
        """ 
        手机号提取
        @param Text: 待提取的消息
        @return: 可能的手机号
        """ 
        if Text=='':
            return None
        eng_texts = self.Replace_Chinese(Text)
        sep = ',!?:; ：，.。！？《》、|\\/abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
        eng_split_texts = [''.join(g) for k, g in groupby(eng_texts, sep.__contains__) if not k]
        eng_split_texts_clean = [ele for ele in eng_split_texts if len(ele)>=7 and len(ele)<17]
    
        phone_pattern = r'^((\+86)?([- ])?)?(|(13[0-9])|(14[0-9])|(15[0-9])|(17[0-9])|(18[0-9])|(19[0-9]))([- ])?\d{3}([- ])?\d{4}([- ])?\d{4}$'

        phones = []
        for eng_text in eng_split_texts_clean:
            result = re.match(phone_pattern, eng_text, flags=0)
            if result:
                phones.append(result.string.replace('+86','').replace('-',''))
        virtual = []
        union_set = set()
        for i in phones:
            if i not in union_set:
                union_set.updata(i)
                virtual.append(i)
        if virtual == []:
            return None
        return virtual

    def Extract_QQ(self,Text:str):
        """ 
        qq 提取
        @param Text: 待提取的消息
        @return: 可能的qq号 
        """ 
        if Text=='':
            return None
        eng_texts = self.Replace_Chinese(Text)
        sep = '@,!?:; ：，.。！？《》、|\\/abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
        eng_split_texts = [''.join(g) for k, g in groupby(eng_texts, sep.__contains__) if not k]
        eng_split_texts_clean = [ele for ele in eng_split_texts if len(ele)>=5 and len(ele)<13]

        qq_pattern = r"[1-9][0-9]{4,11}"

        qq_accounts = []
        for eng_text in eng_split_texts_clean:
            result = re.match(qq_pattern, eng_text, flags=0)
            if result and (result.string not in qq_accounts):
                qq_accounts.append(result.string)
        if qq_accounts == []:
            return None
        return qq_accounts

    def Extract_Wechat(self,Text:str):
        """ 
        wechat 提取
        @param Text: 待提取的消息
        @return: 可能的微信号 
        """ 
        if 'wechat' in Text or '微信' in Text or 'Wechat' in Text:
            wechat_pattern = r'\b[a-zA-Z_]\w{5,19}\b'
            wechat_accounts = re.findall(wechat_pattern, Text)
            return wechat_accounts
        else:
            return None

    def Extract_Ids(self,Text:str):
        """ 
        身份证号提取
        @param Text: 待提取的消息
        @return: 可能的身份证号 
        """ 
        if Text == '':
            return None
        eng_texts = self.Replace_Chinese(Text)
        sep = ',!?:; ：，.。！？《》、|\\/abcdefghijklmnopqrstuvwyzABCDEFGHIJKLMNOPQRSTUVWYZ'
        eng_split_texts = [''.join(g) for k, g in groupby(eng_texts, sep.__contains__) if not k]
        eng_split_texts_clean = [ele for ele in eng_split_texts if len(ele) == 18]

        id_pattern = r'(^\d{15}$)|(^\d{18}$)|(^\d{17}(\d|X|x)$)'

        ids = []
        for eng_text in eng_split_texts_clean:
            result = re.match(id_pattern, eng_text, flags=0)
            if result:
                ids.append(result.string)
        if ids == []:
            return None
        return ids

    def Extract_Url(self,Text:str):
        """ 
        url 提取
        @param Text: 待提取的消息
        @return: 可能的url  
        """ 
        extractor = URLExtract()
        url_address = extractor.find_urls(Text)
        if url_address == []:
            return None
        return url_address

    async def Msg_Media(self,Event:events):
        """ 
        检查是否是多媒体文件，并提取多媒体文件的信息
        非多媒体文件返回 None
        @param Event: msg 事件
        @return: 多媒体文件属性
        """ 
        media = None
        if Event.photo:
            logging.info(f'The message have photo')
            file_name,file_path =  await self.Download_File(Event)
            if file_path is not None:
                file_store = file_path+'/' + file_name
                file_size = os.path.getsize(file_store)
                file_md5 = self.File_Md5(file_store)
                media = {
                    'type':'.jpg',                              # 目前只存储图片
                    'store':file_store,
                    'name':file_name,
                    'md5':file_md5,
                    'size':file_size,
                    'file_id':None,                             # 这个字段主要是在软件中有用
                }
        return media

    async def Download_File(self,Event:events):
        """ 
        将图片存储到指定路径
        @param Event: 消息事件
        @return: 文件名、文件存储路径
        """ 
        file_name = self.GetImageName(Event)
        file_path = self.GetImagePath(Event)
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        download_path = file_path+'/' + file_name
        try:
            await Event.download_media(download_path)
            logging.info(f'picture down OK')
            return (file_name,file_path)
        except Exception as e:
            logging.error(f"Download event file get an error:{e}.")
            return (None,None)

    def GetImageName(self,Event:events):
        """ 
        获得图片文件名，用于存储
        @param Event: msg 事件
        @return: 图片名称
        """ 
        image_name = str(Event.message.id)+'_'+str(Event.message.grouped_id)+'.jpg'
        return image_name

    def GetImagePath(self,Event:events):
        """ 
        获得图片的存储路径
        @param Event: 新消息事件
        @return: 图片将要存储的文件夹路径
        """
        file_path = './picture/dialog'
        if isinstance(Event.message.to_id, PeerChannel):
            channel_id = Event.message.to_id.channel_id
        elif isinstance(Event.message.to_id, PeerChat):
            channel_id = Event.message.to_id.chat_id
        else:
            channel_id = 'None'
        now = datetime.now()
        file_path = file_path+ '/'+str(channel_id)+'/'+now.strftime("%y_%m_%d")
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        return file_path

    def File_Md5(self,File_Path:str)->str:
        """ 
        计算文件的 md5 值
        @param File_Path: 文件完整路径
        @return: 文件的md5值
        """ 
        m = hashlib.md5()
        with open(File_Path,'rb') as file:
            while True:
                data = file.read(4096)
                if not data:
                    break
                m.update(data)
        return m.hexdigest()

    async def Get_Msg_Info(self,Event:events,Media:dict,Ana_Result:dict)->dict:
        """ 
        从 msg 事件中提取 msg 的属性
        @param Event: msg 事件
        @param Media: 消息中的多媒体文件属性
        @param Ana_Result: 对消息内容的分析结果
        @return: msg 的属性
        """ 

        message_obj = Event.message
        if isinstance(message_obj.to_id, PeerChannel):
            channel_id = Event.message.to_id.channel_id
            is_channel = True
            is_group = False
        elif isinstance(message_obj.to_id, PeerChat):
            channel_id = Event.message.to_id.chat_id
            is_channel = False
            is_group = True

        is_bot = False if message_obj.via_bot_id is None else True
        is_scheduled = True if message_obj.from_scheduled is True else False
        msg_content = Event.raw_text

        # 提到的用户检测
        mentioned_users = []
        mentioned_break = False
        for ent, txt in Event.get_entities_text():
            if isinstance(ent ,types.MessageEntityMention):
                for i in mentioned_users:
                    if i == txt :
                        mentioned_break = True
                        break
                if mentioned_break:
                    mentioned_break = False
                    continue
                else:
                    logging.info(f'get one mention member {txt}')
                    mentioned_users.append(txt)
        if mentioned_users == []:
            mentioned_users = None

        # 本消息发送者检测
        sender_name = message_obj.post_author
        if sender_name == None:
            sender = await Event.get_sender()
            sender_name = utils.get_display_name(sender)
            if sender_name == '':
                sender_name = None
        # 发送者 id
        chat_user_id  =  Event.sender_id   
        if chat_user_id == None:
            # 理论上这个不应该有的
                chat_user_id = 000000000
                if sender_name == None:
                    sender_name = '匿名用户'

        # 如果是转发消息
        fwd_msg = None
        if message_obj.fwd_from is not None:
            # 基本方法获取
            fwd_msg = {
                # 发送时间
                'fwd_message_date': message_obj.fwd_from.date, 
                # 原消息发送者名称
                'fwd_message_post_author':message_obj.fwd_from.post_author,
            }
            # id 与名称
            fwd_sender_name = None
            fwd_sender_id = None
            if message_obj.forward.sender is not None:
                fwd_sender_name = utils.get_display_name(message_obj.forward.sender)
                fwd_sender_id = message_obj.forward.sender.id
            if (fwd_sender_id is None) or (fwd_msg['fwd_message_post_author'] is None):
                if isinstance(message_obj.fwd_from.from_id, PeerUser):
                    fwd_sender_id = message_obj.fwd_from.from_id.user_id
                    if fwd_sender_name == None:
                        try:
                            fwd_user = await self.CLIENT.get_entity(PeerUser(fwd_sender_id))
                            name = ''
                            if fwd_user.first_name is not None:
                                name += fwd_user.first_name
                            if fwd_user.last_name is not None:
                                name += fwd_user.last_name
                            fwd_sender_name = name if name != '' else None
                        except:
                            logging.error(f"Message not found fwd user: {fwd_sender_id}")
                elif isinstance(message_obj.fwd_from.from_id, PeerChannel):
                    fwd_sender_id = message_obj.fwd_from.from_id.channel_id
                    if fwd_sender_name == None:
                        try:
                            fwd_channel = await self.CLIENT.get_entity(PeerChannel(fwd_sender_id))
                            if fwd_channel.title is not None:
                                name = fwd_channel.title
                            fwd_sender_name = name
                        except:
                            logging.error(f"Message not found fwd channel: {fwd_sender_id}")
                elif isinstance(message_obj.fwd_from.from_id, PeerChat):
                    fwd_sender_id = message_obj.fwd_from.from_id.chat_id
                    if fwd_sender_name == None:
                        try:
                            fwd_chat = await self.CLIENT.get_entity(PeerChat(fwd_sender_id))
                            if fwd_chat.title is not None:
                                name = fwd_chat.title
                            fwd_sender_name = name
                        except:
                            logging.error(f"Message not found fwd chat: {fwd_sender_id}")


            fwd_msg['fwd_message_send_id'] = fwd_sender_id
            if (fwd_msg['fwd_message_post_author'] is not None):
                fwd_msg['fwd_message_post_author'] = fwd_sender_name
            
        # 如果为回复消息
        reply_msg = None
        if not message_obj.reply_to is None:
            reply_obj = await Event.get_reply_message()
            if reply_obj != None:
                # 基本属性获取
                reply_msg = {
                    'reply_message_msg_txt':reply_obj.message,
                    'reply_message_msg_id':message_obj.reply_to.reply_to_msg_id,
                    'reply_message_date':reply_obj.date,
                    'reply_message_post_author':reply_obj.post_author,

                    'reply_message_scheduled':message_obj.reply_to.reply_to_scheduled,
                    'reply_message_to_top_id':message_obj.reply_to.reply_to_top_id,
                    'reply_message_forum_topic':message_obj.reply_to.forum_topic
                }
                reply_sender_name = reply_obj.post_author
                reply_sender_id = reply_obj.sender_id
                if reply_sender_name == None:
                    reply_sender_name = utils.get_display_name(reply_obj.sender)
                    if reply_sender_name == '':
                        reply_sender_name = None
                if (reply_sender_id == None and reply_obj.from_id is not None) :
                    if isinstance(reply_obj.from_id, PeerUser):
                        reply_sender_id = reply_obj.from_id.user_id
                        if reply_sender_name == None:
                            try:
                                reply_user = await self.CLIENT.get_entity(PeerUser(reply_sender_id))
                                name = ''
                                if reply_user.first_name is not None:
                                    name += reply_user.first_name
                                if reply_user.last_name is not None:
                                    name += reply_user.last_name
                                reply_sender_name = name if name != '' else None
                            except:
                                logging.error(f"Message not found reply user: {reply_sender_id}")
                    elif isinstance(reply_obj.from_id, PeerChannel):
                        reply_sender_id = reply_obj.from_id.channel_id
                        if reply_sender_name == None:
                            try:
                                reply_channel = self.CLIENT.get_entity(PeerChannel(reply_sender_id))
                                if reply_channel.title is not None:
                                    name = reply_channel.title
                                reply_sender_name = name
                            except:
                                logging.error(f"Message not found reply channel: {reply_sender_id}")
                    elif isinstance(reply_obj.from_id, PeerChat):
                        reply_sender_id = reply_obj.from_id.chat_id
                        if reply_sender_name == None:
                            try:
                                reply_chat = self.CLIENT.get_entity(PeerChat(reply_sender_id))
                                if reply_chat.title is not None:
                                    name = reply_chat.title
                                reply_sender_name = name
                            except:
                                logging.error(f"Message not found reply chat: {reply_sender_id}")
                if (reply_msg['reply_message_post_author'] is None):
                    reply_msg['reply_message_post_author'] = reply_sender_name
                reply_msg['reply_message_send_id'] = reply_sender_id
            
        # 获得消息的 channel 信息
        groupname = None
        groupabout = None
        strip_channel_id = self.Strip_Pre(channel_id)
        # 检查当前消息所在会话是否在列表中
        for i in self.CHANNEL_META:
            test_channel_id = self.Strip_Pre(i['channel id'])
            if  test_channel_id == strip_channel_id:
                groupname = i['channel name']
                groupabout = i['channel about']
                break
        # 若遍历完仍旧没有找到
        if groupname == None:
            logging.info (f'New channel :########{strip_channel_id}####################')
            with self.LOCK_CHANNEL_ADD:
                if strip_channel_id not in self.CHANNEL_ADD:
                    self.CHANNEL_ADD.add(strip_channel_id)
                    logging.info(f'The channel is adding:{[i for i in self.CHANNEL_ADD]}')
            await self.Channel_Flush()
            for i in self.CHANNEL_META:
                test_channel_id = self.Strip_Pre(i['channel id'])
                if  test_channel_id == strip_channel_id:
                    groupname = i['channel name']
                    groupabout = i['channel about']
                    break

        msg_info = {
            'message_id':Event.message.id,   
            'message_text':msg_content,   
            'user_name':sender_name,
            'user_id':chat_user_id,
            'group_name':groupname,
            'group_about':groupabout,
            'group_id':channel_id,
            'msg_date':message_obj.date,
            'fwd_message':fwd_msg,
            'reply_message':reply_msg,
            'mentioned_user': mentioned_users,
            'is_scheduled':is_scheduled,
            'is_bot':is_bot,
            'is_group':is_group,
            'is_channel':is_channel,
            'media':Media,
            'Ana_tag':Ana_Result,
            'account':self.ACCOUNT['account_id']
        }
        return msg_info

    def Filter_Msg(self,Msg_Info:dict)->bool:
        """ 
        过滤无效的 msg 消息，如：bot 发送的广告（短时间内大量相同内容）
        @return: 是否为无效 msg
        """ 
        # 去除非机器人消息
        if not Msg_Info['is_bot']:
            return False
        channel_id = self.Strip_Pre(Msg_Info['group_id'])
        if not channel_id in self.DIALOG_MESSAGE_BOT:
            logging.error(f"Not found dialog({Msg_Info['group_name']}) in message cache")
            return False
        with self.LOCK_FILTER_MSG:
            message_queue = self.DIALOG_MESSAGE_BOT[channel_id]
            if Msg_Info['message_text'] != '':
                filter_text = Msg_Info['message_text']
            elif Msg_Info['media'] != None:
                filter_text = Msg_Info['media']['md5']
            else:
                return False
            flag = False
            empty = message_queue.empty()
            if not empty:
                queue_len = message_queue.qsize()
                for i in range(queue_len):
                    item = message_queue.get()
                    if item == filter_text:
                        flag = True
                    else:
                        message_queue.put(item)
                if flag:
                    try:
                        message_queue.put(filter_text)
                    except Exception as e:
                        logging.error(f"Get an error({e})")
                else:
                    if message_queue.full():
                        item = message_queue.get()
                        message_queue.put(filter_text)
                    else:
                        message_queue.put(filter_text)
            else:
                message_queue.put(filter_text)
        return flag

    async def Dump_Msg(self,Msg_Info:dict):
        """ 
        将提取到的 msg 属性进行格式转换、存储准备和根据配置是否本地存储
        @param Msg_Info: 提取到的完整的 msg 的属性
        """ 
        format_msg = self.Format_Msg(Msg_Info)

        if self.DUMP_MODEL == '1':
            self.Store_Msg_In_Json_File(format_msg)

        self.Update_Msg_List(format_msg)
        pass

    def Format_Msg(self,Full_Msg:dict):
        """ 
        将获得的完整 msg 属性转换为所需要的格式
        @param Full_Msg: 完整的 msg 属性
        @return: 规范格式的 msg 属性
        """ 
        to_id = self.Strip_Pre(Full_Msg['group_id'])
        message_date = int(datetime.timestamp(Full_Msg['msg_date']))*1000
        format_fwd =  None
        if Full_Msg['fwd_message'] is not None:
            fwd_date = int(datetime.timestamp(Full_Msg['fwd_message']['fwd_message_date']))*1000
            format_fwd = {
                'data':fwd_date,
                'from_id':Full_Msg['fwd_message']['fwd_message_send_id'],
                'channel_id':Full_Msg['fwd_message']['fwd_message_send_id'],
            }
        format_media = None
        if Full_Msg['media'] is not None:
            format_media = {
                'type':Full_Msg['media']['type'],
                'store':Full_Msg['media']['store'],
                'name':Full_Msg['media']['name'],
                'md5':Full_Msg['media']['md5'],
                'size':Full_Msg['media']['size'],
                'file_id':Full_Msg['media']['file_id'],
            }
        format_msg = {
            'id':Full_Msg['message_id'],
            'to_id':to_id,
            'to_id_type':None,
            'date':message_date,
            'message':Full_Msg['message_text'],
            'from_id':Full_Msg['user_id'],
            'fwd_from':format_fwd,
            'reply_to_msg_id':Full_Msg['reply_message']['reply_message_msg_id'] if Full_Msg['reply_message'] else None,
            'media':format_media,
            'userName':Full_Msg['user_name'],
            'groupName':Full_Msg['group_name'],
            'groupAbout':Full_Msg['group_about'],
        }
        return format_msg

    def Store_Msg_In_Json_File(self,Format_Msg:dict):
        """ 
        将获得的 msg 属性保存到本地（以 json 格式）
        @param Format_Msg: msg 属性
        """ 
        now = datetime.now()
        file_date =  now.strftime("%y_%m_%d")
        json_file_name = './local_store/message/'+file_date+'_messages.json'

        self.Store_Data_Json(json_file_name, self.LOCK_MSG, 'messages', Format_Msg)

    def Store_Data_Json(self,File_Name:str,Lock:threading.Lock,Data_Type:str,Data:dict):
        """ 
        打开 json 文件，并将数据存入
        @param File_Name: 存储文件的完整路径
        @param Lock: 异步锁
        @param Data_Type: 数据的类型
        @param Data: 数据内容
        """ 
        with Lock:
            if not os.path.exists(File_Name):
                with open(File_Name,'w') as f:
                    init_json ={}
                    json_first = json.dumps(init_json)
                    f.write(json_first)
            with open(File_Name, 'r') as f:
                file_data = json.load(f)
            try:
                file_data[Data_Type].append(Data)
            except KeyError as e:
                file_data[Data_Type] = []
                file_data[Data_Type].append(Data)
            json_data = json.dumps(file_data,ensure_ascii=False,indent=4)
            with open(File_Name,'w') as f:
                f.write(json_data)

    def Update_Msg_List(self,Format_Msg:dict):
        """ 
        将获得的 msg 属性添加到待上传的 msg 列表中
        @param Format_Msg: msg 属性
        """ 
        with self.LOCK_UPDATA_MSG:
            self.UPDATA_MESSAGE.append(Format_Msg)

    async def Transfer_Msg(self):
        """ 
        根据配置选项，定期对监听到的 msg 进行传输
        """ 
        if self.UPDATA_MODEL == '1':
            # ES 为传输目标
            self.Msg_Es_Transfer()
        elif self.UPDATA_MODEL == '2':
            # 自定义的传输目标
            self.Msg_Custom_Transfer()
        elif self.UPDATA_MODEL == '0':
            # 没有传输目标
            self.Msg_None_Transfer()
        else:
            # 其它情况，也可以认为是没有传输目标
            self.Msg_None_Transfer()

    def Msg_Es_Transfer(self):
        """
        msg 的属性需要传输给 es 服务器上
        """ 

        es = self.ES_CONNECT
        with self.LOCK_UPDATA_MSG:
            messages_info = copy.deepcopy(self.UPDATA_MESSAGE)
            self.UPDATA_MESSAGE = []

        # 批量上传
        if messages_info == []: 
            return

        # 检查 index
        es_index = self.ES_MESSAGE_INDEX
        if es_index == None :
            logging.error('Es msg index is None!!!!! Fix it !!')
            sys.exit(0)
        if not es.indices.exists(index=es_index):
            result = es.indices.create(index=es_index)
            logging.info ('creat index new_message_info')
        actions = (
            {
                '_index': es_index,
                '_type': '_doc',
                '_id': str(Message['to_id'])+'_'+str(Message['id']),
                '_source': Message,
            }
            for Message in messages_info
        )
        n,_ = bulk(es, actions)
        logging.info(f'es data: total:{len(messages_info)} message, insert {n} message successful')

    def Msg_Custom_Transfer(self):
        """
        需要用户自定义传输方法，默认调用无目标的函数
        """ 
        self.Msg_None_Transfer()

    def Msg_None_Transfer(self):
        """
        没有传输目标，所以需要清空
        """ 
        with self.LOCK_UPDATA_MSG:
            self.UPDATA_MESSAGE = []

    async def Get_Channel_Info_By_Dialog(self,Dialog:Dialog):
        """ 
        从会话中获得 channel 的信息
        @param Dialog: 会话
        @return: 获取到的频道属性
        """ 
        channel_id = Dialog.id 
        channel_title = Dialog.title 
        channel_date = Dialog.date 
        is_group = Dialog.is_group
        is_channel = Dialog.is_channel
        is_megagroup = True if Dialog.is_group and Dialog.is_channel else False

        # channel 成员数量
        participants_count = self.Channel_Participants_Count(Dialog)

        # 需要额外获取的属性
        invite_link = None
        is_restricted = False
        username = None

        full_chat = None

        about = None
        location = None
        link_chat_id = None
        
        admins_count = None
        is_enable = None
        
        # channel 的属性
        if is_channel:
            if hasattr(Dialog.entity,'restricted'):
                is_restricted = getattr(Dialog.entity,'restricted')
            if hasattr(Dialog.entity,'username'):
                if Dialog.entity.username is not None:
                    username = Dialog.entity.username
                    invite_link = f"https://t.me/{username}"

        # 获取 full_chat 的频道属性
        channel_full = None
        try:
            if Dialog.is_channel:
                channel_full = await self.CLIENT(GetFullChannelRequest(Dialog.input_entity))
                logging.info(f'full channel:{Dialog.title}')
            elif Dialog.is_group:
                channel_full = await self.CLIENT(GetFullChatRequest(chat_id=Dialog.id))
                logging.info(f'full chat:{Dialog.title}')
            if hasattr(channel_full,'full_chat'):
                full_chat = channel_full.full_chat
            else:
                logging.error(f'chat ({Dialog.title}) has no attribute full_chat')
        except ChannelPrivateError as e:
            logging.error(f'the channel is private,we can\'t get channel full info: {Dialog.id}')
        except Exception as e:
            logging.error(f"get a error :{e}")


        if full_chat is not None:
            about = full_chat.about
            if is_channel:
                link_chat_id = full_chat.linked_chat_id
                admins_count = full_chat.admins_count
                if full_chat.location != None :
                    location = full_chat.location.address

        if is_group:
            if hasattr(Dialog.entity,'deactivated'):
                is_enable = getattr(Dialog.entity,'deactivated')


        if admins_count == None:
            admins_count = self.Admin_Count(Dialog)

        # 频道图片获取
        real_photo_path = None
        photo_path = r'./picture/channel/'+str(Dialog.id)+'.jpg'
        if os.path.exists(photo_path):
            real_photo_path = photo_path
        else:
            try:
                real_photo_path = await self.CLIENT.download_profile_photo(Dialog.input_entity,file=photo_path)
            except Exception as e:
                logging.error(f"Get an error: {e}")
        
        # channel 其它属性
        last_msg_date = Dialog.date
        now = datetime.now()
        update_time = now

        channel_data = {
            'channel_id':channel_id,            # -100类型
            'channel_title':channel_title,
            'channel_date':channel_date,        # 原本想设置为频道创建时间，但没有办法做到，后续看看如何做到
            'invite_link':invite_link,
            'account_id':self.ACCOUNT['account_id'],
            'last_msg_date':last_msg_date,      # 最新消息发送时间
            'channel_about':about,
            'is_megagroup':is_megagroup,
            'is_group':is_group,
            'is_channel':is_channel,
            'participants_count':participants_count,
            'channel_photo':real_photo_path,
            'username':username,
            'is_enable':is_enable,
            'location':location,
            'is_restricted':is_restricted,
            'admins_count':admins_count,
            'update_time':update_time           # 更新时间
            }
        
        return channel_data

    def Admin_Count(self,Dialog:Dialog):
        """ 
        TODO:通过查询存储在服务器上管理员成员来获得数量
        @param Dialog: 需要统计管理员人数的会话
        @return: 统计得到的管理员人数
        """ 
        admins_count = None

        return admins_count

    def Channel_Participants_Count(self,Dialog:Dialog):
        """
        获得 channel 的用户人数
        @param Dialog: 需要统计用户人数的会话
        @return: 会话的人数
        """
        size = None
        try:
            if Dialog.is_channel or Dialog.is_group:
                size = Dialog.entity.participants_count
        except Exception:
            logging.error(f"ERROR: can't counting the {Dialog.name}" )
        return size

    async def Dump_Channel_Info(self,Channel_Info:dict):
        """ 
        将 channel 信息存储下来
        @param Channel_Info: 获得的完整 Channel 信息
        """ 

        format_channel = self.Format_Channel(Channel_Info)

        if self.DUMP_MODEL == '1':
            self.Store_Channel_In_Json_File(format_channel)

        self.Transfer_Channel(format_channel)

    def Format_Channel(self,Full_Channel:dict):
        """ 
        将获得的完整 channel 属性转换为所需要的格式
        @param Full_Channel: 完整的 channel 属性
        @return: 规范格式的 channel 属性
        """ 
        format_location = {
            'address':Full_Channel['location'],
            'long':None,
            'lat':None,
        }

        channel_date = int(datetime.timestamp(Full_Channel['channel_date'])*1000) if Full_Channel['channel_date'] is not None else None
        last_msg_date = int(datetime.timestamp(Full_Channel['last_msg_date'])*1000) if Full_Channel['last_msg_date'] is not None else None
        update_time = int(datetime.timestamp(Full_Channel['update_time'])*1000) if Full_Channel['update_time'] is not None else None

        format_channel = {
            'id':Full_Channel['channel_id'],
            'title':Full_Channel['channel_title'],
            'photo':Full_Channel['channel_photo'],
            'megagroup':Full_Channel['is_megagroup'],
            'restricted':Full_Channel['is_restricted'],
            'username':Full_Channel['username'],
            'date':channel_date,
            'about':Full_Channel['channel_about'],
            'participants_count':Full_Channel['participants_count'],
            'account':Full_Channel['account_id'],
            'location':format_location,
            'last_msg_date':last_msg_date,
            'update_time':update_time,
        }

        return format_channel

    def Store_Channel_In_Json_File(self,Format_Channel:dict):
        """ 
        将获得的 channel 属性保存到本地（以 json 格式）
        @param Format_Channel: 规范的channel 属性
        """ 
        now = datetime.now()
        file_date = now.strftime("%y_%m_%d")
        json_file_name = './local_store/channel_info/'+file_date+'_channel_info.json'

        self.Store_Data_Json(json_file_name, self.LOCK_CHANNEL,str(Format_Channel['id']), Format_Channel)

    def Transfer_Channel(self,Format_Channel:dict):
        """ 
        根据配置选项，对得到的 channel 信息进行传输
        """ 
        if self.UPDATA_MODEL == '1':
            # ES 为传输目标
            self.Channel_Es_Transfer(Format_Channel)
        elif self.UPDATA_MODEL == '2':
            # 自定义的传输目标
            self.Channel_Custom_Transfer(Format_Channel)
        elif self.UPDATA_MODEL == '0':
            # 没有传输目标
            pass
        else:
            # 其它情况，也可以认为是没有传输目标
            pass

    def Channel_Es_Transfer(self,Format_Channel:dict):
        """ 
        channel 的属性需要传输给 es 服务器上
        @param Format_Channel: 规范的channel 属性
        """ 

        es = self.ES_CONNECT

        # 检查 index
        es_index = self.ES_CHANNEL_INDEX
        if es_index == None :
            logging.error('Es channel index is None!!!!! Fix it !!')
            sys.exit(0)
        if not es.indices.exists(index=es_index):
            result = es.indices.create(index=es_index)
            logging.info ('creat index channel')

        channel_id = self.Strip_Pre(Format_Channel['id'])
        es_id = channel_id

        # 将数据进行上传
        n = es.index(index=es_index,doc_type='_doc',body=Format_Channel,id = es_id)
        logging.info(f'es data:{json.dumps(n,ensure_ascii=False,indent=4)} , {n}')

    def Channel_Custom_Transfer(self,Format_Channel:dict):
        """ 
        需要用户自定义传输方法，默认采用 pass
        @param Format_Channel: 规范的channel 属性
        """ 
        pass

    async def Dump_Channel_User_Info(self,Dialog:Dialog,Channel_Info:dict):
        """ 
        将会话的所有成员的信息存储下来
        @param Dialog: 会话
        @param Channel_Info: 获得的完整 Channel 信息
        """ 
        users_info = await self.Get_Users_By_Dialog(Dialog)

        if users_info == None:
            return
        format_users = self.Format_Users(users_info,Channel_Info)
        if format_users == None:
            return
        if self.DUMP_MODEL == '1':
            self.Store_Users_In_Json_File(format_users)

        self.Transfer_Users(format_users)

    async def Get_Users_By_Dialog(self,Dialog:Dialog):
        """ 
        提取目标会话成员的完整信息
        @param Dialog: 目标会话
        @return: 所有成员的属性信息
        """ 
        users_info_list = []
        if not Dialog.is_group :
            return []
        logging.info(f'start log dialog user:{Dialog.title}:{Dialog.id}')
        async for user in self.CLIENT.iter_participants(Dialog, aggressive=True):
            user_id = user.id
            user_name = user.username
            first_name = user.first_name
            last_name = user.last_name
            is_bot = user.bot
            user_phone = user.phone
            is_restricted = user.restricted
            is_verified = user.verified
            access_hash = user.access_hash
            is_self = user.is_self
            is_contact = user.contact 
            is_mutual_contact = user.mutual_contact
            is_bot_chat_history = user.bot_chat_history
            is_bot_nochats = user.bot_nochats
            last_date = None
            if  isinstance(user.participant,ChannelParticipant):
                last_date = user.participant.date
            modified = None
            photo_path = r'./picture/user/'+str(user_id)+'.jpg'
            real_photo_path = None
            try:
                if os.path.exists(photo_path):
                    real_photo_path = photo_path
                else:
                    real_photo_path =  await self.CLIENT.download_profile_photo(user,file=photo_path)
            except Exception as e:
                logging.error(f"User photo get an error:{e}.")
            is_deleted = user.deleted
            group_id = Dialog.id
            group_name = Dialog.title
            if isinstance(user.participant,ChannelParticipantAdmin):
                super = True
            else:
                super = False
            user_about = None
            user_full = None
            if user.username is not None:
                try:
                    user_full = await self.CLIENT(GetFullUserRequest(id=user.username))
                except Exception as e:
                    logging.error("")
            if user_full is not None:
                if hasattr(user_full,'full_user'):
                    user_about = user_full.full_user.about
            now = datetime.now()
            update_time = now
            user_info={
                'user_id':user_id,
                'group_id':group_id,
                'group_name':group_name,
                'user_name' : user_name,
                'first_name' : first_name,
                'last_name': last_name,
                'is_bot': is_bot,
                'is_restricted': is_restricted,
                'is_verified':is_verified,
                'is_self':is_self,
                'is_contact':is_contact,
                'is_mutual_contact':is_mutual_contact,
                'is_bot_chat_history':is_bot_chat_history,
                'is_bot_nochats':is_bot_nochats,
                'user_phone':user_phone,
                'is_deleted':is_deleted,
                'user_photo':real_photo_path,  
                'last_date':last_date,
                'user_about':user_about,
                'super':super,
                'update':update_time,
            }
            users_info_list.append(user_info)
        logging.info(f'Logging the users account {len(users_info_list)} ... \n')
        return users_info_list

    def Format_Users(self,Users_Info:dict,Channel_Info:dict):
        """ 
        将获得的 users 完整信息转换为符合需求的格式
        @param Users_Info: 完整的 user 的信息
        @param Channel_Info: 完整的 channel 信息
        @return: 规范的 users 信息列表
        """ 
        if Users_Info == []:
            return None

        format_channel = self.Format_Channel(Channel_Info)
        
        format_users = []
        for user_info in Users_Info:
            user_date = int(datetime.timestamp(user_info['last_date'])*1000) if user_info['last_date'] is not None else None
            update_time = int(datetime.timestamp(user_info['update'])*1000) if user_info['update'] is not None else None
            user = {
                'id':user_info['user_id'],
                'is_self':user_info['is_self'],
                'contact':user_info['is_contact'],
                'mutual_contact':user_info['is_mutual_contact'],
                'deleted':user_info['is_deleted'] ,
                'bot':user_info['is_bot'] ,
                'bot_chat_history':user_info['is_bot_chat_history'] ,
                'bot_nochats':user_info['is_bot_nochats'] ,
                'verified':user_info['is_verified'] ,
                'restricted':user_info['is_restricted'] ,
                'date':user_date,
                'about':user_info['user_about'] ,
                'username' :user_info['user_name'],
                'phone':user_info['user_phone'],
                'first_name' :user_info['first_name'] ,
                'last_name':user_info['first_name'] ,
                'photo':user_info['user_photo'] ,
                'channel':format_channel,
                'super':user_info['super'] ,
                'update_time':update_time,
            }
            format_users.append(user)
        return format_users

    def Store_Users_In_Json_File(self,Format_Users:dict):
        """ 
        将规范的 users 信息进行本地保存（以 json 格式）
        @param Format_Users: 规范的 users 信息列表
        """ 
        now = datetime.now()
        file_date =  now.strftime("%y_%m_%d")
        json_file_name = './local_store/user_info/'+file_date+'_chat_user.json'
        self.Store_Data_Json(json_file_name, self.LOCK_CHAT_USER, str(Format_Users[0]['channel']['id']),Format_Users)

    def Transfer_Users(self,Format_Users:dict):
        """ 
        根据配置，进行相应的信息保存操作
        @param Format_Users: 规范的 users 信息列表
        """ 
        if self.UPDATA_MODEL == '1':
            # ES 为传输目标
            self.Users_Es_Transfer(Format_Users)
        elif self.UPDATA_MODEL == '2':
            # 自定义的传输目标
            self.Users_Custom_Transfer(Format_Users)
        elif self.UPDATA_MODEL == '0':
            # 没有传输目标
            pass
        else:
            # 其它情况，也可以认为是没有传输目标
            pass

    def Users_Es_Transfer(self,Format_Users:dict):
        """ 
        channel 的属性需要传输给 es 服务器上
        @param Format_Users: 规范的 users 列表
        """ 

        es = self.ES_CONNECT

        # 检查 index
        es_index = self.ES_USER_INDEX
        if es_index == None :
            logging.error('Es users index is None!!!!! Fix it !!')
            sys.exit(0)
        if not es.indices.exists(index=es_index):
            result = es.indices.create(index=es_index)
            logging.info ('creat index user')

        actions = (
            {
                '_index': es_index,
                '_id':str(User_info['channel']['id'])+'_'+str(User_info['id']),
                '_source':User_info,
            }
            for User_info in Format_Users
        )

        # 将数据进行上传
        n,_ = bulk(es, actions)
        logging.info(f'total:{len(Format_Users)} user, insert {n} user successful')

    def Users_Custom_Transfer(self,Format_Users:dict):
        """ 
        需要用户自定义传输方法，默认采用 pass
        @param Format_Users: 规范的 users 列表
        """ 
        pass

    # 以下均为实验性代码，能否有用，等待测试
    async def Chat_Updata(self,Event:events):
        """ 
        处理 chat 中出现的更新消息
        @param Event: 出现的事件
        """ 
        chat_index = self.ES_CHANNEL_INDEX
        chat = Event.get_chat()
        chat_id = Event.chat_id
        if Event.new_photo:
            pair = {
                'channel_id':chat_id,
                'account_id':self.account['account_id'],
            }
            data = self.es_search(index=chat_index,filed=pair)
            if data == None:
                return
            chat_data = data['_source']
            es_id = data['_id']
            now = datetime.now()
            update_time = int(datetime.timestamp(now)*1000)
            day_str = now.strftime("%y_%m_%d")
            photo_path = f"./picture/channel/{str(chat_id)}.jpg"
            if os.path.exists(photo_path):
                old_name = f"./picture/channel/{str(chat_id)}_{day_str}.jpg"
                os.rename(photo_path,old_name)
            try :
                real_photo_path = await self.CLIENT.download_profile_photo(Event.input_chat,file=photo_path)
                chat_data['channel_photo'] = real_photo_path
                chat_data['updata_time'] = update_time
                n = es.index(index=chat_index,body=chat_data,id = es_id)
                logging.info(f'es Channel photo data update:{json.dumps(n,ensure_ascii=False,indent=4)} , {n}')
            except Exception as e:
                logging.error(f" Channel photo updata get an error:{e}")
        elif Event.user_joined:
            user_index = self.ES_USER_INDEX
            pair = {
                'channel_id':chat_id,
                'account_id':self.account['account_id'],
            }
            data = self.es_search(index=chat_index,filed=pair)
            if data == None:
                return
            chat_data = data['_source']
            es_id = data['_id']
            now = datetime.now()
            update_time = int(datetime.timestamp(now)*1000)
            chat_data['updata_time'] = update_time
            chat_data['participants_count'] = chat.participants_count
            n = es.index(index=chat_index,body=chat_data,id = es_id)
            logging.info(f'es Channel user added update:{json.dumps(n,ensure_ascii=False,indent=4)} , {n}')
            user = Event.get_user()
            real_photo_path = None
            photo_path = f"./picture/user/{str(user.id)}.jpg"
            user_input = Event.get_input_user()
            try :
                real_photo_path = await self.CLIENT.download_profile_photo(user_input,file=photo_path)
            except Exception as e:
                logging.error(f"Fail to get user photo")
            if isinstance(user.participant,ChannelParticipantAdmin):
                is_super = True
            else:
                is_super = False
            user_about = None
            try:
                if user.username is not None:
                    full = await self.CLIENT(GetFullUserRequest(id=user.username))
                    if (hasattr(full,'full_user')):
                        user_about = full.full_user.about
            except UserIdInvalidError as e:
                logging.error(f'the user id ({user.username})is invalid!!!')
            except Exception as e:
                logging.error(f"The error:{e}!!!")
            last_data = None
            if  isinstance(user.participant,ChannelParticipant):
                last_date = user.participant.date
            user_data = {
                'user_id':user.id,
                'group_id':chat_id,
                'group_name':chat.title,
                'user_name' : user.username,
                'first_name' : user.first_name,
                'last_name': user.last_name,
                'is_bot': user.bot,
                'is_restricted': user.restricted,
                'is_verified':user.verified,
                'is_self':user.is_self,
                'is_contact':user.contact,
                'is_mutual_contact':user.mutual_contact,
                'is_bot_chat_history':user.bot_chat_history,
                'is_bot_nochats':user.bot_nochats,
                'user_phone':user.phone,
                'is_deleted':user.deleted,
                'user_photo':real_photo_path,  
                'super':is_super,
                'update':update_time,
                'last_date':last_data,
                'user_about':user_about,
            }
            es_user_id = str(user_data['group_id'])+'_'+str(user_data['user_id'])
            n = es.index(index=user_index,body=user_data,id = es_user_id)
            logging.info(f'es Channel user added update:{json.dumps(n,ensure_ascii=False,indent=4)} , {n}')
        elif Event.user_left:
            pair = {
                'channel_id':chat_id,
                'account_id':self.account['account_id'],
            }
            data = self.es_search(index=chat_index,filed=pair)
            if data == None:
                return
            chat_data = data['_source']
            es_id = data['_id']
            now = datetime.now()
            update_time = int(datetime.timestamp(now)*1000)
            chat_data['updata_time'] = update_time
            chat_data['participants_count'] = chat.participants_count
            n = es.index(index=chat_index,body=chat_data,id = es_id)
            logging.info(f'es Channel user left update:{json.dumps(n,ensure_ascii=False,indent=4)} , {n}')
        elif Event.new_title:
            pair = {
                'channel_id':chat_id,
                'account_id':self.account['account_id'],
            }
            data = self.es_search(index=chat_index,filed=pair)
            if data == None:
                return
            chat_data = data['_source']
            es_id = data['_id']
            now = datetime.now()
            update_time = int(datetime.timestamp(now)*1000)
            day_str = now.strftime("%y_%m_%d")

            chat_data['updata_time'] = update_time
            chat_data['channel_title'] = chat.title

            n = es.index(index=chat_index,body=chat_data,id = es_id)
            logging.info(f'es Channel title data update:{json.dumps(n,ensure_ascii=False,indent=4)} , {n}')
        else:
            logging.info(f"Other chataction happened.")
        pass

    async def User_Updata(self,Event:events):
        """ 
        处理 user 出现的更新消息
        @param Event: 出现的事件
        """ 
        if Event.online:
            user_index = self.ES_USER_INDEX
            pair = {
                'user_id':Event.user_id,
            }
            data = self.es_search(Index=user_index,Filed=pair)
            if data == None or Event.last_seen == None :
                return 
            es_id = data['_id']
            user_data = data['_source']
            last_time = int(datetime.timestamp(Event.last_seen)*1000)
            user_data['last_date'] = last_time
            now = datetime.now()
            update_time = int(datetime.timestamp(now)*1000)
            user_data['update_time'] = update_time
            n = es.index(index=user_index,body=user_data,id = es_id)
            logging.info(f'es user data update:{json.dumps(n,ensure_ascii=False,indent=4)} , {n}')
        else:
            return

    def Es_Search(self,Index,Filed):
        """ 
        从 es 中根据指定条件获取返回的结果列表
        @param index: 需要查询的页
        @param filed: 进行过滤的条件字段
        @return: 返回查询到的结果列表
        """ 
        es = self.es_connect
        body = {
            "query":{
                "bool":{
                    "must":[]
                }
            }
        }

        for key,value in filed.items():
            pattem = {
                "term":{
                    key:value
                }
            }
            body['query']['bool']['must'].append(pattem)
        try:
            full_data_list = es.search(index=index,body=body)
            if full_data_list['hits']['total']['value'] < 1:
                return None
            data_list = full_data_list['hits']['hits']
            return data_list
        except Exception as e:
            logging.error(f"Get an error during search es:{e}")
            return None

    def es_get_index(self,index_name):
        es = self.es_connect

        # 检查 index 是否存在
        if not es.indices.exists(index=index_name):
            logging.error(f"Can't find {index_name} in es")
            return None

        data = []

        # 获取目标数据属性
        page = es.search(index=index_name,scroll="2m",size=100)
        scroll_id = page['_scroll_id']
        scroll_size = page['hits']['total']['value']
    
        while scroll_size > 0:
            for source in page['hits']['hits']:
                data.append(source)
                scroll_size -= 1
            page = es.scroll(scroll_id=scroll_id,scroll="2m")
            scroll_id = page['_scroll_id']
            scroll_size = len(page['hits']['hits'])
        return data

    def add_channel(self,url:str)->bool:
        """
        通过链接加入群组（待测试）
        @param url: 群组的邀请链接
        @return:  加入结果
        """ 
        try:
            channel_username = self.get_channel_username(url)
            result = self.CLIENT(JoinChannelRequest(channel=channel_username))
            if result == True:
                return True
            return False
        except Exception as e:
            logging.error(f'Fail to join channel:{url}')

    def quit_channel(self,dialog:Dialog)->bool:
        """
        退出指定群组
        @param dialog: 需要退出的群组
        @return: 退出结果
        """ 
        try:
            result = self.CLIENT.delete_dialog(dialog)
            if hasattr(result,'updates'):
                if isinstance(result.updates,UpdateDeleteChannelMessages):
                    return True
            return False
        except Exception as e:
            logging.error(f"Fail to quit channel for the error:{e}.")
            return False

    def count_active_user(self,message_info):
        """ 
        统计活跃用户
        """
        with self.lock_active_user:

            es_index = self.ES_ACTIVE_USER_INDEX
            pair = {
                'user_id':message_info['user_id'],
                'group_id':message_info['group_id'],
            }
            data = self.es_search(index=es_index,filed=pair)

            user_id = None
            user_data = None

            today_begin = begin_time(message_info['msg_date'])
            now = datetime.now()
            update_time = int (datetime.timestamp(now)*1000)

            if data == None:
                user_data = {
                    'user_name':message_info['user_name'],
                    'user_id':message_info['user_id'],
                    'count':1,
                    'day':today_begin,
                    'channel_id':message_info['group_id'],
                    'channel_title':message_info['group_name'],
                    'updata_time':update_time,
                }
            # 获取当前日期的选项
            else:
                for i in range(len(data)):
                    if data[i]['_source']['day'] == today_begin:
                        user_data = data[i]['_source']
                        user_id = data[i]['_id']
                        break
                if user_data == None:
                    user_data = {
                        'user_name':message_info['user_name'],
                        'user_id':message_info['user_id'],
                        'count':1,
                        'day':today_begin,
                        'channel_id':message_info['group_id'],
                        'channel_title':message_info['group_name'],
                        'updata_time':update_time,
                    }
                else:
                    user_data['updata_time'] = update_time
                    user_data['count'] += 1
                

            if user_id == None:
                n = es.index(index=es_index,body=user_data)
                logging.info(f'es data:{json.dumps(n,ensure_ascii=False,indent=4)} , {n}')
            else:
                n = es.index(index=es_index,body=user_data,id = user_id)
                logging.info(f'es data:{json.dumps(n,ensure_ascii=False,indent=4)} , {n}')

    async def dialog_count_admin(self)->dict:
        """ 
        统计各个群组中的管理员
        """ 
        user_index = self.ES_USER_INDEX
        user_list = self.es_get_index(user_index)
        dialog_index = self.ES_CHANNEL_INDEX
        dialog_list = self.es_get_index(dialog_index)
        if (user_list == None or dialog_list == None):
            return None
        count_info = {}
        for i in user_list:
            info = i['_source']
            if info['super']:
                if info['group_id'] in count_info:
                    count_info[info['group_id']].append({
                        'user_name':info['user_name'],
                        'first_name':info['first_name'],
                        'last_name':info['last_name'],
                        'is_bot':info['is_bot'],
                    })
                else:
                    count_info[info['group_id']] = [{
                        'user_name':info['user_name'],
                        'first_name':info['first_name'],
                        'last_name':info['last_name'],
                        'is_bot':info['is_bot'],
                    }]
        now = datetime.now()
        count_info['update']=int(now.timestamp()*1000)
        return count_info

