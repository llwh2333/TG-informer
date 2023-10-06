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
监控 tg
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
version = '1.2.0'

logFilename = './tgout.log'

logging.basicConfig(
                encoding='utf-8',
                level    = logging.DEBUG,              # 定义输出到文件的log级别，                                                            
                format   = '%(asctime)s  %(filename)s : %(levelname)s  %(message)s',    # 定义输出log的格式
                datefmt  = '%Y-%m-%d %A %H:%M:%S',                                     # 时间
                filename = logFilename,                # log文件名
                filemode = 'w')                        # 写入模式“w”或“a”


class TGInformer:
    def __init__(self,
    ):
        """
        初始化，并开始启动监控程序
        """
        # 读取配置文件
        with open('./informer.yaml','r',encoding='utf-8') as f:
            env = yaml.load(f.read() ,Loader=yaml.FullLoader)
        # ES 相关变量
        self.ES_IP =env['ES_IP']            # es 服务器 ip
        self.ES_PORT = env['ES_PORT']       # es 服务器端口
        self.ES_MESSAGE_INDEX = env['ES_MESSAGE_INDEX']     # es 消息页名称
        self.ES_CHANNEL_INDEX = env['ES_CHANNEL_INDEX']     # es 频道页名称
        self.ES_USER_INDEX = env['ES_USER_INDEX']           # es 用户页名称
        self.ES_ACCOUNT_INDEX = env['ES_ACCOUNT_INDEX']     # es 账号页名称
        self.ES_VIR_INDEX = env['ES_VIR_INDEX']             # es 虚拟身份页名称
        self.ES_MEDIO_INDEX = env['ES_MEDIO_INDEX']         # ？？？

        self.es_messages = []               # 等待上传的 message
        self.dialog_message_bot = {}        # 用于消息过滤

        # 配置参数
        self.MIN_CHANNEL_JOIN_WAIT = 30         # 用于加入频道等待的最小间隔时间
        self.MAX_CHANNEL_JOIN_WAIT = 120        # 用于加入频道等待的最大间隔时间
        self.CHANNEL_REFRESH_WAIT = 15 * 60         # 重新检查的间隔（15min）
        self.models = env['INFO_DUMP_LOCAL']        # 是否存储到本地中

        # 数据部分
        self.channel_meta = []                      # 已加入 channel 的信息
        self.channel_add= set()                     # 等待添加到 es 的 channel
        self.bot_task = None                        #
        self.client = None                          # tg 客户端实例
        self.account = None                         # tg 账户信息
        self.loop = asyncio.get_event_loop()
        self.active_user = {}                       # 统计活跃用户
        self.admin_count = None

        # 异步锁（后面看看有什么锁不在需要）
        self.lock_message = threading.Lock()        # 本地消息存储异步锁
        self.lock_channel = threading.Lock()        # 本地频道存储异步锁
        self.lock_chat_user = threading.Lock()      # 本地用户存储异步锁
        self.lock_es_message = threading.Lock()     # es 消息累积存储异步锁
        self.lock_channe_add = threading.Lock()     # 等待添加更新频道异步锁
        self.lock_active_user = threading.Lock()    # 更新用户活跃信息异步锁
        self.lock_filter_msg = threading.Lock()     # 过滤消息异步锁


        # TG 账号信息
        self.account = {
            'account_id' : env['TELEGRAM_ACCOUNT_ID'],
            'account_api_id':env['TELEGRAM_API_APP_ID'],
            'account_api_hash':env['TELEGRAM_API_HASH'],
            'account_first_name':env['TELEGRAM_ACCOUNT_FIRST_NAME'],
            'account_last_name':env['TELEGRAM_ACCOUNT_LAST_NAME'],
            'account_user_name':env['TELEGRAM_ACCOUNT_USER_NAME'],
            'account_phone':env['TELEGRAM_ACCOUNT_PHONE_NUMBER'], 
        }

        # 连接 es 数据库
        self.es_connect = None
        logging.info(banner)
        logging.info('Version:'+version)
        try:
            if (self.ES_IP != '' and self.ES_PORT != ''):
                address = f"http://{self.ES_IP}:{self.ES_PORT}"
                self.es_connect = Elasticsearch([address])
            else:
                logging.error('You ip or port is None!!!!')
        except ConnectionError as e:
            logging.error(f'Received error {e} \n Connecting the es is wrong!!! we will try againe.')
            self.Es_rebuilt(5)
        except ConnectionTimeout as e:
            logging.error(f'Received error {e} \n Connecting the es is timeout!!! we will try againe.')
            self.Es_rebuilt(5)

        if self.es_connect == None:
            logging.error('Connection is fail. \n Exiting......')
            sys.exit(0)

        # 启动协程开始执行
        self.loop.run_until_complete(self.bot_interval())

    def ES_rebuilt(self,times:int):
        """ 
        尝试重新建立 ES 连接
        @param times: 重试次数上限
        """  
        for i in range(0,times):
            logging.info(f'Attempting times:{i+1}......')
            address = f"http://{self.ES_IP}:{self.ES_PORT}"
            try:
                self.es_connect = Elasticsearch([address])
            except:
                logging.error(f'The {i}th times Connection is Wrong!!!')
            if self.es_connect :
                break
            else:
                logging.error(f'The {i}th times Connection is Fail!!!')

        if self.es_connect == None:
            raise Exception('ES connection rebuilt is wrong!!!!!!!!!')

    async def bot_interval(self):
        """ 
        根据当前的配置建立会话，并保存 session ，方便下一次登录，并调用监控函数开始监控
        """ 
        logging.info(f'Logging in with account # {self.account["account_phone"]} ... \n')

        # 用来存储会话文件的地址，方便下一次的会话连接
        session_file = self.account['account_phone']

        # 实例化一个 tg 端对象，初次登录会记录指定路径中，后续登录会直接使用以前的账户信息
        self.client = TelegramClient(session_file, self.account['account_api_id'], self.account['account_api_hash'])

        # 异步的启动这个实例化的 tg 客户端对象，其中手机号为配置文件中的手机号
        await self.client.start(phone=f'{self.account["account_phone"]}')

        # 检查当前用户是否已经授权使用 API
        if not await self.client.is_user_authorized():
            logging.info(f'Client is currently not logged in, please sign in! Sending request code to {self.account["account_phone"]}, please confirm on your mobile device')
            
            # 当发现没有授权时，向手机号发送验证码
            await self.client.send_code_request(self.account['account_phone'])
            self.tg_user = await self.client.sign_in(self.account['account_phone'], input('Enter code: '))

        # 统计 channel 数量和初始化监控频道
        await self.init_monitor_channels()

        # 循环
        count = 0
        while True:
            count +=1
            logging.info(f'### {count} Running bot interval')
            await asyncio.sleep(self.CHANNEL_REFRESH_WAIT)
            # 检查并更新 channel
            await self.channel_count()

    async def init_monitor_channels(self):
        """  
        开始执行监控机器
        """  
        logging.info('Init the monitor to channels')

        # 检测图片路径是否存在
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
        if self.models == '1':
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

        # 第一次加载 channel 信息
        await self.channel_flush()

        # 处理新消息
        @self.client.on(events.NewMessage)
        async def message_event_handler(event):
            # 只处理来自频道和群组的消息
            message = event.raw_text
            if isinstance(event.message.to_id, PeerChannel):
                logging.info(f'############################### \n Get the channel message is ({message}) \n ###############################')
            elif isinstance(event.message.to_id, PeerChat):
                logging.info(f'############################### \n Get the chat message is ({message}) \n ###############################')
            else:
                # 两者均不是，跳过
                return
            # 通过协程存储当前的新消息
            await self.message_dump(event)

        # 每隔 10s 上传一次 message
        while True:
            if self.es_connect != None:
                await self.update_message_to_es()
            await asyncio.sleep(10)

    async def channel_count(self):
        """ 
        定期(15min)检查是否存在更新的 channel 信息，存在就上传到 ES 中
        """
        count = 0
        update = False
        async for dialog in self.client.iter_dialogs():
            # 会话不能是用户间的对话
            if not dialog.is_user:
                channel_id = dialog.id 
                # 异步安全的加入会话信息
                with self.lock_channe_add:
                    channel_list = self.channel_meta
                    for i in channel_list:
                        if i['channel id'] == channel_id :
                            channel_id = None
                            break
                    # 若当前会话并不在列表中
                    if channel_id != None:
                        # 获取会话的完成信息
                        try:
                            if dialog.is_channel:
                                channel_full = await self.client(GetFullChannelRequest(dialog.input_entity))
                            elif dialog.is_group:
                                channel_full = self.client(GetFullChatRequest(chat_id=dialog.id))
                        except ChannelPrivateError as e:
                            logging.error(f'the channel is private,we can\'t get channel full info: {dialog.id}')
                        except ChannelPublicGroupNaError as e:
                            logging.error(f'channel/supergroup not available: {dialog.id}')
                        except TimeoutError as e:
                            logging.error(f'A timeout occurred while fetching data from the worker: {dialog.id}')
                        except ChatIdInvalidError as e:
                            logging.error(f'Invalid object ID for a chat: {dialog.id}')
                        except PeerIdInvalidError as e:
                            logging.error (f'An invalid Peer was used: {dialog.id}')

                        # 将 channel 加入现在可监控的 channel 列表
                        try :
                            about = channel_full.full_chat.about
                        except AttributeError as e:
                            logging.error(f'chat has no attribute full_chat')
                            about = None
                        channel_list.append({
                            'channel id':dialog.id,
                            'channel name':dialog.name,
                            'channel about': about,
                            })

                    # 从添加列表中除移
                    id_name = self.strip_pre(dialog.id)
                    if not id_name in self.dialog_message_bot:
                        self.dialog_message_bot[id_name] = queue.Queue(10)
                    if id_name in self.channel_add:
                        self.channel_add.discard(id_name)

                e = await self.get_channel_info_by_dialog(dialog)
                await self.dump_channel_info(e)
                await self.dump_channel_user_info(dialog)
                count +=1
        
        logging.info(f'{sys._getframe().f_code.co_name}: Monitoring channel: {json.dumps(channel_list,ensure_ascii=False,indent=4)}')
        logging.info(f'Count:{count}')

    async def message_dump(self,event:events):
        """   
        处理消息的存储
        @param event: 新消息事件
        """  
        tag = None
        media = None
        #检查消息是否含有图片，如果有图片，就存储图片到本地（picture 文件中）
        if event.photo:
            logging.info(f'the message have photo')
            file_name,file_path =  await self.download_file(event)
            file_store = file_path+'/' + file_name
            file_size = os.path.getsize(file_store)
            file_md5 = self.get_file_md5(file_store)
            media = {
                'type':'.jpg',
                'store':file_store,
                'name':file_name,
                'md5':file_md5,
                'size':file_size,
                'file_id':None,
            }
        else:
            # 消息分析处理
            tag = await self.analysis_message(event)

        e = await self.get_message_info_from_event(event,tag,media)
        if self.filter_message(e):
            return 
        if self.models == '1':
            self.store_message_in_json_file(e)
        self.update_es_message(e)

    def get_file_md5(self,fname:str)->str:
        """ 
        计算文件的 md5 值
        @param fname: 文件完整路径
        @return: 文件的md5值
        """
        m = hashlib.md5()
        with open(fname,'rb') as file:
            while True:
                data = file.read(4096)
                if not data:
                    break
                m.update(data)
        return m.hexdigest()

    async def get_message_info_from_event(self,event:events,tag:list,media:dict)->dict:
        """ 
        从 event 中获得需要的 info
        @param event: 新消息事件
        @param tag: 对于消息的的打标结果
        @param media: 对于媒体文件的属性鉴定
        @return: 消息的所有需要的属性
        """ 

        message_obj = event.message
        if isinstance(message_obj.to_id, PeerChannel):
            channel_id = event.message.to_id.channel_id
            is_channel = True
            is_group = False
            is_private = False
        elif isinstance(message_obj.to_id, PeerChat):
            channel_id = event.message.to_id.chat_id
            is_channel = False
            is_group = True
            is_private = False

        is_bot = False if message_obj.via_bot_id is None else True
        is_scheduled = True if message_obj.from_scheduled is True else False
        message_txt = event.raw_text

        # 提到的用户检测
        mentioned_users = []
        mentioned_break = False
        for ent, txt in event.get_entities_text():
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
            sender = await event.get_sender()
            sender_name = utils.get_display_name(sender)
            if sender_name == '':
                sender_name = None
        # 发送者 id
        chat_user_id  =  event.sender_id   
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
                # 转发次数
                'fwd_message_times':message_obj.forwards,
                # 原消息 id(无效)
                'fwd_message_msg_id':message_obj.fwd_from.saved_from_msg_id,
                'fwd_message_channel_post':message_obj.fwd_from.channel_post,
            }
            # id 与名称
            fwd_sender_name = None
            fwd_sender_id = None
            if message_obj.forward.sender is not None:
                fwd_sender_name = utils.get_display_name(message_obj.forward.sender)
                fwd_sender_id = message_obj.forward.sender.id
            if isinstance(message_obj.fwd_from.from_id, PeerUser):
                fwd_sender_id = message_obj.fwd_from.from_id.user_id
                if fwd_sender_name == None:
                    try:
                        fwd_user = await self.client.get_entity(PeerUser(fwd_sender_id))
                        name = ''
                        if fwd_user.first_name is not None:
                            name += fwd_user.first_name
                        if fwd_user.last_name is not None:
                            name += fwd_user.last_name
                        fwd_sender_name = name if name != '' else None
                    except:
                        logging.error(f"message not found fwd user: {fwd_sender_id}")
            elif isinstance(message_obj.fwd_from.from_id, PeerChannel):
                fwd_sender_id = message_obj.fwd_from.from_id.channel_id
                if fwd_sender_name == None:
                    try:
                        fwd_channel = self.client.get_entity(PeerChannel(fwd_sender_id))
                        if fwd_channel.title is not None:
                            name = fwd_channel.title
                        fwd_sender_name = name
                    except:
                        logging.error(f"message not found fwd channel: {fwd_sender_id}")
            elif isinstance(message_obj.fwd_from.from_id, PeerChat):
                fwd_sender_id = message_obj.fwd_from.from_id.chat_id
                if fwd_sender_name == None:
                    try:
                        fwd_chat = self.client.get_entity(PeerChat(fwd_sender_id))
                        if fwd_chat.title is not None:
                            name = fwd_chat.title
                        fwd_sender_name = name
                    except:
                        logging.error(f"message not found fwd chat: {fwd_sender_id}")
            fwd_msg['fwd_message_send_id'] = fwd_sender_id
            fwd_msg['fwd_message_post_author'] = fwd_sender_name
            
        # 如果为回复消息
        reply_msg = None
        if not message_obj.reply_to is None:
            reply_obj = await event.get_reply_message()
            if reply_obj != None:
                # 基本属性获取
                reply_msg = {
                    'reply_message_msg_txt':reply_obj.message,
                    'reply_message_msg_id':message_obj.reply_to.reply_to_msg_id,
                    'reply_message_date':reply_obj.date,
                    'reply_message_post_author':reply_obj.post_author,

                    #被回复消息的被回复次数
                    'reply_message_times':message_obj.replies,
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
                                reply_user = await self.client.get_entity(PeerUser(reply_sender_id))
                                name = ''
                                if reply_user.first_name is not None:
                                    name += reply_user.first_name
                                if reply_user.last_name is not None:
                                    name += reply_user.last_name
                                reply_sender_name = name if name != '' else None
                            except:
                                logging.error(f"message not found reply user: {reply_sender_id}")
                    elif isinstance(reply_obj.from_id, PeerChannel):
                        reply_sender_id = reply_obj.from_id.channel_id
                        if reply_sender_name == None:
                            try:
                                reply_channel = self.client.get_entity(PeerChannel(reply_sender_id))
                                if reply_channel.title is not None:
                                    name = reply_channel.title
                                reply_sender_name = name
                            except:
                                logging.error(f"message not found reply channel: {reply_sender_id}")
                    elif isinstance(reply_obj.from_id, PeerChat):
                        reply_sender_id = reply_obj.from_id.chat_id
                        if reply_sender_name == None:
                            try:
                                reply_chat = self.client.get_entity(PeerChat(reply_sender_id))
                                if reply_chat.title is not None:
                                    name = reply_chat.title
                                reply_sender_name = name
                            except:
                                logging.error(f"message not found reply chat: {reply_sender_id}")
                reply_msg['reply_message_post_author'] = reply_sender_name
                reply_msg['reply_message_send_id'] = reply_sender_id
            
        # 获得消息的 channel 信息
        groupname = None
        groupabout = None
        strip_channel_id = self.strip_pre(channel_id)
        # 检查当前消息所在会话是否在列表中
        for i in self.channel_meta:
            test_channel_id = self.strip_pre(i['channel id'])
            if  test_channel_id == strip_channel_id:
                groupname = i['channel name']
                groupabout = i['channel about']
                break
        # 若遍历完仍旧没有找到
        if groupname == None:
            logging.info (f'New channel :########{strip_channel_id}####################')
            with self.lock_channe_add:
                if strip_channel_id not in self.channel_add:
                    self.channel_add.add(strip_channel_id)
                    logging.info(f'{[i for i in self.channel_add]}')
            await self.channel_count()
            for i in self.channel_meta:
                test_channel_id = self.strip_pre(i['channel id'])
                if  test_channel_id == strip_channel_id:
                    groupname = i['channel name']
                    groupabout = i['channel about']
                    break

        msg_info = {
            'message_id':event.message.id,   
            'message_text':message_txt,   
            'user_name':sender_name,
            'user_id':chat_user_id,
            'group_name':groupname,
            'group_about':groupabout,
            'group_id':channel_id,
            'msg_date':message_obj.date,
            'fwd_message':fwd_msg,
            'reply_message':reply_msg,
            'media':media,
            'mentioned_user': mentioned_users,
            'is_scheduled':is_scheduled,
            'is_bot':is_bot,
            'tag':tag,
            'is_group':is_group,
            'is_channel':is_channel,
        }

        return msg_info

    def strip_pre(self,text:str|int)->str:
        """
        去除 channel id 的前缀
        @param text: 需要去除前缀的 channel id
        @return: 去除后的 channel id
        """
        test_str = str(text)
        if test_str.startswith("-100"):
            test_str = test_str[4:]
        elif test_str.startswith("-"):
            test_str = test_str[1:]
        elif test_str.startswith("100"):
            test_str = test_str[3:]
        return test_str

    def store_message_in_json_file(self,message_info:dict):
        """  
        将 message 存储到本地 json 文件中
        @param message_info: 需要存储的字典
        """  
        now = datetime.now()
        file_date =  now.strftime("%y_%m_%d")
        json_file_name = './local_store/message/'+file_date+'_messages.json'
        if not message_info['fwd_message'] is None:
            # 深拷贝转发消息
            fwd_msg = copy.deepcopy(message_info['fwd_message'])
            fwd_msg['fwd_message_date'] = fwd_msg['fwd_message_date'].strftime('%Y %m %d:%H %M %S')
        else:
            fwd_msg = None
        if not message_info['reply_message'] is None:
            reply_msg = copy.deepcopy(message_info['reply_message'])
            reply_msg= reply_msg['reply_message_date'].strftime('%Y %m %d:%H %M %S')
        else:
            reply_msg = None

        new_message = {
            'message_id':message_info['message_id'],
            'message_txt':message_info['message_text'],
            'user_name':message_info['user_name'],
            'group_name':message_info['group_name'],
            'group_about':message_info['group_about'],
            'group_id':message_info['group_id'],
            'msg_date':message_info['msg_date'].strftime('%Y %m %d:%H %M %S'),
            'fwd_message':fwd_msg,
            'reply_message':reply_msg,
            'media':message_info['media'],
            'mentioned_user':message_info['mentioned_user'],
            'is_scheduled':message_info['is_bot'],
            'tag':message_info['tag'],
            'is_group':message_info['is_group'],
            'is_channel':message_info['is_channel'],
            }

        self.store_data_in_json_file(json_file_name, self.lock_message, 'messages', new_message)

    def store_data_in_json_file(self,file_name:str,lock:threading.Lock,data_key:str,data:dict):
        """ 
        打开 json 文件，并将数据存入
        @param file_name: 存储文件的完整路径
        @param lock: 异步锁
        @param data_key: 数据的类型
        @param data: 数据内容
        """ 
        with lock:
            if not os.path.exists(file_name):
                with open(file_name,'w') as f:
                    init_json ={}
                    json_first = json.dumps(init_json)
                    f.write(json_first)
            with open(file_name, 'r') as f:
                file_data = json.load(f)
            try:
                file_data[data_key].append(data)
            except KeyError:
                file_data[data_key] = []
                file_data[data_key].append(data)
            json_data = json.dumps(file_data,ensure_ascii=False,indent=4)
            with open(file_name,'w') as f:
                f.write(json_data)

    def update_es_message(self,message_info:dict):
        """  
        将获得的 message 属性进行转化，等待后续批量上传
        @param message_info: 将要上传的消息字典
        """  
        #
        message_date = int(datetime.timestamp(message_info['msg_date'])*1000)
        # 深度拷贝，方便后续修改
        fwd_msg = message_info['fwd_message']
        reply_msg = message_info['reply_message']
        msg_info = {
            'message_id':message_info['message_id'],
            'message_txt':message_info['message_text'],
            'user_id':message_info['user_id'],
            'user_name':message_info['user_name'],
            'group_name':message_info['group_name'],
            'group_about':message_info['group_about'],
            'group_id':message_info['group_id'],
            'msg_date':message_date,
            'fwd_message':fwd_msg,
            'reply_message':reply_msg,
            'media':message_info['media'],
            'mentioned_user':message_info['mentioned_user'],
            'is_scheduled':message_info['is_bot'],
            'tag':message_info['tag'],
            'is_group':message_info['is_group'],
            'is_channel':message_info['is_channel']
        }

        if msg_info['fwd_message'] is not None:
            msg_info['fwd_message']['fwd_message_date'] = int(datetime.timestamp(fwd_msg['fwd_message_date']))*1000 if fwd_msg['fwd_message_date'] is not None else None
        if msg_info['reply_message'] is not None:
            msg_info['reply_message']['reply_message_date'] = int(datetime.timestamp(reply_msg['reply_message_date']))*1000 if reply_msg['reply_message_date'] is not None else None
        
        with self.lock_es_message:
            self.es_messages.append(msg_info)

    async def download_file(self,event:events)->tuple:
        """  
        将图片存储到指定路径
        @param event: 消息事件
        @return: 文件名、文件存储路径
        """  
        file_name = self.GetImageName(event)
        file_path = self.GetImagePath(event)
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        download_path = file_path+'/' + file_name
        await event.download_media(download_path)
        logging.info(f'picture down OK')
        return (file_name,file_path)

    def GetImageName(self,event:events)->str:
        """ 
        获得图片文件名，用于存储
        @param event: 消息事件
        @return: 图片名称
        """
        image_name = str(event.message.id)+'_'+str(event.message.grouped_id)+'.jpg'
        return image_name

    def GetImagePath(self,event:events)->str:
        """ 
        获得图片的存储路径
        @param event: 新消息事件
        @return: 图片将要存储的文件夹路径
        """
        file_path = './picture/dialog'
        if isinstance(event.message.to_id, PeerChannel):
            channel_id = event.message.to_id.channel_id
        elif isinstance(event.message.to_id, PeerChat):
            channel_id = event.message.to_id.chat_id
        else:
            channel_id = 'None'
        now = datetime.now()
        file_path = file_path+ '/'+str(channel_id)+'/'+now.strftime("%y_%m_%d")
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        return file_path

    async def update_message_to_es(self):
        """  
        将获得到的 message 信息批量存入 es 系统中
        """  
        # 获取消息
        es = self.es_connect
        with self.lock_es_message:
            message_info = copy.deepcopy(self.es_messages)
            self.es_messages = []

        # 批量上传
        if message_info == []: 
            return
        else:
            # 检查 index
            es_index = self.ES_MESSAGE_INDEX
            if not es.indices.exists(index=es_index):
                result = es.indices.create(index=es_index)
                logging.info ('creat index new_message_info')
            actions = (
                {
                    '_index': es_index,
                    '_type': '_doc',
                    '_id': str(Message['group_id'])+'-'+str(Message['message_id']),
                    '_source': Message,
                }
                for Message in message_info
                )
        n,_ = bulk(es, actions)
        logging.info(f'es data: total:{len(message_info)} message, insert {n} message successful')

    async def analysis_message(self,event:events)->dict:
        """  
        TODO: 对消息进行处理、分析
        @param event: 新消息事件
        @return: 从不同角度打标的结果
        """  
        virtual_identity = await self.extract_virtual_identity(event)
        #打标
        tags = await self.tag_message(event)
        tag = {
            'tag':tags,
            'virtual_identity':virtual_identity
        }
        return tag

    async def tag_message(self,event:events)->dict:
        """  
        对消息进行打标
        @param event: 新消息事件
        @return: 从不同角度打标的结果
        """  
        tag = {
            'region':None,
            'language':None,
        }
        regiontag = self.region_tag(event.raw_text)
        tag['region'] = regiontag
        """
        if regiontag is not None:
            tag['region'] = ''
            for i in regiontag:
                tag['region'] += f'{i} '
        """
        language = self.language_tag(event.raw_text)
        tag['language'] = language
        return tag

    def region_tag(self,text:str)->list:
        """
        对消息进行分析打标
        @param text: 消息内容
        @return: 地区打标结果
        """
        # 英文地区识别部分
        places = GeoText(text)
        eng = places.cities
        # 中文地区识别部分
        chi = jio.recognize_location(text)
        #print (chi)
        result = []
        if eng is not []:
            result.extend(eng)
        if chi['domestic'] is not None:
            for i in range(len(chi['domestic'])):
                if chi['domestic'][i][0]['county'] is not None:
                    domestic = chi['domestic'][i][0]['county']
                elif chi['domestic'][i][0]['city'] is not None:
                    domestic = chi['domestic'][i][0]['city']
                elif chi['domestic'][i][0]['province'] is not None:
                    domestic = chi['domestic'][i][0]['province']
                result.append(domestic)
        if chi['foreign'] is not None:
            for i in range(len(chi['foreign'])):
                if chi['foreign'][i][0]['country'] == '中国':
                    continue
                if chi['foreign'][i][0]['city'] is not None:
                    foreign = chi['foreign'][i][0]['city']
                elif chi['foreign'][i][0]['country'] is not None:
                    foreign = chi['foreign'][i][0]['country']
                result.append(foreign)
        if chi['others'] is not None:
            for i in chi['others'].keys():
                result.append(i)
        if result == []:
            result = None
        return result

    def language_tag(self,text:str)->str:
        """
        识别语言的类型
        @param text: 消息内容
        @return: 识别语言类型结果
        """
        result = None
        try:
            result = detect_langs(text)[0].lang
        except :
            result = None
            logging.error('Fail to detect the language')
        return result

    async def extract_virtual_identity(self,event:events)->dict:
        """ 
        对消息中的虚拟身份进行提取
        @param event: 新消息事件
        @return: 不同的虚拟身份提取结果
        """ 
        message_txt = event.raw_text

        # 虚拟身份：qq、微信、手机、url、Email

        vir_data = {
            'wechat':None,
            'qq':None,
            'phone':None,
            'url':None,
            'e-mail':None,
            'id_card':None,
        }

        # 身份提取
        text = message_txt
        email_accounts = self.email_extract(text)
        phone_accounts = self.phone_extract(text)
        qq_accounts = self.qq_extract(text)
        wechat_accounts = self.wechat_extract(text)
        ids = self.ids_extract(text)
        url_address = self.url_extract(text)


        # 身份存储
        vir_data['wechat'] = wechat_accounts
        vir_data['qq'] = qq_accounts
        vir_data['phone'] = phone_accounts
        vir_data['url'] = url_address
        vir_data['e-mail'] = email_accounts
        vir_data['id_card'] = ids

        if (len(wechat_accounts) + len(qq_accounts) + len(phone_accounts) + len(url_address) + len(email_accounts) + len(ids)) < 1:
            return None
        return vir_data

    def replace_chinese(self,text:str)->str:
        """ 
        去除中文，替换位空格(默认非空字符串)
        @param text: 待处理字符串
        @return: 处理后字符串
        """  
        filtrate = re.compile(u'[\u4E00-\u9FA5]')
        text_without_chinese = filtrate.sub(r' ', text)
        return text_without_chinese

    def phone_extract(self,text:str)->list:
        """  
        手机号提取
        @param text: 待提取的消息
        @return: 可能的手机号
        """  
        if text=='':
            return []
        eng_texts = self.replace_chinese(text)
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
        return virtual

    def email_extract(self,text:str)->list:
        """  
        email 提取
        @param text: 待提取的消息
        @return: 可能的邮箱
        """ 

        if text=='':
            return []
        eng_texts = self.replace_chinese(text)
        eng_texts = eng_texts.replace(' at ','@').replace(' dot ','.')
        sep = ',!?:; ，。！？《》、|\\/'
        eng_split_texts = [''.join(g) for k, g in groupby(eng_texts, sep.__contains__) if not k]

        email_pattern = r'^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\.[a-zA-Z_-]+)+$'

        emails = []
        for eng_text in eng_split_texts:
            result = re.match(email_pattern, eng_text, flags=0)
            if result:
                emails.append(result.string)
        return emails

    def ids_extract(self,text:str)->list:
        """ 
        身份证号提取
        @param text: 待提取的消息
        @return: 可能的身份证号        
        """  
        if text == '':
            return []
        eng_texts = self.replace_chinese(text)
        sep = ',!?:; ：，.。！？《》、|\\/abcdefghijklmnopqrstuvwyzABCDEFGHIJKLMNOPQRSTUVWYZ'
        eng_split_texts = [''.join(g) for k, g in groupby(eng_texts, sep.__contains__) if not k]
        eng_split_texts_clean = [ele for ele in eng_split_texts if len(ele) == 18]

        id_pattern = r'(^\d{15}$)|(^\d{18}$)|(^\d{17}(\d|X|x)$)'

        ids = []
        for eng_text in eng_split_texts_clean:
            result = re.match(id_pattern, eng_text, flags=0)
            if result:
                ids.append(result.string)
        return ids

    def url_extract(self,text:str)->list:
        """  
        url 提取
        @param text: 待提取的消息
        @return: 可能的url        
        """ 
        extractor = URLExtract()
        url_address = extractor.find_urls(text)
        return url_address

    def qq_extract(self,text:str)->list:
        """  
        qq 提取
        @param text: 待提取的消息
        @return: 可能的qq号        
        """ 
        if text=='':
            return []
        eng_texts = self.replace_chinese(text)
        sep = '@,!?:; ：，.。！？《》、|\\/abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
        eng_split_texts = [''.join(g) for k, g in groupby(eng_texts, sep.__contains__) if not k]
        eng_split_texts_clean = [ele for ele in eng_split_texts if len(ele)>=5 and len(ele)<13]

        qq_pattern = r"[1-9][0-9]{4,11}"

        qq_accounts = []
        for eng_text in eng_split_texts_clean:
            result = re.match(qq_pattern, eng_text, flags=0)
            if result and (result.string not in qq_accounts):
                qq_accounts.append(result.string)
        return qq_accounts

    def wechat_extract(self,text:str)->list:
        """  
        wechat 提取
        @param text: 待提取的消息
        @return: 可能的微信号        
        """ 
        if 'wechat' in text or '微信' in text:
            wechat_pattern = r'\b[a-zA-Z_]\w{5,19}\b'
            wechat_accounts = re.findall(wechat_pattern, text)
            return wechat_accounts
        else:
            return []

    async def channel_flush(self):
        """  
        在第一次登录时将 channel 信息上传至 ES 中
        """  
        count = 0
        channel_list = []
        async for dialog in self.client.iter_dialogs():
            if not dialog.is_user:
                e = await self.get_channel_info_by_dialog(dialog)
                channel_list.append({
                    'channel id':dialog.id,
                    'channel name':dialog.name,
                    'channel about':e['channel_about'],
                    })
                # 添加对应频道消息缓存队列
                id_name = self.strip_pre(dialog.id)
                if not id_name in self.dialog_message_bot:
                    self.dialog_message_bot[id_name] = queue.Queue(10)
                await self.dump_channel_info(e)
                await self.dump_channel_user_info(dialog)
        self.channel_meta = channel_list
        logging.info(f'{sys._getframe().f_code.co_name}: Count:{count} ;Monitoring channel: {json.dumps(channel_list,ensure_ascii=False,indent=4)}')

    async def dump_channel_info(self,channel_info:dict):
        """
        将 channel 信息存储下来
        @return: 需要存储的
        """  
        if self.models == '1':
            self.store_channel_info_in_json_file(channel_info)
        self.update_channel_to_es(channel_info)

    async def get_channel_info_by_dialog(self,dialog:Dialog):
        """  
        从会话中获得 channel 的信息
        @param dialog: 会话
        @return: 获取到的频道属性
        """  

        # 基础部分的属性
        channel_id = dialog.id if dialog.id else None
        channel_title = dialog.title if dialog.title else None
        channel_date = dialog.date if dialog.date else None
        is_group = dialog.is_group
        is_channel = dialog.is_channel
        participants_count = self.get_channel_user_count(dialog)
        is_megagroup = True if dialog.is_group and dialog.is_channel else False
        # 群用户名
        username = dialog.entity.username if dialog.is_channel else None
        invite_link = None
        # 获取 full 的频道属性
        channel_full = None
        try:
            if dialog.is_channel:
                channel_full = await self.client(GetFullChannelRequest(dialog.input_entity))
                logging.info(f'fullchannel:{dialog.title}')
            elif dialog.is_group:
                channel_full = self.client(GetFullChatRequest(chat_id=dialog.id))
                logging.info(f'fullchat:{dialog.title}')
        except ChannelPrivateError as e:
            logging.error(f'the channel is private,we can\'t get channel full info: {dialog.id}')
        except TimeoutError as e:
            logging.error(f'A timeout occurred while fetching data from the worker: {dialog.id}')
        except Except as e:
            logging.error(f"get a error :{e}")

        about = None
        location = None
        link_chat_id = None
        if hasattr(channel_full,'full_chat'):
            about = channel_full.full_chat.about
        else:
            logging.error(f'chat ({dialog.title}) has no attribute full_chat')

        is_restricted = False
        admins_count = None
        is_enable = None

        if is_channel:
            if hasattr(channel_full,'full_chat'):
                chat_full = channel_full.full_chat
                if chat_full.location != None :
                    location = chat_full.location.address
                if hasattr(chat_full,'admins_count'):
                    admins_count = chat_full.admins_count
                link_chat_id = getattr(channel_full.full_chat,'linked_chat_id')
            if hasattr(dialog.entity,'restricted'):
                is_restricted = getattr(dialog.entity,'restricted')
            if hasattr(dialog.entity,'username'):
                if dialog.entity.username is not None:
                    invite_link = f"https://t.me/{dialog.entity.username}"
        if admins_count == None:
            if self.admin_count == None:
                self.admin_count  = await self.dialog_count_admin()
            the_channel_id = self.strip_pre(dialog.id)
            for key in self.admin_count:
                if the_channel_id == self.strip_pre(key):
                    admins_count = len(self.admin_count[key])

        if is_group:
            if hasattr(channel_full,'full_chat'):
                if hasattr(dialog.entity,'deactivated'):
                    is_enable = getattr(dialog.entity,'deactivated')
            else:
                logging.error(f'chat ({dialog.title}) has no attribute full_chat')

       
        # 频道图片获取
        photo_path = r'./picture/channel/'+str(dialog.id)+'.jpg'
        if os.path.exists(photo_path):
            real_photo_path = photo_path
        else:
            real_photo_path = None
            try:
                real_photo_path = await self.client.download_profile_photo(dialog.input_entity,file=photo_path)
            except Exception:
                logging.error(Exception)
        last_msg_date = dialog.date
        now = datetime.now()
        update_time = now
        channel_data = {
            'channel_id':channel_id,            # -100类型
            'channel_title':channel_title,
            'channel_date':channel_date,        # 原本想设置为频道创建时间，但没有办法做到，后续看看如何做到
            'invite_link':invite_link,
            'account_id':self.account['account_id'],
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

    def get_channel_user_count(self,dialog:Dialog)->int:
        """  
        获得 channel 的用户人数
        @param dialog: 需要统计用户人数的会话
        @return: 会话的人数
        """  
        size = 0
        try:
            if dialog.is_channel:
                size = dialog.entity.participants_count
            elif dialog.is_group:
                size = dialog.entity.participants_count
        except Exception:
            logging.error(f"ERROR: can't counting the {dialog.name}" )
        return size

    def store_channel_info_in_json_file(self,channel_info:dict):
        """  
        将 channel 信息存储到 json 文件中
        @param channel_info: 需要存储的会话属性
        """  
        now = datetime.now()
        file_date = now.strftime("%y_%m_%d")
        json_file_name = './local_store/channel_info/'+file_date+'_channel_info.json'

        channel_date = channel_info['channel_date'].strftime('%Y %m %d:%H %M %S') if channel_info['channel_date'] is not None else None
        last_msg_date = channel_info['last_msg_date'].strftime('%Y %m %d:%H %M %S') if channel_info['last_msg_date'] is not None else None
        update_time = channel_info['update_time'].strftime('%Y %m %d:%H %M %S') if channel_info['update_time'] is not None else None

        channel_info_data = {
            'channel_id':channel_info['channel_id'],
            'channel_title':channel_info['channel_title'],
            'channel_date':channel_date,
            'account_id':channel_info['account_id'],
            'invite_link':channel_info['invite_link'],
            'is_megagroup':channel_info['is_megagroup'],
            'is_group':channel_info['is_group'],
            'is_channel':channel_info['is_channel'],
            'participants_count':channel_info['participants_count'],
            'username':channel_info['username'],
            'channel_photo':channel_info['channel_photo'],
            'is_enable':channel_info['is_enable'],
            'location':channel_info['location'],
            'admins_count':channel_info['admins_count'],
            'is_restricted':channel_info['is_restricted'],
            'last_msg_date':last_msg_date,
            'update_time':update_time
        }
        self.store_data_in_json_file(json_file_name, self.lock_channel,str(channel_info['account_id']), channel_info_data)

    def update_channel_to_es(self,channel_info:dict):
        """  
        将获得到的 channel 信息存入 es 中
        @param channel_info: 等待存储的信息
        """  
        es = self.es_connect

        # 检查 index
        es_index = self.ES_CHANNEL_INDEX
        if not es.indices.exists(index=es_index):
            result = es.indices.create(index=es_index)
            logging.info ('creat index channel')

        channel_id = self.strip_pre(channel_info['channel_id'])
        channel_date = int(datetime.timestamp(channel_info['channel_date']))*1000 if channel_info['channel_date'] is not None else None
        last_msg_date = int(datetime.timestamp(channel_info['last_msg_date']))*1000 if channel_info['last_msg_date'] is not None else None
        update_time = int(datetime.timestamp(channel_info['update_time']))*1000 if channel_info['update_time'] is not None else None

        channel_data = {
            'channel_id':channel_info['channel_id'],
            'channel_title':channel_info['channel_title'],
            'channel_date':channel_date,
            'account_id':channel_info['account_id'],
            'invite_link':channel_info['invite_link'],
            'is_megagroup':channel_info['is_megagroup'],
            'is_group':channel_info['is_group'],
            'is_channel':channel_info['is_channel'],
            'participants_count':channel_info['participants_count'],
            'username':channel_info['username'],
            'channel_photo':channel_info['channel_photo'],
            'is_enable':channel_info['is_enable'],
            'location':channel_info['location'],
            'admins_count':channel_info['admins_count'],
            'is_restricted':channel_info['is_restricted'],
            'last_msg_date':last_msg_date,
            'update_time':update_time
        }

        # 将要上传的数据
        es_channel = channel_data

        es_id = channel_id

        # 将数据进行上传
        n = es.index(index=es_index,doc_type='_doc',body=es_channel,id = es_id)
        logging.info(f'es data:{json.dumps(n,ensure_ascii=False,indent=4)} , {n}')

    async def dump_channel_user_info(self,dialog:Dialog):
        """  
        将会话的所有成员的信息存储下来
        @param dialog: 会话
        """  
        e = await self.get_users_info_from_dialog(dialog)
        if e == []:
            return 
        if self.models == '1':
            self.store_users_info_in_json_file(e,dialog)
        self.update_users_to_es(e)
    
    async def get_users_info_from_dialog(self,dialog:Dialog)->list:
        """ 
        获取当前会话的所有成员信息
        @param dialog: 目标会话
        @return: 所有成员的属性信息
        """ 
        users_info_list = []
        if not dialog.is_group :
            return []
        count = 0
        logging.info(f'start log dialog user:{dialog.title}:{dialog.id}')
        users_list = None
        try :
            async for user in self.client.iter_participants(dialog, aggressive=True):
                user_id = user.id
                user_name = user.username
                first_name = user.first_name
                last_name = user.last_name
                is_bot = user.bot
                user_phone = user.phone
                is_restricted = user.restricted
                is_verified = user.verified
                access_hash = user.access_hash
                last_date = None
                if  isinstance(user.participant,ChannelParticipant):
                    last_date = user.participant.date
                modified = None
                photo_path = r'./picture/user/'+str(user_id)+'.jpg'
                if os.path.exists(photo_path):
                    photo = photo_path
                else:
                    real_photo_path =  await self.client.download_profile_photo(user,file=photo_path)
                    photo = real_photo_path
                is_deleted = user.deleted
                group_id = dialog.id
                group_name = dialog.title
                if isinstance(user.participant,ChannelParticipantAdmin):
                    super = True
                else:
                    super = False
                user_about = None
                user_full = None
                try:
                    if user.username is not None:
                        user_full = await self.client(GetFullUserRequest(id=user.username))
                except UserIdInvalidError as e:
                    logging.error(f'the user id ({user.username})is invalid!!!')
                except Exception as e:
                    logging.error(f"The error:{e}!!!")
                if user_full is not None:
                    if hasattr(user_full,'full_user'):
                        user_about = user_full.full_user.about

                user_info={
                    'user_id':user_id,
                    'group_id':group_id,
                    'group_name':group_name,
                    'user_name' : user_name,
                    'first_name' : first_name,
                    'last_name': last_name,
                    'is_bot': is_bot,
                    'is_restricted': is_restricted,
                    'user_phone':user_phone,
                    'is_deleted':is_deleted,
                    'user_photo':photo,  
                    'last_date':last_date,
                    'user_about':user_about,
                    'super':super,
                }
                users_info_list.append(user_info)
                count += 1
        except FloodWaitError as e:
            logging.error(f'get a floodwaiterror {e}')

        logging.info(f'Logging the users account {count} ... \n')
        return users_info_list

    def store_users_info_in_json_file(self,users_info_list:list,dialog:Dialog):
        """  
        将获得的 user 列表信息存储到本地 json 中
        @param users_info_list: 需要存储的用户属性列表
        @param dialog: 用户们所在频道
        """  
        now = datetime.now()
        file_date =  now.strftime("%y_%m_%d")
        json_file_name = './local_store/user_info/'+file_date+'_chat_user.json'
        users_info = []
        for user in users_info_list:
            user_info={
                'user_id':user['user_id'],
                'user_name' :user['user_name'],
                'first_name' :user['first_name'] ,
                'last_name':user['first_name'] ,
                'group_id':user['group_id'] ,
                'group_name':user['group_name'] ,
                'super':user['super'] ,
                'user_about':user['user_about'] ,
                'last_date':user['last_date'].strftime('%Y %m %d:%H %M %S') if user['last_date'] is not None else None ,
                'user_photo':user['user_photo'] ,
                'is_deleted':user['is_deleted'] ,
                'is_bot':user['is_bot'] ,
                'is_restricted': user['is_restricted'],
                'user_phone':user['user_phone'],
            }
            users_info.append(user_info)
        self.store_data_in_json_file(json_file_name, self.lock_chat_user, str(dialog.id),users_info)

    def update_users_to_es(self,users_info:list):
        """  
        将获得到的 channel 的 user 信息存入 es 中
        @param users_info: 需要存储的用户们的属性列表
        """  
        es = self.es_connect

        es_users_info = []
        for user_info in users_info:
            user_date = int(datetime.timestamp(user_info['last_date']))*1000 if user_info['last_date'] is not None else None
            
            mix_user = {
                'user_id':user_info['user_id'],
                'user_name' :user_info['user_name'],
                'first_name' :user_info['first_name'] ,
                'last_name':user_info['first_name'] ,
                'group_id':user_info['group_id'] ,
                'group_name':user_info['group_name'] ,
                'super':user_info['super'] ,
                'user_about':user_info['user_about'] ,
                'last_date':user_date,
                'user_photo':user_info['user_photo'] ,
                'is_deleted':user_info['is_deleted'] ,
                'is_bot':user_info['is_bot'] ,
                'is_restricted': user_info['is_restricted'],
                'user_phone':user_info['user_phone'],
            }

            es_user_info= mix_user
            es_users_info.append(es_user_info)

        # 检查 index
        es_index = self.ES_USER_INDEX
        if not es.indices.exists(index=es_index):
            result = es.indices.create(index=es_index)
            logging.info ('creat index user')

        actions = (
            {
                '_index': es_index,
                '_type': '_doc',
                '_id':str(User_info['group_id'])+'_'+str(User_info['user_id']),
                '_source':User_info,
            }
            for User_info in es_users_info
        )

        # 将数据进行上传
        n,_ = bulk(es, actions)
        logging.info(f'total:{len(es_users_info)} user, insert {n} user successful')

    def add_channel(self,url:str)->bool:
        """
        通过链接加入群组（待测试）
        @param url: 群组的邀请链接
        @return:  加入结果
        """ 
        try:
            channel_username = self.get_channel_username(url)
            result = self.client(JoinChannelRequest(channel=channel_username))
            if result == True:
                return True
            return False
        except:
            logging.error(f'fail to join:{url}')

    def quit_channel(self,dialog:Dialog)->bool:
        """
        退出指定群组
        @param dialog: 需要退出的群组
        @return: 退出结果
        """ 
        result = self.client.delete_dialog(dialog)
        if hasattr(result,'updates'):
            if isinstance(result.updates,UpdateDeleteChannelMessages):
                return True
        return False

    def filter_message(self,message_info:dict)->bool:
        """ 
        检测获取到的消息是否需要过滤
        @param message_info: 消息属性
        @return: 判断结果
        """
        # 去除非机器人消息
        if not message_info['is_bot']:
            return False
        channel_id = self.strip_pre(message_info['group_id'])
        if not channel_id in self.dialog_message_bot:
            logging.error(f"not found dialog({message['group_name']}) in message cache")
            return False
        with self.lock_filter_msg:
            message_queue = self.dialog_message_bot[channel_id]
            if message_info['message_text'] != '':
                filter_text = message_info['message_text']
            elif message_info['message_text'] == '' and message_info['media'] != None:
                filter_text = message_info['media']['md5']
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
                    if message_queue.full():
                        logging.error('message caches error, please fix it!!!!')
                        item = message_queue.get()
                        message_queue.put(filter_text)
                    else:
                        message_queue.put(filter_text)
                else:
                    if message_queue.full():
                        item = message_queue.get()
                        message_queue.put(filter_text)
                    else:
                        message_queue.put(filter_text)
            else:
                message_queue.put(filter_text)
        return flag

    def get_index_info(self,index_name):
        es = self.es_connect

        # 检查 index 是否存在
        if not es.indices.exists(index=index_name):
            print('{} not find in es'.format(index_name))
            return None

        data = []

        # 获取目标数据属性
        page = es.search(index=index_name,scroll="2m",size=100)
        scroll_id = page['_scroll_id']
        print(f'scroll_id:{scroll_id}')
        scroll_size = page['hits']['total']['value']
        print(f'scroll_size:{scroll_size}')
    
        while scroll_size > 0:
            for source in page['hits']['hits']:
                data.append(source)
                scroll_size -= 1
            page = es.scroll(scroll_id=scroll_id,scroll="2m")
            scroll_id = page['_scroll_id']
            print(f'scroll_id:{scroll_id}')
            scroll_size = len(page['hits']['hits'])
        return data

    async def dialog_count_admin(self)->dict:
        """ 
        统计各个群组中的管理员
        """ 
        user_index = self.ES_USER_INDEX
        user_list = self.get_index_info(user_index)
        dialog_index = self.ES_CHANNEL_INDEX
        dialog_list = self.get_index_info(dialog_index)

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

    def count_active_user(self,message_info):
        """ 
        统计活跃用户
        """
        with self.lock_active_user:
            today_begin =  str(begin_time(message_info['msg_date']))

            if today_begin in self.active_user:
                self.active_user[today_begin]['total'] += 1
                group_id = str(message_info['group_id'])
                if group_id in self.active_user[today_begin]:
                    user_id = str(message_info['user_id'])
                    if user_id in self.active_user[today_begin][group_id]:
                        self.active_user[today_begin][group_id][user_id]['count'] += 1
                    else:
                        self.active_user[today_begin][group_id][user_id] = {
                            'user_name':message_info['user_name'],
                            'count':1,
                            'channel_id':message_info['group_id'],
                            'channel_title':message_info['group_name']
                        }
                else:
                    self.active_user[today_begin][group_id] = {}
                    self.active_user[today_begin][group_id][user_id] = {
                        'user_name':message_info['user_name'],
                        'count':1,
                        'channel_id':message_info['group_id'],
                        'channel_title':message_info['group_name']
                    }
            else:
                past_time = str(begin_time(message_info['msg_date'] - 86400 *1000))
                if past_time in self.active_user:
                    del self.active_user[past_time]
                self.active_user[today_begin]={
                    'total':1
                }
                self.active_user[today_begin][group_id] = {}
                self.active_user[today_begin][group_id][user_id] = {
                    'user_name':message_info['user_name'],
                    'count':1,
                    'channel_id':message_info['group_id'],
                    'channel_title':message_info['group_name']
                }

def begin_time(time):
    """ 
    获取输入时间戳的当日起点时间戳
    """ 
    timestamp = time/1000

    today = datetime.fromtimestamp(timestamp)

    today_begin = datetime(today.year,today.month,today.day,0,0,0)
    today_stamp = today_begin.timestamp()

    return int(today_stamp)*1000
