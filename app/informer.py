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
from telethon.tl.types import PeerUser, PeerChat, PeerChannel,ChannelParticipant
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

""" 
监控 tg
""" 

banner = """
    --------------------------------------------------
        ____      ____                              
       /  _/___  / __/___  _________ ___  ___  _____
       / // __ \/ /_/ __ \/ ___/ __ `__ \/ _ \/ ___/
     _/ // / / / __/ /_/ / /  / / / / / /  __/ /    
    /___/_/ /_/_/  \____/_/  /_/ /_/ /_/\___/_/ 
    --------------------------------------------------
"""

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
        self.ES_IP =env['ES_IP']
        self.ES_PORT = env['ES_PORT']
        self.ES_MESSAGE_INDEX = env['ES_MESSAGE_INDEX']
        self.ES_CHANNEL_INDEX = env['ES_CHANNEL_INDEX']
        self.ES_USER_INDEX = env['ES_USER_INDEX']
        self.ES_ACCOUNT_INDEX = env['ES_ACCOUNT_INDEX']
        self.ES_VIR_INDEX = env['ES_VIR_INDEX']
        self.ES_MEDIO_INDEX = env['ES_MEDIO_INDEX']
        self.es_messages = []

        # 配置参数
        self.MIN_CHANNEL_JOIN_WAIT = 30         #用于等待的参数
        self.MAX_CHANNEL_JOIN_WAIT = 120
        self.CHANNEL_REFRESH_WAIT = 15 * 60         # 重新检查的间隔（15min）
        self.models = env['INFO_DUMP_LOCAL']

        # 数据部分
        self.channel_meta = []                      # 已加入 channel 的信息
        self.channel_add= set()
        self.bot_task = None
        self.client = None
        self.account = None
        self.loop = asyncio.get_event_loop()

        # 异步锁（后面看看有什么锁不在需要）
        self.lock_message = threading.Lock()
        self.lock_channel = threading.Lock()
        self.lock_chat_user = threading.Lock()
        self.lock_es_message = threading.Lock()
        self.lock_channe_add = threading.Lock()

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
            self.es_connect = Elasticsearch([address])
            if self.es_connect :
                break
            else:
                logging.error('Fail!!!!!')

        if self.es_connect == None:
            raise Exception('ES connection is wrong!!!!!!!!!')

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
        logging.info('Running the monitor to channels')

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
                channel_id = event.message.to_id.channel_id
                logging.info(f'############################### \n Get the channel message is ({message}) \n ###############################')
            elif isinstance(event.message.to_id, PeerChat):
                channel_id = event.message.to_id.chat_id
                logging.info(f'############################### \n Get the chat message is ({message}) \n ###############################')
            else:
                # 两者均不是，跳过
                return
            # 通过协程存储当前的新消息
            await self.message_dump(event,channel_id)

        # 每隔 10s 上传一次 message
        while True:
            if self.es_connect != None:
                await self.updata_message_to_es()
            await asyncio.sleep(10)

    async def channel_count(self):
        """ 
        定期(15min)检查是否存在更新的 channel 信息，存在就上传到 ES 中
        """
        count = 0
        channel_list = self.channel_meta
        updata = False
        async for dialog in self.client.iter_dialogs():
            # 会话不能是用户间的对话
            if not dialog.is_user:
                channel_id = dialog.id 
                # 检查是否是已经存在于列表中
                for i in channel_list:
                    if i['channel id'] == channel_id :
                        channel_id = None
                        break
                if channel_id != None:
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
                    if id_name in self.channel_add:
                        with self.lock_channe_add:
                            self.channel_add.discard(id_name)
                    e = await self.get_channel_info_by_dialog(dialog)
                    await self.dump_channel_info(e)
                    await self.dump_channel_user_info(dialog)
                count +=1
        self.channel_meta = channel_list
        logging.info(f'{sys._getframe().f_code.co_name}: Monitoring channel: {json.dumps(channel_list,ensure_ascii=False,indent=4)}')
        logging.info(f'Count:{count}')
        pass

    async def message_dump(self,event:events,channel_id:int):
        """   
        处理消息的存储
        @param event: 新消息事件
        @param channel_id: 消息所在 channel id(message.to_id.channel_id)
        """  
        tag = None
        media = None
        #检查消息是否含有图片，如果有图片，就存储图片到本地（picture 文件中）
        if event.photo:
            logging.info(f'the message have photo')
            await self.download_file(event)
            file_name = self.GetImageName(event)
            file_path = self.GetImagePath(event)
            file_store = file_path+'/' + file_name+'.jpg'
            file_size = os.path.getsize(file_store)
            file_md5 = self.get_file_md5(file_store)
            media = {
                'type':'.jpg',
                'store':file_store,
                'name':file_name+'.jpg',
                'md5':file_md5,
                'size':file_size,
                'file_id':None,
            }
        else:
            # 消息分析处理
            tag = await self.analysis_message(event,channel_id)

        e = await self.get_message_info_from_event(event,channel_id,tag,media)

        if self.models == '1':
            self.store_message_in_json_file(e)
        self.updata_es_message(e)

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

    async def get_message_info_from_event(self,event:events,channel_id:int,tag:list,media:dict)->dict:
        """ 
        从 event 中获得需要的 info
        @param event: 新消息事件
        @param channel_id: 消息所在 channel
        @param tag: 对于消息的的打标结果
        @param media: 对于媒体文件的属性鉴定
        @return: 消息的所有需要的属性
        """ 

        message_obj = event.message
        if isinstance(message_obj.to_id, PeerChannel):
            is_channel = True
            is_group = False
            is_private = False
        elif isinstance(message_obj.to_id, PeerChat):
            is_channel = False
            is_group = True
            is_private = False

        is_bot = False if message_obj.via_bot_id is None else True
        is_scheduled = True if message_obj.from_scheduled is True else False
        content = event.raw_text

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
        
        # 如果是转发消息
        fwd_msg = None
        if not message_obj.fwd_from is None:
            fwd_msg = {
                # 发送时间
                'fwd_message_date': message_obj.fwd_from.date, 
                # 原消息发送者名称
                'fwd_message_from_name':message_obj.fwd_from.from_name,
                # 转发次数
                'fwd_message_times':message_obj.forwards,
                # 原消息 id
                'fwd_message_saved_msg_id':message_obj.fwd_from.saved_from_msg_id,
                'fwd_message_imported':message_obj.fwd_from.imported,
                'fwd_message_channel_post':message_obj.fwd_from.channel_post,
                'fwd_message_post_author':message_obj.fwd_from.post_author,
                'fwd_message_psa_type':message_obj.fwd_from.psa_type,
            }
            if message_obj.fwd_from.from_id is not None:
                if isinstance(message_obj.fwd_from.from_id, PeerUser):
                    fwd_msg['fwd_message_from_id']= message_obj.fwd_from.from_id.user_id
                elif isinstance(message_obj.fwd_from.from_id, PeerChat):
                    fwd_msg['fwd_message_from_id']=message_obj.fwd_from.from_id.chat_id
                elif isinstance(message_obj.fwd_from.from_id, PeerChannel):
                    fwd_msg['fwd_message_from_id']=message_obj.fwd_from.from_id.channel_id
            else:
                # 转发消息是匿名的
                fwd_msg['fwd_message_from_id']: 000000000

        # 如果回复了某消息
        reply_msg = None
        if not message_obj.reply_to is None:
            reply_obj = await event.get_reply_message()
            if reply_obj != None:
                reply_msg = {
                    'reply_message_txt':reply_obj.message,
                    'reply_message_id':message_obj.reply_to.reply_to_msg_id,
                    'reply_message_date':reply_obj.date,
                    'reply_message_times':message_obj.replies,
                    'reply_message_scheduled':message_obj.reply_to.reply_to_scheduled,
                    'reply_message_to_top_id':message_obj.reply_to.reply_to_top_id,
                    'reply_message_forum_topic':message_obj.reply_to.forum_topic
                }
                if reply_obj.from_id is not None:
                    if isinstance(reply_obj.from_id, PeerUser):
                        reply_msg['reply_message_from_id'] = reply_obj.from_id.user_id
                    elif isinstance(reply_obj.from_id, PeerChat):
                        reply_msg['reply_message_from_id'] =  reply_obj.from_id.chat_id
                    elif isinstance(reply_obj.from_id, PeerChannel):
                        reply_msg['reply_message_from_id'] = reply_obj.from_id.channel_id
                else:
                    reply_msg['reply_message_from_id'] = 000000000

        chat_user_id  =  event.sender_id
        if chat_user_id == None:
            if event.message.from_id == None:
                chat_user_id = 000000000
            elif isinstance(event.message.from_id, PeerUser):
                chat_user_id = event.message.from_id.chat_id
            elif isinstance(event.message.from_id, PeerUser):
                chat_user_id = event.message.from_id.user_id
            elif isinstance(event.message.from_id, PeerChannel):
                chat_user_id = event.message.from_id.channel_id

        # 获得消息的 channel 信息
        groupname = None
        groupabout = None
        my_channel_id = self.strip_pre(channel_id)
        for i in self.channel_meta:
            test_channel_id = self.strip_pre(i['channel id'])
            if  test_channel_id == my_channel_id:
                groupname = i['channel name']
                groupabout = i['channel about']
                break

        if groupname == None:
            logging.info (f'New channel :#######################{type(my_channel_id)} : {my_channel_id} :{type(test_channel_id)} : {test_channel_id}####################')
            with self.lock_channe_add :
                if my_channel_id not in self.channel_add:
                    self.channel_add.add(my_channel_id)
                    logging.info(f'{[i for i in self.channel_add]}')
            await self.channel_count()
            for i in self.channel_meta:
                test_channel_id = self.strip_pre(i['channel id'])
                if  test_channel_id == my_channel_id:
                    groupname = i['channel name']
                    groupabout = i['channel about']
                    break

        if not fwd_msg is None:
            fwd_from = {
                'date':fwd_msg['fwd_message_date'],
                'from_id':fwd_msg['fwd_message_from_id'],
                'channel_id':str(message_obj.fwd_from.channel_post)
            }
        else:
            fwd_from = None

        msg_info = {
            'is_post':message_obj.post, # 是否是广播频道的帖子
            'is_legacy':message_obj.legacy,  # 是否为以前遗留的消息
            'is_pinned':message_obj.pinned,    # 当前是否固定此消息
            'is_noforwards':message_obj.noforwards, # 此消息是否可以转发
            'via_bot_id':message_obj.via_bot_id,    # 通过内联模式发送此消息的机器人程序的ID（例如“via@like”）。
            'msg_views':message_obj.views,  # 此消息的浏览次数
            'msg_forwards':message_obj.forwards,    # 当前此消息被转发次数
            'msg_replies':message_obj.replies,    # 当前此消息被回复次数
            'edit_date':message_obj.edit_date,  # 该消息的最后一次编辑时间
            'grouped_id':message_obj.grouped_id,    # 如果该消息属于一组消息，则会拥有相同的 grouped_id
        }

        message_es = {
            'to_id_type':False,
            'media':media,
            'write':int(datetime.timestamp(message_obj.date))*1000,
            'natures':'Unknown',        #可能是自然语言标注
            'classifyTag':None,         #分类标注
            'industry':None,            #可能是页面
            'language':None,            #可能是文本语言检测
            'file_id':None,             #未知      
            'preciseTag':None,          #精度标签？？？
            'company':None,             #内容涉及的公司
            'fwd_from':fwd_from,
            'region':None,              #内容涉及的地区
        }

        message_info = { 
            'message_id':event.message.id,
            'chat_user_id':chat_user_id,
            'chat_user_name':message_obj.post_author,
            'message_attribute':msg_info,
            'account_id':self.account['account_id'],                            # 傀儡账户 id
            'channel_id':channel_id,                                            # 频道的 id
            'channel_name':groupname,
            'channel_about':groupabout,
            'message_text':content,                                      # 消息内容
            'is_bot':is_bot,                                            # 是否机器人发出
            'is_group':is_group,
            'is_private':is_private,
            'is_channel':is_channel , 
            'is_scheduled':is_scheduled,
            'fwd_message':fwd_msg,
            'reply_message':reply_msg,
            'tcreate':message_obj.date,                         # 消息发送时间
            'mentioned_user': mentioned_users,
            'message_channel_size':0,
            'tag':tag,
            'media':media,
            'message_es':message_es,
            'appended':None     #留给以后添加信息
            }

        return message_info

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

        new_message = {
            'message_id':message_info['message_id'],
            'channel_id':message_info['channel_id'],
            'message_txt':message_info['message_text'],
            'mentioned_user_name':message_info['mentioned_user'],
            'sender_id':message_info['chat_user_id'],
            'is_scheduled':message_info['is_scheduled'],
            'is_bot':message_info['is_bot'],
            'is_group':message_info['is_group'],
            'is_private':message_info['is_private'],
            'is_channel':message_info['is_channel'],
            'message_date':message_info['tcreate'].strftime('%Y %m %d:%H %M %S'),
            'fwd_message':message_info['fwd_message'],
            'reply_message':message_info['reply_message'],
            'tag':message_info['tag'],
            'appended':message_info['appended']
            }
        if not new_message['fwd_message'] is None:
            new_message['fwd_message']['fwd_message_date'] = new_message['fwd_message']['fwd_message_date'].strftime('%Y %m %d:%H %M %S')
        if not new_message['reply_message'] is None:
            new_message['reply_message']['reply_message_date'] = new_message['reply_message']['reply_message_date'].strftime('%Y %m %d:%H %M %S')
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

    def updata_es_message(self,message_info:dict):
        """  
        将获得的 message 属性进行转化，等待后续批量上传
        @param message_info: 将要上传的消息字典
        """  
        # 完成 mix 部分的 
        message_date = int(datetime.timestamp(message_info['tcreate']))*1000
        write = message_info['message_es']['write']
        to_id = self.strip_pre(message_info['channel_id'])

        full_msg = {
            'message_id':message_info['message_id'],
            'chat_user_id':message_info['chat_user_id'],
            'chat_user_name':message_info['chat_user_name'],
            'account_id':message_info['account_id'],
            'channel_id':message_info['channel_id'],
            'channel_name':message_info['channel_name'],
            'channel_about':message_info['channel_about'],
            'message_text':message_info['message_text'],
            'is_scheduled':message_info['is_scheduled'],
            'is_bot':message_info['is_bot'],
            'is_group':message_info['is_group'],
            'is_private':message_info['is_private'],
            'is_channel':message_info['is_channel'],
            'fwd_message':message_info['fwd_message'],
            'reply_message':message_info['reply_message'],
            'tag':message_info['tag'],
            'appended':message_info['appended'],
            'tcreate':message_info['tcreate'],
            'message_channel_size':message_info['message_channel_size']
        }

        mix_message = {
            'natures':message_info['message_es']['natures'],
            'id':message_info['message_id'],
            'classifyTag':message_info['message_es']['classifyTag'],
            'to_id':to_id,
            'industry':message_info['message_es']['industry'],
            'language':message_info['message_es']['language'],
            'file_id':message_info['message_es']['file_id'],
            'to_id_type':None,
            'date':message_date,
            'message':message_info['message_text'],
            'from_id':message_info['chat_user_id'],
            'fwd_from':message_info['message_es']['fwd_from'],
            'reply_to_msg_id':message_info['reply_message']['reply_message_id'] if message_info['reply_message'] else None,
            'media':message_info['message_es']['media'],
            'userName':message_info['chat_user_name'],
            'groupName':message_info['channel_name'],
            'groupAbout':message_info['channel_about'],
            'preciseTag':message_info['message_es']['preciseTag'],
            'company':message_info['message_es']['company'],
            'region':message_info['message_es']['region'],
            'write':write,
        }
        es_message = mix_message

        with self.lock_es_message:
            self.es_messages.append(es_message)

    async def download_file(self,event:events):
        """  
        将图片存储到指定路径
        @param event: 消息事件
        """  
        file_name = self.GetImageName(event)
        file_path = self.GetImagePath(event)
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        download_path = file_path+'/' + file_name+'.jpg'
        await event.download_media(download_path)
        logging.info(f'picture down OK')

    def GetImageName(self,event:events)->str:
        """ 
        获得图片文件名，用于存储
        @param event: 消息事件
        @return: 图片名称
        """
        image_name = str(event.message.id)+'_'+str(event.message.grouped_id)
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

    async def updata_message_to_es(self):
        """  
        将获得到的 message 信息批量存入 es 系统中
        """  
        # 获取消息
        es = self.es_connect
        with self.lock_es_message:
            message_info = self.es_messages
            self.es_messages = []

        # 检查 index
        es_index = self.ES_MESSAGE_INDEX
        if not es.indices.exists(index=es_index):
            result = es.indices.create(index=es_index)
            logging.info ('creat index new_message_info')

        # 批量上传
        if not message_info == []: 
            if 'message_id' in message_info[0].keys():
                actions = (
                    {
                        '_index': es_index,
                        '_type': '_doc',
                        '_id': str(Message['channel_id'])+'-'+str(Message['message_id']),
                        '_source': Message,
                    }
                    for Message in message_info
                )
            elif 'to_id' in message_info[0].keys():
                actions = (
                    {
                        '_index': es_index,
                        '_type': '_doc',
                        '_id': str(Message['to_id'])+'-'+str(Message['id']),
                        '_source': Message,
                    }
                    for Message in message_info
                )
        else:
            actions = ()
        n,_ = bulk(es, actions)
        logging.info(f'es data: total:{len(message_info)} message, insert {n} message successful')

    async def analysis_message(self,event:events,channel_id:int)->dict:
        """  
        TODO: 对消息进行处理、分析
        @param event: 新消息事件
        @param channel_id: 消息坐在 channel id(message.to_id.channel_id)
        @return: 从不同角度打标的结果
        """  
        print(f'channel:{channel_id} and type:{type(channel_id)}')
        await self.extract_virtual_identity(event,channel_id)
        #打标
        tags = await self.tag_message(event)
        return tags

    async def tag_message(self,event:events)->dict:
        """  
        对消息进行打标
        @param event: 新消息事件
        @return: 从不同角度打标的结果
        """  
        return None
        tag = {
            'region':None,
            'language':None,
            'company':None,
            'classifyTag':None,
            'natures':None,
            'preciseTag':None,
        }
        regiontag = self.region_tag(event.raw_text)
        if regiontag != None:
            tag['region'] = ''
            for i in regiontag:
                tag['region'] += i+' '
        language = self.language_tag(event.raw_text)
        company = self.company_tag(event.raw_text)


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
        print (chi)
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

    def company_tag(self,text:str)->list:
        pass

    async def extract_virtual_identity(self,event:events,channel_id:int)->dict:
        """ 
        对消息中的虚拟身份进行提取
        @param event: 新消息事件
        @param channel_id: 消息坐在 channel id(message.to_id.channel_id)
        @return: 不同的虚拟身份提取结果
        """ 
        message_txt = event.raw_text

        # 虚拟身份：qq、微信、手机、url、Email
        # 虚拟身份、channel id、message id

        vir_data = {
            'channel_id':channel_id,
            'message_id':event.message.id,
            'message_txt':message_txt,
            'vir_identity':{
                'wechat':None,
                'qq':None,
                'phone':None,
                'url':None,
                'e-mail':None,
                'id_card':None,
            },
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
        vir_data['vir_identity']['wechat'] = wechat_accounts
        vir_data['vir_identity']['qq'] = qq_accounts
        vir_data['vir_identity']['phone'] = phone_accounts
        vir_data['vir_identity']['url'] = url_address
        vir_data['vir_identity']['e-mail'] = email_accounts
        vir_data['vir_identity']['id_card'] = ids

        if (len(wechat_accounts) + len(qq_accounts) + len(phone_accounts) + len(url_address) + len(email_accounts) + len(ids)) < 1:
            return

        es = self.es_connect

        # 检查 index
        es_index = self.ES_VIR_INDEX
        if not es.indices.exists(index=es_index):
            result = es.indices.create(index=es_index)
            logging.info ('creat index channel')

        es_id = str(vir_data['channel_id'])+str(vir_data['message_id'])

        n = es.index(index=es_index,doc_type='_doc',body=vir_data,id = es_id)
        logging.info(f'es vir_data:{json.dumps(n,ensure_ascii=False,indent=4)} ,{n}')

    def  replace_chinese(self,text:str)->str:
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
        self.updata_channel_to_es(channel_info)

    async def get_channel_info_by_dialog(self,dialog:Dialog):
        """  
        从会话中获得 channel 的信息
        @param dialog: 会话
        @return: 获取到的频道属性
        """  
        # 后面看看怎么获取到 url
        channel_url = 'unknown'

        # 基础部分的属性
        channel_id = dialog.id if dialog.id else None
        channel_name = dialog.name if dialog.name else None
        channel_title = dialog.title if dialog.title else None
        channel_date = dialog.date if dialog.date else None
        is_group = dialog.is_group
        is_channel = dialog.is_channel
        channel_size = self.get_channel_user_count(dialog)
        is_private = True
        is_mega_group = True if dialog.is_group and dialog.is_channel else False
        username = dialog.entity.username if dialog.is_channel else None
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
        except ChannelPublicGroupNaError as e:
            logging.error(f'channel/supergroup not available: {dialog.id}')
        except TimeoutError as e:
            logging.error(f'A timeout occurred while fetching data from the worker: {dialog.id}')
        except ChatIdInvalidError as e:
            logging.error(f'Invalid object ID for a chat: {dialog.id}')
        except PeerIdInvalidError as e:
            logging.error (f'An invalid Peer was used: {dialog.id}')

        about = None
        try :
            about = channel_full.full_chat.about
        except AttributeError as e:
            logging.error(f'chat has no attribute full_chat')

        chat_info = None
        channel_info = None

        if is_channel:
            channel_attr = [
                'broadcast',
                'verified',
                'megagroup',
                'restricted',
                'signatures',
                'min',
                'scam',
                'has_link',
                'has_geo',
                'slowmode_enabled',
                'call_active',
                'call_not_empty',
                'fake',
                'gigagroup',
                'noforwards',
                'join_to_send',
                'join_request',
                'forum',
                'access_hash',
                'username'
            ]
            channel_info = {}
            for i in channel_attr:
                if hasattr(dialog.entity,i):
                    channel_info[i] = getattr(dialog.entity,i)
                else:
                    logging.error(f"channel:{dialog.name} don't have {i}")
            full_channel_attr = [
                'hidden_prehistory',
                'can_set_location',
                'has_scheduled',
                'can_view_stats',
                'blocked',
                'antispam',
                'participants_hidden',
                'translations_disabled',
                'admins_count',
                'kicked_count',
                'banned_count',
                'online_count',
                'migrated_from_chat_id',
                'migrated_from_max_id',
                'pinned_msg_id',
                'available_min_id',
                'folder_id',
                'linked_chat_id',
                'slowmode_seconds',
                'slowmode_next_send_date',
                'stats_dc',
                'theme_emoticon',
                'requests_pending',
                'recent_requesters',
            ]
            if channel_full is not None:
                for i in full_channel_attr:
                    if hasattr(channel_full.full_chat,i):
                        channel_info[i] = getattr(channel_full.full_chat,i)
                    else:
                        logging.error(f"full channel: {dialog.name} don't have {i}")
            if dialog.entity.username is not None:
                is_private = False
        elif is_group:
            chat_attr = [
                'version',
                'deactivated',
                'call_active',
                'call_not_empty',
            ]
            chat_info = {}
            for i in chat_attr:
                if hasattr(dialog.entity,i):
                    chat_info[i] = getattr(dialog.entity,i)
                else:
                    logging.error(f"chat: {dialog.name} don't have {i}")
            full_chat_attr = [
                'can_set_username',
                'has_scheduled',
                'translations_disabled',
                'pinned_msg_id',
                'folder_id',
                'theme_emoticon',
                'requests_pending',
                'recent_requesters',
            ]
            if channel_full is not None:
                for i in full_chat_attr:
                    if hasattr(channel_full,'full_chat'):
                        if hasattr(channel_full.full_chat,i):
                            chat_info[i] = getattr(channel_full.full_chat,i)
                        else:
                            logging.error(f"full chat: {dialog.name} don't have {i}")   
       
        # 频道图片获取
        photo_path = r'./picture/dialog/'+str(dialog.id)
        if not os.path.exists(photo_path):
            real_photo_path = None
            try:
                real_photo_path = await self.client.download_profile_photo(dialog.input_entity,file=photo_path)
            except Exception:
                logging.error(Exception)
        else:
            real_photo_path = photo_path

        channel_data = {
            'channel_id':channel_id,            # -100类型
            'channel_name':channel_name,
            'channel_title':channel_title,
            'channel_date':channel_date,        # 时间戳类型，且es为 long 类型
            'channel_about':about,
            'account_id':self.account['account_id'],
            'is_mega_group':is_mega_group,
            'is_group':is_group,
            'is_private':is_private,
            'is_channel':is_channel,
            'channel_size':channel_size,
            'channel_photo':real_photo_path,
            'username':username,
            'chat_info':chat_info,
            'channel_info':channel_info,
            'channel_url':channel_url,
            'is_enable':False,
            'splitter_grade':0,
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
        json_file_name = './channel_info/'+file_date+'_channel_info.json'

        channel_info_data = {
            'channel_id':channel_info['channel_id'],
            'channel_name':channel_info['channel_name'],
            'channel_title':channel_info['channel_title'],
            'channel_date':channel_info['channel_date'].strftime('%Y %m %d:%H %M %S') if channel_info['channel_date'] is not None else None ,
            'channel_url':channel_info['channel_url'],
            'account_id':channel_info['account_id'],
            'is_mega_group':channel_info['is_mega_group'],
            'is_group':channel_info['is_group'],
            'is_private':channel_info['is_private'],
            'is_channel':channel_info['is_channel'],
            'channel_size':channel_info['channel_size'],
            'channel_info':channel_info['channel_info'],
            'chat_info':channel_info['chat_info'],
            'username':channel_info['username'],
            'channel_photo':channel_info['channel_photo'],
            'channel_url':channel_info['channel_url'],
        }
        self.store_data_in_json_file(json_file_name, self.lock_channel,str(channel_info['account_id']), channel_info_data)

    def updata_channel_to_es(self,channel_info:dict):
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

        full_channel = channel_info
        channel_id = self.strip_pre(channel_info['channel_id'])
        if channel_info['channel_date'] == None:
            mix_channel_date = None
        else:
            mix_channel_date = int(datetime.timestamp(channel_info['channel_date']))*1000

        channel_restricted = None
        if channel_info['channel_info'] is not None:
            try:
                channel_restricted = channel_info['channel_info']['restricted']
            except Exception as e:
                logging.error('get data wrong:{e}')
        mix_channel = {
            'participants_count':channel_info['channel_size'],
            'is_enable':channel_info['is_enable'],
            'about':channel_info['channel_about'],
            'photo':channel_info['channel_photo'],
            'id':channel_id,
            'title':channel_info['channel_title'],
            'megagroup':channel_info['is_mega_group'] ,
            'username':channel_info['username'],  
            'splitter_grade':channel_info['splitter_grade'],
            'date':mix_channel_date,
            'restricted':channel_restricted
        }

        # 将要上传的数据
        es_channel = mix_channel

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
        self.updata_users_to_es(e)
    
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
                is_verified = user.verified
                is_restricted = user.restricted
                access_hash = user.access_hash
                tlogin = None
                if  isinstance(user.participant,ChannelParticipant):
                    tlogin = user.participant.date
                modified = None
                photo_path = r'./picture/user/'+str(user_id)
                if not os.path.exists(photo_path):
                    real_photo_path =  await self.client.download_profile_photo(user,file=photo_path)
                    photo = real_photo_path
                else:
                    photo = photo_path
                contact = user.contact
                about = None
                super = False
                is_self = user.is_self
                mutual_contact = user.mutual_contact
                deleted = user.deleted
                bot_chat_history = user.bot_chat_history
                bot_nochats = user.bot_nochats

                user_attr = [
                    'deleted',
                    'min',
                    'bot_inline_geo',
                    'support',
                    'scam',
                    'apply_min_photo',
                    'fake',
                    'bot_attach_menu',
                    'premium',
                    'attach_menu_enabled',
                    'bot_can_edit',
                    'close_friend',
                    'stories_hidden',
                    'stories_unavailable',
                    'bot_info_version',
                    'bot_inline_placeholder',
                    'lang_code',
                    'stories_max_id'
                ]
                user_info={
                    'user_id':user_id,
                    'channel_id':dialog.id,
                    'user_name' : user_name,
                    'first_name' : first_name,
                    'last_name': last_name,
                    'is_bot': is_bot,
                    'is_verified': is_verified,
                    'is_restricted': is_restricted,
                    'user_phone':user_phone,
                    'modified':modified,
                    'access_hash':access_hash,
                    'is_self':is_self,
                    'contact':contact,
                    'mutual_contact':mutual_contact,
                    'deleted':deleted,
                    'bot_chat_history':bot_chat_history,
                    'bot_nochats':bot_nochats,
                    'user_photo':photo,  
                    'user_date':tlogin,
                    'user_about':'unknown',
                    'super':super,
                }
                for i in user_attr:
                    if hasattr(user,i):
                        user_info[i] = getattr(user,i)
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
        json_file_name = './user_info/'+file_date+'_chat_user.json'
        users_info = []
        for user in users_info_list:
            user_info={
                'user_id':user['user_id'],
                'user_name' :user['user_name'],
                'first_name' :user['first_name'] ,
                'last_name':user['first_name'] ,
                'is_bot':user['is_bot'] ,
                'is_verified': user['is_verified'],
                'is_restricted': user['is_restricted'],
                'user_phone':user['user_phone'],
                'tlogin':user['user_date'].strftime('%Y %m %d:%H %M %S') if user['user_date'] is not None else None,
                'modified':user['modified'],
                'access_hash':user['access_hash'],
            }
            users_info.append(user_info)
        self.store_data_in_json_file(json_file_name, self.lock_chat_user, str(dialog.id),users_info)

    def updata_users_to_es(self,users_info:list):
        """  
        将获得到的 channel 的 user 信息存入 es 中
        @param users_info: 需要存储的用户们的属性列表
        """  
        es = self.es_connect

        es_users_info = []
        for user_info in users_info:
            if user_info['user_date']:
                if user_info['user_date'] == None:
                    user_date = None
                else:
                    user_date = int(datetime.timestamp(user_info['user_date']))*1000
            else:
                user_date = None
            
            full_user = {
            }
            for i,j in user_info.items():
                if i == 'user_date':
                    full_user[i] = j.strftime('%Y %m %d:%H %M %S') if j is not None else None
                else:
                    full_user[i] = j       
            
            mix_user = {
                'date':user_date,
                'bot':user_info['is_bot'],
                'about':user_info['user_about'],
                'verified':user_info['is_verified'],
                'last_name':user_info['last_name'],
                'photo':user_info['user_photo'],
                'super':user_info['super'],
                'bot_nochats':user_info['bot_nochats'],
                'deleted':user_info['deleted'],
                'phone':user_info['user_phone'],
                'restricted':user_info['is_restricted'],
                'contact':user_info['contact'],
                'id':user_info['user_id'],
                'mutual_contact':user_info['mutual_contact'],
                'first_name':user_info['first_name'],
                'is_self':user_info['is_self'],
                'bot_chat_history':user_info['bot_chat_history'],
                'username':user_info['user_name'],
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
                '_id':str(User_info['id']),
                '_source':User_info,
            }
            for User_info in es_users_info
        )

        # 将数据进行上传
        n,_ = bulk(es, actions)
        logging.info(f'total:{len(es_users_info)} user, insert {n} user successful')


