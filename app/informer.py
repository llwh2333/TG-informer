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

logging.getLogger().setLevel(logging.INFO)
class TGInformer:
    def __init__(self,
    ):
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

    def stop_bot_interval(self):
        self.bot_task.cancel()

    def ES_rebuilt(self,times):
        """ 
        尝试重新建立 ES 连接
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
        根据当前的配置建立会话，并保存 session ，方便下一次登录
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
        picture_path = './picture'
        if not os.path.exists(picture_path):
            os.makedirs(picture_path)
            logging.info(f'Create the picture dir:{picture_path}')
        photo_path = picture_path+r'/info_picture'
        if not os.path.exists(photo_path):
            os.makedirs(photo_path)
            logging.info(f'Create the picture dir:{photo_path}')
        if self.models == '1':
            message_path = './message'
            if not os.path.exists(message_path):
                os.makedirs(message_path)
                logging.info(f'Create the message dir:{message_path}')

            channel_path = './channel_info'
            if not os.path.exists(channel_path):
                os.makedirs(channel_path)
                logging.info(f'Create the channel info dir:{channel_path}')

            user_path = './user_info'
            if not os.path.exists(user_path):
                os.makedirs(user_path)
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

        # 每隔 1 min 上传一次 message
        while True:
            if self.es_connect != None:
                await self.updata_message_to_es()
            await asyncio.sleep(60)

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

    async def message_dump(self,event,channel_id):
        """   
        处理消息的存储
        """  
        message = event.raw_text
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

    def get_file_md5(self,fname):
        """ 
        计算文件的 md5 值
        """
        m = hashlib.md5()
        with open(fname,'rb') as file:
            while True:
                data = file.read(4096)
                if not data:
                    break
                m.update(data)
        return m.hexdigest()

    async def get_message_info_from_event(self,event,channel_id,tag,media):
        """ 
        从 event 中获得需要的 info
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
        else:
            is_channel = False
            is_group = False
            is_private = False
        is_bot = False if message_obj.via_bot_id is None else True

        mentioned_users = []
        content = event.raw_text
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
            is_mention = False
        else:
            is_mention = True

        is_scheduled = True if message_obj.from_scheduled is True else False

        is_fwd = False if message_obj.fwd_from is None else True
        if is_fwd:
            fwd_message_date = message_obj.fwd_from.date
            fwd_message_send_name = message_obj.fwd_from.from_name
            fwd_message_times = message_obj.forwards
            fwd_message_saved_id = message_obj.fwd_from.saved_from_msg_id
            fwd_message_send_id = None
            if message_obj.fwd_from.from_id is not None:
                if isinstance(message_obj.fwd_from.from_id, PeerUser):
                    fwd_message_send_id = message_obj.fwd_from.from_id.user_id
                elif isinstance(message_obj.fwd_from.from_id, PeerChat):
                    fwd_message_send_id = message_obj.fwd_from.from_id.chat_id
                elif isinstance(message_obj.fwd_from.from_id, PeerChannel):
                    fwd_message_send_id = message_obj.fwd_from.from_id.channel_id
            else:
                fwd_message_send_id = 000000000
        else:
            fwd_message_date = None
            fwd_message_txt = None
            fwd_message_send_name = None
            fwd_message_send_id = None
            fwd_message_saved_id = None
            fwd_message_times = None
        is_reply = False if message_obj.reply_to is None else True
        if is_reply:
            reply_obj = await event.get_reply_message()
            if reply_obj == None:
                is_reply = False
                reply_message_txt = None
                reply_message_send_id = None
                reply_message_id = None
                reply_message_date = None
                reply_message_times = None
            else:
                reply_message_txt = reply_obj.message
                reply_message_send_id = None
                if reply_obj.from_id is not None:
                    if isinstance(reply_obj.from_id, PeerUser):
                        reply_message_send_id = reply_obj.from_id.user_id
                    elif isinstance(reply_obj.from_id, PeerChat):
                        reply_message_send_id = reply_obj.from_id.chat_id
                    elif isinstance(reply_obj.from_id, PeerChannel):
                        reply_message_send_id = reply_obj.from_id.channel_id
                else:
                    reply_message_send_id = 000000000
                reply_message_id = message_obj.reply_to.reply_to_msg_id 
                reply_message_date = reply_obj.date
                reply_message_times = message_obj.replies
        else:
            reply_message_txt = None
            reply_message_send_id = None
            reply_message_id = None
            reply_message_date = None
            reply_message_times = None

        chat_user_id  =  event.sender_id
        if chat_user_id == None:
            if event.message.from_id == None:
                chat_user_id = 000000000
            elif isinstance(event.message.from_id, PeerUser):
                chat_user_id = event.message.from_id.chat_id
            elif isinstance(event.message.from_id, PeerUser):
                chat_user_id = reply_obj.from_id.user_id
            elif isinstance(event.message.from_id, PeerChannel):
                chat_user_id = reply_obj.from_id.channel_id

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
        if is_fwd:
            fwd_from = {
                'date':fwd_message_date,
                'from_id':fwd_message_send_id,
                'channel_id':str(message_obj.fwd_from.channel_post)
            }
        else:
            fwd_from = None
        message_info = { 
            'message_id':event.message.id,
            'chat_user_id':chat_user_id,
            'account_id':self.account['account_id'],                               # 傀儡账户 id
            'channel_id':channel_id,                                            # 频道的 id
            'message_text':event.raw_text,                                      # 消息内容
            'is_bot':is_bot,                                            # 是否机器人发出
            'is_group':is_group,
            'is_private':is_private,
            'is_channel':is_channel ,
            'is_scheduled':is_scheduled,
            'is_fwd':is_fwd,
            'is_mention':is_mention,
            'tcreate':message_obj.date,                         # 消息创建时间
            'mentioned_user': mentioned_users,
            'fwd_message_date':fwd_message_date,
            'fwd_message_send_id':fwd_message_send_id, 
            'fwd_message_send_name':fwd_message_send_name,
            'fwd_message_saved_id':fwd_message_saved_id,
            'fwd_message_times':fwd_message_times,
            'is_reply':is_reply,
            'reply_message_txt':reply_message_txt,
            'reply_message_send_id':reply_message_send_id, 
            'reply_message_id':reply_message_id,
            'reply_message_date':reply_message_date,
            'reply_message_times':reply_message_times,
            'message_channel_size':0,
            'tag':tag,
            'to_id_type':False,
            'username':message_obj.post_author,
            'media':media,
            'groupname':groupname,
            'groupabout':groupabout,
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
        return message_info

    def strip_pre(self,text):
        test_str = str(text)
        if test_str.startswith("-100"):
            test_str = test_str[4:]
        elif test_str.startswith("-"):
            test_str = test_str[1:]
        elif test_str.startswith("100"):
            test_str = test_str[3:]
        return test_str

    def store_message_in_json_file(self,message_info):
        """  
        将 message 存储到本地 json 文件中
        """  
        now = datetime.now()
        file_date =  now.strftime("%y_%m_%d")
        json_file_name = './message/'+file_date+'_messages.json'

        new_message = {
            'message_id':message_info['message_id'],
            'channel_id':message_info['channel_id'],
            'message_txt':message_info['message_text'],
            'sender_id':message_info['chat_user_id'],
            'is_scheduled':message_info['is_scheduled'],
            'is_bot':message_info['is_bot'],
            'is_group':message_info['is_group'],
            'is_private':message_info['is_private'],
            'is_channel':message_info['is_channel'],
            'message_date':message_info['tcreate'].strftime('%Y %m %d:%H %M %S')
            }
        if (message_info['is_mention']):
            mention_data = {
                'is_mention':message_info['is_mention'],
                'mentioned_user_name':message_info['mentioned_user']
            }
        else:
            mention_data = {
                'is_mention':message_info['is_mention'],
            }
        new_message.update(mention_data)

        if (message_info['is_fwd']):
            fwd_data = {
                'is_fwd':message_info['is_fwd'],
                'fwd_message_send_id':message_info['fwd_message_send_id'],
                'fwd_message_send_name':message_info['fwd_message_send_name'],
                'fwd_message_times':message_info['fwd_message_times'],
                'fwd_message_saved_id':message_info['fwd_message_saved_id'],
                'fwd_message_date':message_info['fwd_message_date'].strftime('%Y %m %d:%H %M %S')
            }
        else:
            fwd_data = {
                'is_fwd':message_info['is_fwd'],
            }
        new_message.update(fwd_data)

        if (message_info['is_reply']):
            reply_data = {
                'is_reply':message_info['is_reply'],
                'reply_message_txt':message_info['reply_message_txt'],
                'reply_message_send_id':message_info['reply_message_send_id'],
                'reply_message_id':message_info['reply_message_id'],
                'reply_message_times':message_info['reply_message_times'],
                'reply_message_date':message_info['reply_message_date'].strftime('%Y %m %d:%H %M %S')
            }
        else:
            reply_data = {
                'is_reply':message_info['is_reply']
            }
        new_message.update(reply_data)

        self.store_data_in_json_file(json_file_name, self.lock_message, 'messages', new_message)

    def store_data_in_json_file(self,file_name,lock,data_key,data):
        """ 
        打开 json 文件，并将数据存入
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

    def updata_es_message(self,message_info):
        """  
        将获得的 message 信息存入字典中，等待后续批量上传
        """  
        # 获取数据
        message_date = int(datetime.timestamp(message_info['tcreate']))*1000
        write = int(datetime.timestamp(message_info['write'])*1000)
        to_id = self.strip_pre(message_info['channel_id'])
        es_message = {
            'natures':message_info['natures'],
            'id':message_info['message_id'],
            'classifyTag':message_info['classifyTag'],
            'to_id':to_id,
            'industry':message_info['industry'],
            'language':message_info['language'],
            'file_id':message_info['file_id'],
            'to_id_type':None,
            'date':message_date,
            'message':message_info['message_text'],
            'from_id':message_info['chat_user_id'],
            'fwd_from':message_info['fwd_from'],
            'reply_to_msg_id':message_info['reply_message_id'] if message_info['is_reply'] else None,
            'media':message_info['media'],
            'userName':message_info['username'],
            'groupName':message_info['groupname'],
            'groupAbout':message_info['groupabout'],
            'preciseTag':message_info['preciseTag'],
            'company':message_info['company'],
            'region':message_info['region'],
            'write':write,
        }

        with self.lock_es_message:
            self.es_messages.append(es_message)

    async def download_file(self,event):
        """  
        将图片存储到指定路径
        """  
        file_name = self.GetImageName(event)
        file_path = self.GetImagePath(event)
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        download_path = file_path+'/' + file_name+'.jpg'
        await event.download_media(download_path)
        logging.info(f'picture down OK')

    def GetImageName(self,event):
        """ 
        获得图片文件名，用于存储
        """
        image_name = str(event.message.id)+'_'+str(event.message.grouped_id)
        return image_name

    def GetImagePath(self,event):
        """ 
        获得图片的存储路径
        """
        file_path = './picture'
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
        actions = (
            {
                '_index': es_index,
                '_type': '_doc',
                '_id': str(Message['to_id'])+'-'+str(Message['id']),
                '_source': Message,
            }
            for Message in message_info
        )
        n,_ = bulk(es, actions)
        logging.info(f'es data: total:{len(message_info)} message, insert {n} message successful')

    async def analysis_message(self,event,channel_id):
        """  
        TODO: 对消息进行处理、分析
        """  
        await self.extract_virtual_identity(event,channel_id)
        #打标
        tags = await self.tag_message(event)
        return tags

    async def tag_message(self,event):
        """  
        对消息进行打标
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

    def region_tag(self,text):
        pass

    def language_tag(self,text):
        result = None
        try:
            result = detect(text)
        except Exception as e:
            logging.error(e)
        return result

    def company_tag(self,text):
        pass

    async def extract_virtual_identity(self,event,channel_id):
        """ 
        对消息中的虚拟身份进行提取
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

    def  replace_chinese(self,text):
        """ 
        去除中文，替换位空格(默认非空字符串)
        """  
        filtrate = re.compile(u'[\u4E00-\u9FA5]')
        text_without_chinese = filtrate.sub(r' ', text)
        return text_without_chinese

    def phone_extract(self,text):
        """  
        手机号提取
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

    def email_extract(self,text):
        """  
        email 提取
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

    def ids_extract(self,text):
        """ 
        身份证号提取
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

    def url_extract(self,text):
        """  
        url 提取
        """ 
        extractor = URLExtract()
        url_address = extractor.find_urls(text)
        return url_address

    def qq_extract(self,text):
        """  
        qq 提取
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

    def wechat_extract(self,text):
        """  
        wechat 提取
        """ 
        wechat_pattern = r'\b[a-zA-Z_]\w{5,19}\b'
        wechat_accounts = re.findall(wechat_pattern, text)
        return wechat_accounts

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

    async def dump_channel_info(self,channel_info):
        """
        将 channel 信息存储下来
        """  
        if self.models == '1':
            self.store_channel_info_in_json_file(channel_info)
        self.updata_channel_to_es(channel_info)

    async def get_channel_info_by_dialog(self,dialog):
        """  
        从会话中获得 channel 的信息
        """  
        # 后面看看怎么获取到 url
        channel_url = 'unknown'

        channel_access_hash = None
        is_private = True
        if dialog.is_channel:
            channel_access_hash = dialog.entity.access_hash
            if dialog.entity.username is not None:
                is_private = False
            else:
                is_private = True

        channel_size = self.get_channel_user_count(dialog)
        channel_full = None
        about = None
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
        
        try :
            about = channel_full.full_chat.about
        except AttributeError as e:
            logging.error(f'chat has no attribute full_chat')
                
        photo_path = r'./picture/info_picture/'+str(dialog.id)
        real_photo_path = None
        real_photo_path = await self.client.download_media(dialog.entity.photo)

        channel_id = dialog.id if dialog.id else None
        channel_name = dialog.name if dialog.name else None
        channel_title = dialog.title if dialog.title else None
        is_mega_group = True if dialog.is_group and dialog.is_channel else False
        is_group = dialog.is_group
        is_restricted = dialog.entity.restricted if dialog.is_channel else None
        is_broadcast = dialog.is_channel
        username = dialog.entity.username if dialog.is_channel else None
        channel_date = dialog.entity.date
        is_restricted = dialog.entity.restricted if dialog.is_channel else None
        
        channel_info = {
            'channel_id':channel_id,            # -100类型
            'channel_name':channel_name,
            'channel_title':channel_title,
            'account_id':self.account['account_id'],
            'is_mega_group':is_mega_group ,
            'is_group':is_group,
            'channel_url':channel_url,
            'is_private':is_private,
            'is_restricted':is_restricted,
            'is_broadcast':is_broadcast,
            'access_hash':channel_access_hash,
            'channel_size':channel_size,
            'channel_about':about,
            'channel_photo':real_photo_path,
            'username':username,
            'channel_date':channel_date,        #时间戳类型，且es为 long 类型
            'is_enable':False,
            'splitter_grade':0,
            'is_restricted':is_restricted,
            }
        return channel_info

    def get_channel_user_count(self,dialog):
        """  
        获得 channel 的用户人数
        """  
        size = 0
        if dialog.is_channel:
            size = dialog.entity.participants_count
        elif dialog.is_group:
            size = dialog.entity.participants_count
        return size

    def store_channel_info_in_json_file(self,channel_info):
        """  
        将 channel 信息存储到 json 文件中
        """  
        now = datetime.now()
        file_date = now.strftime("%y_%m_%d")
        json_file_name = './channel_info/'+file_date+'_channel_info.json'

        channel_info_data = {
            'channel_id':channel_info['channel_id'],
            'channel_name':channel_info['channel_name'],
            'channel_title':channel_info['channel_title'],
            'channel_url':channel_info['channel_url'],
            'account_id':channel_info['account_id'],
            'is_mega_group':channel_info['is_mega_group'],
            'is_group':channel_info['is_group'],
            'is_private':channel_info['is_private'],
            'is_broadcast':channel_info['is_broadcast'],
            'channel_access_hash':channel_info['access_hash'],
            'channel_size':channel_info['channel_size'],
        }

        self.store_data_in_json_file(json_file_name, self.lock_channel,str(channel_info['account_id']), channel_info_data)

    def updata_channel_to_es(self,channel_info):
        """  
        将获得到的 channel 信息存入 es 中
        """  
        es = self.es_connect

        # 检查 index
        es_index = self.ES_CHANNEL_INDEX
        if not es.indices.exists(index=es_index):
            result = es.indices.create(index=es_index)
            logging.info ('creat index channel')

        channel_id = self.strip_pre(channel_info['channel_id'])
        channel_date = int(datetime.timestamp(channel_info['channel_date']))*1000
        # 将要上传的数据
        es_channel = {
            'date':channel_date,
            'participants_count':channel_info['channel_size'],
            'is_enable':channel_info['is_enable'],
            'restricted':channel_info['is_restricted'],
            'about':channel_info['channel_about'],
            'splitter_grade':channel_info['splitter_grade'],
            'photo':channel_info['channel_photo'],
            'id':channel_id,
            'title':channel_info['channel_title'],
            'megagroup':channel_info['is_mega_group'] ,
            'username':channel_info['username'],  
        }
        es_id = channel_id

        # 将数据进行上传
        n = es.index(index=es_index,doc_type='_doc',body=es_channel,id = es_id)
        logging.info(f'es data:{json.dumps(n,ensure_ascii=False,indent=4)} , {n}')

    async def dump_channel_user_info(self,dialog):
        """  
        将会话的所有成员的信息存储下来
        """  
        e = await self.get_users_info_from_dialog(dialog)
        if e == []:
            return 
        if self.models == '1':
            self.store_users_info_in_json_file(e,dialog)
        self.updata_users_to_es(e)
    
    async def get_users_info_from_dialog(self,dialog):
        """ 
        获取当前会话的所有成员信息
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

                photo_path = r'./picture/info_picture/'+str(user_id)
                real_photo_path =  await self.client.download_media(user.photo,file=photo_path)
                photo = real_photo_path
                contact = user.contact
                about = None
                super = False
                is_self = user.is_self
                mutual_contact = user.mutual_contact
                deleted = user.deleted
                bot_chat_history = user.bot_chat_history
                bot_nochats = user.bot_nochats
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
                    'tlogin':tlogin,
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

                users_info_list.append(user_info)
                count += 1
        except FloodWaitError as e:
            logging.error(f'get a floodwaiterror {e}')

        logging.info(f'Logging the users account {count} ... \n')
        return users_info_list

    def store_users_info_in_json_file(self,users_info_list,dialog):
        """  
        将获得的 user 列表信息存储到本地 json 中
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
                'tlogin':user['tlogin'].strftime('%Y %m %d:%H %M %S') if user['tlogin'] is not None else None,
                'modified':user['modified'],
                'access_hash':user['access_hash'],
            }
            users_info.append(user_info)
        self.store_data_in_json_file(json_file_name, self.lock_chat_user, str(dialog.id),users_info)

    def updata_users_to_es(self,users_info):
        """  
        将获得到的 channel 的 user 信息存入 es 中
        """  
        es = self.es_connect

        es_users_info = []
        for user_info in users_info:
            if user_info['user_date']:
                user_date = int(datetime.timestamp(user_info['user_date']))*1000
            else:
                user_date = None
            es_user_info={
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
                '_id':str(User_info['user_id']),
                '_source':User_info,
            }
            for User_info in users_info
        )

        # 将数据进行上传
        n,_ = bulk(es, actions)
        logging.info(f'total:{len(users_info)} user, insert {n} user successful')


