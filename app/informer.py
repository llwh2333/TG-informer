import sys
import os
import json
import re
import asyncio
import gspread
import logging
import build_database
import sqlalchemy as db
from datetime import datetime, timedelta
from random import randrange
from telethon import utils
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError, InterfaceError, ProgrammingError
from telethon.tl.functions.users import GetFullUserRequest
from telethon import TelegramClient, events
from telethon.tl.types import PeerUser, PeerChat, PeerChannel
from telethon.errors.rpcerrorlist import FloodWaitError, ChannelPrivateError, UserAlreadyParticipantError
from telethon.tl.functions.channels import  JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from oauth2client.service_account import ServiceAccountCredentials
from models import Account, Channel, ChatUser, Message
import threading
import json

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
        # 数据库的配置参数
        db_database = os.environ['MYSQL_DATABASE'],
        db_user = os.environ['MYSQL_USER'],
        db_password = os.environ['MYSQL_PASSWORD'],
        db_ip_address = os.environ['MYSQL_IP_ADDRESS'],
        db_port = os.environ['MYSQL_PORT'],

        #傀儡账号配置参数
        tg_account_id = os.environ['TELEGRAM_ACCOUNT_ID'],
        tg_notifications_channel_id = os.environ['TELEGRAM_NOTIFICATIONS_CHANNEL_ID'],
        tg_phone_number = os.environ['TELEGRAM_ACCOUNT_PHONE_NUMBER']
        ): 

        # 实例变量
        self.channel_list = []                      # 当前已加入的 channel
        self.channel_meta = {}                      # 已加入 channel 的信息
        self.bot_task = None
        self.CHANNEL_REFRESH_WAIT = 15 * 60         # 重新检查的间隔（15min）
        self.MIN_CHANNEL_JOIN_WAIT = 30
        self.MAX_CHANNEL_JOIN_WAIT = 120
        self.client = None
        self.loop = asyncio.get_event_loop()
        self.lock_message = threading.Lock()
        self.lock_channel = threading.Lock()
        self.lock_chat_user = threading.Lock()

        # 展示横幅
        print(banner)

        # 获取程序环境与引擎
        self.SERVER_MODE = os.environ['ENV']
        self.MYSQL_CONNECTOR_STRING = f'mysql+mysqlconnector://{db_user}:{db_password}@{db_ip_address}:{db_port}/{db_database}?charset=utf8mb4&collation=utf8mb4_general_ci'

        # 设置通知的频道
        self.monitor_channel = tg_notifications_channel_id

        # 检测是否有傀儡账户
        if not tg_account_id:
            raise Exception('Must specify "tg_account_id" in informer.env file for bot instance')

        # 调用 build_database.py 对数据库开始建立
        logging.info(f'Setting up MySQL connector with connector string: {self.MYSQL_CONNECTOR_STRING} ... \n')     
        self.engine = db.create_engine(self.MYSQL_CONNECTOR_STRING)  #
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

        # 检测数据库是否已经初始化过了
        self.tg_user = None
        try:
            # 尝试从数据库中获取我们配置文件中的傀儡账户
            self.account = self.session.query(Account).filter_by(account_id=tg_account_id).first()
        except ProgrammingError as e:
            # 如果无法从数据库中获取，利用配置文件对数据库进行初始化
            logging.error(f'Received error {e} \n Database is not set up, setting it up')
            build_database.initialize_db()
            # 重新尝试获取傀儡账户的
            self.account = self.session.query(Account).filter_by(account_id=tg_account_id).first()

        # 如果仍旧无法获取傀儡账户，就报错
        if not self.account:
            raise Exception(f'Invalid account_id {tg_account_id} for bot instance')

        # 执行协程，即开始我们的监控
        self.session.close()
        self.loop.run_until_complete(self.bot_interval())
        logging.info('the monitor will done?????')

    async def get_channel_info_by_dialog(self,dialog):
        """ 
        从会话中获得 channel 的信息
        （待完善）
        """ 
        #后面看看怎么获取到 url
        channel_url = 'unknown'

        channel_access_hash = None
        if dialog.is_channel:
            channel_access_hash = dialog.entity.access_hash

        #后面调用函数获得 channel 大小
        channel_size = 0

        channel_info = {
            'channel_id':dialog.id if dialog.id else None,
            'channel_name':dialog.name if dialog.name else None,
            'channel_title':dialog.title if dialog.title else None,
            'channel_url':channel_url,
            'account_id':self.account.account_id,
            'channel_is_mega_group':True if dialog.is_group and dialog.is_channel else False ,
            'channel_is_group':dialog.is_group,
            'channel_is_private':None,
            'channel_is_broadcast':dialog.is_channel,
            'channel_access_hash':channel_access_hash,
            'channel_size':channel_size,
            }
        return channel_info

    def store_channel_info_in_json_file(self,channel_info):
        """ 
        将 channel 信息存储到 json 文件中
        """ 
        lock = threading.Lock()
        now = datetime.now()
        file_data = now.strftime("%d_%m_%y")
        json_file_name = file_data+'_channel_info.json'

        self.store_data_in_json_file(json_file_name, self.lock_channel,channel_info['account_id'], channel_info)

    def dump_channel_info(self,channel_info):
        """ 
        将 channel 信息存储下来
        """ 
        self.store_channel_info_in_json_file(channel_info)
        self.store_channel_info_in_sql(channel_info)

    def get_user_info_from_dialog(self,dialog):
        """ 
        获取当前会话的所有成员信息
        （之前有一些 bug 等后面排除）
        """ 
        
        users_info_list = {}
        if str(abs(dialog.id))[:3] == '100':
            channel_id =dialog.id
        else :
            channel_id = int('-100'+str(abs(dialog.id)))
        channel = self.client.get_entity(PeerChat(channel_id))
        users = self.client.get_participants(channel)
        count = 0
        for user in users:
            user_name = user.username
            first_name = user.first_name
            last_name = user.last_name
            is_bot = user.bot
            user_phone = user.phone
            is_verified = user.verified
            is_restricted = user.restricted
            tlogin = None
            modified = None
            user_info={
                'user_name' : user_name,
                'first_name' : first_name,
                'last_name': last_name,
                'is_bot': is_bot,
                'is_verified': is_verified,
                'is_restricted': is_restricted,
                'user_phone':user_phone,
                'tlogin':None,
                'modified':None,
            }
            users_info_list[str(user_info['first_name']+user_info['last_name']+str(count))] = user_info
            count += 1
        return users_info_list

    def store_user_info_in_json_file(self,user_info_list,dialog):
        """ 
        将获得的 user 列表信息存储到本地 json 中
        """ 
        now = datetime.now()
        file_data =  now.strftime("%d_%m_%y")
        json_file_name = file_data+'_chat_user.json'

        self.store_data_in_json_file(json_file_name, self.lock_chat_user, dialog.name,user_info_list)

    def dump_channel_user_info(self,dialog):
        """ 
        将会话的所有成员的信息存储下来
        """ 
        e = self.get_user_info_from_dialog(dialog)
        self.store_user_info_in_json_file(e,dialog)
        self.store_user_info_in_sql(e,dialog)

    async def get_message_info_from_event(self,event,channel_id):
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

        message_info = {
            'message_id':event.message.id,
            'chat_user_id':event.sender_id,
            'account_id':self.account.account_id,                               # 傀儡账户 id
            'channel_id':channel_id,                                            # 频道的 id
            'message_text':event.raw_text,                                      # 消息内容
            'message_is_scheduled':message_obj.from_scheduled,                  # 是否预设发送
            'message_is_bot':is_bot,                                            # 是否机器人发出
            'message_is_group':is_group,
            'message_is_private':is_private,
            'message_is_channel':is_channel ,
            'message_tcreate':datetime.now()
            }
        return message_info

    def store_data_in_json_file(self,file_name,lock,data_key,data,):
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
            else:
                with open(file_name,'r') as f:
                    file_data = json.load(f)
            try:
                file_data[data_key].append(data)
            except KeyError:
                file_data[data_key] = []
                file_data[data_key].append(data)
            json_data = json.dumps(file_data,ensure_ascii=False,indent=4)
            with open(file_name,'w') as f:
                f.write(json_data)

    def store_message_in_json_file(self,message_info):
        """
        将获得的消息信息，存入json 文件中
        """
        now = datetime.now()
        file_data =  now.strftime("%d_%m_%y")
        json_file_name = file_data+'_messages.json'

        new_message = {
            'channel_id':message_info['channel_id'],
            'message_data':message_info['message_text'],
            'sender_id':message_info['chat_user_id'],
            'is_bot':message_info['message_is_bot'],
            'is_group':message_info['message_is_group'],
            'is_private':message_info['message_is_private'],
            'is_channel':message_info['message_is_channel']
            }

        self.store_data_in_json_file(json_file_name, self.lock_message, 'messages', new_message)

    def flush_status_in_sql(self,message_info):
        """ 
        根据 message 更新一些状态信息
        用户的登录时间
        """ 
        status_session = self.Session()
        user_object = status_session.query(ChatUser).filter_by(chat_user_id=message_info['chat_user_id']).first()
        if user_object:
            user_object.chat_user_tlogin = message_info['message_tcreate']
        else:
            return

    def GetImageName(self,event):
        """ 
        获得图片文件名，用于存储
        """ 
        now = datetime.now()
        file_data = now.strftime("%d_%m_%y")
        image_name = event.message.file.name
        if image_name == None:
            image_name='no_name'
        file_name = file_data+image_name+str(event.sender_id)
        return file_name

    async def download_file(self,event,file_path):
        """ 
        将图片存储到指定路径
        """ 
        if event.photo:
            pass
        else:
            logging.info(f'not picture ')
            return 
        file_name = self.GetImageName(event)
        download_path = file_path+'/' + file_name+'.jpg'
        await event.download_media(download_path)
        logging.info(f'picture down OK')
        """
        mime_type = 'unknown/unknown'
        if hasattr(event.message.media, 'document'):
            mime_type = event.message.media.document.mime_type
        elif hasattr(event.message.media, 'photo'):
            mime_type = 'image/jpg'
        meida_type = mime_type.split('/')

        file_name = 'unknown'
        if (meida_type == 'image'):
            file_name = self.GetImageName(event)
        else:
            return 
        download_path = file_path+'/' + file_name+'.jpg'
        await event.download_media(download_path)
        logging.info(f'picture down OK')
        """
        #await event.message.download_media(download_path)

    async def message_dump(self,event):
        """ 
        将收到的消息进行存储，存储到数据库和 json 文件中
        """ 

        #检查消息是否含有图片，如果有图片，就存储图片到本地（picture 文件中）
        #图片名称：time_message_id
        if event.message.media is not None:
            file_path = './picture'
            logging.info(f'the message have media')
            await self.download_file(event,file_path)

        message = event.raw_text

        if isinstance(event.message.to_id, PeerChannel):
            channel_id = event.message.to_id.channel_id
            if channel_id == self.monitor_channel:
                logging.info(f'the message is from monitor channel')
                return
            logging.info(f'........get the channel message is ({message})!!!!!!!!!!!!!!!')
        # 如果是群组，获得群组的 id
        elif isinstance(event.message.to_id, PeerChat):
            channel_id = event.message.chat_id
            logging.info(f'........get the chat message is ({message})!!!!!!!!!!!!!!!')
        else:
            # 两者均不是，跳过
            return
    
        e = await self.get_message_info_from_event(event,channel_id)
        #self.flush_status_in_sql(e)
        self.store_message_in_json_file(e)
        self.store_message_in_sql(e)

    async def init_monitor_channels(self):
        """ 
        初始化要监控的频道
        """ 
        logging.info('Running the monitor to channels')

        # 处理新消息
        @self.client.on(events.NewMessage)
        async def message_event_handler(event):
            # 通过协程存储当前的新消息
            await self.message_dump(event)
            
        #join_channel()

        async for dialog in self.client.iter_dialogs():
            e = await self.get_channel_info_by_dialog(dialog)
            self.channel_list.append(e['channel_id'])

            self.channel_meta[e['channel_id']] = {
               'channel_id': e['channel_id'],
               'channel_title': e['channel_title'],
               'channel_url': e['channel_url'],
               'channel_size': e['channel_size'],
            }

            #self.dump_channel_info(e)
            #self.dump_channel_user_info(dialog)

        @self.client.on(events.ChatAction)
        async def channel_action_handler(event):
            await self.updata_channel_user_info(event)

        logging.info(f"{sys._getframe().f_code.co_name}: Monitoring channels: {json.dumps(self.channel_list, indent=4)}")
        logging.info(f'Channel METADATA: {self.channel_meta}')

    def stop_bot_interval(self):
        self.bot_task.cancel()

    def check_informer_info(self):
        """ 
        每隔一定时间，对于 sql 和账户内容进行对齐
        """ 
        self.check_channels_info_in_sql()
        self.check_channels_user_info_in_sql()

    async def channel_count(self):
        """ 
        统计当前账户中的 channel 的数量
        """
        count = 0
        channel_list = []
        async for dialog in self.client.iter_dialogs():
            # 会话不能是用户间的对话
            if not dialog.is_user:
                # 将 channel 加入现在可监控的 channel 列表
                channel_list.append({
                    'channel id':dialog.id,
                    'channel name':dialog.name
                    })
                count +=1
                logging.info(f'{sys._getframe().f_code.co_name}: Monitoring channel: {json.dumps(channel_list, indent=4)}')
        logging.info(f'Count:{count}')

    async def bot_interval(self):
        """ 
        根据当前的配置建立会话，并保存 session ，方便下一次登录
        """ 
        logging.info(f'Logging in with account # {self.account.account_phone} ... \n')

        # 用来存储会话文件的地址，方便下一次的会话连接
        #session_file = self.account.account_phone.replace('+', '' )+'.session'
        logging.info('bot_name is the path')

        # 实例化一个 tg 端对象，初次登录会记录指定路径中，后续登录会直接使用以前的账户信息
        self.client = TelegramClient('bot_name', self.account.account_api_id, self.account.account_api_hash)

        # 异步的启动这个实例化的 tg 客户端对象，其中手机号为配置文件中的手机号
        await self.client.start(phone=f'{self.account.account_phone}')

        # 检查当前用户是否已经授权使用 API
        if not await self.client.is_user_authorized():
            logging.info(f'Client is currently not logged in, please sign in! Sending request code to {self.account.account_phone}, please confirm on your mobile device')
            
            # 当发现没有授权时，向手机号发送验证码
            await self.client.send_code_request(self.account.account_phone)
            self.tg_user = await self.client.sign_in(self.account.account_phone, input('Enter code: '))
        
        #获取当前的傀儡账户信息
        self.tg_user = await self.client.get_me()

        # 统计 channel 数量和初始化监控频道
        # await self.init_keywords()
        await self.channel_count()
        await self.init_monitor_channels()
        

        # 循环
        count = 0
        while True:
            count +=1
            logging.info(f'### {count} Running bot interval')

            await self.channel_count()
            self.check_informer_info()
            await asyncio.sleep(self.CHANNEL_REFRESH_WAIT)

    def check_channels_user_info_in_sql(self):
        """ 
        TODO: 将 sql 中的 channel 参与者信息与 account 中的保持一致 
        """ 
        pass

    def check_channels_info_in_sql(self):
        """ 
        TODO: 将 sql 中的 channel 信息与 account 中的保持一致 
        """ 
        pass

    def store_message_in_sql(self,message_info):
        """
        TODO:将获得的消息信息存储进入 sql 中(暂时不弄)
        """
        pass

    def join_channel(self):
        """ 
        TODO:根据数据库中未加入的频道信息，加入频道（暂时不弄，下面的是以前写的）
        """ 
        pass

    def store_user_info_in_sql(self,user_info_list,dialog):
        """ 
        TODO:将获得的 user 列表信息存储到 sql 库中(暂时不弄)
        """ 
        pass

    def store_channel_info_in_sql(self,channel_info):
        """ 
        TODO:将 channel 信息存储到 sql 中
        """ 
        pass

    async def get_channel_user_count(self,channel_id):
        """ 
        获得 channel 的用户人数
        """ 
        return 0

    def updata_channel_user_info(self,event):
        """ 
        TODO: 根据 channel 的成员变动事件，更新 channel 成员
        """ 

    def updata_channel_user_info(self,event):
        """ 
        TODO: 根据 channel 的成员变动事件，更新 channel 成员
        """ 

