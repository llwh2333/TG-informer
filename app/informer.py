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

    def updata_channel_user_info(self,event):
        """ 
        TODO: 根据 channel 的成员变动事件，更新 channel 成员
        """ 

    async def get_channel_user_count(self,channel_id):
        """ 
        获得 channel 的用户人数
        """ 
        data = await self.client.get_entity(PeerChannel(-channel))
        users = await self.client.get_participants(data)
        return users.total

    def get_channel_info_by_dialog(self,dialog):
        """ 
        从会话中获得 channel 的信息
        """ 
        channel_url = f'https://t.me/{dialog.entity.username}'

        if dialog.is_channel:
            channel_access_hash = dialog.entity.access_hash
        elif dialog.is_group:
            channel_access_hash = None
        channel_info = {
            'channel_id':dialog.id,
            'channel_name':dialog.name,
            'channel_title':dialog.title,
            'channel_url':channel_url,
            'account_id':self.account.account_id,
            'channel_is_mega_group':True if  dialog.is_group and dialog.is_channel else False ,
            'channel_is_group':dialog.is_group,
            'channel_is_private':None,
            'channel_is_broadcast':dialog.is_channel,
            'channel_access_hash':channel_access_hash,
            'channel_size':dialog.entity.participants_count,
            }
        return channel_info
        pass

    def store_channel_info_in_json_file(self,channel_info):
        """ 
        将 channel 信息存储到 json 文件中
        """ 
        lock = threading.Lock()
        now = datetime.now()
        file_data =  now.strftime("%d_%m_%y")
        json_file_name = file_data+'_channel_info.json'

        self.store_data_in_json_file(json_file_name, self.lock_channel,channel_info['account_id'], channel_info)

    def store_channel_info_in_sql(self,channel_info):
        """ 
        TODO:将 channel 信息存储到 sql 中
        """ 
        pass

    def dump_channel_info(self,channel_info):
        """ 
        将 channel 信息存储下来
        """ 
        self.store_channel_info_in_json_file(channel_info)
        self.store_channel_info_in_sql(channel_info)

    def get_user_info_from_dialog(self,dialog):
        """ 
        获取当前会话的所有成员信息
        """ 
        users_info_list = {}
        for user in client.get_participants(dialog.entity):
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
                'modified':None
            }
            users_info_list[user_info['user_name']] = user_info
        return users_info_list

    def store_user_info_in_json_file(self,user_info_list,dialog):
        """ 
        将获得的 user 列表信息存储到本地 json 中
        """ 
        now = datetime.now()
        file_data =  now.strftime("%d_%m_%y")
        json_file_name = file_data+'_chat_user.json'

        self.store_data_in_json_file(json_file_name, self.lock_chat_user, dialog.name,user_info_list)

    def store_user_info_in_sql(self,user_info_list,dialog):
        """ 
        TODO:将获得的 user 列表信息存储到 sql 库中(暂时不弄)
        """ 
        pass

    def dump_channel_user_info(self,dialog):
        """ 
        将会话的所有成员的信息存储下来
        """ 
        e = self.get_user_info_from_dialog(dialog)
        self.store_user_info_in_json_file(e,dialog)
        self.store_user_info_in_sql(e,dialog)

    def join_channel(self):
        """ 
        TODO:根据数据库中未加入的频道信息，加入频道（暂时不弄，下面的是以前写的）
        """ 
        """ 
        #记录现在已经加入的 channel
        current_channels = []

        # 获取傀儡账户的现存的会话信息
        for dialog in self.client.iter_dialogs():
            # 会话的 channel id
            channel_id = dialog.id

            # 会话不能是用户间的对话
            if not dialog.is_user:
                # 如果 channel id 是正整数，则去除前三位，取正整数channel id
                if str(abs(channel_id))[:3] == '100':
                    channel_id = int(str(abs(channel_id))[3:])
                # 将 channel 加入现在可监控的 channel 列表
                current_channels.append(channel_id)
            
        ########################### 获取数据库中待添加的 channel
        self.session = self.Session()
        Channels = self.session.query(Channel).filter_by(account_id=self.account.account_id).all()

        channel_to_join = []
        for channel in Channels:
            channel_data = {
                'channel_id': channel.channel_id, 
                'channel_name': channel.channel_name,
                'channel_title': channel.channel_title,
                'channel_url': channel.channel_url,
                'account_id': channel.account_id,
                'channel_is_megagroup': channel.channel_is_mega_group,
                'channel_is_group': channel.channel_is_group,
                'channel_is_private': channel.channel_is_private,
                'channel_is_broadcast': channel.channel_is_broadcast,
                'channel_access_hash': channel.channel_access_hash,
                'channel_size': channel.channel_size,
                'channel_is_enabled': channel.channel_is_enabled,
                'channel_tcreate': channel.channel_tcreate
            }
            if channel.channel_is_enabled is False:
                channel_to_join.append(channel_data)
        self.session.close()

        # 将数据库中的 channel 加入账户中
        self.session = self.Session()
        for channel in channel_to_join:
            sql_channel_id = channel['channel_id'] 
            if str(abs(sql_channel_id))[:3] == '100':
                sql_channel_id = int(str(abs(sql_channel_id))[3:])
            # 如果早已加入跳过这个 channel
            if sql_channel_id in current_channels:
                channel['channel_is_enabled'] = True
                continue

            # 通过 url 加入channel
            # 当 channel url 中有 joinchat 时
            if channel['channel_url'] and '/joinchat/' in channel['channel_url']:
                channel_hash = channel['channel_url'].replace('https://t.me/joinchat/', '')
                try:
                    # 导入并加入指定哈希值的聊天组
                    await self.client(ImportChatInviteRequest(hash=channel_hash))
                    sec = randrange(self.MIN_CHANNEL_JOIN_WAIT, self.MAX_CHANNEL_JOIN_WAIT)
                    logging.info(f'sleeping for {sec} seconds')
                    await asyncio.sleep(sec)
                # 成功加入
                except InviteRequestSentError as e:
                    logging.info(f'sussesful join the channel: {channel["channel_url"]}')
                    channel['channel_is_enabled'] = True
                except UserAlreadyParticipantError as e:
                    logging.info('Already in channel, skipping')
                    channel['channel_is_enabled'] = True
                # 其它错误
                except FloodWaitError as e:
                    logging.info(f'Received FloodWaitError, waiting for {e.seconds} seconds..')
                    await asyncio.sleep(e.seconds * 2)
                except SessionPasswordNeededError as e:
                    logging.info('Two-steps verification is enabled and a password is required.')
                    self.send_notification(channel['channel_url'])
                    # 删除这个 channel 的数据库信息 todo：
                except ChannelPrivateError as e:
                    logging.info('Channel is private or we were banned bc we didnt respond to bot')
                    self.send_notification(channel['channel_url'])
                    # 删除这个 channel 的数据库信息 todo：
        """ 
        pass

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

        
        mentioned_users = []
        for ent, txt in event.get_entities_text():
            if isinstance(ent ,type.MessageEntityMention):
                mentioned_users.append(txt)
        if mentioned_users == []:
            is_mention = False
            mention_user_id = None
        else:
            is_mention = True
            """ 
            user_entities = await self.client.get_entity(mentioned_users)
            mention_id = []
            for entity in user_entities:
                mention_id.append(entity.id)
            mention_user_id = mention_id[0]
            """ 
            mention_user_id = None
        is_fwd = False if message_obj.fwd_from is None else True
        if is_fwd:
            fwd_message_txt = message_obj.fwd_from.data
            fwd_message_seed_id = message_obj.fwd_from.from_id
            fwd_message_date = None
        else:
            fwd_message_txt = None
            fwd_message_seed_id = None
            fwd_message_date = None

        is_reply = False if message_obj.reply_to_msg_id is None else True

        reply_obj = await event.get_reply_message()
        reply_message_txt = reply_obj.message
        reply_message_seed_id = reply_obj.sender
        reply_message_date = reply_obj.date
        if is_reply:
            reply_message_txt = reply_message_txt
            reply_message_seed_id = reply_message_seed_id
            reply_message_date = reply_message_date
        else:
            reply_message_txt = None
            reply_message_seed_id = None
            reply_message_date = None

        if channel_id in self.channel_list:
            channel_size = self.channel_meta[channel_id]['channel_size']
        else :
            channel_size = self.get_channel_user_count(channel_id)

        message_info = {
            'message_id':event.message.id,
            'chat_user_id':event.sender_id,
            'account_id':self.account.account_id,                               # 傀儡账户 id
            'channel_id':channel_id,                                            # 频道的 id
            'message_text':event.raw_text,                                      # 消息内容
            'message_is_mention':is_mention,                                    # 是否提及他人
            'message_mentioned_user_id':mention_user_id,
            'message_is_scheduled':message_obj.from_scheduled,                  # 是否预设发送
            'message_is_fwd':is_fwd,                                            # 是否转发消息
            'fwd_message_txt':fwd_message_txt ,
            'fwd_message_seed_id':fwd_message_seed_id, 
            'fwd_message_date':fwd_message_date ,
            'message_is_reply':is_reply,                                        # 是否是回复
            'reply_message_txt':reply_message_txt ,
            'reply_message_seed_id':reply_message_seed_id, 
            'reply_message_date':reply_message_date,
            'message_is_bot':is_bot,                                            # 是否机器人发出
            'message_is_group':is_group,
            'message_is_private':is_private,
            'message_is_channel':is_channel ,
            'message_channel_size':channel_size,
            'message_tcreate':datetime.now()
            }
        return message_info

    def store_message_in_sql(self,message_info):
        """
        TODO:将获得的消息信息存储进入 sql 中(暂时不弄)
        """
        pass

    def store_data_in_json_file(self,file_name,lock,data_key,data,):
        """ 
        打开 json 文件，并将数据存入
        """ 
        with lock:
            if not os.path.exists(json_file_name):
                with open(file_name,'w') as f:
                    init_json ={}
                    json_first = json.dumps(init_json)
                    f.write(json_first)
                    data = json.load(f)
            else:
                with open(json_file_name,'r') as f:
                    data = json.load(f)
            try:
                data[data_key].append(data)
            except KeyError:
                data[data_key] = data
            json_data = json_dumps(data,indent=4)
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
            'message_data':message_info['message_txt'],
            'sender_id':message_info['chat_user_id'],
            'is_bot':message_info['message_is_bot'],
            'is_group':message_info['message_is_group'],
            'is_private':message_info['message_is_private'],
            'is_channel':message_info['message_is_channel'],
            'channel_size':message_info['message_channel_size'],
            'message_tcreate':message_info['messsage_tcreate'],
            }
        if (message_info['message_is_mention']):
            mention_data = {
                'is_mention':message_info['message_is_mention'],
                'mentioned_user_id':message_info['message_mention_user_id'],
            }
        else:
            mention_data = {
                'is_mention':message_info['message_is_mention']
            }
            pass

        new_message.update(mention_data)
        if (message_info['message_is_fwd']):
            fwd_data = {
                'is_fwd':message_info['message_is_fwd'],
                'fwd_message_txt':message_info['fwd_message_txt'],
                'fwd_message_seed_id':message_info['fwd_message_seed_id'],
                'fwd_message_date':message_info['fwd_message_date']
            }
        else:
            fwd_data = {
                'is_fwd':message_info['message_is_fwd'],
            }
        new_message.update(fwd_data)

        if (message_info['message_is_reply']):
            reply_data = {
                'is_reply':message_info['message_is_reply'],
                'reply_message_txt':message_info['reply_message_txt'],
                'reply_message_seed_id':message_info['reply_message_seed_id'],
                'reply_message_date':message_info['reply_message_date']
            }
            pass
        else:
            reply_data = {
                'reply_message_txt':message_info['reply_message_txt'],
                'reply_message_seed_id':message_info['reply_message_seed_id'],
                'reply_message_date':message_info['reply_message_date']
            }
        new_message.update(reply_data)
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

    async def message_dump(self,event):
        """ 
        将收到的消息进行存储，存储到数据库和 json 文件中
        """ 
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
    
        e = self.get_message_info_from_event(event,channel_id)
        self.flush_status_in_sql(e)
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

        for dialog in self.client.iter_dialogs():
            e = self.get_channel_info_by_dialog(dialog)
            self.channel_list.append(e['channel_id'])

            self.channel_meta[e['channel_id']] = {
               'channel_id': e['channel_id'],
               'channel_title': e['channel_title'],
               'channel_url': e['channel_url'],
               'channel_size': e['channel_size'],
               'channel_texpire': datetime.now() + timedelta(hours=3)
            }

            self.dump_channel_info(e)
            self.dump_channel_user_info(dialog)

        @self.client.on(events.ChatAction)
        async def channel_action_handler(event):
            await self.updata_channel_user_info(event)

        logging.info(f"{sys._getframe().f_code.co_name}: Monitoring channels: {json.dumps(self.channel_list, indent=4)}")
        logging.info(f'Channel METADATA: {self.channel_meta}')

    def stop_bot_interval(self):
        self.bot_task.cancel()

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

    def check_informer_info(self):
        """ 
        每隔一定时间，对于 sql 和账户内容进行对齐
        """ 
        self.check_channels_info_in_sql()
        self.check_channels_user_info_in_sql()

    def channel_count(self):
        """ 
        统计当前账户中的 channel 的数量
        """
        count = 0
        channel_list = []
        for dialog in self.client.iter_dialogs():
            # 会话不能是用户间的对话
            if not dialog.is_user:
                # 将 channel 加入现在可监控的 channel 列表
                channel_list.append({
                    'channel id':dialog.id,
                    'channel name':dialog.name
                    })
                cout +=1
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
        self.channel_count()
        await self.init_monitor_channels()
        

        # 循环
        count = 0
        while True:
            count +=1
            logging.info(f'### {count} Running bot interval')

            self.channel_count()
            self.check_informer_info()
            await asyncio.sleep(self.CHANNEL_REFRESH_WAIT)
