#See:https://github.com/paulpierre/informer
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
from telethon.tl.types import PeerUser, PeerChat, PeerChannel,ChannelParticipant

from models import Account, Channel, ChatUser, Message
import threading
import json
from telethon.tl import types
from telethon.tl.functions.channels import GetFullChannelRequest
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
        self.channel_meta = {}                      # 已加入 channel 的信息
        self.channel_list = []
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

    async def bot_interval(self):
        """ 
        根据当前的配置建立会话，并保存 session ，方便下一次登录
        """ 
        logging.info(f'Logging in with account # {self.account.account_phone} ... \n')

        # 用来存储会话文件的地址，方便下一次的会话连接
        session_file = self.account.account_phone

        # 实例化一个 tg 端对象，初次登录会记录指定路径中，后续登录会直接使用以前的账户信息
        self.client = TelegramClient(session_file, self.account.account_api_id, self.account.account_api_hash)

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
        await self.init_monitor_channels()
        
        # 循环
        count = 0
        while True:
            count +=1
            logging.info(f'### {count} Running bot interval')

            await self.channel_count()
            self.check_informer_info()
            await asyncio.sleep(self.CHANNEL_REFRESH_WAIT)

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
        logging.info(f'{sys._getframe().f_code.co_name}: Monitoring channel: {json.dumps(channel_list,ensure_ascii=False,indent=4)}')
        logging.info(f'Count:{count}')

    def check_informer_info(self):
        """ 
        每隔一定时间，对于 sql 和账户内容进行对齐
        """ 
        self.check_channels_info_in_sql()
        self.check_channels_user_info_in_sql()

    async def init_monitor_channels(self):
        """ 
        初始化要监控的频道
        """ 
        logging.info('Running the monitor to channels')
        picture_path = './picture'
        if not os.path.exists(picture_path):
            os.makedirs(picture_path)
            logging.info(f'Create the picture dir:{picture_path}')

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

        # 处理新消息
        @self.client.on(events.NewMessage)
        async def message_event_handler(event):
            # 通过协程存储当前的新消息
            await self.message_dump(event)
            
        #join_channel()

        async for dialog in self.client.iter_dialogs():
            if not dialog.is_user:
                e = await self.get_channel_info_by_dialog(dialog)
                self.channel_meta[e['channel_id']] = {
                   'channel_id': e['channel_id'],
                   'channel_title': e['channel_title'],
                   'channel_url': e['channel_url'],
                   'channel_size': e['channel_size'],
                }
        
                self.dump_channel_info(e)
                await self.dump_channel_user_info(dialog)

        @self.client.on(events.ChatAction)
        async def channel_action_handler(event):
            await self.updata_channel_user_info(event)

        logging.info(f'Channel METADATA: {json.dumps(self.channel_meta,ensure_ascii=False,indent=4)}')

    def stop_bot_interval(self):
        self.bot_task.cancel()

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
            try:
                file_data[data_key].append(data)
            except KeyError:
                file_data[data_key] = []
                file_data[data_key].append(data)
            json_data = json.dumps(file_data,ensure_ascii=False,indent=4)
            with open(file_name,'w') as f:
                f.write(json_data)

    async def message_dump(self,event):
        """ 
        将收到的消息进行存储，存储到数据库和 json 文件中
        """ 

        #检查消息是否含有图片，如果有图片，就存储图片到本地（picture 文件中）
        if event.message.media is not None:
            file_path = './picture'
            logging.info(f'the message have media')
            await self.download_file(event,file_path)

        message = event.raw_text
        if message == '':
            return 
        if isinstance(event.message.to_id, PeerChannel):
            channel_id = event.message.to_id.channel_id
            logging.info(f'############### Get the channel message is ({message})!!!!!!!!!!!!!!!')
        # 如果是群组，获得群组的 id
        elif isinstance(event.message.to_id, PeerChat):
            channel_id = event.message.to_id.chat_id
            logging.info(f'############### Get the chat message is ({message})!!!!!!!!!!!!!!!')
        else:
            # 两者均不是，跳过
            return
    
        e = await self.get_message_info_from_event(event,channel_id)
        #self.flush_status_in_sql(e)
        self.store_message_in_json_file(e)
        self.store_message_in_sql(e)

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
        content = event.raw_text
        for ent, txt in event.get_entities_text():
            if isinstance(ent ,types.MessageEntityMention):
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
            fwd_message_txt = message_obj.fwd_from.message
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
            fwd_message_date = None
            fwd_message_txt = None
            fwd_message_send_name = None
            fwd_message_send_id = None
            fwd_message_saved_id = None
            fwd_message_times = None

        is_reply = False if message_obj.reply_to is None else True
        if is_reply:
            reply_obj = await event.get_reply_message()
            reply_message_txt = reply_obj.message if reply_obj else ''
            reply_message_send_id = None
            if reply_obj.from_id is not None:
                if isinstance(reply_obj.from_id, PeerUser):
                    reply_message_send_id = reply_obj.from_id.user_id
                elif isinstance(reply_obj.from_id, PeerChat):
                    reply_message_send_id = reply_obj.from_id.chat_id
                elif isinstance(reply_obj.from_id, PeerChannel):
                    reply_message_send_id = reply_obj.from_id.channel_id
            reply_message_id = message_obj.reply_to.reply_to_msg_id 
            reply_message_date = reply_obj.date
            reply_message_times = message_obj.replies
        else:
            reply_message_txt = None
            reply_message_send_id = None
            reply_message_id = None
            reply_message_date = None
            reply_message_times = None

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
            'message_tcreate':datetime.now(),
            'is_mention':is_mention,
            # 'mentioned_user':mentioned_users[0] if mentioned_users else None,
            'mentioned_user': None,
            'is_scheduled':is_scheduled,
            'is_fwd':is_fwd,
            'fwd_message_txt':fwd_message_txt,
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
            'message_channel_size':await self.get_channel_users_count(channel_id)
            }
        return message_info

    def store_message_in_json_file(self,message_info):
        """
        将获得的消息信息，存入json 文件中
        """
        now = datetime.now()
        file_data =  now.strftime("%y_%m_%d")
        json_file_name = './message/'+file_data+'_messages.json'

        new_message = {
            'message_id':message_info['message_id'],
            'channel_id':message_info['channel_id'],
            'message_txt':message_info['message_text'],
            'sender_id':message_info['chat_user_id'],
            'is_scheduled':message_info['is_scheduled'],
            'is_bot':message_info['message_is_bot'],
            'is_group':message_info['message_is_group'],
            'is_private':message_info['message_is_private'],
            'is_channel':message_info['message_is_channel'],
            'message_data':message_info['message_tcreate'].strftime('%Y %m %d:%H %M %S')
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

    def GetImageName(self,event):
        """ 
        获得图片文件名，用于存储
        """ 
        now = datetime.now()
        file_data = now.strftime("%y_%m_%d_")
        if isinstance(event.message.to_id, PeerChannel):
            channel_id = event.message.to_id.channel_id
        elif isinstance(event.message.to_id, PeerChat):
            channel_id = event.message.to_id.chat_id
        else:
            return
        image_name = str(channel_id)+'_'+str(event.message.id)+'_'+str(event.message.grouped_id)
        file_name = file_data+image_name
        return file_name

    def flush_status_in_sql(self,message_info):
        """ 
        根据 message 更新一些状态信息
        用户的登录时间（未检查过这个函数运行是否正确）
        """ 
        status_session = self.Session()
        user_object = status_session.query(ChatUser).filter_by(chat_user_id=message_info['chat_user_id']).first()
        if user_object:
            user_object.chat_user_tlogin = message_info['message_tcreate']
        else:
            return

    async def get_channel_info_by_dialog(self,dialog):
        """ 
        从会话中获得 channel 的信息
        """ 
        #后面看看怎么获取到 url
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

        channel_info = {
            'channel_id':dialog.id if dialog.id else None,
            'channel_name':dialog.name if dialog.name else None,
            'channel_title':dialog.title if dialog.title else None,
            'account_id':self.account.account_id,
            'is_mega_group':True if dialog.is_group and dialog.is_channel else False ,
            'is_group':dialog.is_group,
            'channel_url':channel_url,
            'is_private':is_private,
            'is_broadcast':dialog.is_channel,
            'access_hash':channel_access_hash,
            'channel_size':channel_size,
            }
        return channel_info

    def dump_channel_info(self,channel_info):
        """ 
        将 channel 信息存储下来
        """ 
        self.store_channel_info_in_json_file(channel_info)
        self.store_channel_info_in_sql(channel_info)

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

    async def dump_channel_user_info(self,dialog):
        """ 
        将会话的所有成员的信息存储下来
        """ 
        
        e = await self.get_user_info_from_dialog(dialog)
        if e == None:
            return 
        self.store_user_info_in_json_file(e,dialog)
        self.store_user_info_in_sql(e,dialog)

    async def get_user_info_from_dialog(self,dialog):
        """ 
        获取当前会话的所有成员信息
        """ 
        
        users_info_list = []

        if not dialog.is_group :
            return None

        # users = await self.client.get_participants(dialog)

        count = 0

        async for user in self.client.iter_participants(dialog,limit=200):
            print("{} : {}".format(user.id,user.username))
        # for user in users:
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
            }

            users_info_list.append(user_info)
            count += 1
        logging.info(f'Logging the users account {count} ... \n')

        return users_info_list
    
    async def get_channel_users_count(self, channel_id):

            if isinstance(channel_id, PeerChat):
                channel = await self.client.get_entity(int(channel_id))

                channel_info = await self.client(GetFullChannelRequest(channel=channel))
                # 频道订阅数
                subscriber_count = channel_info.full_chat.participants_count
            else:
                subscriber_count = 0

            return subscriber_count

    def store_user_info_in_json_file(self,user_info_list,dialog):
        """ 
        将获得的 user 列表信息存储到本地 json 中
        """ 
        now = datetime.now()
        file_data =  now.strftime("%y_%m_%d")
        json_file_name = './user_info/'+file_data+'_chat_user.json'
        users_info = []
        for user in user_info_list:
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

        message = Message(message_id = message_info['message_id'],
                            chat_user_id = message_info['chat_user_id'],
                            account_id = message_info['account_id'],
                            channel_id = message_info['channel_id'],
                            message_text = message_info['message_text'],
                            message_is_mention = message_info['is_mention'],
                            message_mentioned_user_id = message_info['mentioned_user'],
                            message_is_scheduled = message_info['message_is_scheduled'],
                            message_is_fwd = message_info['is_fwd'],
                            fwd_message_txt = message_info['fwd_message_txt'],
                            fwd_message_send_id = message_info['fwd_message_send_id'],
                            fwd_message_date = message_info['fwd_message_date'],
                            message_is_reply = message_info['is_reply'],
                            reply_message_txt = message_info['reply_message_txt'],
                            reply_message_send_id = message_info['reply_message_send_id'],
                            reply_message_date = message_info['reply_message_date'],
                            message_is_bot = message_info['message_is_bot'],
                            message_is_group = message_info['message_is_group'],
                            message_is_channel = message_info['message_is_channel'],
                            message_channel_size = message_info['message_channel_size'],
                            message_tcreate = message_info['message_tcreate']
                            )


        self.session.add(message)
        self.session.commit()



    def join_channel(self):
        """ 
        TODO:根据数据库中未加入的频道信息，加入频道（暂时不弄，下面的是以前写的）
        """ 
        pass

    def store_user_info_in_sql(self,user_info_list,dialog):
        """ 
        TODO:将获得的 user 列表信息存储到 sql 库中(暂时不弄)
        """ 

        for user_info in user_info_list:
            user = ChatUser(chat_user_id = user_info['user_id'],
                            channel_id = user_info['channel_id'],
                            chat_user_name = user_info['user_name'],
                            chat_user_first_name = user_info['first_name'][:50] if user_info['first_name'] else None,
                            chat_user_last_name = user_info['last_name'][:50] if user_info['last_name'] else None,
                            chat_user_is_bot = user_info['is_bot'],
                            chat_user_is_verified = user_info['is_verified'],
                            chat_user_is_restricted = user_info['is_restricted'],
                            chat_user_phone = user_info['user_phone'],
                            chat_user_tlogin = user_info['tlogin']
                            )

            # 查询频道是否存在
            q = self.session.query(ChatUser).filter_by(chat_user_id=user_info['user_id'],channel_id=user_info['channel_id']).all()
        
            if q:
                q = user
            else:
                self.session.add(user)
        self.session.commit()

    def store_channel_info_in_sql(self,channel_info):
        """ 
        TODO:将 channel 信息存储到 sql 中
        """ 
        
        channel = Channel(channel_id = channel_info['channel_id'],
                            channel_name = channel_info['channel_name'],
                            channel_title = channel_info['channel_title'],
                            channel_url = channel_info['channel_url'],
                            account_id = channel_info['account_id'],
                            channel_is_mega_group = channel_info['is_mega_group'],
                            channel_is_group = channel_info['is_group'],
                            channel_is_private = channel_info['is_private'],
                            channel_is_broadcast = channel_info['is_broadcast'],
                            channel_access_hash = channel_info['access_hash'],
                            channel_size = channel_info['channel_size']
                            )
        # 查询频道是否存在
        q = self.session.query(Channel).filter_by(channel_id=channel_info['channel_id']).all()
        
        if q:
            q = channel
        else:
            self.session.add(channel)

        self.session.commit()

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

    def updata_channel_user_info(self,event):
        """ 
        TODO: 根据 channel 的成员变动事件，更新 channel 成员
        """ 
