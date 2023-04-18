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
from models import Account, Channel, ChatUser, Keyword, Message, Monitor, Notification
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

        """ 
        """ 

        # 实例变量
        self.channel_list = []

        self.channel_meta = {}
        self.bot_task = None
        self.CHANNEL_REFRESH_WAIT = 15 * 60 # Every 15 minutes
        self.MIN_CHANNEL_JOIN_WAIT = 30
        self.MAX_CHANNEL_JOIN_WAIT = 120
        self.bot_uptime = 0
        self.client = None
        self.loop = asyncio.get_event_loop()

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
        self.loop.run_until_complete(self.bot_interval())
        logging.info('the monitor will done?????')



    def get_channel_all_users(self, channel_id):
        """ 
        获得 channel 的所有用户信息(todo!!!!!!!!)
        """ 

        # TODO: this function is not complete

        # 获得群组的实例对象
        channel = self.client.get_entity(PeerChat(channel_id))
        users = self.client.get_participants(channel)
        print(f'total users: {users.total}')
        for user in users:
            if user.username is not None and not user.is_self:
                print(utils.get_display_name(user), user.username, user.id, user.bot, user.verified, user.restricted, user.first_name, user.last_name, user.phone, user.is_self)

    def stop_bot_interval(self):
        self.bot_task.cancel()

    async def get_channel_user_count(self, channel):
        """ 
        获得 channel 的用户人数
        """ 
        data = await self.client.get_entity(PeerChannel(-channel))
        users = await self.client.get_participants(data)
        return users.total

        pass

    async def get_channel_info_by_url(self,url):
        """ 
        通过 url 获得 channel 信息
        """ 

        logging.info(f'{sys._getframe().f_code.co_name}: Getting channel info with url: {url}')
        # 获得 channel 的 hash 部分
        channel_hash = utils.parse_username(url)[0]

        # 尝试直接通过 channel 哈希获得实体
        try:
            channel = await self.client.get_entity(channel_hash)
        except ValueError:
            logging.info(f'{sys._getframe().f_code.co_name}: Not a valid telegram URL: {url}')
            return False
        except FloodWaitError as e:
            logging.info(f'{sys._getframe().f_code.co_name}: Got a flood wait error for: {url}')
            await asyncio.sleep(e.seconds * 2)

        return {
            'channel_id': channel.id,
            'channel_title': channel.title,
            'is_broadcast': channel.broadcast,
            'is_mega_group': channel.megagroup,
            'channel_access_hash': channel.access_hash,
        }
        pass

    async def get_channel_info_by_channel_url(self,url):
        """ 
        通过 url 获得 channel 的信息
        """ 
        channel = self.client.get_entity(url)

        return {
            'channel_id': channel.id,
            'channel_title': channel.title,
            'is_broadcast': channel.broadcast if channel.broadcast else False,
            'is_mega_group': channel.megagroup if channel.megagroup else False,
            'channel_access_hash': channel.access_hash if channel.megagroup else None,
        }
        pass

    async def send_notification(self, sender_id=None, event=None, channel_id=None, keyword=None, keyword_id=None, message_obj=None):
        """ 
        发送报告信息，报告检测到的关键字信息(absolute)
        """ 

        # 获得报文信息
        message_text = message_obj.message

        ################# 设置元数据
        # 是否提到其它用户
        is_mention = message_obj.mentioned
        # 是否是计划消息（通常判断是否是自动发送的）
        is_scheduled = message_obj.from_scheduled
        # 是否是转发的消息
        is_fwd = False if message_obj.fwd_from is None else True
        # 是否是回复消息
        is_reply = False if message_obj.reply_to_msg_id is None else True
        # 是否是机器人发送的
        is_bot = False if message_obj.via_bot_id is None else True

        # 判断是频道还是群组
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

        
        # 如果在元数据中且频道规模为0（刚刚监控）或现在的时间超过设置的过期时间，更新规模，并设置下一个过期时间
        if channel_id in self.channel_meta and self.channel_meta[channel_id]['channel_size'] == 0 or datetime.now() > self.channel_meta[channel_id]['channel_texpire']:
            logging.info('refreshing the channel information')
            channel_size = await self.get_channel_user_count(channel_id)
        else:
            channel_size = self.channel_meta[channel_id]['channel_size']

        # 获得发送者信息
        sender = await event.get_sender()
        sender_username = sender.username

        channel_id = abs(channel_id)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


        # 设置要在通知频道中发送的报告信息（关键字、发送者、channel url、时间）
        message = f'⚠️ "{keyword}" mentioned by {sender_username} in => "{self.channel_meta[channel_id]["channel_title"]}" url: {self.channel_meta[channel_id]["channel_url"]}\n\n Message:\n"{message_text}\ntimestamp: {timestamp}'
        logging.info(f'{sys._getframe().f_code.co_name} Sending notification {message}')

        # 发送出去报告
        await self.client.send_message(self.monitor_channel, message)

        ########################## 添加报告消息的发送者到数据库中
        # 获得发送者的 id
        o = await self.get_user_by_id(sender_id)

        self.session = self.Session()
        # 如果发送者还没有存储在数据库中
        if not bool(self.session.query(ChatUser).filter_by(chat_user_id=sender_id).all()):

            self.session.add(ChatUser(
                chat_user_id=sender_id,
                chat_user_is_bot=o['is_bot'],
                chat_user_is_verified=o['is_verified'],
                chat_user_is_restricted=o['is_restricted'],
                chat_user_first_name=o['first_name'],
                chat_user_last_name=o['last_name'],
                chat_user_name=o['username'],
                chat_user_phone=o['phone'],
                chat_user_tlogin=datetime.now(),
                chat_user_tmodified=datetime.now()
            ))

        # -----------
        # Add message
        # -----------

        ############ 添加报告的消息到数据中
        msg = Message(
            chat_user_id=sender_id,
            account_id=self.account.account_id,
            channel_id=channel_id,
            keyword_id=keyword_id,
            message_text=message_text,
            message_is_mention=is_mention,
            message_is_scheduled=is_scheduled,
            message_is_fwd=is_fwd,
            message_is_reply=is_reply,
            message_is_bot=is_bot,
            message_is_group=is_group,
            message_is_private=is_private,
            message_is_channel=is_channel,
            message_channel_size=channel_size,
            message_tcreate=datetime.now()
        )
        self.session.add(msg)

        self.session.flush()

        message_id = msg.message_id
        # 添加报告信息
        self.session.add(Notification(
            keyword_id=keyword_id,
            message_id=message_id,
            channel_id=channel_id,
            account_id=self.account.account_id,
            chat_user_id=sender_id
        ))

        # 写入数据库
        try:
            self.session.commit()
        except IntegrityError:
            pass
        self.session.close()

        pass

    async def filter_message(self,event):
        """ 
        过滤传输进来的消息，包含了关键字就进行报告(absolute)
        """ 
        # 如果是频道，获取频道的 id
        
        if isinstance(event.message.to_id, PeerChannel):
            logging.info('........Get a channel message')
            channel_id = event.message.to_id.channel_id
        # 如果是群组，获得群组的 id
        elif isinstance(event.message.to_id, PeerChat):
            logging.info('........Get a chat message')
            channel_id = event.message.chat_id
        else:
            # 两者均不是，跳过
            return

        # 由于 api 中的 id 是带符号，我们获取绝对值
        channel_id = abs(channel_id)

        message = event.raw_text
        logging.info(f'get the message is ({message})!!!!!!!!!!!!!!!')

        if (message == 'stop the message monitor yes'):
            logging.info('I will close the monitor')
            self.client.disconnect()
            logging.info('the monitor will done!!!!!!!!!!!!!!')

        # 检测是否在我们实际监控的列表中
        if channel_id in self.channel_list:
            logging.info(f'filter the message from {channel_id}')
            # 遍历我们监控的关键字，如果存在就报告
            for keyword in self.keyword_list:
                if re.search(keyword['regex'], message, re.IGNORECASE):
                    logging.info(
                        f'Filtering: {channel_id}\n\nEvent raw text: {event.raw_text} \n\n Data: {event}')

                    # 发送报告
                    await self.send_notification(
                        message_obj=event.message,
                        event=event, sender_id=event.sender_id,
                        channel_id=channel_id,
                        keyword=keyword['name'],
                        keyword_id=keyword['id']
                    )
        pass

    async def message_dump(self,event):
        """ 
        将收到的消息进行存储，存储到数据库和 json 文件中
        """ 
        flag = -1
        channel_id = abs(channel_id)

        message = event.raw_text

        if isinstance(event.message.to_id, PeerChannel):
            logging.info(f'........get the channel message is ({message})!!!!!!!!!!!!!!!')
            flag = 1
            channel_id = event.message.to_id.channel_id
        # 如果是群组，获得群组的 id
        elif isinstance(event.message.to_id, PeerChat):
            logging.info(f'........get the chat message is ({message})!!!!!!!!!!!!!!!')
            flag = 0
            channel_id = event.message.chat_id
        else:
            # 两者均不是，跳过
            return

        lock = threading.Lock()


        with lock:    
            logging.info('begin store message')
            with open('messages.json','r') as f:
                data = json.load(f)
            new_message = {'channel_id':str(channel_id),'message_data':message,'sender_id':str(event.sender_id)}
            data['messages'].append(new_message)
            json_data = json.dumps(data,indent = 4)
            with open('messages.json','w') as f:
                f.write(json_data)
            logging.info('end store message')

        pass




    def delate_channel_from_sql(self,channel)

    def send_notification(self,channel_info):
        pass

    def join_channel(self):
        """ 
        根据数据库中的 channel 信息进行加入
        """ 
        # 由于自动加入问题较多暂时不用实现，下面的是以前修改好的

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

        pass

    async def init_monitor_channels(self):
        """ 
        初始化要监控的频道
        """ 
        logging.info('Running the monitor to channels')

        # 处理新消息
        @self.client.on(events.NewMessage)
        async def message_event_handler(event):
            #logging.info('!!!!!!!!!!!Get a message')
            # 通过协程存储当前的新消息
            await self.message_dump(event)

        #join_channel()

        # 更新频道信息，同时对数据库中 channel 信息进行清理
        for dialog in self.client.iter_dialogs():
            channel_id = dialog.id
            channel_obj = self.session.query(Channel).filter_by(channel_id=channel_id, account_id=self.account.account_id).first()
            
            # 更新数据库
            if dialog.is_channel:

                channel_obj.channel_id = channel_id
                channel_obj.channel_name = dialog.name
                channel_obj.channel_title = dialog.title
                channel_obj.account_id = self.account.account_id
                channel_obj.channel_is_group = dialog.is_group
                channel_obj.channel_is_broadcast = True
                channel_obj.channel_is_enabled = True
                channel_obj.channel_is_mega_group = dialog.megagroup
                channel_obj.channel_access_hash = dialog.entity.access_hash
                channel_obj.channel_size = dialog.entity.participants_count

                channel_obj.channel_tcreate = 

                channel_obj.channel_is_private = 


            elif dialog.is_group:
            
            
            
            
            pass

        # 

        #################################
        # 处理我们账户中的用户信息收集挑战 #
        #################################



        channel_obj = self.session.query(Channel).filter_by(channel_id=channel['channel_id'], account_id=self.account.account_id).first()

        #self.account.account_id
        logging.info(f'{sys._getframe().f_code.co_name}: ### Current channels {json.dumps(current_channels, indent=4)}')
        
        ########## 获取数据库中的想监控的 channel
        # 获取数据库引擎
        self.session = self.Session()
        # 获取数据库中的第一个傀儡账户
        account = self.session.query(Account).first()
        logging.info('get the sql account')
        # 获取数据库中傀儡账户需要监控的所有 channel
        monitors = self.session.query(Monitor).filter_by(account_id=account.account_id).all()
        # 建立欲监控列表
        channels_to_monitor = []
        # 遍历数据库中需要监控的 channel 
        for monitor in monitors:
            channel_data = {
                'channel_id': monitor.channel.channel_id,
                'channel_name': monitor.channel.channel_name,
                'channel_title': monitor.channel.channel_title,
                'channel_url': monitor.channel.channel_url,
                'account_id': monitor.channel.account_id,
                'channel_is_megagroup': monitor.channel.channel_is_mega_group,
                'channel_is_group': monitor.channel.channel_is_group,
                'channel_is_private': monitor.channel.channel_is_private,
                'channel_is_broadcast': monitor.channel.channel_is_broadcast,
                'channel_access_hash': monitor.channel.channel_access_hash,
                'channel_size': monitor.channel.channel_size,
                'channel_is_enabled': monitor.channel.channel_is_enabled,
                'channel_tcreate': monitor.channel.channel_tcreate
            }
            # 如果频道是需要监控的，则添加到列表中
            if monitor.channel.channel_is_enabled is True:
                logging.info('channels_to_monitor append one !!!')
                channels_to_monitor.append(channel_data)
        self.session.close()
        ########## 测试欲监控的 channel
        # 遍历欲监控 channel 列表
        for channel in channels_to_monitor:
            # 获取数据库引擎
            self.session = self.Session()
            # 获得当前数据库中欲监控的对象，对其的修改会改变数据库的内容
            channel_obj = self.session.query(Channel).filter_by(channel_id=channel['channel_id']).first()
            
            # 如果欲监控列表中包含 channel ID
            if channel['channel_id']:
                # 列入实际成功监控的 channel 目录中
                self.channel_list.append(channel['channel_id'])
                logging.info(f"Adding channel {channel['channel_name']} to monitoring w/ ID: {channel['channel_id']} hash: {channel['channel_access_hash']}")
                # 填入成功监控 channel 的信息
                self.channel_meta[channel['channel_id']] = {
                    'channel_id': channel['channel_id'],
                    'channel_title': channel['channel_title'],
                    'channel_url': channel['channel_url'],
                    'channel_size': 0,
                    'channel_texpire': datetime.now() + timedelta(hours=3)
                }
            # 如果没有 channel ID，就获取信息
            else:
                logging.info('test get channel info')
                # 如果有 url 字段并且非公共 channel（可能需要验证），获取频道信息
                if channel['channel_url'] and '/joinchat/' not in channel['channel_url']:
                    logging.info('from url(no joinchat) get info')
                    o = await self.get_channel_info_by_url(channel['channel_url'])

                    # 如果 channel 是无效的，跳过这个 channel
                    if o is False:
                        logging.info('false get channel info')
                        logging.error(f"Invalid channel URL: {channel['channel_url']}")
                        continue

                    logging.info(f"{sys._getframe().f_code.co_name}: ### Successfully identified {channel['channel_name']}")


                # 如果 channel 是群组
                elif channel['channel_url'] and '/joinchat/' in channel['channel_url']:
                    o = await self.get_channel_info_by_channel_url(channel['channel_url'])

                    logging.info(f"{sys._getframe().f_code.co_name}: ### Successfully identified {channel['channel_name']}")
                
                # 如果都不知道，就解析失败，跳过
                else:
                    logging.info(f"{sys._getframe().f_code.co_name}: Unable to indentify channel {channel['channel_name']}")
                    continue

                # 对欲监控 channel 信息进行填充
                channel_obj.channel_id = o['channel_id']
                channel_obj.channel_title = o['channel_title']
                channel_obj.channel_is_broadcast = o['is_broadcast']
                channel_obj.channel_is_mega_group = o['is_mega_group']
                channel_obj.channel_access_hash = o['channel_access_hash']

                # 填充 channel 元数据
                self.channel_meta[o['channel_id']] = {
                    'channel_id': o['channel_id'],
                    'channel_title': o['channel_title'],
                    'channel_url': channel['channel_url'],
                    'channel_size': 0,
                    'channel_texpire':datetime.now() + timedelta(hours=3)
                }
    
            # 确定是否是私人 channel
            channel_is_private = True if (channel['channel_is_private'] or '/joinchat/' in channel['channel_url']) else False
            if channel_is_private:
                logging.info(f'channel_is_private: {channel_is_private}')

            # 如果 channel 不是群组，非私人，且不在正在监控频道中
            if channel['channel_is_group'] is False and channel_is_private is False and channel['channel_id'] not in current_channels:
                logging.info(f"{sys._getframe().f_code.co_name}: Joining channel: {channel['channel_id']} => {channel['channel_name']}")
                try:
                    # 根据 url 加入 channel
                    await self.client(JoinChannelRequest(channel=await self.client.get_entity(channel['channel_url'])))
                    sec = randrange(self.MIN_CHANNEL_JOIN_WAIT, self.MAX_CHANNEL_JOIN_WAIT)
                    logging.info(f'sleeping for {sec} seconds')
                    await asyncio.sleep(sec)

                except FloodWaitError as e:
                    logging.info(f'Received FloodWaitError, waiting for {e.seconds} seconds..')
                    # Lets wait twice as long as the API tells us for posterity
                    await asyncio.sleep(e.seconds * 2)

                except ChannelPrivateError as e:
                    logging.info('Channel is private or we were banned bc we didnt respond to bot')
                    channel['channel_is_enabled'] = False
            # 如果 channel 是私人，且不在监控中
            elif channel_is_private and channel['channel_id'] not in current_channels:
                channel_obj.channel_is_private = True
                logging.info(f"{sys._getframe().f_code.co_name}: Joining private channel: {channel['channel_id']} => {channel['channel_name']}")

                #获得 channel 的 secret hash
                if channel['channel_url'] == None:
                    continue
                channel_hash = channel['channel_url'].replace('https://t.me/joinchat/', '')

                try:
                    # 导入并加入指定哈希值的聊天组
                    await self.client(ImportChatInviteRequest(hash=channel_hash))


                    sec = randrange(self.MIN_CHANNEL_JOIN_WAIT, self.MAX_CHANNEL_JOIN_WAIT)
                    logging.info(f'sleeping for {sec} seconds')
                    await asyncio.sleep(sec)
                # 如果发生了 floodwaiterror（请求太频繁）
                except FloodWaitError as e:
                    logging.info(f'Received FloodWaitError, waiting for {e.seconds} seconds..')
                    await asyncio.sleep(e.seconds * 2)
                except ChannelPrivateError as e:
                    logging.info('Channel is private or we were banned bc we didnt respond to bot')
                    channel['channel_is_enabled'] = False

                # 已经加入了所以跳过
                except UserAlreadyParticipantError as e:
                    logging.info('Already in channel, skipping')
                    self.session.close()
                    continue

            # 如果被欺骗了，就回滚
            try:
                self.session.commit()
            except IntegrityError:
                self.session.rollback()
            except InterfaceError:
                pass
            self.session.close()








        logging.info(f"{sys._getframe().f_code.co_name}: Monitoring channels: {json.dumps(self.channel_list, indent=4)}")
        logging.info(f'Channel METADATA: {self.channel_meta}')
        pass






    def channel_count(self):
        """ 
        统计当前账户中的 channel 的数量
        """
        count = 0
        channel_list = []

        for dialog in self.client.iter_dialogs():
            # 会话的 channel id
            channel_id = dialog.id

            # 会话不能是用户间的对话
            if not dialog.is_user:
                # 如果 channel id 是正整数，则去除前三位，取正整数channel id
                if str(abs(channel_id))[:3] == '100':
                    channel_id = int(str(abs(channel_id))[3:])
                # 将 channel 加入现在可监控的 channel 列表
                channel_list.append({
                    'id':count,
                    'channel id':channel_id,
                    'channel name':dialog.name
                    })
                logging.info(f'{sys._getframe().f_code.co_name}: Monitoring channel: {json.dumps(channel_list, indent=4)}')

        logging.info(f'Count:{count}')
        pass

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
        

        # 循环重置敏感字列表
        count = 0
        while True:
            count +=1
            logging.info(f'### {count} Running bot interval')

            # await self.init_keywords()
            self.channel_count()
            await asyncio.sleep(self.CHANNEL_REFRESH_WAIT)
    pass

