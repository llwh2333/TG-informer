#See:https://github.com/paulpierre/informer
from sqlalchemy import Boolean, Column, Integer, String, BigInteger, DateTime, ForeignKey, Table
from sqlalchemy.orm import relationship
from datetime import datetime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
""" 
MYSQL 数据库程序存储的基础类声明
"""

class Account(Base):
    """
    将要被用作监控的 tg 账户
    """
    __tablename__ = 'account'
    id = Column(BigInteger, index=True, primary_key=True, autoincrement=True)
    account_id = Column(BigInteger, nullable=False, index=True)         # 傀儡账户的 id
    account_api_id = Column(Integer, default=None, nullable=False)      # API
    account_api_hash = Column(String(50), default=None, nullable=False) # API hash
    account_is_bot = Column(Boolean(), default=None)                    # 是否机器人
    account_is_verified = Column(Boolean(), default=None)               # 是否验证过
    account_is_restricted = Column(Boolean(), default=None)             # 是否受限账户
    account_first_name = Column(String(50), default=None)               # 账户昵称
    account_last_name = Column(String(50), default=None)
    account_user_name = Column(String(100), default=None, nullable=False)       #傀儡账户用户名
    account_phone = Column(String(25), unique=True, default=None, nullable=False)       # 手机号
    account_tlogin = Column(DateTime, default=None) 
    account_is_enabled = Column(Boolean(), default=True)
    account_tcreate = Column(DateTime, default=datetime.now())
    account_tmodified = Column(DateTime, default=datetime.now())


class Channel(Base):
    """ 
    tg 用户所在的 channel
    """ 
    __tablename__ = 'channel'
    id = Column(BigInteger, index=True)
    channel_id = Column(BigInteger, unique=True,primary_key=True, nullable=True)             # 频道 id
    channel_name = Column(String(256), default=None, nullable=True)                     # 频道名称（系统分配）
    channel_title = Column(String(256), default=None, nullable=True)                    # 频道公开
    channel_url = Column(String(256), nullable=True)                                    # 频道地址
    account_id = Column(BigInteger, nullable=False)   # 监控这个频道的账户
    channel_is_mega_group = Column(Boolean(), nullable=True)                            # 是否超级群组
    channel_is_group = Column(Boolean(), nullable=True)                                 # 是否为群组
    channel_is_private = Column(Boolean(), nullable=True)                               # 是否私密
    channel_is_broadcast = Column(Boolean(), nullable=True)                             # 是广播吗
    channel_access_hash = Column(String(50), nullable=True)                             # 频道的 hash
    channel_size = Column(Integer, nullable=True)                                       # 频道大小（成员数）
    # 在数据库中当 is_enabled 为 False 将被添加，失败将被删除，并发送通知
    channel_is_enabled = Column(Boolean(), nullable=True, default=False)                   
    channel_tcreate = Column(DateTime, default=datetime.now())                          # 创建时间

class ChatUser(Base):
    """ 
    tg 频道的参与者
    """ 
    __tablename__ = 'chat_user'
    id = Column(BigInteger, primary_key=True, index=True)
    chat_user_id = Column(BigInteger, unique=True, index=True, nullable=False)      # 参与者 id

    chat_user_name = Column(String(100), default=None)                              # 参与者账户用户名
    chat_user_first_name = Column(String(50), default=None)                         # 参与者昵称
    chat_user_last_name = Column(String(50), default=None)

    chat_user_is_bot = Column(Boolean(), default=None)                              # 参与者是否为 bot
    chat_user_is_verified = Column(Boolean(), default=None)                         # 参与者是否验证

    chat_user_is_restricted = Column(Boolean(), default=None)                       # 是否受限用户

    chat_user_phone = Column(String(25), default=None)                              # 参与者手机号
    chat_user_tlogin = Column(DateTime, default=None)                               # 登录时间
    chat_user_tcreate = Column(DateTime, default=datetime.now())                    # 创建时间
    chat_user_tmodified = Column(DateTime, default=datetime.now())                  # 最近修改时间


class Message(Base):
    """ 
    来自频道和用户的消息
    """
    __tablename__ = 'message'
    message_id = Column(BigInteger, primary_key=True, index=True)  # 消息的 id
    chat_user_id = Column(BigInteger, nullable=False)  # 消息发送者 id
    account_id = Column(BigInteger, nullable=False)  # 傀儡账户 id
    channel_id = Column(BigInteger, nullable=False)  # 频道的 id
    message_text = Column(String(1000), default=None)  # 消息内容
    message_is_bot = Column(Boolean(), default=None)  # 是否机器人发出
    message_is_group = Column(Boolean(), default=None)
    message_is_private = Column(Boolean(), default=None)
    message_is_channel = Column(Boolean(), default=None)
    message_tcreate = Column(DateTime, default=datetime.now())
    message_is_mention = Column(Boolean(), default=None)  # 是否提及他人
    message_mention_user = Column(String(100), default=None)
    message_is_scheduled = Column(Boolean(), default=None)  # 是否预设发送
    message_is_fwd = Column(Boolean(), default=None)  # 是否转发消息
    fwd_message_send_id = Column(BigInteger, default=None)
    fwd_message_send_name = Column(String(100), default=None)
    fwd_message_saved_id = Column(String(300), default=None)
    fwd_message_times = Column(Integer, default=None)
    fwd_message_date = Column(DateTime, default=None)
    message_is_reply = Column(Boolean(), default=None)  # 是否是回复
    reply_message_txt = Column(String(1000), default=None)
    reply_message_id = Column(BigInteger, default=None)
    reply_message_send_id = Column(BigInteger, default=None)
    reply_message_date = Column(DateTime, default=None)
    reply_message_times = Column(Integer, default=None)
