# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: telegram.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0etelegram.proto\x12\rtelegram.base\"\xfb\x01\n\tChannelPb\x12\n\n\x02id\x18\x01 \x01(\x03\x12\r\n\x05title\x18\x02 \x01(\t\x12\r\n\x05photo\x18\x03 \x01(\t\x12\x11\n\tmegagroup\x18\x04 \x01(\x08\x12\x12\n\nrestricted\x18\x05 \x01(\x08\x12\x10\n\x08username\x18\x06 \x01(\t\x12\x0c\n\x04\x64\x61te\x18\x07 \x01(\t\x12\r\n\x05\x61\x62out\x18\x08 \x01(\t\x12\x1a\n\x12participants_count\x18\t \x01(\x05\x12\'\n\x07\x61\x63\x63ount\x18\n \x01(\x0b\x32\x16.telegram.base.Account\x12)\n\x08location\x18\x14 \x01(\x0b\x32\x17.telegram.base.Location\"6\n\x08Location\x12\x0f\n\x07\x61\x64\x64ress\x18\x01 \x01(\t\x12\x0c\n\x04long\x18\x02 \x01(\x02\x12\x0b\n\x03lat\x18\x03 \x01(\x02\"\x18\n\x07\x41\x63\x63ount\x12\r\n\x05phone\x18\x03 \x01(\t\"I\n\x0cUserLocation\x12\x10\n\x08\x64istance\x18\x01 \x01(\x03\x12\x0c\n\x04long\x18\x02 \x01(\x02\x12\x0b\n\x03lat\x18\x03 \x01(\x02\x12\x0c\n\x04\x61rea\x18\x04 \x01(\t\"\x9e\x03\n\x06UserPb\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x0f\n\x07is_self\x18\x02 \x01(\x08\x12\x0f\n\x07\x63ontact\x18\x03 \x01(\x08\x12\x16\n\x0emutual_contact\x18\x04 \x01(\x08\x12\x0f\n\x07\x64\x65leted\x18\x05 \x01(\x08\x12\x0b\n\x03\x62ot\x18\x06 \x01(\x08\x12\x18\n\x10\x62ot_chat_history\x18\x07 \x01(\x08\x12\x13\n\x0b\x62ot_nochats\x18\x08 \x01(\x08\x12\x10\n\x08verified\x18\t \x01(\x08\x12\x12\n\nrestricted\x18\n \x01(\x08\x12\x0c\n\x04\x64\x61te\x18\x0b \x01(\t\x12\r\n\x05\x61\x62out\x18\x0c \x01(\t\x12\x10\n\x08username\x18\x0e \x01(\t\x12\r\n\x05phone\x18\x0f \x01(\t\x12\x12\n\nfirst_name\x18\x10 \x01(\t\x12\x11\n\tlast_name\x18\x11 \x01(\t\x12\r\n\x05photo\x18\x12 \x01(\t\x12)\n\x07\x63hannel\x18\r \x01(\x0b\x32\x18.telegram.base.ChannelPb\x12\r\n\x05super\x18\x13 \x01(\x08\x12-\n\x08location\x18\x14 \x01(\x0b\x32\x1b.telegram.base.UserLocation\"k\n\x11\x43hannelJoinUserPb\x12\x0b\n\x03\x63id\x18\x01 \x01(\x03\x12\x0b\n\x03uid\x18\x02 \x01(\x03\x12\x0c\n\x04\x64\x61te\x18\x03 \x01(\t\x12\r\n\x05title\x18\x04 \x01(\t\x12\x10\n\x08username\x18\x05 \x01(\t\x12\r\n\x05super\x18\x06 \x01(\x08\"\x96\x02\n\tMessagePb\x12\n\n\x02id\x18\x01 \x01(\x03\x12\r\n\x05to_id\x18\x02 \x01(\x03\x12\x12\n\nto_id_type\x18\x03 \x01(\x08\x12\x0c\n\x04\x64\x61te\x18\x04 \x01(\t\x12\x0f\n\x07message\x18\x05 \x01(\t\x12\x0f\n\x07\x66rom_id\x18\x06 \x01(\x03\x12\x31\n\x08\x66wd_from\x18\x07 \x01(\x0b\x32\x1f.telegram.base.MessageFwdFromPb\x12\x17\n\x0freply_to_msg_id\x18\x08 \x01(\t\x12%\n\x05media\x18\t \x01(\x0b\x32\x16.telegram.base.MediaPb\x12\x10\n\x08userName\x18\n \x01(\t\x12\x11\n\tgroupName\x18\x0b \x01(\t\x12\x12\n\ngroupAbout\x18\x0c \x01(\t\"E\n\x10MessageFwdFromPb\x12\x0c\n\x04\x64\x61te\x18\x01 \x01(\t\x12\x0f\n\x07\x66rom_id\x18\x02 \x01(\x03\x12\x12\n\nchannel_id\x18\x03 \x01(\t\"`\n\x07MediaPb\x12\x0c\n\x04type\x18\x01 \x01(\t\x12\r\n\x05store\x18\x02 \x01(\t\x12\x0c\n\x04name\x18\x03 \x01(\t\x12\x0b\n\x03md5\x18\x04 \x01(\t\x12\x0c\n\x04size\x18\x05 \x01(\x03\x12\x0f\n\x07\x66ile_id\x18\x06 \x01(\tB\x16\n\x14\x63om.wanfang.proto.imb\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'telegram_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'\n\024com.wanfang.proto.im'
  _CHANNELPB._serialized_start=34
  _CHANNELPB._serialized_end=285
  _LOCATION._serialized_start=287
  _LOCATION._serialized_end=341
  _ACCOUNT._serialized_start=343
  _ACCOUNT._serialized_end=367
  _USERLOCATION._serialized_start=369
  _USERLOCATION._serialized_end=442
  _USERPB._serialized_start=445
  _USERPB._serialized_end=859
  _CHANNELJOINUSERPB._serialized_start=861
  _CHANNELJOINUSERPB._serialized_end=968
  _MESSAGEPB._serialized_start=971
  _MESSAGEPB._serialized_end=1249
  _MESSAGEFWDFROMPB._serialized_start=1251
  _MESSAGEFWDFROMPB._serialized_end=1320
  _MEDIAPB._serialized_start=1322
  _MEDIAPB._serialized_end=1418
# @@protoc_insertion_point(module_scope)
