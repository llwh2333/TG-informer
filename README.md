# Aim: message, user, channel

## message:

- message_id(date.day+num)
- seed_id
- account_id
- channel_id
- message_txt
- is_mention
  - no
  - yes
    -  mentioned_account_id (othoer_info)
- is_scheduled
- is_fwd
  - no
  - yes
    - fwd_message_txt
    - fwd_message_seed_id
    - fwd_message_date
- is_bot
- is_reply
  - no
  - yes
    - reply_message_txt
    - reply_message_seed_id
    - reply_message_date
- message_date



## user:

- user_id
- user_name
- is_bot
- account_is_verified
  - no
  - yes
    - content
- is_restricted
- first_name
- last_name
- user_name
- phone
- chat_user_tmodified
- log_time



## channel:

- channel_id
- channel_name
- channel_title
- channel_url
- accout_id
- is_broadcast
- is_group
- is_mege_group
- is_private
- access_hash
- channel_size





# 监控流程

- 获得当前账户中 channel 
- 获得数据库中未加入 channel 
- 排除账户已加入 channel
- 加入数据库中 channel
- 根据账户 channel 更新数据库channel 信息
- 检查数据库中 channel 和账户 channel 是否匹配


















