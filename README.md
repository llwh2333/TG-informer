# 目标：message、user、channel

先获取到基本的 message 信息，并存储到本地

再获得所有 channel 信息

然后获得 channel 中所有 user 信息

 最后，完善前面实现的 message 信息获取
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

















