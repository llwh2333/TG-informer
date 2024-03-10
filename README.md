

# TG——informer

------

## 实现功能：

​		获取用户加入的 channel 的消息与相关信息，并将其存入mysql 中，并提供上传到 es 的功能。

## 获取信息：

​		Message、User、Channel

### message：

​		channel 中所发出的信息，包含了照片和文字信息，照片存储到本地之中

### channel：

​		用户所加入的 channel，包含了 channel 的一些基本属性

### user：

​		channel 中参与的用户信息

## 后续功能：

​		对数据进行处理