FROM centos:7

#  yum 更新
RUN set -ex \
    && yum -y update \
    && yum -y groupinstall "Development tools" \
	&& yum -y install zlib-devel bzip2-devel libffi-devel  openssl-devel ncurses-devel sqlite-devel readline-devel tk-devel gcc make wget \
    && mkdir /home/data

#   复制所有文件到 home/data/目录
COPY ./app_docker  /home/data


RUN set -ex \
    && cd /home/data \
    && ls \
    && cd /home/data/openssl-1.1.1q \
    && /bin/bash ./config --prefix=/usr/local/openssl-1.1.1 \
    && make && make install 

RUN set -ex \
    && ls \
    && cd /home/data/Python-3.10.6 \
    && /bin/bash ./configure prefix=/usr/local/python --enable-optimizations --with-openssl=/usr/local/openssl-1.1.1 --with-openssl-rpath=auto \
    && make altinstall

RUN set -ex \
	&& yum install -y epel-release \
    && yum install -y python-pip \
	&& rm -f /usr/bin/python \
	&& rm -f /usr/bin/pip \
    && ln -s /usr/local/python/bin/python3.10 /usr/bin/python  \
    && ln -s /usr/local/python/bin/pip3.10 /usr/bin/pip 

RUN set -ex \
    && sed -i "s#/usr/bin/python#/usr/bin/python2.7#" /usr/bin/yum \
    && sed -i "s#/usr/bin/python#/usr/bin/python2.7#" /usr/libexec/urlgrabber-ext-down \
    && yum install -y deltarpm

RUN set -ex \
	&& python -V \
	&& python -m pip install -i https://pypi.tuna.tsinghua.edu.cn/simple --upgrade pip

RUN set -ex \
 	&& yum  install -y lrzsz \
	&& yum  install -y net-tools \
 	&& yum  install -y git \
 	&& yum  install -y zip unzip 

# 启动配置
RUN set -ex \
	&& cd /home \
 	&& pip list \
	&& echo "构建成功"

COPY app /usr/local/app
WORKDIR /usr/local/app
RUN set -ex \
    && pip install --upgrade pip -i http://pypi.douban.com/simple/ --trusted-host pypi.douban.com \
    && pip install -r requirements.txt -i http://pypi.douban.com/simple/ --trusted-host pypi.douban.com