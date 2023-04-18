FROM python:3.7-alpine
RUN set -eux && sed -i 's/dl-cdn.alpinelinux.org/mirrors.ustc.edu.cn/g' /etc/apk/repositories
RUN apk add curl
RUN apk add --no-cache gcc musl-dev
RUN apk update && apk upgrade && \
    apk add git alpine-sdk bash python3
COPY app /usr/local/app
WORKDIR /usr/local/app
RUN pip3 install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple/