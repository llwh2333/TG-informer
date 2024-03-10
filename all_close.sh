#!/bin/bash
# 关闭
docker-compose down
# 清理
docker volume prune && docker container prune