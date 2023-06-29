#!/bin/bash


# change to app directory
cd ./app

# create virtual env
python -m venv testvenv1

# load venv
source testvenv1/bin/activate

# install requirements in this env
pip install -r requirements.txt 

#run the bot
python bot.py 
