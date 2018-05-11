# coding: utf-8
import sys,os,getopt,traceback
import json
from kafka_wrapper import Kafka_consumer 

consumer = Kafka_consumer('10.47.216.245:9092,10.47.220.179:9092,10.47.220.237:9092', 'actionlog','wk_ai_group')
consumer.consume_data()          

