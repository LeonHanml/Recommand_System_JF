# coding: utf-8
import sys,os,traceback
#sys.path.append("../utils/python")
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json

from user_actionlog_manager import insert_user_action

#milli seconds
reconnect_backoff_ms_value = 200
reconnect_backoff_max_ms_value = 120*1000

#seconds
flush_time_out_value = 3

# define action
USER_VIEW_ACTION = 'event18'

class Kafka_producer():
    def __init__(self, kafka_host,kafka_topic):
        self.kafka_host = kafka_host
        self.kafka_topic = kafka_topic
        self.producer = KafkaProducer(
                bootstrap_servers = self.kafka_host,
                reconnect_backoff_ms = reconnect_backoff_ms_value)
    
    def send_data(self,params):
        try:
            params_msg = json.dumps(params)
            self.producer.send(self.kafka_topic,params_msg.encode('utf-8'))
            self.producer.flush(flush_time_out_value)
        except KafkaError as e:
            print("Kafka producer error %s" %(e))
            return False
        else:
            return True

#Note however that there cannot be more consumer instances in a consumer group than partitions.
class Kafka_consumer():
    def __init__(self, kafka_host, kafka_topic, group_id):
        self.kafka_host = kafka_host
        self.kafka_topic = kafka_topic
        self.group_id = group_id
		
        self.consumer = KafkaConsumer(
                self.kafka_topic,
                enable_auto_commit = False,
                group_id = self.group_id,
                fetch_max_bytes = 104785600,
                max_partition_fetch_bytes = 104785600,
                reconnect_backoff_ms = reconnect_backoff_ms_value,
                reconnect_backoff_max_ms = reconnect_backoff_max_ms_value,
                bootstrap_servers=self.kafka_host,
                auto_offset_reset='earliest')
    
    def consume_data(self):
        try:
            for message in self.consumer:
                data = message.value
                print "Kafka offset: ", message.offset
                decoded_data = self.decode_data(data)
                if decoded_data is not None:
                    user_id, product_no, event_action = decoded_data
                    if event_action == USER_VIEW_ACTION:
                        insert_user_action(user_id, product_no)
                self.consumer.commit()
        except KafkaError as e :
            print("Kafka consumer error %s" %(e))

    def decode_data(self, data):
        if data is None:
            return None
        try:
            data = json.loads(data)
        except Exception:
            print 'json format error'
            return None
        user_id = data.get('userId')
        product_no = data.get('skuId')
        event_action = data.get('eventAction')
        if user_id is None or product_no is None or event_action is None \
            or user_id == '' or product_no == '' or event_action == '':
            return None

        return user_id.encode('utf-8'), product_no.encode('utf-8'), event_action.encode('utf-8')





