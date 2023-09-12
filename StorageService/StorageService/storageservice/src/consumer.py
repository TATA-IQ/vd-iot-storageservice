from kafka import KafkaConsumer
from concurrent.futures import ThreadPoolExecutor
import cv2
import numpy as np
import json
import base64
import threading
import redis
import time
from datetime import datetime
import os
import ast
from queue import Queue
from kafka import TopicPartition
from shared_memory_dict import SharedMemoryDict
from PIL import Image
from io import BytesIO
os.environ["SHARED_MEMORY_USE_LOCK"]="1"

topic_smd = SharedMemoryDict(name='topics', size=10000000)
class RawTopicConsumer():
    def __init__(self,kafkashost,cameraid,logger):
        self.kill=False
        self.camera_id=cameraid
        self.kafkahost=kafkashost
        self.consumer=None
        self.log=logger
        self.check=False
        self.previous_time=datetime.now()
        data=topic_smd[cameraid]
        
        self.topic=data["topic_name"]
        self.log.info(f"Starting for {self.camera_id} and topic {self.topic}")
        
    def closeConsumer(self):
        if self.consumer:
            self.consumer.close()
            return True
        else:
            return False
    
    def connectConsumer(self):
        
        #session_timeout_ms=10000,heartbeat_interval_ms=7000,
        self.queue=Queue(100)
        self.consumer=KafkaConsumer("out_"+self.topic, bootstrap_servers=self.kafkahost, auto_offset_reset="latest",
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')),group_id=self.topic)
        #self.consumer.assign([TopicPartition(self.topic, 1)])
        self.log.info(f"Connected Consumer {self.camera_id} for {self.topic}")

        return
        
    def isConnected(self):
        #print("====Check Self COnsumer====",self.consumer)
        return self.consumer.bootstrap_connected()
    def messageParser(self,msg):
        #msg=ast.literal_eval(msg)
        msg=json.loads(msg.value)
        #your message parser code
        
    
    def runConsumer(self):
        print(f"=={self.camera_id} Message Parse Connected for Topic {self.topic}====")
        self.check=True
        
        self.log.info(f"Starting Message Parsing {self.camera_id} for {self.topic}")
        # while True:
        #     print(self.consumer)
        for message in self.consumer:
            print("===Running=====")
            #print("*****Running Consumer****")
            
