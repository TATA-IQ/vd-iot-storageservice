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
from src.createclient import CreateClient
from src.saveimages import MinioSave, MongoDBSave

os.environ["SHARED_MEMORY_USE_LOCK"]="1"

topic_smd = SharedMemoryDict(name='topics', size=10000000)
class RawTopicConsumer():
    def __init__(self,kafkashost,cameraid,config,logger):
        self.kill=False
        self.camera_id=cameraid
        self.kafkahost=kafkashost
        self.config=config
        # self.minioclient=minioclient
        # self.mongoclient=mongoclient
        self.consumer=None
        self.log=logger
        self.check=False
        self.previous_time=datetime.now()
        data=topic_smd[cameraid]

        # print(self.config)
        # print(self.minioclient)
        # print(self.mongoclient)
        
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
        clientobj = CreateClient(self.config)
        self.minioclient = clientobj.minio_client()
        self.mongoclient = clientobj.mongo_client()
        self.log.info(f"Connected Consumer {self.camera_id} for {self.topic}")

        return
        
    def isConnected(self):
        #print("====Check Self Consumer====",self.consumer)
        return self.consumer.bootstrap_connected()
    def messageParser(self,msg):
        #msg=ast.literal_eval(msg)
        msg=json.loads(msg.value)
        raw_image = msg['raw_image']
        process_image = msg['process_image']
        incident_event = msg['incident_event']
        usecase_inform = msg['usecase_inform']
        #your message parser code
        return raw_image, process_image, incident_event, usecase_inform
    
    def runConsumer(self):
        print(f"===={self.camera_id} Message Parse Connected for Topic {self.topic}====")
        self.check=True
        
        self.log.info(f"Starting Message Parsing {self.camera_id} for {self.topic}")
        # print("#"*100)
        # print(self.minioclient)
        # while True:
        #     print(self.consumer)
        for message in self.consumer:
            print("===Running=====")
            #print("*****Running Consumer****")
            raw_image, process_image, incident_event, usecase_inform = self.messageParser(message)
            try:
                mongo_obj = MongoDBSave(self.mongoclient, incident_event)
                mongo_obj.save_mongodata()
            except Exception as e:
                print(e)
            try:
                minio_obj = MinioSave(self.minioclient, raw_image, process_image, incident_event)
                minio_saved_paths = minio_obj.save_raw_processed_image()
            except Exception as e:
                print(e)
            
