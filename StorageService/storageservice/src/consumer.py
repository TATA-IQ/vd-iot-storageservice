from kafka import KafkaConsumer
from concurrent.futures import ThreadPoolExecutor
import multiprocessing as mp
import cv2
import numpy as np
import json
import base64
import threading
import uuid
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
from src.parser import Config
from src.createclient import CreateClient
from src.storeimages import StorageClass, MinioStorage, MongoStorage
# from src.saveimages import MinioSave, MongoDBSave

os.environ["SHARED_MEMORY_USE_LOCK"]="1"

topic_smd = SharedMemoryDict(name='topics', size=10000000)

manager=mp.Manager()
# minio_queue=manager.Queue()
# mongo_queue=manager.Queue()
# config = Config.yamlconfig("config/config.yaml")[0]
# clientobj = CreateClient(config)
# minioclient = clientobj.minio_client()
# mongoclient = clientobj.mongo_client()
# mongobackupclient = clientobj.mongo_backupclient()

def future_callback_error_logger(future):
    e = future.exception()
    print("*****", e)

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
        self.executor=None
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
        # self.manager=mp.Manager()
        self.minio_queue = Queue()
        self.mongo_queue = Queue()
        self.minioclient = clientobj.minio_client()
        self.mongoclient = clientobj.mongo_client()
        self.mongobackupclient = clientobj.mongo_backupclient()
        self.mongoreportsclient = clientobj.mongo_reportsclient()
        self.executor= ThreadPoolExecutor(max_workers=3)
        self.log.info(f"Connected Consumer {self.camera_id} for {self.topic}")
        

        
        
    def isConnected(self):
        #print("====Check Self Consumer====",self.consumer)
        return self.consumer.bootstrap_connected()

    
    def convert_image(self,image_str):
        try:
            stream = BytesIO(image_str.encode("ISO-8859-1"))
            image = Image.open(stream).convert("RGB")
            imagearr=np.array(image)
            return imagearr
        except:
            try:
                if image_str:
                    imgarr = base64.b64decode(image_str)
                    imgarr = np.frombuffer(imgarr, dtype=np.uint8)
                    return cv2.imdecode(imgarr, cv2.IMREAD_COLOR)
            except Exception as e:
                print("exception===>",e)
            return None



    def messageParser(self,msg):
        # print("== in msg parser ==")
        # msg=ast.literal_eval(msg)
        # print(msg.value)
        msg=json.loads(msg.value)
        # msg = msg.value
        # try:
        #     msg=json.loads(msg.value)
        # except:
        #     msg=None
        
        try:
            raw_image_str = msg['raw_image']
            raw_image = self.convert_image(raw_image_str)
        except:
            raw_image_str = None

        process_image_str = msg['processed_image']
        process_image = self.convert_image(process_image_str)

        incident_event = msg['incident_event']
        # print("K"*100)
        # print(incident_event)
        try:
            usecase_inform = msg['usecase']
        except:
            usecase_inform = {}
        return raw_image, process_image, incident_event, usecase_inform
    
    

    def minio_thread(self):
        print("in minio thread==========")
        count=0
        while True:
            # print(self.minio_queue)
            # print("========minio queue size before getting getting==========")
            # print("len of q",self.minio_queue.qsize())
            #print("values",self.minio_queue.get())
            #print("len of q",self.minio_queue.qsize())

            if not self.minio_queue.empty():
                # print("minio queue is not empty")
                data = self.minio_queue.get()
                # print(data)
                client,raw_image,processed_image,dataconfig,mongobackup_client=data
                # print(raw_image)
                # cv2.imwrite("/home/sridhar.bondla10/gitdev_v2/vd-iot-storageservice/StorageService/storageservice/image/"+str(uuid.uuid4())+".jpg",raw_image)
                # print("#-"*100)
                # break
                MinioStorage.save_miniodata(client,raw_image,processed_image,dataconfig,mongobackup_client)
                print("saved image in minio")
            else:
                continue
                # print("minio queue is empty")

    def mongo_thread(self):
        print("in mongo thread==========")
        while True:
            # print("here")
            # print(self.mongo_queue)
            # print("========mongo queue size before getting==========")
            # print(self.mongo_queue.qsize())
            if not self.mongo_queue.empty():
                # print("mongo queue is not empty")
                
                mongoclient,dataconfig,mongoreportsclient = self.mongo_queue.get()
                #print(dataconfig)
                MongoStorage.save_mongodata(mongoclient,dataconfig,mongoreportsclient)
                # print("saved in mongo")
            else:
                continue
                # print("mongo queue is empty")
        
    # def saveData(self):
        
    #     print("=== saving the data in minio ===")
    #     self.minio_thread()
    #     print("=== saving the data in mongo ===")
    #     self.mongo_thread()
    #     # with ThreadPoolExecutor(max_workers=2) as executor:
    #     #     print("Starting thread pool executors")
    #     #     minio_future = executor.submit(self.minio_thread,)
    #     #     mongo_future = executor.submit(self.mongo_thread,)
        
    def data_store(self):
        # with ThreadPoolExecutor(max_workers=3) as executor:
        runConsumer_future = self.executor.submit(self.runConsumer)
        minio_future = self.executor.submit(self.minio_thread)
        mongo_future = self.executor.submit(self.mongo_thread)
        
        runConsumer_future.add_done_callback(future_callback_error_logger)
        minio_future.add_done_callback(future_callback_error_logger)
        mongo_future.add_done_callback(future_callback_error_logger)
        
    
    def runConsumer(self):
        
        print(f"===={self.camera_id} Message Parse Connected for Topic {self.topic}====")
        self.check=True
        
        self.log.info(f"Starting Message Parsing {self.camera_id} for {self.topic}")
        # with ThreadPoolExecutor(max_workers=2) as executor:
        #     print("Starting thread pool executors")
        #     minio_future = executor.submit(self.minio_thread,)
        #     mongo_future = executor.submit(self.mongo_thread,)
            
        for message in self.consumer:
            
            # self.minio_queue.put([1])
            print("*****Running Consumer****")
            raw_image, process_image, incident_event, usecase_inform = self.messageParser(message)
            bucketname = "images"
            storageobj = StorageClass(incident_event,bucketname)
            final_incident_event = storageobj.update_dataconfig()
            # print("final_incident_event")
            # print(process_image.shape, raw_image.shape,len(incident_event),usecase_inform)

            # print("7"*100)
            #minio_queue.put([minioclient, raw_image, process_image, final_incident_event, mongobackupclient])
            print("=====Image Pushed To the Q========")
            self.minio_queue.put([self.minioclient, raw_image, process_image, final_incident_event, self.mongobackupclient])
            # print("========minio queue size before putting==========")
            # print(self.minio_queue.qsize())
            # mongo_queue.put([self.mongoclient, final_incident_event])
        
            self.mongo_queue.put([self.mongoclient,final_incident_event, self.mongoreportsclient])
            #self.minio_thread()
            #self.mongo_thread()
            # # self.minio_thread(self.minio_queue)
            # # self.mongo_thread(self.mongo_queue)
            # # with ThreadPoolExecutor(max_workers=2) as executor:
            # #     minio_future = executor.submit(self.minio_thread)
            # #     mongo_future = executor.submit(self.mongo_thread)


            # # minio_queue.put([self.minioclient, raw_image, process_image, final_incident_event, self.mongobackupclient])
            # # mongo_queue.put([self.mongoclient, final_incident_event])

            # # with ThreadPoolExecutor(max_workers=20) as executor:
            # #     minio_future = executor.submit(MinioStorage.save_miniodata, minio_queue)
            # #     mongo_future = executor.submit(MongoStorage.save_mongodata, mongo_queue)

            # #     minio_result = minio_future.result()
            # #     mongo_result = mongo_future.result() 
                

            
