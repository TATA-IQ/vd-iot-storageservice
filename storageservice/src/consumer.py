""" consumer code"""
import ast
import base64
import json
import multiprocessing as mp
import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from io import BytesIO
from queue import Queue

import cv2
import numpy as np
from kafka import KafkaConsumer, TopicPartition
from PIL import Image
from shared_memory_dict import SharedMemoryDict
from src.createclient import CreateClient
from src.parser import Config
from src.storeimages import MinioStorage, MongoStorage, StorageClass
from console_logging.console import Console
console=Console()

# from src.saveimages import MinioSave, MongoDBSave

os.environ["SHARED_MEMORY_USE_LOCK"] = "1"

topic_smd = SharedMemoryDict(name="topics", size=10000000)

manager = mp.Manager()
# minio_queue=manager.Queue()
# mongo_queue=manager.Queue()
# config = Config.yamlconfig("config/config.yaml")[0]
# clientobj = CreateClient(config)
# minioclient = clientobj.minio_client()
# mongoclient = clientobj.mongo_client()
# mongobackupclient = clientobj.mongo_backupclient()


def future_callback_error_logger(future):
    e = future.exception()
    console.error(f"Consumer Future error: {e}")


class RawTopicConsumer:
    """
    This class represents a Kafka consumer for processing raw image data.

    Args:
        kafkashost (str): The Kafka server's host address.
        cameraid (str): The ID of the camera.
        config (dict): A dictionary containing configuration data.
        logger: A logger for logging messages
    """

    def __init__(self, kafkashost, cameraid, config, logger):
        self.kill = False
        self.camera_id = cameraid
        self.kafkahost = kafkashost
        self.config = config
        # self.minioclient=minioclient
        # self.mongoclient=mongoclient
        self.consumer = None
        self.log = logger
        self.check = False
        self.previous_time = datetime.now()
        self.executor = None
        data = topic_smd[cameraid]

        
        self.topic = data["topic_name"]
        self.log.info(f"Starting for {self.camera_id} and topic {self.topic}")

    def closeConsumer(self):
        """
        Closes the Kafka consumer.
        """
        if self.consumer:
            self.consumer.close()
            return True
        else:
            return False

    def loop(self):
        while True:
            pass

    def connectConsumer(self):
        """
        Connects the Kafka consumer to the specified topic and initializes client objects.

        Returns:
            None

        """

        # session_timeout_ms=10000,heartbeat_interval_ms=7000,
        # self.queue=Queue(100)
        console.info(f"Creating Kafka consumer for topic {0}".format(self.topic))
        self.consumer = KafkaConsumer(
            "out_" + self.topic,
            bootstrap_servers=self.kafkahost,
            auto_offset_reset="latest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id=self.topic,
        )
        # self.consumer.assign([TopicPartition(self.topic, 1)])
        console.success(f"Created Kafka consumer for topic {0}".format(self.topic))
        console.info(f"Mongo and Minio client intialization topic {0}".format(self.topic))

        clientobj = CreateClient(self.config)
        # self.manager=mp.Manager()
        
        
        self.minio_queue = Queue()
        self.mongo_queue = Queue()
        
        console.info(f"Mongo and Minio client creation topic {0}".format(self.topic))
        self.minioclient = clientobj.minio_client()
        self.mongoclient = clientobj.mongo_client()
        console.success(f"Mongo and Minio client created topic {0}".format(self.topic))
        console.info(f"Mongo and Minio Backup client creation topic {0}".format(self.topic))
        self.mongobackupclient = clientobj.mongo_backupclient()
        self.mongoreportsclient = clientobj.mongo_reportsclient()
        console.success(f"Mongo and Minio Backup client creation topic {0}".format(self.topic))
        self.executor = ThreadPoolExecutor(max_workers=3)
        self.log.info(f"Connected Consumer {self.camera_id} for {self.topic}")
        

    def isConnected(self):
        """
        checks if the Kafka consumer is connected or not.
        """
        
        return self.consumer.bootstrap_connected()

    def convert_image(self, image_str):
        """
        Converts an string image to a NumPy array.

        Args:
            image_str (str): The image data encoded as a string.

        Returns:
            image (np.ndarray): Image in NumPy or None if fails.

        """
        try:
            stream = BytesIO(image_str.encode("ISO-8859-1"))
            image = Image.open(stream).convert("RGB")
            imagearr = np.array(image)
            return imagearr
        except:
            try:
                if image_str:
                    imgarr = base64.b64decode(image_str)
                    imgarr = np.frombuffer(imgarr, dtype=np.uint8)
                    return cv2.imdecode(imgarr, cv2.IMREAD_COLOR)
            except Exception as e:
                cosnole.error(f"Image conversion error: {ex}")
                
            return None

    def messageParser(self, msg):
        """
        Parses a JSON message and extracts raw image, processed image, incident event, and usecase information.

        Args:
            msg (json): The JSON message to be parsed.

        Returns:
            tuple (tuple): containing raw image, processed image, incident_event (dict), usecase_inform(dict)

        """
        
        try:
            msg = json.loads(msg.value)
        except Exception as ex:
            console.error(f"Kafka Message parsing exception: {0}".format(ex))
            # print(msg)
        # msg = msg.value
        # try:
        #     msg=json.loads(msg.value)
        # except:
        #     msg=None

        try:
            raw_image_str = msg["raw_image"]
            raw_image = self.convert_image(raw_image_str)
        except:
            raw_image_str = None

        process_image_str = msg["processed_image"]
        process_image = self.convert_image(process_image_str)

        incident_event = msg["incident_event"]
        
        try:
            usecase_inform = msg["usecase"]
        except:
            usecase_inform = {}
        return raw_image, process_image, incident_event, usecase_inform

    def minio_thread(self):
        """
        Thread for processing and saving images in Minio storage.

        It continuously checks the Minio queue for image data and, when available, retrieves the data and saves it
        in Minio storage

        Returns:
            None

        """
        
        console.info(f"Minio Thread started for {self.topic}")
        count = 0
        while True:
            

            if not self.minio_queue.empty():
                
                data = self.minio_queue.get()
                
                client, raw_image, processed_image, dataconfig, mongobackup_client = data
                MinioStorage.save_miniodata(client, raw_image, processed_image, dataconfig, mongobackup_client)
                console.success("Saved image in minio")
            else:
                continue
                

    def mongo_thread(self):
        """
        Thread for saving the storage details for incidents and reports in Mongo database.

        It continuously checks the Mongo queue for data and, when available, retrieves the data and saves it
        in incident and reports collection in Mongo database

        Returns:
            None

        """

        console.info(f"Mongo thread started for {self.topic}")
        while True:
            
            if not self.mongo_queue.empty():
                

                mongoclient, dataconfig, mongoreportsclient = self.mongo_queue.get()
                
                MongoStorage.save_mongodata(mongoclient, dataconfig, mongoreportsclient)
                console.success("Stored data in mongo")
            else:
                continue
                

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
        """
        Starts the runConsumer, minio, and mongo threads using ThreadPoolExecutor.
        Returns:
            None

        """
        # with ThreadPoolExecutor(max_workers=3) as executor:
        runConsumer_future = self.executor.submit(self.runConsumer)
        minio_future = self.executor.submit(self.minio_thread)
        mongo_future = self.executor.submit(self.mongo_thread)

        runConsumer_future.add_done_callback(future_callback_error_logger)
        minio_future.add_done_callback(future_callback_error_logger)
        mongo_future.add_done_callback(future_callback_error_logger)
        return minio_future, mongo_future

    def runConsumer(self):
        """
        Runs the Kafka consumer for processing messages and storing data.

        It continuously processes messages from the Kafka topic, extracts data information and
        pushes data to the Minio and MongoDB queues for storage.

        Returns:
            None

        """

        console.info(f"===={self.camera_id} Message Parse Connected for Topic {self.topic}====")
        self.check = True

        self.log.info(f"Starting Message Parsing {self.camera_id} for {self.topic}")
        # with ThreadPoolExecutor(max_workers=2) as executor:
        #     print("Starting thread pool executors")
        #     minio_future = executor.submit(self.minio_thread,)
        #     mongo_future = executor.submit(self.mongo_thread,)

        for message in self.consumer:
            # self.minio_queue.put([1])
            # print("*****Running Consumer****")
            # print("====message parsing====")
            raw_image, process_image, incident_event, usecase_inform = self.messageParser(message)
            # print("===parsing done====")
            # bucketname = "images"
            storageobj = StorageClass(incident_event)
            final_incident_event = storageobj.update_dataconfig()
            self.minio_queue.put(
                [self.minioclient, raw_image, process_image, final_incident_event, self.mongobackupclient]
            )
            
            self.mongo_queue.put([self.mongoclient, final_incident_event, self.mongoreportsclient])
            try:
                usecase_id=usecase_inform["usecase_id"]
            except:
                usecase_id=""
            self.log.info(f"stored image for cameraid: {self.camera_id} and  usecaseid: {usecase_id}")
            console.success(f"stored image for cameraid: {self.camera_id} and usecaseid: {usecase_id}")
            

            