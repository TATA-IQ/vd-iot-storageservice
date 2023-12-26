import json
import multiprocessing as mp
import os
import time
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager

import redis
from kafka import KafkaConsumer
from shared_memory_dict import SharedMemoryDict
from src.consumer import RawTopicConsumer
from console_logging.console import Console
console=Console()

os.environ["SHARED_MEMORY_USE_LOCK"] = "1"
from src.createclient import CreateClient

topic_smd = SharedMemoryDict(name="topics", size=10000000)


def testcallbackFuture(future):
    if not future.running():
        console.info(f"future status: {0}".format(future))
    
    console.info(f"future result: {0} ".format(future.result()))
    console.error("EXCEPTION Consumerpool Future: {0}".format(future.exception()))


def testFuture(obj):
    # obj.loop()

    obj.connectConsumer()
    minio_future, mongo_future = obj.data_store()
    while True:
        if not minio_future.running() or not mongo_future.running():
            minio_future.add_done_callback(testcallbackFuture)
            mongo_future.add_done_callback(testcallbackFuture)
            break

        
    

class PoolConsumer:
    def __init__(self, storageconf,dbconf,kafkaconf, logger):
        """
        Initialize the  Camera Group and connect with redis to take the recent configuration
        """
        self.config = storageconf
        
        redis_server=storageconf["redis"]
        self.kafkahost = kafkaconf["kafka"]
        pool = redis.ConnectionPool(host=redis_server["host"], port=redis_server["port"], db=0)
        self.r = redis.Redis(connection_pool=pool)
        self.dict3 = {}
        self.log = logger
        # self.smd = SharedMemoryDict(name='tokens', size=1024)

    def startFuture(self, obj):
        
        a = 0
        obj.connectConsumer()
        # obj.startConsumer()
        # obj.messageParse()

        # while obj.isConnected():
        #     a=a+1
        # while True:
        #     a=a+1
        return 1

    def getScheduleState(self, scheduledata, camdata):
        """
        Get the current state of scheduling for each use case and camera
        Args:
            scheduledata
            camdata
        Returns:
            camdata
        """
        usecase = list(camdata.keys())
        # print(camdata)
        for i in usecase:
            schedule_id = camdata[i]["scheduling_id"]
            # print("=====>scheule===>",camdata[i])
            camdata[i]["current_state"] = scheduledata[str(schedule_id)]["current_state"]
        return camdata
    def remove_topic(self, camlist, futuredict):
        camlist = list(map(lambda x: int(x), camlist))
        return list(set(futuredict.keys()) - set(camlist))

    def checkState(self):
        """
        Always updates the data from the caching
        For ex: If any camera is added in group, it will check the group and start new process for camera or remove camera if
        camera is deleted from the group
        """

        listcam = []
        manager = Manager()
        statusdict = manager.dict()
        futuredict = {}
        # statusdict={}
        executor = ProcessPoolExecutor(100)
        listapp = []
        while True:
            try:
                topicdata = json.loads(self.r.get("topics"))
            except:
                continue
            camtoremove = self.remove_topic(topicdata.keys(), futuredict)
            self.log.info(f" These camera topics Have been Removed From Group {camtoremove}")
            for cam in camtoremove:
                
                futuredict[cam].cancel()
                del futuredict[cam]
                del statusdict[cam]
                self.log.info(f"Killing camera {cam} topic")
                console.info(f"Killing camera {cam} topic")

            for cam in topicdata.keys():
                
                topic = topicdata[cam]["topic_name"]

                cam_id = topicdata[cam]["camera_id"]

                if cam_id not in statusdict:
                    # print("*********",preproceesdata)
                    topic_smd[cam_id] = topicdata[cam]
                    clientobj = CreateClient(self.config)
                    
                    obj = RawTopicConsumer(self.kafkahost, cam_id, self.config, self.log)
                    # print("=====Topic Consumer created====")
                    statusdict[cam_id] = obj
                    
                    future1 = executor.submit(testFuture, obj)
                    
                    future1.add_done_callback(testcallbackFuture)
                    
                    futuredict[cam_id] = future1
                    self.log.info(f"Starting Conusmer for {cam_id}")

                else:
                    topic_smd[cam_id] = topicdata[cam]
                    self.log.info(f"Updating Data for {cam_id}")
                    #console.info(f"Updating Data for {cam_id}")
                    if futuredict[cam_id].running() == False:
                        futuredict[cam_id].cancel()
                        topic_smd[cam_id] = topicdata[cam]
                        obj = RawTopicConsumer(self.kafkahost, cam_id, self.config, self.log)
                        statusdict[cam_id] = obj
                        future1 = executor.submit(testFuture, obj)
                        futuredict[cam_id] = future1
                        self.log.info(f"Starting New Conusmer for {cam_id}")
                        #console.info(f"Starting New Consumer for  {cam_id}")


                    else:
                        topic_smd[cam_id] = topicdata[cam]
                        # self.log.info(f"Updating Data for {cam_id}")
                        # console.info(f"Updating Data for {cam_id}")
                time.sleep(2)
            time.sleep(2)
            