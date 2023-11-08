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

os.environ["SHARED_MEMORY_USE_LOCK"] = "1"
from src.createclient import CreateClient

topic_smd = SharedMemoryDict(name="topics", size=10000000)


def testcallbackFuture(future):
    if not future.running():
        print("===>", future)
    print("=======result====")
    print(future.result())
    print("=======callback future====", future.exception())


def testFuture(obj):
    # obj.loop()

    obj.connectConsumer()
    minio_future, mongo_future = obj.data_store()
    while True:
        # if not minio_future.running() or not mongo_future.running():
        #     minio_future.add_done_callback(testcallbackFuture)
        #     mongo_future.add_done_callback(testcallbackFuture)

        pass
    # print("======eception because of minio and mongo=====")

    # obj.data_store()
    # with ThreadPoolExecutor(max_workers=2) as executor:
    #     executor.submit(obj.runConsumer)
    #     executor.submit(obj.saveData)
    # o
    # # obj.saveData()

    # obj.callConsumer()
    print("=====created and connected=====")


class PoolConsumer:
    def __init__(self, data, logger):
        """
        Initialize the  Camera Group and connect with redis to take the recent configuration
        """
        self.config = data
        print("&" * 100)
        print(self.config)
        self.kafkahost = data["kafka"]
        pool = redis.ConnectionPool(host="localhost", port=6379, db=0)
        self.r = redis.Redis(connection_pool=pool)
        self.dict3 = {}
        self.log = logger
        # self.smd = SharedMemoryDict(name='tokens', size=1024)

    def startFuture(self, obj):
        print("Future")
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
        executor = ProcessPoolExecutor(10)
        listapp = []
        while True:
            topicdata = json.loads(self.r.get("topics"))

            for cam in topicdata.keys():
                # print("#####",camdata[cam])
                # print(camdata)
                # self.usecaseids=list(self.cachedata[str(cameraid)].keys())
                topic = topicdata[cam]["topic_name"]

                cam_id = topicdata[cam]["camera_id"]

                # if cam_id<50:
                print("======cam id=====", cam_id)
                if cam_id not in statusdict:
                    # print("*********",preproceesdata)
                    topic_smd[cam_id] = topicdata[cam]
                    clientobj = CreateClient(self.config)
                    # self.minioclient = clientobj.minio_client()
                    # self.mongoclient = clientobj.mongo_client()
                    obj = RawTopicConsumer(self.kafkahost, cam_id, self.config, self.log)
                    print("=====Topic Consumer created====")
                    statusdict[cam_id] = obj
                    print("=====obj=====")
                    future1 = executor.submit(testFuture, obj)
                    print("===========callback====", future1)
                    future1.add_done_callback(testcallbackFuture)
                    # listapp.append(future1)
                    futuredict[cam_id] = future1
                    print("====futuredict====")
                    print(futuredict)
                    self.log.info(f"Starting Conusmer for {cam_id}")

                else:
                    topic_smd[cam_id] = topicdata[cam]
                    self.log.info(f"Updating Data for {cam_id}")
                    # print(futuredict)
                    # print("=====else===",cam_id)
                    # print(futuredict[cam_id].done())
                    # print(futuredict[cam_id].running())
                    # #print("=====else===",cam_id)
                    if futuredict[cam_id].running() == False:
                        futuredict[cam_id].cancel()
                        topic_smd[cam_id] = topicdata[cam]
                        # preprocess_smd[cam_id]=camdata[cam]
                        obj = RawTopicConsumer(self.kafkahost, cam_id, self.config, self.log)
                        statusdict[cam_id] = obj
                        print("Starting consumer else====", cam_id)
                        future1 = executor.submit(testFuture, obj)
                        # listapp.append(future1)
                        print("======future====", future1)
                        futuredict[cam_id] = future1
                        self.log.info(f"Starting New Conusmer for {cam_id}")

                    else:
                        # print("Updating===>",cam_id)
                        # preproceesdata=self.getScheduleState(scheduledata,camdata[cam])
                        topic_smd[cam_id] = topicdata[cam]
                        self.log.info(f"Updating Data for {cam_id}")
                time.sleep(3)
            # print(statusdict)
            time.sleep(5)
            # print("preprocess_smd===>",postprocess_smd)