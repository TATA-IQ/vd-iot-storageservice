"""
Code to start PreProcessing and Consumers
"""
import os

from shared_memory_dict import SharedMemoryDict
from sourcelogs.logger import create_rotating_log
from src.consumerpool import PoolConsumer
from src.parser import Config
import requests

os.environ["SHARED_MEMORY_USE_LOCK"] = "1"

topic_smd = SharedMemoryDict(name="topics", size=10000000)
def get_confdata(conf):
    res=requests.get(conf[0]["consul_url"])
    data=res.json()
    dbconf =None
    
    storageconf=None
    env=None
    consulconf=None
    if "pipelineconfig" in data:
        port=data["pipelineconfig"]["Port"]
        while True:
            endpoints=requests.get("http://pipelineconfig.service.consul:"+str(port)+"/").json()
            #print(endpoints)
            if "preprocess" in endpoints["endpoint"]:
                try:
                    storageconf=requests.get("http://pipelineconfig.service.consul:"+str(port)+"/"+endpoints["endpoint"]["storage"]).json()
                except Exception as ex:
                    print(ex)
                    time.sleep(5)
                    continue
            if "dbapi" in endpoints["endpoint"] and "dbapi" in data:
                try:
                    dbconf=requests.get("http://pipelineconfig.service.consul:"+str(port)+"/"+endpoints["endpoint"]["dbapi"]).json()
                except Exception as ex:
                    print(ex)
                    time.sleep(5)
                    continue
            
            if "kafka" in endpoints["endpoint"]:
                try:
                    kafkaconf=requests.get("http://pipelineconfig.service.consul:"+str(port)+"/"+endpoints["endpoint"]["kafka"]).json()
                except Exception as ex:
                    print(ex)
                    time.sleep(5)
                    continue
            if "consul" in endpoints["endpoint"]:
                try:
                    consulconf=requests.get("http://pipelineconfig.service.consul:"+str(port)+"/"+endpoints["endpoint"]["consul"]).json()
                except Exception as ex:
                    print(ex)
                    time.sleep(5)
                    continue
            print(dbconf)
            print(storageconf)
            if dbconf is not None and storageconf is not None and kafkaconf is not None:
                break
    print("******")
    print(dbconf)
    return  dbconf,storageconf,kafkaconf,consulconf
if __name__ == "__main__":
    try:
        topic_smd.shm.close()
        topic_smd.shm.unlink()
        del topic_smd
        confdata = Config.yamlconfig("config/config.yaml")
        logg = create_rotating_log("logs/logs.log")
        dbconf,storageconf,kafkaconf,consulconf=get_confdata(confdata)
        cg = PoolConsumer(storageconf,dbconf,kafkaconf, logg)
        cg.checkState()
    except KeyboardInterrupt:
        print("=====Removing Shared Memory Refrence=====")
        topic_smd.shm.close()
        topic_smd.shm.unlink()
        del topic_smd
