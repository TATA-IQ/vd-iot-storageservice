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
def get_service_address(consul_client,service_name,env):
    while True:
        
        try:
            services=consul_client.catalog.service(service_name)[1]
            print(services)
            for i in services:
                if env == i["ServiceID"].split("-")[:-1]:
                    return i
        except:
            time.sleep(10)
            continue
def get_confdata(consul_conf):
    consul_client = consul.Consul(host=consul_conf["host"],port=consul_conf["port"])
    pipelineconf=get_service_address(consul_client,"pipelineconfig",consul_conf["env"])

    
    
    env=consul_conf["env"]
    
    endpoint_addr="http://"+pipelineconf["ServiceAddress"]+":"+str(pipelineconf["ServicePort"])
    print("endpoint addr====",endpoint_addr)
    while True:
        
        try:
            res=requests.get(endpoint_addr+"/")
            endpoints=res.json()
            print("===got endpoints===",endpoints)
            break
        except Exception as ex:
            print("endpoint exception==>",ex)
            time.sleep(10)
            continue
    
    while True:
        try:
            res=requests.get(endpoint_addr+endpoints["endpoint"]["storage"])
            storageconf=res.json()
            print("storageconf===>",storageconf)
            break
            

        except Exception as ex:
            print("storageconf exception==>",ex)
            time.sleep(10)
            continue
    while True:
        try:
            res=requests.get(endpoint_addr+endpoints["endpoint"]["kafka"])
            kafkaconf=res.json()
            print("kafkaconf===>",kafkaconf)
            break
            

        except Exception as ex:
            print("kafkaconf exception==>",ex)
            time.sleep(10)
            continue
    print("=======searching for dbapi====")
    while True:
        try:
            print("=====consul search====")
            dbconf=get_service_address(consul_client,"dbapi",consul_conf["env"])
            print("****",dbconf)
            dbhost=dbconf["ServiceAddress"]
            dbport=dbconf["ServicePort"]
            res=requests.get(endpoint_addr+endpoints["endpoint"]["dbapi"])
            dbres=res.json()
            print("===got db conf===")
            print(dbres)
            break
        except Exception as ex:
            print("db discovery exception===",ex)
            time.sleep(10)
            continue
    for i in dbres["apis"]:
        print("====>",i)
        dbres["apis"][i]="http://"+dbhost+":"+str(dbport)+dbres["apis"][i]

    
    
    print("======dbres======")
    print(dbres)
    print(storageconf)
    #postprocessapi="http://"+pphost+":"+str(ppport)+storageconf["postprocess"]
    return  dbres,storageconf,kafkaconf

if __name__ == "__main__":
    try:
        topic_smd.shm.close()
        topic_smd.shm.unlink()
        del topic_smd
        confdata = Config.yamlconfig("config/config.yaml")
        logg = create_rotating_log("logs/logs.log")
        dbconf,storageconf,kafkaconf=get_confdata(confdata[0]["consul"])
        cg = PoolConsumer(storageconf,dbconf,kafkaconf, logg)
        cg.checkState()
    except KeyboardInterrupt:
        print("=====Removing Shared Memory Refrence=====")
        topic_smd.shm.close()
        topic_smd.shm.unlink()
        del topic_smd
