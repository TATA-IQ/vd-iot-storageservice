"""cache code"""
from caching.rediscaching import Caching
import requests
import consul
import time
from src.parser import Config
def get_service_address(consul_client,service_name,env):
    while True:
        
        try:
            services=consul_client.catalog.service(service_name)[1]
            print(services)
            for i in services:
                if env == i["ServiceID"].split("-")[-1]:
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

path = "config/config.yaml"
configdata = Config.yamlconfig(path)
dbconf,storageconf,kafkaconf,consulconf=get_confdata(configdata[0]["consul"])
print(configdata)
api = dbconf["apis"]
kafka=kafkaconf["kafka"]
topic=kafkaconf["event_topic"][0]
print("=======")
print(storageconf)
redis_server=storageconf["redis"]
camera_group=[]
customer=[]
subsite=[]
location=[]

if storageconf["read_config"]:
    try:
        customer = storageconf["config"]["customer"]
    except RuntimeWarning as ex:
        print("Customer Ids Not found: ", ex)
        customer = []
    try:
        subsite = storageconf["config"]["subsite"]
    except RuntimeWarning as ex:
        print("Subsite Ids Not Found: ", ex)
        subsite = []
    try:
        location = storageconf["config"]["location"]
    except RuntimeWarning as ex:
        print("Location not found: ", ex)
        location = []
    try:
        camera_group = storageconf["config"]["group"]
    except RuntimeWarning as ex:
        print("Camera Group Not Found: ", ex)
        camera_group = []

cs = Caching(api, redis_server,camera_group, customer, location, subsite)
cs.checkEvents(kafka,topic)
