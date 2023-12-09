"""cache code"""
from caching.rediscaching import Caching
import requests
from src.parser import Config
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
            print("http://pipelineconfig.service.consul:"+str(port)+"/"+endpoints["endpoint"]["storage"])
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

path = "config/config.yaml"
configdata = Config.yamlconfig(path)
dbconf,storageconf,kafkaconf,consulconf=get_confdata(configdata)
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
