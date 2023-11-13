"""cache code"""
from caching.rediscaching import Caching
from src.parser import Config

path = "config/config.yaml"
configdata = Config.yamlconfig(path)[0]
print(configdata)
api = configdata["apis"]
kafka=configdata["kafka"]
topic=configdata["event_topic"][0]
camera_group=[]
customer=[]
subsite=[]
location=[]

if configdata["readConfig"]:
    try:
        customer = configdata["config"]["customer"]
    except RuntimeWarning as ex:
        print("Customer Ids Not found: ", ex)
        customer = []
    try:
        subsite = configdata["config"]["subsite"]
    except RuntimeWarning as ex:
        print("Subsite Ids Not Found: ", ex)
        subsite = []
    try:
        location = configdata["config"]["location"]
    except RuntimeWarning as ex:
        print("Location not found: ", ex)
        location = []
    try:
        camera_group = configdata["config"]["group"]
    except RuntimeWarning as ex:
        print("Camera Group Not Found: ", ex)
        camera_group = []

cs = Caching(api, camera_group, customer, location, subsite)
cs.checkEvents(kafka,topic)
