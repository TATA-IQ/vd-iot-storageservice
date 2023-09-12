from src.parser import Config
from caching.rediscaching import Caching 

path="config/config.yaml"
configdata=Config.yamlconfig(path)[0]
print (configdata)
api=configdata["apis"]
if configdata:
    try:
        customer = configdata["config"]["customer"]
    except RuntimeWarning as ex:
        print("Customer Ids Not found: ", ex)
        customer = None
    try:
        subsite = configdata["config"]["subsite"]
    except RuntimeWarning as ex:
        print("Subsite Ids Not Found: ", ex)
        subsite = None
    try:
        location = configdata["config"]["location"]
    except RuntimeWarning as ex:
        print("Location not found: ", ex)
        location = None
    try:
        camera_group = configdata["config"]["group"]
    except RuntimeWarning as ex:
        print("Camera Group Not Found: ", ex)
        camera_group = None

cs = Caching(api,camera_group,customer,location,subsite)
cs.persistData()
    
    

 
