import redis
import json
pool = redis.ConnectionPool(host="localhost", port=6379, db=0)
r = redis.Redis(connection_pool=pool)
data=r.get("topics")
print(data)
print("===============Scheduling Cache Data========")
print(json.loads(data))
print("*********************************")