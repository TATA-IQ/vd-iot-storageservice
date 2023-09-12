"""
Code to start PreProcessing and Cosnumers
"""
import os
from shared_memory_dict import SharedMemoryDict
from src.consumerpool import PoolConsumer
from src.parser import Config
from sourcelogs.logger import create_rotating_log


os.environ["SHARED_MEMORY_USE_LOCK"] = "1"

topic_smd = SharedMemoryDict(name="topics", size=10000000)

if __name__ == "__main__":
    try:
        topic_smd.shm.close()
        topic_smd.shm.unlink()
        del topic_smd
        data = Config.yamlconfig("config/config.yaml")
        # print(data[0]["kafka"])
        logg = create_rotating_log("logs/logs.log")
        cg = PoolConsumer(data[0]["kafka"], logg)
        cg.checkState()
    except KeyboardInterrupt:
        print("=====Removing Shared Memory Refrence=====")
        topic_smd_smd.shm.close()
        
        del topic_smd
