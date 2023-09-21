import cv2
import fastapi
import json
import base64
import imutils
import io
import uuid
import numpy as np
from datetime import datetime
from multiprocessing import Process, current_process

import threading

from minio import Minio
from minio.error import S3Error
from src.storefailedimages import Minio_fail


class StorageClass:
    def __init__(self, dataconfig, bucket_name):
        self.dataconfig=dataconfig
        self.bucket_name = bucket_name
        print("%"*100)
        print(dataconfig)
        try:
            self.image_name = dataconfig['image']['name']
        except:
            self.image_name = dataconfig['image_meta']['name']
        self.customer_id = dataconfig['hierarchy']['customer_id']
        self.camera_id = dataconfig['hierarchy']['camera_id']
        self.subsite_id = dataconfig['hierarchy']['subsite_id']
        self.location_id = dataconfig['hierarchy']['location_id']
        self.zone_id = dataconfig['hierarchy']['zone_id']
        self.usecase_id = dataconfig['usecase']['id']
        self.image_time = dataconfig['time']['incident_time']

        # self.image_date = self.image_time[:10]
        # self.image_date = datetime.utcnow().strftime("%Y%m%d")
        self.image_date = "".join(dataconfig['time']['UTC_time'][:10].split("-"))
        self.customer_name = "customer" + str(self.customer_id).zfill(5)
        self.subsite_name = "subsite" + str(self.subsite_id).zfill(5)
        self.location_name = "location" + str(self.location_id).zfill(5)
        self.usecase_name = "usecase" + str(self.usecase_id).zfill(5)
        self.camera_name = "camera" + str(self.camera_id).zfill(5)
        self.zone_name = "zone" + str(self.zone_id).zfill(5)

    def rawimage_path(self):
        return self.customer_name + "/" + self.location_name + "/" + self.subsite_name + "/" + self.zone_name + "/" + self.camera_name + "/" + self.image_date  + "/raw/" 

    def processedimage_path(self):
        return self.customer_name + "/" + self.location_name + "/" + self.subsite_name + "/" + self.zone_name + "/" + self.camera_name  + "/" + self.image_date  + "/processed/"  + self.usecase_name + "/"

    def createsavepaths(self):
        rawpath = self.customer_name + "/" + self.location_name + "/" + self.subsite_name + "/" + self.zone_name + "/" + self.camera_name + "/" + self.image_date  + "/raw/" 
        processpath = self.customer_name + "/" + self.location_name + "/" + self.subsite_name + "/" + self.zone_name + "/" + self.camera_name  + "/" + self.image_date  + "/processed/"  + self.usecase_name + "/"
        return [rawpath,processpath]
    
    def update_dataconfig(self):
        self.dataconfig['documentId'] = str(uuid.uuid4())
        try:
            self.dataconfig['image']['storage']['raw'] = self.bucket_name + "/" + self.rawimage_path() +self.image_name
            self.dataconfig['image']['storage']['processed'] = self.bucket_name +"/"+ self.processedimage_path()+self.image_name
        except:
            self.dataconfig['image_meta']['storage']['raw'] = self.bucket_name + "/" + self.rawimage_path() +self.image_name
            self.dataconfig['image_meta']['storage']['processed'] = self.bucket_name +"/"+ self.processedimage_path()+self.image_name
        return self.dataconfig

class MinioStorage:
    # def save_miniodata(client,raw_image,processed_image,dataconfig):
    def save_miniodata(minio_queue):
        client,raw_image,processed_image,dataconfig,mongobackup_client = minio_queue.get()

        try:
            complete_raw_path = dataconfig['image']['storage']['raw']
            complete_process_path = dataconfig['image']['storage']['processed']
        except:
            complete_raw_path = dataconfig['image_meta']['storage']['raw']
            complete_process_path = dataconfig['image_meta']['storage']['processed']

        bucket_name = complete_raw_path.split("/")[0]
        raw_path =  "/".join(complete_raw_path.split("/")[1:])
        processed_path =  "/".join(complete_process_path.split("/")[1:])
        rawimage_bytes =  io.BytesIO(cv2.imencode(".jpg", raw_image)[1])
        processedimage_bytes =  io.BytesIO(cv2.imencode(".jpg", processed_image)[1])
        rawimage_val = rawimage_bytes.getvalue()
        processedimage_val = processedimage_bytes.getvalue()

        try:
            client.put_object(bucket_name, raw_path, rawimage_bytes, len(rawimage_val))
            client.put_object(bucket_name, processed_path, processedimage_bytes, len(processedimage_val))
            print(f"Raw img saved at {bucket_name}/{raw_path}")
            print(f"Processed img at {bucket_name}/{processed_path}")
        except: 
            minio_fail_obj = Minio_fail(raw_image,processed_image,dataconfig,mongobackup_client)
            minio_fail_obj.minio_store()
            print("error occurred.")

        complete_raw_path = bucket_name + "/" + raw_path
        complete_processed_path = bucket_name + "/" + processed_path 

class MongoStorage:
    # def save_mongodata(mongoclient,dataconfig):
    def save_mongodata(mongo_queue):
        mongoclient,dataconfig = mongo_queue.get()
        # print(dataconfig['image']['storage']['raw'])
        # print(dataconfig['image']['storage']['processed'] )
        mongoclient.insert_one(dataconfig)
        print(f"data inserted into {mongoclient}")       

    



 
