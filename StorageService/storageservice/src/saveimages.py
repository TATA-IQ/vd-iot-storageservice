import cv2
import fastapi
import json
import base64
import imutils
import io
import numpy as np
from datetime import datetime
from multiprocessing import Process, current_process

import threading

from minio import Minio
from minio.error import S3Error

# client = Minio(endpoint="172.16.0.170:9000",
#         access_key="R8mQTw7uevt178G216KM",
#         secret_key="yMHbGFngNM5OnuWFj5lUDBJozEjryGAPBeF34WuG",
#         secure = False
#     )

class MinioSave:
    def __init__(self, minioclient, raw_image, processed_image, dataconfig):
        self.client = minioclient
        self.raw_image = raw_image
        self.processed_image = processed_image

        self.image_name = dataconfig['image']['name']
        self.customer_id = dataconfig['hierarchy']['customer_id']
        self.camera_id = dataconfig['hierarchy']['camera_id']
        self.subsite_id = dataconfig['hierarchy']['subsite_id']
        self.location_id = dataconfig['hierarchy']['location_id']
        self.zone_id = dataconfig['hierarchy']['zone_id']
        self.usecase_id = dataconfig['usecase']['id']
        self.image_time = dataconfig['time']['incident_time']

        # self.image_date = self.image_time[:10]
        self.image_date = datetime.utcnow().strftime("%Y%m%d")
        self.customer_name = "customer" + str(self.customer_id).zfill(5)
        self.subsite_name = "subsite" + str(self.subsite_id).zfill(5)
        self.location_name = "location" + str(self.location_id).zfill(5)
        self.usecase_name = "usecase" + str(self.usecase_id).zfill(5)
        self.camera_name = "camera" + str(self.camera_id).zfill(5)
        self.zone_name = "zone" + str(self.zone_id).zfill(5)

        # self.bucket_name = self.customer_name
        self.bucket_name = "images"
        if not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)
            print(f"Bucket {self.bucket_name} created")
        else:
            print(f"Bucket {self.bucket_name} already exists")
    
    def create_bucket(self):
        self.bucket_name = self.customer_name
        if not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)
        else:
            print(f"Bucket {self.bucket_name} already exists")

    def rawimage_path(self):
        return self.customer_name + "/" + self.location_name + "/" + self.subsite_name + "/" + self.zone_name + "/" + self.camera_name + "/" + self.image_date  + "/raw/" 

    def processedimage_path(self):
        return self.customer_name + "/" + self.location_name + "/" + self.subsite_name + "/" + self.zone_name + "/" + self.camera_name  + "/" + self.image_date  + "/processed/"  + self.usecase_name + "/"



    def save_raw_processed_image(self,):
                
        raw_path = self.rawimage_path() + self.image_name
        processed_path = self.processedimage_path() + self.image_name

        rawimage_bytes =  io.BytesIO(cv2.imencode(".jpg", self.raw_image)[1])
        processedimage_bytes =  io.BytesIO(cv2.imencode(".jpg", self.processed_image)[1])
        rawimage_val = rawimage_bytes.getvalue()
        processedimage_val = processedimage_bytes.getvalue()

        try:
            self.client.put_object(self.bucket_name, raw_path, rawimage_bytes, len(rawimage_val))
            self.client.put_object(self.bucket_name, processed_path, processedimage_bytes, len(processedimage_val))
            print(f"Raw img saved at {self.bucket_name}/{raw_path}")
            print(f"Processed img at {self.bucket_name}/{processed_path}")
        except S3Error as exc:
            print("error occurred.", exc)

        complete_raw_path = self.bucket_name + "/" + raw_path
        complete_processed_path = self.bucket_name + "/" + processed_path        

        return [complete_raw_path,complete_processed_path]
        
class MongoDBSave:
    def __init__(self, mongodbclient, dataconfig):
        self.mongoclient = mongodbclient
        self.dataconfig = dataconfig
        # self.rawimage_path = saved_path[0]
        # self.processedimage_path = saved_path[1]
        self.image_name = dataconfig['image']['name']
        self.customer_id = dataconfig['hierarchy']['customer_id']
        self.camera_id = dataconfig['hierarchy']['camera_id']
        self.subsite_id = dataconfig['hierarchy']['subsite_id']
        self.location_id = dataconfig['hierarchy']['location_id']
        self.zone_id = dataconfig['hierarchy']['zone_id']
        self.usecase_id = dataconfig['usecase']['id']
        self.image_time = dataconfig['time']['incident_time']

        # self.image_date = self.image_time[:10]
        self.image_date = datetime.utcnow().strftime("%Y%m%d")
        self.customer_name = "customer" + str(self.customer_id).zfill(5)
        self.subsite_name = "subsite" + str(self.subsite_id).zfill(5)
        self.location_name = "location" + str(self.location_id).zfill(5)
        self.usecase_name = "usecase" + str(self.usecase_id).zfill(5)
        self.camera_name = "camera" + str(self.camera_id).zfill(5)
        self.zone_name = "zone" + str(self.zone_id).zfill(5)

        # self.bucket_name = self.customer_name
        self.bucket_name = "images"

    def rawimage_path(self):
        return self.customer_name + "/" + self.location_name + "/" + self.subsite_name + "/" + self.zone_name + "/" + self.camera_name + "/" + self.image_date  + "/raw/" 

    def processedimage_path(self):
        return self.customer_name + "/" + self.location_name + "/" + self.subsite_name + "/" + self.zone_name + "/" + self.camera_name  + "/" + self.image_date  + "/processed/"  + self.usecase_name + "/"


    def save_mongodata(self,):
        self.dataconfig['documentId'] = str(uuid.uuid4())
        self.dataconfig['image']['storage']['raw'] = self.bucket_name +"/" + self.rawimage_path()
        self.dataconfig['image']['storage']['processed'] = self.bucket_name +"/"+ self.processedimage_path()
        print(self.bucket_name +"/" +self.rawimage_path())
        print(self.bucket_name +"/" +self.processedimage_path())
        self.mongoclient.insert_one(self.dataconfig)
        print(f"data inserted into {self.mongoclient}")

    



 
