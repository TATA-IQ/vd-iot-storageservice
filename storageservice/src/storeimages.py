#import base64
import io
# import json
# import threading
import uuid
from datetime import datetime
from multiprocessing import Process, current_process

import cv2
# import fastapi
# import imutils
import numpy as np
# from minio import Minio
# from minio.error import S3Error
from src.storefailedimages import Minio_fail


class StorageClass:
    """
    This class represents a storage configuration for image data.

    Args:
        dataconfig (dict): A dictionary containing configuration data
        for the image storage from kafka topics.
    """

    def __init__(self, dataconfig):
        # self.bucket_name = "images"
        self.dataconfig = dataconfig
        # self.bucket_name = bucket_name
        # print("%"*100)
        # print(dataconfig)
        try:
            self.image_name = dataconfig["image"]["name"]
        except:
            self.image_name = dataconfig["image_meta"]["name"]
        self.camera_id = dataconfig["hierarchy"]["camera_id"]
        self.customer_id = dataconfig["hierarchy"]["customer_id"]
        self.subsite_id = dataconfig["hierarchy"]["subsite_id"]
        self.location_id = dataconfig["hierarchy"]["location_id"]
        self.zone_id = dataconfig["hierarchy"]["zone_id"]
        self.usecase_id = dataconfig["usecase"]["usecase_id"]
        self.image_time = dataconfig["time"]["incident_time"]
        # try:
        #     print("timestamp== ",dataconfig['time']['timestamp'])
        # except Exception as e:
        #     print(e)
        # self.image_date = self.image_time[:10]
        # self.image_date = datetime.utcnow().strftime("%Y%m%d")
        self.image_date = "".join(dataconfig["time"]["UTC_time"][:10].split("-"))
        self.customer_name = "customer-" + str(self.customer_id)
        self.subsite_name = "subsite-" + str(self.subsite_id)
        self.location_name = "location-" + str(self.location_id)
        self.usecase_name = "usecase-" + str(self.usecase_id)
        self.camera_name = "camera-" + str(self.camera_id)
        self.zone_name = "zone-" + str(self.zone_id)

        self.bucket_name = self.customer_name

    def rawimage_path(self):
        """
        Returns the path for storing the raw image data.

        Returns:
            str: The path for storing the raw image data.
        """
        return (
            self.location_name
            + "/"
            + self.subsite_name
            + "/"
            + self.zone_name
            + "/"
            + self.camera_name
            + "/"
            + self.image_date
            + "/raw/"
        )

    def processedimage_path(self):
        """
        Returns the path for storing the processed image data.

        Returns:
            str: The path for storing the processed image data.
        """
        return (
            self.location_name
            + "/"
            + self.subsite_name
            + "/"
            + self.zone_name
            + "/"
            + self.camera_name
            + "/"
            + self.usecase_name
            + "/"
            + self.image_date
            + "/processed/"
        )

    def createsavepaths(self):
        """
        Creates and returns the paths for both raw and processed image storage.

        Returns:
            list: A list containing paths for raw and processed image storage.
        """
        rawpath = (
            self.customer_name
            + "/"
            + self.location_name
            + "/"
            + self.subsite_name
            + "/"
            + self.zone_name
            + "/"
            + self.camera_name
            + "/"
            + self.image_date
            + "/raw/"
        )
        processpath = (
            self.customer_name
            + "/"
            + self.location_name
            + "/"
            + self.subsite_name
            + "/"
            + self.zone_name
            + "/"
            + self.camera_name
            + "/"
            + self.image_date
            + "/processed/"
            + self.usecase_name
            + "/"
        )
        return [rawpath, processpath]

    def update_dataconfig(self):
        """
        Updates the data configuration with storage path of raw and processed images in minio and returns the modified dataconfig.

        Returns:
            dict: The modified data configuration.
        """
        self.dataconfig["documentId"] = str(uuid.uuid4())
        try:
            self.dataconfig["image"]["storage"]["raw"] = self.bucket_name + "/" + self.rawimage_path() + self.image_name
            self.dataconfig["image"]["storage"]["processed"] = (
                self.bucket_name + "/" + self.processedimage_path() + self.image_name
            )
        except:
            self.dataconfig["image_meta"]["storage"]["raw"] = (
                self.bucket_name + "/" + self.rawimage_path() + self.image_name
            )
            self.dataconfig["image_meta"]["storage"]["processed"] = (
                self.bucket_name + "/" + self.processedimage_path() + self.image_name
            )
        return self.dataconfig


class MinioStorage:
    """
    Saves the raw and processed image data in the Minio storage system.
    """

    def save_miniodata(client, raw_image, processed_image, dataconfig, mongobackup_client):
        """
        Saves raw and processed image data in the Minio storage system.

        Args:
            client: Minio client object for interacting with Minio storage.
            raw_image (numpy.ndarray): Raw image data to be saved.
            processed_image (numpy.ndarray): Processed image data to be saved.
            dataconfig (dict): Configuration data for storing the images.
            mongobackup_client: Client object for MongoDB backup.

        Returns:
            None
        """
        # def save_miniodata(minio_queue):
        # client,raw_image,processed_image,dataconfig,mongobackup_client = minio_queue.get()
        try:
            complete_raw_path = dataconfig["image"]["storage"]["raw"]
            complete_process_path = dataconfig["image"]["storage"]["processed"]
        except:
            complete_raw_path = dataconfig["image_meta"]["storage"]["raw"]
            complete_process_path = dataconfig["image_meta"]["storage"]["processed"]

        bucket_name = complete_raw_path.split("/")[0]
        raw_path = "/".join(complete_raw_path.split("/")[1:])
        processed_path = "/".join(complete_process_path.split("/")[1:])

        try:
            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)
                print(f"Bucket {bucket_name} created")
            else:
                print(f"Bucket {bucket_name} already exists")
        except Exception as e:
            print("exception occured while created bucket",e)

        rawimage_bytes = io.BytesIO(cv2.imencode(".jpg", raw_image)[1])
        processedimage_bytes = io.BytesIO(cv2.imencode(".jpg", processed_image)[1])
        rawimage_val = rawimage_bytes.getvalue()
        processedimage_val = processedimage_bytes.getvalue()

        try:
            client.put_object(bucket_name, raw_path, rawimage_bytes, len(rawimage_val))
            client.put_object(bucket_name, processed_path, processedimage_bytes, len(processedimage_val))
            print(f"Raw img saved at {bucket_name}/{raw_path}")
            print(f"Processed img at {bucket_name}/{processed_path}")
        except:
            minio_fail_obj = Minio_fail(raw_image, processed_image, dataconfig, mongobackup_client)
            minio_fail_obj.minio_store()
            print("error occurred.")

        complete_raw_path = bucket_name + "/" + raw_path
        complete_processed_path = bucket_name + "/" + processed_path


class MongoStorage:
    """
    Saves of storage data in a MongoDB database.
    """

    def save_mongodata(mongoclient, dataconfig, mongo_reportsclient):
        """
        Saves data to the MongoDB database.

        Args:
            mongoclient: MongoDB client object for interacting with the database.
            dataconfig (dict): Configuration data to be saved to the database.
            mongo_reportsclient: MongoDB client object for reports collection used for summarization.

        Returns:
            None

        """

        # def save_mongodata(mongo_queue):
        # mongoclient,dataconfig = mongo_queue.get()
        # print(dataconfig['image']['storage']['raw'])
        # print(dataconfig['image']['storage']['processed'] )
        mongoclient.insert_one(dataconfig)
        print(f"data inserted into {mongoclient}")

        reportkeys = ["documentId", "usecase", "image", "hierarchy", "time"]

        print("incident count: ", dataconfig["incident_count"])
        if dataconfig["incident_count"] > 0:
            print("saving in reports mongodb")
            reportsdict = dict(zip(reportkeys, [dataconfig[k] for k in reportkeys]))
            reportsdict["incident"] = reportsdict["usecase"]["incident"]
            del reportsdict["usecase"]["incident"]
            mongo_reportsclient.insert_one(reportsdict)
        else:
            print("no incidents")
