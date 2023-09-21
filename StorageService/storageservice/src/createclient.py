from minio import Minio
from minio.error import S3Error
from pymongo import MongoClient

class CreateClient():
    def __init__(self,config):
        self.config=config
        self.minioconf = self.config['minio']
        self.mongodbconf = self.config['mongodb']
        # print(self.mongodbconf)
    
    def minio_client(self):
        minio_client = Minio(endpoint=self.minioconf["endpoint"],
                    access_key=self.minioconf["access_key"],
                    secret_key=self.minioconf["secret_key"],
                    secure=self.minioconf["secure"],)
        return minio_client
    
    def mongo_client(self):
        mongo_client = MongoClient(host = self.mongodbconf['host'], 
                                    port = self.mongodbconf['port'],
                                    connect=self.mongodbconf['connect'])

        database = mongo_client[self.mongodbconf['database']]
        collection  = database[self.mongodbconf['collection']]

        return collection

    def mongo_backupclient(self):
        mongo_client = MongoClient(host = self.mongodbconf['host'], 
                                    port = self.mongodbconf['port'],
                                    connect=self.mongodbconf['connect'])

        database = mongo_client[self.mongodbconf['database']]
        collection  = database[self.mongodbconf['backupcollection']]

        return collection