from minio import Minio
from minio.error import S3Error
from pymongo import MongoClient


class CreateClient:
    """
    This class is creates Minio and MongoDB client objects.

    Args:
        config (dict): A dictionary containing config data details for minio and mongo.
    """

    def __init__(self, config):
        self.config = config
        self.minioconf = self.config["minio"]
        self.mongodbconf = self.config["mongodb"]
        # print(self.mongodbconf)

    def minio_client(self):
        """
        Creates and returns a Minio client object.

        Returns:
            Minio: A Minio client object for interacting with Minio storage.

        """
        minio_client = Minio(
            endpoint=self.minioconf["endpoint"],
            access_key=self.minioconf["access_key"],
            secret_key=self.minioconf["secret_key"],
            secure=self.minioconf["secure"],
        )
        return minio_client

    def mongo_client(self):
        """
        Creates and returns a MongoDB collection for the primary use case.

        Returns:
            mongocollection: A MongoDB collection object for the primary use case.

        """
        if "username" not in self.mongodbconf or "password" not in self.mongodbconf :
            mongo_client = MongoClient(
            host=self.mongodbconf["host"], port=self.mongodbconf["port"], 
            connect=self.mongodbconf["connect"]
        )


        elif self.mongodbconf['username'] and self.mongodbconf['password']:
            mongo_client = MongoClient(
                host=self.mongodbconf["host"], port=self.mongodbconf["port"], 
                username=self.mongodbconf["username"],
                password=self.mongodbconf["password"],
                connect=self.mongodbconf["connect"],
                authSource=self.mongodbconf["database"]
            )
            
        else:
            mongo_client = MongoClient(
            host=self.mongodbconf["host"], port=self.mongodbconf["port"], 
            connect=self.mongodbconf["connect"]
        )

        database = mongo_client[self.mongodbconf["database"]]
        collection = database[self.mongodbconf["collection"]]

        return collection

    def mongo_backupclient(self):
        """
        Creates and returns a MongoDB collection for backup.

        Returns:
            mongo_backup_colection: A MongoDB collection object for backup.

        """
        if "username" not in self.mongodbconf or "password" not in self.mongodbconf :
            mongo_client = MongoClient(
            host=self.mongodbconf["host"], port=self.mongodbconf["port"], 
            connect=self.mongodbconf["connect"]
        )
           
        elif self.mongodbconf['username'] and self.mongodbconf['password']:
            mongo_client = MongoClient(
                host=self.mongodbconf["host"], port=self.mongodbconf["port"], 
                username=self.mongodbconf["username"],
                password=self.mongodbconf["password"],
                connect=self.mongodbconf["connect"],
                authSource=self.mongodbconf["database"]
            )
            
        else:
            mongo_client = MongoClient(
            host=self.mongodbconf["host"], port=self.mongodbconf["port"], 
            connect=self.mongodbconf["connect"]
        )

        database = mongo_client[self.mongodbconf["database"]]
        collection = database[self.mongodbconf["backupcollection"]]

        return collection

    def mongo_reportsclient(self):
        """
        Creates and returns a MongoDB collection for reports.

        Returns:
            mongo_reports_collection: A MongoDB collection object for reports.

        """
        if "username" not in self.mongodbconf or "password" not in self.mongodbconf :
            mongo_client = MongoClient(
            host=self.mongodbconf["host"], port=self.mongodbconf["port"], 
            connect=self.mongodbconf["connect"]
        )
        elif self.mongodbconf['username'] and self.mongodbconf['password']:
            mongo_client = MongoClient(
                host=self.mongodbconf["host"], port=self.mongodbconf["port"], 
                username=self.mongodbconf["username"],
                password=self.mongodbconf["password"],
                connect=self.mongodbconf["connect"],
                authSource=self.mongodbconf["database"]
            )
            
        else:
            mongo_client = MongoClient(
            host=self.mongodbconf["host"], port=self.mongodbconf["port"], 
            connect=self.mongodbconf["connect"]
        )

        database = mongo_client[self.mongodbconf["database"]]
        collection = database[self.mongodbconf["reportscollection"]]

        return collection
