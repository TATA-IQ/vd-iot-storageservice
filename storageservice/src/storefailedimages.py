import os

import cv2
from console_logging.console import Console
console=Console()

class Minio_fail:
    """
    This class handles storing the failed Minio data and associated MongoDB backup.

    Args:
        raw_image (numpy.ndarray): Raw image data as a NumPy array to be saved.
        process_image (numpy.ndarray): Processed image data as a NumPy array to be saved.
        dataconfig (dict): A dictionary containing configuration data for storing the images.
        mongobackup_client: A MongoDB collection for backup
    """

    def __init__(self, raw_image, process_image, dataconfig, mongobackup_client):
        self.raw_image = raw_image
        self.process_image = process_image
        self.dataconfig = dataconfig
        self.mongobackup_client = mongobackup_client

    def minio_store(
        self,
    ):
        """
        Saves raw and processed image data locally and inserts dataconfig into MongoDB backup.

        Returns:
            None

        """
        try:
            complete_raw_path = self.dataconfig["image"]["storage"]["raw"]
            complete_process_path = self.dataconfig["image"]["storage"]["processed"]
        except:
            complete_raw_path = self.dataconfig["image_meta"]["storage"]["raw"]
            complete_process_path = self.dataconfig["image_meta"]["storage"]["processed"]

        local_raw_path = "/data/" + complete_raw_path
        os.makedirs(local_raw_path, exist_ok=True)
        # os.makedirs("/".join(local_raw_path.split("/")[:-1]), exist_ok=True)
        local_process_path = "/data/" + complete_process_path
        os.makedirs(local_process_path, exist_ok=True)
        # os.makedirs("/".join(local_process_path.split("/")[:-1]), exist_ok=True)

        cv2.imwrite(local_raw_path, self.raw_image)
        cv2.imwrite(local_process_path, self.process_image)
        console.info(" saved in local folder")

        # ================ mongo save ===========================
        self.dataconfig["image"]["storage"]["backup_localpath_raw"] = local_raw_path
        self.dataconfig["image"]["storage"]["backup_localpath_process"] = local_process_path
        self.mongobackup_client.insert_one(self.dataconfig)
        console.success(f"data inserted into {self.mongobackup_client}")
