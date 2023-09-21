import os
import cv2

class Minio_fail:
    def __init__(self, raw_image, process_image, dataconfig,mongobackup_client):
        self.raw_image = raw_image
        self.process_image = process_image
        self.dataconfig = dataconfig
        self.mongobackup_client = mongobackup_client
    
    def minio_store(self,):
        try:
            complete_raw_path = self.dataconfig['image']['storage']['raw']
            complete_process_path = self.dataconfig['image']['storage']['processed']
        except:
            complete_raw_path = self.dataconfig['image_meta']['storage']['raw']
            complete_process_path = self.dataconfig['image_meta']['storage']['processed']

        local_raw_path = "/data/"+complete_raw_path
        os.makedirs("/".join(local_raw_path.split("/")[:-1]),exist_ok = True)
        local_process_path = "/data/"+complete_process_path
        os.makedirs("/".join(local_process_path.split("/")[:-1]),exist_ok = True)

        cv2.imwrite(local_raw_path, self.raw_image)
        cv2.imwrite(local_process_path,self.process_image)
        print("===saved in local folder===")

        # ================ mongo save ===========================
        self.dataconfig['image']['storage']['backup_localpath_raw'] = local_raw_path
        self.dataconfig['image']['storage']['backup_localpath_process']=local_process_path
        self.mongobackup_client.insert_one(self.dataconfig)
        print(f"data inserted into {self.mongobackup_client}") 



