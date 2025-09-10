from utils.load_config import load_cfg
from utils.minio_client import MinioClient
import os 

class UploadMinio:
    def __init__(self, minio_cfg, dataset_cfg):
        self.client =  MinioClient(minio_cfg)
        self.bucket_name = self.client.bucket_name
        self.dataset_cfg = dataset_cfg

    def upload_directory(self):
        try:
            raw_path = f"./{self.dataset_cfg['raw_path']}"
            for file in os.listdir(raw_path):
                file_path = os.path.join(raw_path, file)
                file_name = file.replace('.csv', '')
                if os.path.isfile(file_path) and self.client.is_file_exist(file_name) is False:
                    self.client.client.fput_object(
                        bucket_name=self.bucket_name,
                        object_name=file_name,
                        file_path=file_path
                    )
                    print(f'Uploaded "{file_name}" to bucket "{self.bucket_name}".')
                else:
                    print(f'File "{file_name}" already exists in bucket "{self.bucket_name}" or is not a file.')
        except Exception as e:
            print(f'Error uploading file: {e}')