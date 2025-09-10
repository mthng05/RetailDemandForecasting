from utils.load_config import load_cfg
from utils.upload_datalake import UploadMinio

config = load_cfg('config/config.yaml')
dataset_path = config['dataset']
minio_cfg = config['minio']

class BatchProcessor:
    def __init__(self):
        self.minio = UploadMinio(minio_cfg, dataset_path)

    def run(self):
        self.minio.upload_directory()

if __name__ == "__main__":
    processor = BatchProcessor()
    processor.run()
