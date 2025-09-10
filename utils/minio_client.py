from minio import Minio

class MinioClient:
    def __init__(self, minio_cfg):
        self.client = Minio(
            endpoint=minio_cfg['endpoint'],
            access_key=minio_cfg['access_key'],
            secret_key=minio_cfg['secret_key'],
            secure=minio_cfg['secure']
        )
        self.bucket_name = minio_cfg['bucket_name']
        self.create_bucket()

    def create_bucket(self):
        if not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)
            print(f'Bucket "{self.bucket_name}" created.')
        else:
            print(f'Bucket "{self.bucket_name}" already exists.')

    def get_buckets(self):
        try:
            objects = self.client.list_objects(self.bucket_name, recursive=True)
            for obj in objects:
                print(f"Object: {obj.object_name}, Size: {obj.size}, Last Modified: {obj.last_modified}")
        except Exception as e:
            print(f'Error listing objects: {e}')

    def get_file(self, bucket_name, object_name):
        try:
            response = self.client.get_object(bucket_name, object_name)
            data = response.read().decode('utf-8')
            response.close()
            response.release_conn()
            return data
        except Exception as e:
            print(f'Error getting object: {e}')
            return None

    def is_file_exist(self, objectn_name):
        try:
            self.client.stat_object(self.bucket_name, objectn_name)
            return True
        except Exception as e:
            print(f'Error checking object existence: {e}')
            return False

    def remove_file(self, object_name):
        try:
            self.client.remove_object(self.bucket_name, object_name)
            print(f'Object "{object_name}" removed from bucket "{self.bucket_name}".')
        except Exception as e:
            print(f'Error removing object: {e}')