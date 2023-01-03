import os.path

import oss2
from notefile.compress import tarfile
from notesecret import read_secret

global bucket
bucket = None


def get_bucket():
    global bucket
    if bucket is not None:
        return bucket
    auth = oss2.Auth(
        access_key_id=read_secret("aliyun", "oss", "access_key_id"),
        access_key_secret=read_secret("aliyun", "oss", "access_key_secret"))
    bucket = oss2.Bucket(
        auth,
        endpoint=read_secret("aliyun", "oss", "endpoint"),
        bucket_name=read_secret("aliyun", "oss", "bucket"))


def oss_upload(file1, path='notecoin', tar=False):
    if not tar:
        with open(file1, 'rb') as f:
            bucket.put_object(f"{path}/{os.path.basename(file1)}", f)
        return
    tmp_tar_file = os.path.basename(file1) + ".tar.gz"
    with tarfile.open(tmp_tar_file, "w|xz") as tar:
        tar.add(file1)
    with open(tmp_tar_file, 'rb') as f:
        bucket.put_object(f"{path}/{os.path.basename(tmp_tar_file)}", f)
    os.remove(tmp_tar_file)
