import os
import uuid
import hashlib
import mongo

from settings import STORAGE_DIR, DATA_DIR

def create_digital_object():
    for i in range(10): # try up to 10 times to create an id
        object_uid = uuid.uuid4()
        object_dir = os.path.join(STORAGE_DIR, str(object_uid))
        try:
            os.makedirs(object_dir)
            os.makedirs(os.path.join(object_dir, DATA_DIR))
            return str(object_uid)
        except:
            import traceback
            print(traceback.format_exc())
            # if the directory exists or can't be created we try again
            pass

def get_status(object_id):
    return mongo.get_status(object_id)

def set_status(object_id, status):
    mongo.set_status(
        object_id, status
    )
    return status


def get_obj_dir(object_id):
    return os.path.join(STORAGE_DIR, object_id)
    


def get_data_dir(object_id):
    return os.path.join(get_obj_dir(object_id), DATA_DIR)

def list_dirs(a_dir):
    for name in os.listdir(a_dir):
        if os.path.isdir(os.path.join(a_dir, name)):
            yield name

def list_files(a_dir):
    for name in os.listdir(a_dir):
        if os.path.isfile(os.path.join(a_dir, name)):
            yield name

def count_files(a_dir):
    return len([1 for name in os.listdir(a_dir) \
                if os.path.isfile(os.path.join(a_dir, name))])

def get_checksum(file_name):
    BLOCKSIZE = 65536
    hasher = hashlib.sha1()
    with open(file_name, 'rb') as f:
        buf = f.read(BLOCKSIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = f.read(BLOCKSIZE)
    return hasher.hexdigest()


