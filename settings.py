import os

STORAGE_DIR = '/tmp/data/'
METADATA_DIR = 'metadata'
DATA_DIR = 'data'
MONGODATABASENAME = 'digitalobjects'

STATUS_DRAFT = 'draft'
STATUS_COMMITTED = 'committed'
STATUS_PUBLISHED = 'published'
STATUS_DELETED = 'deleted'


DEFAULTSTATUS = STATUS_DRAFT

# import metadata_file_store
# md_store = metadata_file_store

import metadata_mongo
md_store = metadata_mongo

try:
    os.makedirs(STORAGE_DIR)
except:
    pass
