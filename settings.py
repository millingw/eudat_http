import os

STORAGE_DIR = '/tmp/data/'
DATA_DIR = 'data'
MONGODATABASENAME = 'digitalobjects'
DEFAULTSTATUS = 'draft'

try:
    os.makedirs(STORAGE_DIR)
except:
    pass
