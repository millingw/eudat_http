from pymongo import MongoClient
from settings import MONGODATABASENAME, DEFAULTSTATUS


# stores objects in collection "objects"
# stores entities in collection "entities"

def clear_metadata():
    # clear the collections - for testing purposes only!
    client = MongoClient()
    db = client[MONGODATABASENAME]
    db.objects.delete_many({})
    db.entities.delete_many({})

# return a list of all the object ids in the database
def list_objects():
    client = MongoClient()
    db = client[MONGODATABASENAME]
    objects = db.objects.find()
    return objects

# store metadata for an object
# if the object already exists then the metadata will be updated,
# otherwise a new object will be created.
def store_metadata(object_id, metadata, status=DEFAULTSTATUS):
    client = MongoClient()
    db = client[MONGODATABASENAME]

    db.objects.update_one(
        {'object_id': object_id},
         {'$set':
             {'object_id': object_id, 'metadata': metadata, 'status': status, 'entities': [] },
         },
        upsert=True
    )


# get an object's metadata, or None if the object does not exist
def get_metadata(object_id):
    document = get_object(object_id)
    if document == None:
        return None
    return document['metadata']

# get an object's status
def get_status(object_id):
    document = get_object(object_id)
    if document == None:
        return None
    return document['status']

# get everything that the database knows about the object
def get_object(object_id):
    client = MongoClient()
    db = client[MONGODATABASENAME]
    document = db.objects.find_one({'object_id': object_id})
    if document == None:
        return None
    return document

# set the object status
def set_status(object_id, status):

    client = MongoClient()
    db = client[MONGODATABASENAME]
    db.objects.update_one(
        {'object_id': object_id},
        {'$set':
             {'status': status}
         },
         upsert=False
    )


# add or update an entity
def update_entity_old(entity_id, filename):
    client = MongoClient()
    db = client[MONGODATABASENAME]
    db.entities.update_one(
        {'entity_id': entity_id},
        {'$set':
             {'filename': filename}
         }
        ,
        upsert=True
    )


def update_entity(object_id, entity_id, filename):

    client = MongoClient()
    db = client[MONGODATABASENAME]

    db.objects.update_one(
        {'object_id': object_id},
        { '$addToSet' :
             {"entities" : {'filename' : filename , 'entity_id' : entity_id }}
        }
    )


# get information about an entity
def get_entity_old(entity_id):
    client = MongoClient()
    db = client[MONGODATABASENAME]

     # look for an existing entity
    document = db.entities.find_one({'entity_id': entity_id})
    return document


# get information about an entity
def get_entity(object_id, entity_id):
    client = MongoClient()
    db = client[MONGODATABASENAME]

     # look for an existing entity
    document = db.objects.find_one({'object_id': object_id, 'entities.entity_id': entity_id})
    return document


# delete an entity
def delete_entity_old(entity_id):
     client = MongoClient()
     db = client[MONGODATABASENAME]
     db.entities.delete_one( {'entity_id': entity_id})


# delete an entity
def delete_entity(object_id, entity_id):
     client = MongoClient()
     db = client[MONGODATABASENAME]

     db.objects.update_one(
        {'object_id': object_id},
        { '$pull' :
             {"entities" : {'entity_id' : entity_id }}
        }
    )




