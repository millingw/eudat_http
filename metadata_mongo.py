from pymongo import MongoClient
from settings import MONGODATABASENAME, STATUS_DRAFT


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

def create_object(object_id):
    db = MongoClient()[MONGODATABASENAME]
    db.objects.insert_one(
        {'object_id': object_id,
         'metadata': None,
         'status': STATUS_DRAFT})

# store metadata for an object
# if the object already exists then the metadata will be updated,
# otherwise a new object will be created.
def store_metadata(object_id, metadata):
    client = MongoClient()
    db = client[MONGODATABASENAME]

    result = db.objects.update_one(
        {"object_id": object_id},
        {
            "$set": {'metadata': metadata},
            "$setOnInsert":
                {'object_id': object_id,
                 'metadata': metadata,
                 'status': STATUS_DRAFT},
                 "$currentDate": {"lastModified": True}
        },
        upsert=True)


# get an object's metadata, or None if the object does not exist
def get_metadata(object_id):
    document = get_object(object_id)
    if document is None:
        return None
    return document['metadata']

# get an object's status
def get_status(object_id):
    document = get_object(object_id)
    if document is None:
        return None
    return document['status']

# get everything that the database knows about the object
def get_object(object_id):
    client = MongoClient()
    db = client[MONGODATABASENAME]
    document = db.objects.find_one({'object_id': object_id})
    if document is None:
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

# add a new entity
def add_entity(object_id, entity_id, filename, length, checksum):
    client = MongoClient()
    db = client[MONGODATABASENAME]

    db.entities.update_one(
        {'object_id': object_id, 'entity_id': entity_id},
            { '$set' :
                 {'entity_id' : entity_id, 'object_id': object_id, 'name': filename, 'length': length, 'checksum': checksum }
            },
            upsert=True
    )


# add or update an entity
def update_entity(object_id, entity_id, filename):
    client = MongoClient()
    db = client[MONGODATABASENAME]

    db.entities.update_one(
        {'object_id': object_id, 'entity_id': entity_id},
            { '$set' :
                 {'entity_id' : entity_id, 'object_id': object_id, 'name': filename }
            },
            upsert=False
    )

# get information about an entity
def get_entity(object_id, entity_id):
    client = MongoClient()
    db = client[MONGODATABASENAME]

     # look for an existing entity
    document = db.entities.find_one({'object_id': object_id, 'entity_id': entity_id})
    if document is None:
        return None
    else:
        return document


def store_entity_metadata(object_id, entity_id, entity_md):
    db = MongoClient()[MONGODATABASENAME]
    db.entities.update_one(
        {"object_id": object_id, "entity_id": entity_id},
        {
           "$set": { 'object_id': object_id, 'entity_id': entity_id,
                'name': entity_md['name'], 'length': entity_md['length'], 'checksum': entity_md['checksum']},
           "$currentDate": {"lastModified": True}
        },
        upsert=True
    )


# get the entities for an object
def get_object_entities(object_id):
    client = MongoClient()
    db = client[MONGODATABASENAME]
    entity_cursor = db.entities.find({'object_id': object_id})
    entities = []
    for entity in entity_cursor:
        entities.append({'id': entity['entity_id']})

    return entities

# delete an entity
def delete_entity(object_id, entity_id):
     client = MongoClient()
     db = client[MONGODATABASENAME]

     db.entities.delete_one(
        {'object_id': object_id, 'entity_id': entity_id}
    )