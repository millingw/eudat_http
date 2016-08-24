'''
curl http://127.0.0.1:5000/digitalobjects/
curl http://127.0.0.1:5000/digitalobjects/ed2c0631-6ccf-42ea-9ae0-e0fe93327847/entities
curl -X POST --form "file=@/tmp/test.txt" http://127.0.0.1:5000/digitalobjects/ed2c0631-6ccf-42ea-9ae0-e0fe93327847/entities
'''

import os
import uuid
import json

from flask import Flask, request, send_from_directory
from flask_restful import Resource, Api
import digital_objects as d
import mongo

STATUS_DRAFT = 'draft'
STATUS_COMMITTED = 'committed'
STATUS_PUBLISHED = 'published'
STATUS_DELETED = 'deleted'

app = Flask(__name__)
api = Api(app)

class DigitalObjects(Resource):
    def get(self):
        result = []
        objects = mongo.list_objects()
        for p in objects:
            result.append({"id": p['object_id']})
        return result

    def post(self):
        object_id = d.create_digital_object()
        metadata = request.get_json()
        if not metadata:
            metadata = {}
        mongo.store_metadata(object_id, metadata)

        return {"id": object_id}


class DigitalObject(Resource):

    def get(self, object_id):
        document = mongo.get_object(object_id)
        entities = mongo.get_object_entities(object_id)

        return {"id": object_id, 
                "metadata": document['metadata'],
                "status": document['status'],
                "files_count": len(entities),
                "entities": entities}

    def patch(self, object_id):

        body = request.get_json()
        if 'status' not in body:
            return {'message' : 'Invalid request: Status expected.'}, 400
        new_status = body['status']
        mongo.set_status(object_id, new_status)
        return {"id": object_id, "status": new_status}

    def delete(self, object_id):
        document = mongo.get_object(object_id)
        status = document['status']
        if status == STATUS_DRAFT:
            mongo.set_status(object_id, status)
            return '', 204
        else:
            return {'message': 'Digital object is not in draft status'}, 405



class DigitalEntities(Resource):



    def get(self, object_id):

        result = mongo.get_object_entities(object_id)
        entity_ids = []
        if result == None:
            return entity_ids
        for entity in result:
            entity_ids.append( {'id': entity['entity_id']} )
        return entity_ids

    def post(self, object_id):
        datafile = request.files['file']
        file_name = datafile.filename
        entity_id = str(uuid.uuid4())
        #entity_id = file_name
        entity_path = os.path.join(d.get_data_dir(object_id), entity_id)
        datafile.save(entity_path)
        file_length = os.path.getsize(entity_path)
        file_hash = d.get_checksum(entity_path)
        entity_md =  {"id": entity_id, 
                      "name": file_name,
                      "length": file_length, 
                      "checksum": file_hash}
        mongo.add_entity(object_id, entity_id, file_name, file_length, file_hash)
        return entity_md


def request_wants_json(request):
        best = request.accept_mimetypes \
            .best_match(['application/json', 'text/html'])
        return best == 'application/json' and \
            request.accept_mimetypes[best] > \
            request.accept_mimetypes['text/html']

class DigitalEntity(Resource):




    # look at the content request type -
    # if its json, we return the entity metadata
    # otherwise, we return the contents of the metadata file
    def get(self, object_id, entity_id):

        #### DEBUG ####
        print("***********")
        for header in request.headers:
            print(header)
        print("************")

        if request_wants_json(request):
            return mongo.get_entity(object_id, entity_id)
        else:
            return send_from_directory(directory=d.get_data_dir(object_id), filename=entity_id)

    def delete(self, object_id, entity_id):
        os.remove(os.path.join(d.get_data_dir(object_id), entity_id))
        mongo.delete_entity(object_id, entity_id)
        return '', 204

    def patch(self, object_id, entity_id):
        body = request.get_json()
        if 'filename' not in body:
            return {'message' : 'Invalid request: filename expected.'}, 400
        filename = body['filename']
        mongo.update_entity(object_id, entity_id, filename)
        return '', 204


api.add_resource(DigitalObjects, '/digitalobjects')
api.add_resource(DigitalObject, '/digitalobjects/<object_id>')
api.add_resource(DigitalEntities, '/digitalobjects/<object_id>/entities')
api.add_resource(DigitalEntity, '/digitalobjects/<object_id>/entities/<entity_id>')


if __name__ == '__main__':
    app.config.from_object('settings')
    app.run(debug=True)
