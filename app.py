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
        return {"id": object_id, 
                "metadata": document['metadata'],
                "status": document['status'],
                "files_count": len(os.listdir(d.get_data_dir(object_id)))}

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
        result = []
        for p in d.list_files(d.get_data_dir(object_id)):
            result.append({"id": p})
        return result

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
        mongo.update_entity(entity_id, file_name)
        return entity_md


class DigitalEntity(Resource):

    def get(self, object_id, entity_id):
        return send_from_directory(directory=d.get_data_dir(object_id), filename=entity_id)

    def delete(self, object_id, entity_id):
        os.remove(os.path.join(d.get_data_dir(object_id), entity_id))
        mongo.delete_entity(entity_id)
        return '', 204


api.add_resource(DigitalObjects, '/digitalobjects')
api.add_resource(DigitalObject, '/digitalobjects/<object_id>')
api.add_resource(DigitalEntities, '/digitalobjects/<object_id>/entities')
api.add_resource(DigitalEntity, '/digitalobjects/<object_id>/entities/<entity_id>')


if __name__ == '__main__':
    app.config.from_object('settings')
    app.run(debug=True)
