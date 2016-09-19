'''
curl http://127.0.0.1:5000/digitalobjects/
curl http://127.0.0.1:5000/digitalobjects/ed2c0631-6ccf-42ea-9ae0-e0fe93327847/entities
curl -X POST --form "file=@/tmp/test.txt" http://127.0.0.1:5000/digitalobjects/ed2c0631-6ccf-42ea-9ae0-e0fe93327847/entities
'''

import os
import uuid

from flask import Flask, request, send_from_directory
from flask_restful import Resource, Api
import digital_objects as d
#import mongo

from settings import md_store as md, STATUS_DRAFT, STATUS_DELETED

app = Flask(__name__)
api = Api(app)

class DigitalObjects(Resource):

    def get(self):
        result = []
        objects = md.list_objects()
        for p in objects:
            result.append({"id": p['object_id']})
        return result

    def post(self):
        object_id = d.create_digital_object()
        metadata = request.get_json()
        if not metadata:
            metadata = {}
        md.store_metadata(object_id, metadata)
        md.set_status(object_id, STATUS_DRAFT)

        return {"id": object_id}


class DigitalObject(Resource):

    def get(self, object_id):

        document = md.get_object(object_id)
        if document is None:
            return {'message': 'Digital object not found: %s' % object_id}, 404
        entities = md.get_object_entities(object_id)

        return {"id": object_id, 
                "metadata": document['metadata'],
                "status": document['status'],
                "files_count": len(entities),
                "entities": entities}

    def patch(self, object_id):
        body = request.get_json()
        if body is None or 'status' not in body:
            return {'message' : 'Invalid request: Status expected.'}, 400
        new_status = body['status']
        md.set_status(object_id, new_status)
        return {"id": object_id, "status": new_status}

    def delete(self, object_id):
        status = md.get_status(object_id)
        if status == STATUS_DRAFT:
            md.set_status(object_id, STATUS_DELETED)
            return '', 204
        else:
            return {'message': 'Digital object is not in draft status'}, 405



class DigitalEntities(Resource):

    def get(self, object_id):

        result = md.get_object_entities(object_id)
        if result is None:
            return []
        return result

    def post(self, object_id):
        datafile = request.files['file']
        file_name = datafile.filename
        entity_id = str(uuid.uuid4())
        return d.create_entity(object_id, entity_id, datafile)


class DigitalEntity(Resource):


    # look at the content request type -
    # if its json, we return the entity metadata
    # otherwise, we return the contents of the metadata file
    def get(self, object_id, entity_id):

        entity = md.get_entity(object_id, entity_id)
        if entity is None:
            return '', 404

        accept_type = request.headers['Accept']

        if accept_type == "application/json":
            # cant return it directly as ObjectID is not serializable
            return {'id': entity['entity_id'], 'name': entity['name'],'checksum': entity['checksum'], 'length': entity['length']}
        else:
            return send_from_directory(directory=d.get_data_dir(object_id), filename=entity_id)



    def delete(self, object_id, entity_id):
        status = md.get_status(object_id)
        if status != STATUS_DRAFT:
            return {'message': 'Digital object is not in draft status'}, 405
        try:
            os.remove(os.path.join(d.get_data_dir(object_id), entity_id))
            md.delete_entity(object_id, entity_id)
            return '', 204
        except:
            return '', 404

    def patch(self, object_id, entity_id):
        body = request.get_json()
        if 'name' not in body:
            return {'message' : 'Invalid request: filename expected.'}, 400
        name = body['name']
        md.update_entity(object_id, entity_id, name)
        return '', 204


api.add_resource(DigitalObjects, '/digitalobjects')
api.add_resource(DigitalObject, '/digitalobjects/<object_id>')
api.add_resource(DigitalEntities, '/digitalobjects/<object_id>/entities')
api.add_resource(DigitalEntity, '/digitalobjects/<object_id>/entities/<entity_id>')


if __name__ == '__main__':
    app.config.from_object('settings')
    app.run(debug=True)
