import unittest
import json
import requests
import os, shutil
from ..settings import STORAGE_DIR, md_store as md


TEST_URL = "http://127.0.0.1:5000/digitalobjects"
TEST_REPOSITORY_DIR = "/tmp/data"

# json sorting code from stackoverflow
# http://stackoverflow.com/questions/25851183/how-to-compare-two-json-objects-with-the-same-elements-in-a-different-order-equa
def ordered(obj):
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj





# clear the repository, ie nuke everything
def clear_repository():
    shutil.rmtree(STORAGE_DIR)
    os.makedirs(STORAGE_DIR)
    md.clear_metadata()


class TestAPI(unittest.TestCase):


  # create an object and change its status
  def testCreateObjectSetStatus(self):
      objectid = None
      with open("../metadata.json") as f:
           metadata = json.load(f)
           r = requests.post(TEST_URL, json=metadata )
           self.assertEqual(200, r.status_code)
           objectcontent = r.json()
           objectid = objectcontent['id']

           # now try and set the object status
           r = requests.patch(TEST_URL + "/" + objectid, json={'status': 'deleted'})
           self.assertEqual(200, r.status_code)

           r = requests.get(TEST_URL + "/" + objectid)
           retrievedObject = r.json()
           self.assertEqual('deleted', retrievedObject['status'])


  # create an object using a test json metadata payload
  # then verify we can retrieve it ok
  def testCreateObjectAddEntity(self):

      clear_repository()

      objectid = None
      with open("../metadata.json") as f:
           metadata = json.load(f)
           r = requests.post(TEST_URL, json=metadata )
           self.assertEqual(200, r.status_code)
           objectcontent = r.json()
           objectid = objectcontent['id']

           # now try and request the object information
           r = requests.get(TEST_URL + "/" + objectid)
           self.assertEqual(200, r.status_code)
           retrievedObject = r.json()
           self.assertEqual(objectid, retrievedObject['id'])

           # compare the returned metadata
           self.assertEqual(sorted(metadata), sorted(retrievedObject['metadata']))

      # add an entity, check the returned filesize matches
      entity_size = os.path.getsize('../entity.txt')
      files = {'file': ('entity.txt', open('../entity.txt', 'rb'), 'application/file', {'Expires': '0'})}
      r = requests.post(TEST_URL + "/" + objectid + "/entities", files=files)
      self.assertEqual(200, r.status_code)
      entitycontent = r.json()
      self.assertEqual("entity.txt", entitycontent['name'])
      self.assertEqual(entity_size, entitycontent['length'])

      # check we can retrieve the entity file
      r = requests.get(TEST_URL + "/" + objectid + "/entities/" + entitycontent['id'],
                       headers={"Accept":"application/octet-stream"} )
      self.assertEqual(200, r.status_code)

      # compare the content with the original file
      with open('../entity.txt', 'rb') as myfile:
         data=myfile.read()
         self.assertEqual(data, r.content)


      # delete the entity. subsequent calls to access the entity should give a 404 not found error
      r = requests.delete(TEST_URL + "/" + objectid + "/entities/" + entitycontent['id'])
      self.assertEqual(204, r.status_code)
      r = requests.get(TEST_URL + "/" + objectid + "/entities/" + entitycontent['id'])
      self.assertEqual(404, r.status_code)

      # make sure we can still get the object details
      r = requests.get(TEST_URL + "/" + objectid)
      self.assertEqual(200, r.status_code)
      retrievedObject = r.json()
      self.assertEqual(objectid, retrievedObject['id'])

      # compare the returned metadata
      self.assertEqual(sorted(metadata), sorted(retrievedObject['metadata']))

      # add 5 entities, then get the object information again;
      # confirm that the entity count is correct
      entity_ids = []
      for i in range(0, 5):
         r = requests.post(TEST_URL + "/" + objectid + "/entities", files=files)
         self.assertEqual(200, r.status_code)
         entity_json = r.json()
         entity_ids.append({ 'id': entity_json['id']})
      r = requests.get(TEST_URL + "/" + objectid)
      object_content = r.json()
      self.assertEqual(5, object_content['files_count'])

      # confirm that the entity ids match up with those returned by a get request on the parent object
      r = requests.get(TEST_URL + "/" + objectid + "/entities" )

      self.assertEqual(sorted(entity_ids), sorted(r.json()))

      # update an entity, verify we changed it's name ok
      testfilename = "blah.txt"
      r = requests.patch(TEST_URL + "/" + objectid + "/entities" + "/" + entity_ids[0]['id'], json={'name': testfilename})
      self.assertEqual(204, r.status_code)
      r = requests.get(TEST_URL + "/" + objectid + "/entities" + "/" + entity_ids[0]['id'], headers={'Accept':'application/json'})
      self.assertEqual(200, r.status_code)
      object_content = r.json()
      self.assertEqual(testfilename, object_content['name'])

      # check the existing entity filenames didnt change
      for i in range(1, 4):
          r = requests.get(TEST_URL + "/" + objectid + "/entities" + "/" + entity_ids[i]['id'], headers={'Accept':'application/json'})
          self.assertEqual(200, r.status_code)
          object_content = r.json()
          self.assertEqual("entity.txt", object_content['name'])





 # create several objects and check we can retrieve their ids
  def testCreateAndRetrieveObjects(self):

      clear_repository()

      objectids = []

      for i in range(0, 4):
         with open("../metadata.json") as f:
           metadata = json.load(f)
           r = requests.post(TEST_URL, json=metadata )
           self.assertEqual(200, r.status_code)
           objectcontent = r.json()
           objectids.append({ 'id': objectcontent['id']})

      # now ask for all the object ids, this should match our cached set
      r = requests.get(TEST_URL)
      self.assertEqual(200, r.status_code)
      retrievedObjects = r.json()

      # sort and compare the ids
      self.assertEqual(sorted(objectids), sorted(retrievedObjects))

      # retrieve each object individually as a final check
      for objectid in objectids:
          r = requests.get(TEST_URL + "/" + objectid['id'])
          self.assertEqual(200, r.status_code)


  def test_get(self):
      r = requests.get(TEST_URL)
      self.assertEqual(200, r.status_code)

if __name__ == '__main__':
    unittest.main()
