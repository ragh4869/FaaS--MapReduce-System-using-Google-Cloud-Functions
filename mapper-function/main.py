# Packages
import re
from google.cloud import datastore
from google.cloud import storage
import functions_framework
import os
import json

# Defining mapper class
class map:

    def __init__(self, filename):
        self.kv_store = ""
        self.filename = filename
        self.lines = ""
        self.doc_id = self.filename

    # Defining function to get the word list from line with all transformations
    def line_to_word(self, line):
        line = re.sub(r'[^\x00-\x7F]+',' ', line)
        line = re.sub('\W+',' ', line)
        line = re.sub(r'[0-9]', '', line)
        word_list = line.split()
        return word_list

    # Defining function to set data from KV-store
    def call_set(self, key, val):

        try:
            data = {}
            storage_client = storage.Client()
            bucket = storage_client.get_bucket('gcs-bucket-fall2022')
            jsonfile_name = "map_" + self.filename + ".json"
            blob = bucket.blob(jsonfile_name)
            if blob.exists():
                data = json.loads(blob.download_as_string(client=None))
            data[key] = val
            blob.upload_from_string(data=json.dumps(data),content_type='application/json')
        except:
            print("Did not get data! set")
        
    
    # Defining function to get data from KV-store
    def call_get(self):

        try:
            storage_client = storage.Client()
            bucket = storage_client.get_bucket('gcs-bucket-fall2022')
            blob = bucket.get_blob('data.json')
            data = json.loads(blob.download_as_string(client=None))
        except:
            data = {}

        self.lines = data[self.filename]


# Defining mapper function for word-count
def map_wc(filename):
    
    # Initialize a mapper class instance
    m = map(filename)
    
    # Get the required data to perform the necessary operations
    m.call_get()
    
    # Get the word list from all the line
    word_list = m.line_to_word(m.lines) 

    # Convet to required string format as mapper output
    for word in word_list:
        if len(m.kv_store) == 0:
            m.kv_store = m.kv_store + "{}:1".format(word.lower())
        else:
            m.kv_store = m.kv_store + ",{}:1".format(word.lower())

    # Store the mapper output in KV-store
    key = "map_"+m.filename
    m.call_set(key, m.kv_store)
    
    # Store the status update of the mapper output in KV-store
    key = "map_"+m.filename+"_status"
    m.call_set(key, "yes")


# Defining mapper function for inverted-index
def map_inv_ind(filename):

    # Initialize a mapper class instance
    m = map(filename)
    
    # Get the required data to perform the necessary operations
    m.call_get()
    
    # Get the word list from all the line
    word_list = m.line_to_word(m.lines) 

    # Convet to required string format as mapper output
    for word in word_list:
        if len(m.kv_store) == 0:
            m.kv_store = m.kv_store + "{}@{}:1".format(m.doc_id,word.lower())
        else:
            m.kv_store = m.kv_store + ",{}@{}:1".format(m.doc_id,word.lower())

    # Store the mapper output in KV-store
    key = "map_"+m.filename
    m.call_set(key, m.kv_store)
    
    # Store the status update of the mapper output in KV-store
    key = "map_"+m.filename+"_status"
    m.call_set(key, "yes")

@functions_framework.http
def mapper(request):
    
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "raghav-cskumar-fall2022-387fa080baee.json"

    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'mapper_func' in request_json:
        mapper_func = request_json['mapper_func']
    elif request_args and 'mapper_func' in request_args:
        mapper_func = request_args['mapper_func']
    else:
        mapper_func = 'map_inv_ind'

    if request_json and 'filename' in request_json:
        filename = request_json['filename']
    elif request_args and 'filename' in request_args:
        filename = request_args['filename']
    else:
        filename = 'no_name'
  
    if mapper_func == "map_wc":
        map_wc(filename)
    elif mapper_func == "map_inv_ind":
        map_inv_ind(filename) 

    return ""