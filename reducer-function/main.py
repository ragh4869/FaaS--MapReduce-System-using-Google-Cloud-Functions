# Packages
from google.cloud import datastore
from google.cloud import storage
import functions_framework
import os
import json

# Defining reducer class
class red:

    def __init__(self, filename):
        self.filename = filename
        self.key_value = ""
        self.data = {}
        self.kv_store = {}
    
    # Word Count: Parse the reducer input into dictionary format along with counting each word instance
    def parse_red_wc(self):
        for key_val in self.key_value.split(','):
            key, val = key_val.split(':')
            if key in self.kv_store.keys():
                self.kv_store[key] += int(val)
            else:
                self.kv_store[key] = int(val)

    # Inverted Index: Parse the reducer input into dictionary format along with counting each doc_id-word instance
    def parse_red_inv_ind(self):
        for key_val in self.key_value.split(','):
            key_doc, val = key_val.split(':')
            doc_id, key = key_doc.split('@')
            if key in self.kv_store.keys():
                if doc_id in self.kv_store[key].keys():
                    self.kv_store[key][doc_id] += int(val)
                else:
                    self.kv_store[key][doc_id] = int(val)
            else:
                self.kv_store[key] = {doc_id: int(val)}

    # Word Count: Parse the dictionary output back into string
    def parse_dict_str_wc(self):
        self.key_value = ""
        for key in self.kv_store.keys():
            val = self.kv_store[key]
            if len(self.key_value) == 0:
                self.key_value = self.key_value + "{}:{}".format(key, val)
            else:
                self.key_value = self.key_value + ",{}:{}".format(key, val)

    # Inverted Index: Parse the dictionary output back into string
    def parse_dict_str_inv_ind(self):
        self.key_value = ""
        for key in self.kv_store.keys():
            for doc_id in self.kv_store[key].keys():    
                val = self.kv_store[key][doc_id]
                if len(self.key_value) == 0:
                    self.key_value = self.key_value + "{}@{}:{}".format(doc_id, key, val)
                else:
                    self.key_value = self.key_value + ",{}@{}:{}".format(doc_id, key, val)

    # Defining function to set the data in KV-store
    def call_set(self, key, val):

        try:
            data = {}
            storage_client = storage.Client()
            bucket = storage_client.get_bucket('gcs-bucket-fall2022')
            jsonfile_name = self.filename + ".json"
            blob = bucket.blob(jsonfile_name)
            if blob.exists():
                data = json.loads(blob.download_as_string(client=None))
            data[key] = val            
            blob.upload_from_string(data=json.dumps(data),content_type='application/json')
        except:
            print("Did not get data! set")


    # Defining function to get the data from KV-store
    def call_get(self, key):

        try:
            storage_client = storage.Client()
            bucket = storage_client.get_bucket('gcs-bucket-fall2022')
            blob = bucket.get_blob("data.json")
            data = json.loads(blob.download_as_string(client=None))
        except:
            data = {key:""}

        self.key_value = data[key]


# Defining reducer function for word-count
def red_wc(filename):
    
    # Initialize a reducer class instance
    r = red(filename)

    # Get the required data to perform the necessary operations
    r.call_get(filename)

    # Parse the reducer input string into a dictionary along with counting of words
    r.parse_red_wc()

    # Parse the dictionary output back into string
    r.parse_dict_str_wc()
    
    # Store the reducer output in KV-store
    key = r.filename + "_out"
    r.call_set(key, r.key_value)

    # Store the status update of the reducer output in KV-store
    key = r.filename + "_status"
    r.call_set(key, "yes")

    
# Defining reducer function for inverted-index
def red_inv_ind(filename):
    
    # Initialize an empty key-value store map
    r = red(filename)
    
    # Get the required data to perform the necessary operations
    r.call_get(filename)
    
    # Parse the reducer input string into a dictionary along with counting of doc_id-word
    r.parse_red_inv_ind()
    
    # Parse the dictionary output back into string
    r.parse_dict_str_inv_ind()
     
    # Store the reducer output in KV-store
    key = r.filename + "_out"
    r.call_set(key, r.key_value)
    
    # Store the status update of the reducer output in KV-store
    key = r.filename + "_status"
    r.call_set(key, "yes")


@functions_framework.http
def reducer(request):
    
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "raghav-cskumar-fall2022-387fa080baee.json"

    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'reducer_func' in request_json:
        reducer_func = request_json['reducer_func']
    elif request_args and 'reducer_func' in request_args:
        reducer_func = request_args['reducer_func']
    else:
        reducer_func = 'red_inv_ind'

    if request_json and 'filename' in request_json:
        filename = request_json['filename']
    elif request_args and 'filename' in request_args:
        filename = request_args['filename']
    else:
        filename = 'red_0'
  
    if reducer_func == "red_wc":
        red_wc(filename)
    elif reducer_func == "red_inv_ind":
        red_inv_ind(filename) 

    return ""

