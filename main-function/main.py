# Packages
import functions_framework
from multiprocessing import Process
from multiprocessing import Barrier
from google.cloud import datastore
from google.cloud import storage
from threading import Thread
import os
import json
import requests

# Run the mapper using Thread 
def run_mapper(mapper_barrier, mapper_func, file_name):

    url = "" # Add the mapper-function url link given for the cloud function
    args = {"mapper_func": mapper_func,"filename": file_name}
    # Run the mapper function
    requests.post(url, json=args, verify=False)
    
    print("Mapper: {} is done".format(file_name))
    
    # Wait for all the mapper processes to end
    mapper_barrier.wait()

# Run the reducer using Thread 
def run_reducer(reducer_barrier, reducer_func, file_name):

    url = "" # Add the reducer-function url link given for the cloud function
    args = {"reducer_func": reducer_func,"filename": file_name}
    # Run the reducer function
    requests.post(url, json=args, verify=False)

    print("Reducer: {} is done".format(file_name))

    # Wait for all the reducer processes to end
    reducer_barrier.wait()

# Defining class Master
class Master:

    # Initializing master variables
    def __init__(self, mappers, reducers, mapper_func, reducer_func):
        self.key_value = ""
        self.kv_store = {}
        self.split_files = []
        self.folder_path = ''
        self.mappers = mappers
        self.reducers = reducers
        self.mapper_func = mapper_func
        self.reducer_func = reducer_func
        self.mapper_list = []
        self.reducer_list = [f"red_{i}" for i in range(1, self.reducers+1)]

    # Defining function to check if all mappers have finished running
    def check_mapper_reducer_completion(self, check, check_list):
        c = check
        not_done = []
        while check_list:
            
            check_file = check_list.pop(0)
            status_check_file = check_file+"_status"
            
            response_data = self.call_get(check_file+".json", status_check_file, 0)

            # Get the count of mappers/reducers that have successfully run
            if "yes" in response_data:
                c += 1
            # Get the list of mappers/reducers that have not successfully run
            else:
                not_done.append(check_file)

        # Return the count and list
        return c,not_done
    
    # Defining the function to split the file into chunks
    def file_chunk(self, file_size, sort_list, flag, mapper_count):
        
        storage_client = storage.Client()
        bucket = storage_client.get_bucket('gcs-bucket-fall2022-files')

        # Condition if the file sent is a text file
        if flag == 0:
            l = 1
            no_of_splits = mapper_count
            blob = bucket.get_blob(sort_list[0])
            data = blob.download_as_string().decode("utf-8")
            
            f_size = blob.size//no_of_splits + 1
            start, end = 0, f_size 
            # Split the content into number of mappers and add it to the cloud storage
            while start < blob.size: 
                content = data[start:end+1]
                key = blob.name.replace(".txt","_{}".format(l))
                self.split_files.append(key) # Append the split file keys
                content = content.replace('\n',' ')
                # Store the split file data in KV-store
                self.call_set(key, content)
                
                start += f_size
                end += f_size
                if end > blob.size:
                    end = blob.size
                l += 1
                
                mapper_count -= 1
                
            self.call_set('used_files', blob.name, 1)
        # Condition if the file sent is a folder
        else:
            file_count = len(sort_list) # Get file count
            for i in sort_list:
                l = 1
                blob = bucket.get_blob(i)
                data = blob.download_as_string().decode("utf-8")
                f_size = blob.size
                # If number of files and mappers are same, just read the whole file for each mapper
                if file_count == mapper_count:
                    no_of_splits = 1
                # If only one file is left then split it into the number of files required for the remaining mappers
                elif file_count == 1:
                    no_of_splits = mapper_count
                else:
                    no_of_splits = f_size/file_size
                # If the number of file split is around 1
                if round(no_of_splits) == 0 or round(no_of_splits) == 1:
                    key = blob.name.replace(".txt","_{}".format(l))
                    self.split_files.append(key) # Append the split file keys
                    content = data
                    content = content.replace('\n',' ')
                    # Store the split file data in KV-store
                    self.call_set(key, content)
                    
                    mapper_count -= 1
                    file_count -= 1
                    self.call_set('used_files', blob.name, 1)
                    continue
                else:
                    # If the number of file split is more than 1
                    f_size = f_size//round(no_of_splits) + 1
                    start, end = 0, f_size 
                    while start < blob.size: 
                        content = data[start:end+1]
                        key = blob.name.replace(".txt","_{}".format(l))
                        self.split_files.append(key) # Append the split file keys
                        content = content.replace('\n',' ')
                        # Store the split file data in KV-store
                        self.call_set(key, content)
                        
                        start += f_size
                        end += f_size
                        if end > blob.size:
                            end = blob.size
                        l += 1
                        mapper_count -= 1

                    file_count -= 1
                    self.call_set('used_files', blob.name, 1)
        
    # Defining function to split the files for m mappers
    def split_file(self):
        d = {}

        # Get all the files which are already updated in the inverted index 
        file_list = self.call_get('data.json', 'used_files', 0)
        file_list = file_list.split(',')

        # Get all the text files present in the GCP Bucket
        storage_client = storage.Client()
        bucket = storage_client.get_bucket('gcs-bucket-fall2022-files')
        blobs = bucket.list_blobs()

        file_size, file_num = 0, 0
        for blob in blobs:
            if ".txt" in blob.name and blob.name not in file_list:
                file_size += blob.size
                file_num += 1
                d[blob.name] = blob.size

        if file_num > self.mappers:
            self.mappers = file_num + 5

        if file_num == 1:
            flag = 0
            sort_list = list(d.keys())
            self.mappers = 3
        else:
            flag = 1
            file_size = file_size//self.mappers + 1
            d = dict(sorted(d.items(), key = lambda x: x[1], reverse = False))
            sort_list = list(d.keys())    
        
        # Call the file chunk function to split it according to the mappers
        self.file_chunk(file_size, sort_list, flag, self.mappers)

    # Word Count: Parse the mapper outputs into key:value format for reducer input files
    def parse_map_wc(self):
        key_list = ["" for _ in range(self.reducers)] # Creating list of strings for each reducer input 
        for key_val in self.key_value.split(','):
            key, val = key_val.split(':')
            red_val = abs(hash(key)) % self.reducers # Getting hash value
            # Updating the key & value in string format
            if len(key_list[red_val]) == 0:
                key_list[red_val] = key_list[red_val] + "{}:{}".format(key, val)
            else:
                key_list[red_val] = key_list[red_val] + ",{}:{}".format(key, val)

        # Return word list
        return key_list

    # Inverted Index: Parse the mapper outputs into doc_id@key:value format for reducer input files
    def parse_map_inv_ind(self):
        key_list = ["" for _ in range(self.reducers)] # Creating list of strings for each reducer input 
        for key_val in self.key_value.split(','):
            key_doc, val = key_val.split(':')
            doc_id, key = key_doc.split('@')
            red_val = abs(hash(key)) % self.reducers # Getting hash value
            # Updating the doc_id, key & value in string format
            if len(key_list[red_val]) == 0:
                key_list[red_val] = key_list[red_val] + "{}@{}:{}".format(doc_id, key, val)
            else:
                key_list[red_val] = key_list[red_val] + ",{}@{}:{}".format(doc_id, key, val)

        # Return word list
        return key_list

    # Word Count: Parse the reducer outputs into dictionary format along with counting each word instance
    def final_parse_map_wc(self):
        for key_val in self.key_value.split(','):
            key, val = key_val.split(':')
            if key in self.kv_store.keys():
                self.kv_store[key] += int(val)
            else:
                self.kv_store[key] = int(val)

    # Inverted Index: Parse the reducer outputs into dictionary format along with counting each doc_id-word instance
    def final_parse_map_inv_ind(self):
        for key_val in self.key_value.split(','):
            key_doc, val = key_val.split(':')
            doc_id, key = key_doc.split('@')
            if key in self.kv_store.keys():
                if doc_id[:-2] in self.kv_store[key].keys():
                    self.kv_store[key][doc_id[:-2]] += int(val)
                else:
                    self.kv_store[key][doc_id[:-2]] = int(val)
            else:
                self.kv_store[key] = {doc_id[:-2]: int(val)}

    # Defining function to call the set or delete file function
    def call_set(self, key, val, stat = 0):
        
        try:
            # Get all the key-values from data.json file
            storage_client = storage.Client()
            bucket = storage_client.get_bucket('gcs-bucket-fall2022')
            blob = bucket.get_blob('data.json')
            data = json.loads(blob.download_as_string(client=None))
            
            if stat == 1:
                if key in data.keys():
                    if data[key] == "":
                        data[key] = val
                    else:
                        data[key] = data[key] + "," + val
                else:
                    data[key] = val
            else:
                data[key] = val
            # Update the data.json file with new key-value
            blob.upload_from_string(data=json.dumps(data),content_type='application/json')
        except:
            print("Did not get data! set")

    # Defining function to get data
    def call_get(self, file_val, key, stat):
        
        try:
            # Get the data from the given file
            storage_client = storage.Client()
            bucket = storage_client.get_bucket('gcs-bucket-fall2022')
            blob = bucket.get_blob(file_val)
            data = json.loads(blob.download_as_string(client=None))
        except:
            data = {"key": ""}

        # Return the value of the given key in the data
        if stat == 0:
            if key in data.keys():
                return data[key]
            else:
                return ""
        # Update the key_value string data with the value of the key
        elif stat == 1:
            if len(self.key_value) == 0:
                self.key_value = self.key_value + data[key]
            else:
                self.key_value = self.key_value + ',' + data[key]


# Defining master program
def master_program(mappers, reducers, mapper_func, reducer_func):

    # Initializing Master class
    M = Master(mappers, reducers, mapper_func, reducer_func)

    M.split_file() # Split the files by size into m files where m is the number of mappers and update intermediary key-value store
    
    print("The files are split!")

    mapper_barrier = Barrier(M.mappers+1) # Barrier implementation for mapper threads

    # Running the m mappers
    for i in M.split_files:
        
        mapper_name = "map_"+i
        M.mapper_list.append(mapper_name) # Appending mapper list

        mapper_thread = Thread(target=run_mapper, args=(mapper_barrier, M.mapper_func, i))
        mapper_thread.start()

    # Check if all the mapper threads have completed or not
    print("Main thread waiting for all mapper results...")
    mapper_barrier.wait() # Wait till all mapper threads are done

    print("All mapper threads ran successfully!")

    mapper_list = M.mapper_list.copy() # Getting the list of file status to check for

    print("Running the mapper check!")

    # While loop to ensure the check and fault tolerance of mappers till all are successfully completed for the defined number of times (max_tries)
    max_tries, check = 10, 0
    while max_tries:
        # Running the check function
        check,not_done = M.check_mapper_reducer_completion(check, mapper_list)
        print("Number of mappers successfully completed: {}".format(check))
        if check == M.mappers:
            break
        else:
            max_tries -= 1
            # Implementing fault tolerance in case any of the mappers have not completed successfully
            new_mapper_barrier = Barrier(len(not_done)+1)
            # Running only the files which were not successfully completed
            for i in not_done:

                # Delete the json file of failed Mapper in GCP Bucket 
                storage_client = storage.Client()
                bucket = storage_client.get_bucket('gcs-bucket-fall2022')
                blob = bucket.blob(i+".json")
                if blob.exists():
                    blob.delete()

                file_val = i.replace('map_','') # Get filename

                # Start the new mapper thread
                new_mapper_thread = Thread(target=run_mapper, args=(new_mapper_barrier, M.mapper_func, file_val))
                new_mapper_thread.start()

            print("Main thread waiting for all remaining mapper results...")
            new_mapper_barrier.wait() # Wait till all mapper threads are done
            mapper_list = not_done.copy()

    print("All mapper threads and checks are done!")

    # Implementing hash function and creating reducer files
    for mapper_file in M.mapper_list:
        
        # Getting all the mapper outputs and combining them
        M.call_get(mapper_file+".json", mapper_file, 1)
        
    if M.mapper_func == "map_wc":
        key_list = M.parse_map_wc() # Parse the mapper output to reducer inputs for Word Count
    else:
        key_list = M.parse_map_inv_ind() # Parse the mapper output to reducer inputs for Inverted Index

    for key, val in zip(M.reducer_list, key_list):
        
        # Store the reducer input in KV-store
        M.call_set(key, val, 1)

    print("Reducer inputs are generated!")

    reducer_barrier = Barrier(M.reducers+1) # Barrier implementation for reducer threads

    # Running the r reducers         
    for file_name in M.reducer_list:

        reducer_thread = Thread(target=run_reducer, args=(reducer_barrier, M.reducer_func, file_name))
        reducer_thread.start()

    # Check if all the reducer processes have completed or not
    print("Main thread waiting for all reducer results...")
    reducer_barrier.wait() # Wait till all reducer threads are done

    print("All reducer threads ran successfully!")

    reducer_list = M.reducer_list.copy() # Getting the list of file status to check for

    # While loop to ensure the check and fault tolerance of reducers till all are successfully completed for the defined number of times (max_tries)
    max_tries, check = 10, 0
    while max_tries:
        # Running the check function
        check,not_done = M.check_mapper_reducer_completion(check, reducer_list)
        print("Number of reducers successfully completed: {}".format(check))
        if check == M.reducers:
            break
        else:
            max_tries -= 1
            # Implementing fault tolerance in case any of the reducers have not completed successfully
            new_reducer_barrier = Barrier(len(not_done)+1)
            # Running only the files which were not successfully completed
            for i in not_done:

                # Delete the json file of failed Reducer in GCP Bucket 
                storage_client = storage.Client()
                bucket = storage_client.get_bucket('gcs-bucket-fall2022')
                blob = bucket.blob(i+".json")
                if blob.exists():
                    blob.delete()

                # Start the new reducer thread
                new_reducer_thread = Thread(target=run_reducer, args=(new_reducer_barrier, M.reducer_func, i))
                new_reducer_thread.start()

            print("Main thread waiting for all remaining reducer results...")
            new_reducer_barrier.wait() # Wait till all reducer threads are done
            reducer_list = not_done.copy()

    print("All reducer threads and checks are done!")

    print("Creating the final json output file!")

    # Write a final json output file using the reducer outputs. This is done with the write file function interacting with the GCP Bucket
    M.key_value = ""
    for reducer_file in M.reducer_list:
        
        # Getting all the reducer outputs and combining them
        M.call_get(reducer_file+".json", reducer_file+"_out", 1)

    # Get the string into dictionary format
    if reducer_func == "red_wc":
        M.final_parse_map_wc() 
    else:
        M.final_parse_map_inv_ind()

    # Create the final output json file
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('gcs-bucket-fall2022')
    blob = bucket.blob("output.json")
    data = M.kv_store
    blob.upload_from_string(data=json.dumps(data),content_type='application/json')

    print("The final json output file is created!")

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def master(cloud_event):
   data = cloud_event.data

   event_id = cloud_event["id"]
   event_type = cloud_event["type"]

   bucket = data["bucket"]
   name = data["name"]
   metageneration = data["metageneration"]
   timeCreated = data["timeCreated"]
   updated = data["updated"]

   print(f"Event ID: {event_id}")
   print(f"Event type: {event_type}")
   print(f"Bucket: {bucket}")
   print(f"File: {name}")
   print(f"Metageneration: {metageneration}")
   print(f"Created: {timeCreated}")
   print(f"Updated: {updated}")

   os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "raghav-cskumar-fall2022-387fa080baee.json"

   with open('config.json') as con:
      config_data = json.load(con)
      
   # Running the task
   for key in config_data.keys():
      mappers = config_data[key]["mappers"]
      reducers = config_data[key]["reducers"]
      mapper_func = config_data[key]["mapper_func"]
      reducer_func = config_data[key]["reducer_func"]
      master_program(mappers, reducers, mapper_func, reducer_func)

   return ""
