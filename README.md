<h1 align="center">
FaaS: MapReduce System using Google Cloud Functions
</h1>

### Objective:

This project's main objective is to use Google Cloud Functions to install the MapReduce System as FaaS (Function as a Service) on Google Cloud.

### Implementation:
*	Setup the GCP buckets for cloud storage.
*	Created three cloud functions: main-function, mapper-function and reducer-function.
*	The main-function is triggered to start when any text file is added to the GCP bucket and creates a new json output file or updates the current json output file. This functionality of streaming is thus implemented.

Additionally, the MapReduce implementation has been explained in detail in the project MapReduced-From-Scratch and the same has been used for this project with the usage of Virtual Machines (VMs). The link for the same is here: https://lnkd.in/gY2nXewm

The changes implemented in this code is that the Client-Server architecture and usage of Virtual Machines is eliminated as this is taken care by Google Cloud. This functionality enables the MapReduce system to run faster and more efficiently.

### Set-Up:

For the set-up, two GCP Buckets are created for the Map-Reduce system as explained below – 
*	The first GCP Bucket is the “gcs-bucket-fall2022-files” where the user adds the text files that needs to be split and updated in the inverted index final output.
*	The second GCP Bucket is the “gcs-bucket-fall2022” where the cloud functions use these storage to store the intermediate files and the final json output file.

### How to run or perform the tests using the code?

The three folders main-function, mapper-function and reducer-function have the necessary codes to deploy the MapReduce System using Cloud Functions. these 3 folders need to be converted to zip files and can each be uploaded as a cloud function.

Before uploading the zip files as functions, some code changes need to be made.

#### Code changes and file addition:

Since I have used the GCP Bucket for the cloud storage, do the following changes -
* Add the service account key file to the server_files
* The code changes that need to be done is for the change in the name of the service account key (json file). This name change needs to be implemented in the 3 **main.py** code files which are in the 3 different folders. 

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "raghav-cskumar-fall2022-387fa080baee.json"

Instead of "raghav-cskumar-fall2022-387fa080baee.json", replace it with your service account key name (json file name).

* main-function - main.py: Line 476
* mapper-function - main.py: Line 115
* reducer-function - main.py: Line 142

Apart from these code changes to accommodate for GCP Bucket access, the url links for the mapper-function and reducer-function in the main-function code needs to be updated as illustrated below: 

* main-function: Line 15 (mapper-function url)
* main-function: Line 28 (reducer-function url)

![Code Changes Link](https://user-images.githubusercontent.com/96961381/211072719-bedfb15a-03b6-4eb2-b410-19752506d116.JPG)

### Cloud Functions:

#### main-function:
*	This function is the master function which is triggered by any new file added in the GCP Bucket.
*	When a new file gets added to the GCP Bucket, the main-function starts running and first, splits the file into the number of mappers. 
*	Then the split chunks are then sent to each mapper that is run in parallel by using Thread. 
*	A mapper barrier is implemented which ensures that the next set of code is executed only if all the mapper threads have finished running.
*	A mapper check is performed to see whether all the mappers have run successfully and if not, the failed mappers are run again. 
*	The hashmap distributed group by is then performed on the mapper output files to get the reducer inputs.
*	Reducer inputs are then sent to each reducer that is run in parallel by using Thread. 
*	A reducer barrier is implemented which ensures that the next set of code is executed only if all the reducer threads have finished running.
*	Similar to the mapper check, a reducer check is done to see whether all the mappers have run successfully and if not, the failed reducers are run again.
*	The reducer outputs are then combined to create a final output json file having the inverted index for the uploaded documents.
*	If a new file is again uploaded, the main-function is triggered and updates the inverted index output present in the final output json file.

#### mapper-function: Parallel Map and Barrier Synch for mappers

This function is called by the main-function and run in parallel using Thread with other mapper function calls using the requests.post http function call. The functions takes the given the input - file keys which are the keys used to access the Key-Value store through the get function associated with their respective data. The mapper retrieves the data through get and performs the operation of associating for the applications as follow – 
*	Word Count - word:1 and separated from others by the delimiter ‘,’ 
*	Inverted Index – doc_id@word:1 and separated from others by the delimiter ‘,’ 

Each mapper produces this intermediary output for each associated word. A mapper barrier is implemented to ensure that the next part of the code runs only after all the mapper threads have finished running. This output is given as input to the Distributed Shuffle/GroupBy. The code is well commented and explained.

![Mapper Barrier](https://user-images.githubusercontent.com/96961381/211068251-d5a71b5c-3bac-489e-82ea-32805e9fbc79.jpeg)

![mapper-function link](https://user-images.githubusercontent.com/96961381/211072722-a10f4e56-32af-473d-a92d-dcf6180b2ba5.JPG)

#### reducer-function: Parallel Reduce and Barrier Synch for reducers

This function is called by the main-function and run in parallel using Thread with other reducer function calls using the requests.post http function call. The functions takes the given the input - file keys which are the keys used to access the Key-Value store through the get function associated with their respective data. The reducer retrieves the data through get and performs the operation of associating for the applications as follow – 
*	Word Count - word:total_count and separated from others by the delimiter ‘,’ 
*	Inverted Index – doc_id@word:total_count and separated from others by the delimiter ‘,’  

Each reducer produces this final output for each associated word. A reducer barrier is implemented to ensure that the next part of the code runs only after all the mapper threads have finished running. This output is given as input to the write function which writes a json file using the reducer outputs. The code is well commented and explained.

![Reducer Barrier](https://user-images.githubusercontent.com/96961381/211068252-76123ee1-77f2-4cd2-b352-4b5112c3ec96.jpeg)

![reducer-function link](https://user-images.githubusercontent.com/96961381/211072723-427045f4-001d-4443-bf25-e15a9862478c.JPG)

### Intermediate Output:

The intermediate output is the output got after running the mapper functions. This output is used by the Distributed Shuffle/GroupBy as shown in the below code and sample intermediate outputs –

![Intermediate Output 1](https://user-images.githubusercontent.com/96961381/211068399-dfcfacf2-3620-4967-b1bc-a5be35028360.jpeg)

![Intermediate Output 2](https://user-images.githubusercontent.com/96961381/211068400-3466aa26-7751-4957-945b-368ccda787f3.jpeg)

### Streaming Search:

In this section, I want to emphasize on the automatic updates being done to the final inverted index output file when new files are being uploaded to the GCP Bucket. 

The Streaming Search function in my code starts with the main-function being triggered to run when a new file is being added to the google cloud storage Bucket as shown in the code below –

![Streaming Search 1](https://user-images.githubusercontent.com/96961381/211068537-9235af60-1db2-4ba9-938c-e1ba22b3f490.jpeg)

The above code takes the cloud_event(adding new file) as the input/trigger to start the main-function and the master_program function which is called due to this, accommodates handling the new file and update the current inverted index. The below code handles the number of mappers for the new files –

![Streaming Search 2](https://user-images.githubusercontent.com/96961381/211068538-0805052b-eed4-4a81-a578-c5254d826215.jpeg)

### Outputs:

By uploading the two files Five_years_in_Texas.txt and pg69113.txt, I got the final output json file which I downloaded and renamed it is output_12.json and the below shows the log output – 

![Output 1](https://user-images.githubusercontent.com/96961381/211069525-7a97d8ad-ba07-4a1d-9838-5fb9583cbb16.jpeg)

![Output 2](https://user-images.githubusercontent.com/96961381/211069529-d1085e57-61ad-4900-871b-4aa6c498bf0e.jpeg)

![Output 3](https://user-images.githubusercontent.com/96961381/211069531-7c5d8cb5-c3ad-4205-8484-c9fa68ecd37e.jpeg)

![Output 4](https://user-images.githubusercontent.com/96961381/211069630-90e1922c-bb2c-4340-9097-191c79e389cc.jpeg)

![Output 5](https://user-images.githubusercontent.com/96961381/211069636-d869e176-6835-475a-92c2-8880604ff6a4.jpeg)

![Output 6](https://user-images.githubusercontent.com/96961381/211069637-a1045e56-16bc-4af6-9c2b-0d4becdda7f3.jpeg)

As it can be seen in the above log pictures, it can be clearly seen that the main-function was triggered as soon as the text files were uploaded to the GCP Bucket. To check further and test whether the stream search functionality is working, I added a single new file pg69114.txt after this to get the updated inverted index json output for all the three files. The updated output file is downloaded and renamed to output_123.json. After checking, it can be seen that the inverted index is successfully updating.
