##############################################################
# 
# Author: Fatos Ismali
# Date: 30/03/2020
# 
# Python script that traverses through the JSON transcriptions
# and passes each to EntitiySentimentMask.py for extracting
# entities and sentiment and masking those entities which can be
# PII data
#
##############################################################

from azure.storage.blob import BlockBlobService
import subprocess
import argparse
import json


config_file_name = "config.json"


parser = argparse.ArgumentParser()
parser.add_argument("nonmasked_source_folder")
parser.add_argument("masked_target_folder")
args = parser.parse_args()

with open(config_file_name, 'r') as f:
	configuration = json.load(f)

 
account_name = configuration["storage"]["account_name"] 
account_key = configuration["storage"]["account_key"] 
transcription_container = configuration["storage"]["transcription_container"] 
SAS = configuration["storage"]["SAS"] 




blob_service = BlockBlobService(account_name, account_key)
generator = blob_service.list_blobs(transcription_container,prefix=args.nonmasked_source_folder) 
print(18*"#" + "Extract Entities + 18*"#")
print(16*"#" + "Sentiment Analysis" + 16*"#")
print(19*"#" + "Mask Entities" + 18*"#")


##############################################################
# 
# Initialize the variables for our storage account by reading the values
# provided in the config.json file
#
##############################################################

for blob in generator:
    file_name = blob.name
    print("## Processing blob: ", file_name)
    print("## Extracting Entities, Sentiment and Masking Entities...")

    subprocess.call("python EntitySentimentMask.py " + file_name + " " + args.masked_target_folder, shell=True)
    print("## " + file_name + " processsed successfully!")
	    