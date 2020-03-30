##############################################################
# 
# Author: Fatos Ismali
# Date: 30/03/2020
# 
# Main Python entry point to the transcription process. Traverses through a given 
# Azure storage blob container and folder within it and kicks of the transcription
# process for every .mp3 or .wav file it finds in there.
#
##############################################################


from azure.storage.blob import BlockBlobService
import subprocess
import GetJSONTempFiles
import TranscribeAudioFiles
import argparse
import json




config_file_name = "config.json"


parser = argparse.ArgumentParser()
parser.add_argument("audio_source_folder")
parser.add_argument("json_target_folder")
args = parser.parse_args()

print(18*"#" + "SOURCE FOLDER = ", args.audio_source_folder , + 18*"#")
print(18*"#" + "TARGET FOLDER = ", args.json_target_folder , + 18*"#")

with open(config_file_name, 'r') as f:
	configuration = json.load(f)


##############################################################
# 
# Initialize the variables for our storage account by reading the values
# provided in the config.json file
#
##############################################################

 
account_name = configuration["storage"]["account_name"] 
account_key = configuration["storage"]["account_key"] 
audio_container = configuration["storage"]["audio_container"] 
SAS = configuration["storage"]["SAS"] 
files_to_process = configuration["batch_config"]["files_to_process"]

folder_to_read_audio_from = args.audio_source_folder 

STORAGE_PATH="https://" + account_name + ".blob.core.windows.net/" + audio_container + "/"


blob_service = BlockBlobService(account_name, account_key)
generator = blob_service.list_blobs(audio_container, prefix=folder_to_read_audio_from)

audiofiles_list = []

GetJSONTempFiles.deleteAllTranscriptions()

current_file = 0


##############################################################
# 
# Loop through the given container / folder within it and append each blob (.mp3 or .wav file)
# to a list for processing
# Only process the number of files specified in the files_to_process variable
# 
##############################################################


for blob in generator:
	
	if current_file == files_to_process:
		break
	else:
	    print("Submitted blob for transcription: ", blob.name)
	    RECORDINGS_BLOB_URI = STORAGE_PATH + blob.name + SAS
	    #print(RECORDINGS_BLOB_URI)
	    audiofiles_list.append(TranscribeAudioFiles.bulkTranscriptions(RECORDINGS_BLOB_URI, blob.name))
	current_file += 1

print(audiofiles_list)

print(18*"#" + "PROCESSING....." + 18*"#")

GetJSONTempFiles.getTranscriptionsContent(audiofiles_list, args.json_target_folder)
