
import requests
import json
import pandas as pd
import time


config_file_name = "config.json"

with open(config_file_name, 'r') as f:
    configuration = json.load(f)

account_name = configuration["storage"]["account_name"] 
account_key = configuration["storage"]["account_key"] 
transcription_container = configuration["storage"]["transcription_container"] 
region = configuration["speech_api"]["region"]

BaseURL ="https://" + region + ".cris.ai/api/speechtotext/v2.1/transcriptions/"

SUBSCRIPTION_KEY = configuration["speech_api"]["subscription_key"] 

container_name = configuration["storage"]["audio_container"]
STORAGE_PATH="https://" + account_name + ".blob.core.windows.net/" + container_name + "/"

##############################################################
# 
# List all transcriptions currently in process of transcribing or that 
# have either succeeded or failed transcribing.
#
##############################################################


def listAllTranscriptions() :
    import datetime
    df = pd.DataFrame(columns=["id","name", "status","createdDateTime","lastActionDateTime","CallDuration"])
    headers = {"Ocp-Apim-Subscription-Key": SUBSCRIPTION_KEY}
    r = requests.get(BaseURL, headers=headers)
    js = json.loads(r.text)
    #print(js)
    for TR in js:
        if TR["status"] != "Succeeded" :
            Dur="-"
        else :
            Dur = TR["properties"]["Duration"]
        df = df.append({
         "id":                  TR["id"],
         "name":                TR["name"],
         "status":              TR["status"],
         "createdDateTime":     datetime.datetime.strptime(TR["createdDateTime"],"%Y-%m-%dT%H:%M:%SZ"),            
         "lastActionDateTime":  datetime.datetime.strptime(TR["lastActionDateTime"],"%Y-%m-%dT%H:%M:%SZ"),
         "CallDuration":            Dur      
          }, ignore_index=True)
        df["ProcTime"] = df["lastActionDateTime"] - df["createdDateTime"]
        
    return(df)

def deleteAllTranscriptions() :
   trs=listAllTranscriptions()
   for x in trs["id"] :
      print(x)
      headers = {"Ocp-Apim-Subscription-Key": SUBSCRIPTION_KEY}
      r = requests.delete(BaseURL+x, headers=headers)
      print(r)

def getTranscriptionFiles(TID) :
    import datetime    
    df = pd.DataFrame(columns=["TID","fileName", "resultUrl"])
    headers = {"Ocp-Apim-Subscription-Key": SUBSCRIPTION_KEY}
    r = requests.get(BaseURL+TID, headers=headers)
    js = json.loads(r.text)
    #print(js)
    for r in js["results"] :
      for rurls in r["resultUrls"] :
#        print(rurls["fileName"] +"," +rurls["resultUrl"])
        df = df.append({
         "TID":                      TID,
         "fileName":                 rurls["fileName"],
         "resultUrl":                rurls["resultUrl"]
          }, ignore_index=True)
    return(df)

def getTranscription(df):
    row=df.iloc[0,]
    headers = {"Ocp-Apim-Subscription-Key": SUBSCRIPTION_KEY}
    r = requests.get(row["resultUrl"], headers=headers)
    js = json.loads(r.text)
    return(js)

def deleteTranscription(TID):
      headers = {"Ocp-Apim-Subscription-Key": SUBSCRIPTION_KEY}
      r = requests.delete(BaseURL+TID, headers=headers)
      print(r)
      print(r.text)

from azure.storage.blob import BlockBlobService
block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)

##############################################################
# 
# Logs all transcriptions in a CSV file. Similar code can be build to
# to log transcriptions in an Azure SQL db instance or any other datastore
#
##############################################################


def logTranscriptionToCSV(id, name, status, createdDateTime, lastActionDateTime, CallDuration, ProcTime):
    df = pd.DataFrame(columns=["id","name", "status","createdDateTime","lastActionDateTime","CallDuration", "ProcTime"])
    df = df.append({
         "id":                  id,
         "name":                name,
         "status":              status,
         "createdDateTime":     createdDateTime,            
         "lastActionDateTime":  lastActionDateTime,
         "CallDuration":        CallDuration,
         "ProcTime":            ProcTime     
          }, ignore_index=True)
    
    with open('TranscriptionsLog.csv', 'a') as f:
        df.to_csv(f, header=False)


##############################################################
# 
# Iterate through the transcription process list to check if a transcription has Succeeded or Failed.
# For those that have succeeded transcribing obtain the JSON content and save it
# to the destination container / folder in Azure storage
#
##############################################################

def getTranscriptionsContent(transcription_ids, json_target_folder):

    list_of_transcription_ids = transcription_ids

    completed = False
    number_of_api_calls = 0
    # while not completed:

    counter = 0

    transcriptions_df = listAllTranscriptions()
    
    number_of_rows = len(list_of_transcription_ids)

    while counter != number_of_rows:

        for index, row in transcriptions_df.iterrows():
            if counter == number_of_rows:
                break
            for transcription_id in list_of_transcription_ids:

                if row["id"] == transcription_id and row["status"] in ("Succeeded","Failed"):
                    counter += 1
                    print("counter in transribe1: ", counter)
        time.sleep(15)
        transcriptions_df = listAllTranscriptions()
    


    file_counter = 0
    for index, row in transcriptions_df.iterrows():

        if row["id"] in (list_of_transcription_ids) and row["status"] == 'Succeeded':


            logTranscriptionToCSV(row["id"], row["name"], row["status"], row["createdDateTime"], row["lastActionDateTime"], row["CallDuration"], row["ProcTime"])
            # if transcription has succeeded get the id 
            transcription_id = row["id"]
            # get the link to the actual files of the transcription
            number_of_api_calls += 1
            trans_files_df = getTranscriptionFiles(transcription_id)
            # get the actual JSON contents from the transcription
            number_of_api_calls += 1
            transcription_json = getTranscription(trans_files_df)
            transcription_json_dump = json.dumps(transcription_json)
            json_data = json.loads(transcription_json_dump)
            file_name = json_data['AudioFileResults'][0]['AudioFileUrl']
            file_name = file_name.split("?")[0]
            file_name = file_name[len(STORAGE_PATH):]
            file_name = file_name.split('/')[-1]
            file_name = json_target_folder + file_name.split('.')[0] + ".json"
            print("Saving file: " + file_name + " to blob storage container: " + transcription_container)
            # convert the JSON contents to a byte array
            json_to_bytes = transcription_json_dump.encode("utf-8")
         

            block_blob_service.create_blob_from_bytes(transcription_container, file_name, json_to_bytes)
            deleteTranscription(transcription_id)
            file_counter += 1
        elif row["id"] in (list_of_transcription_ids) and row["status"] == 'Failed':
            logTranscriptionToCSV(row["id"], row["name"], row["status"], row["createdDateTime"], row["lastActionDateTime"], row["CallDuration"], row["ProcTime"])

