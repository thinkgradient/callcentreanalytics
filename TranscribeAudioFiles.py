##############################################################
# 
# Author: Fatos Ismali, Alan Weaver
# Date: 30/03/2020
# 
# Python script that contains useful functions listing transcriptions,
# getting transcription JSON files, and initiating transcription process
# using either a baseline model or custom trained model
#
##############################################################

import requests
import json
import pandas as pd
import argparse
import time


config_file_name = "config.json"

with open(config_file_name, 'r') as f:
    configuration = json.load(f)

model_name = configuration["speech_model"]["model_name"]
endpoint_id = configuration["speech_model"]["endpoint_id"]
account_name = configuration["storage"]["account_name"] 
account_key = configuration["storage"]["account_key"] 
transcription_container = configuration["storage"]["transcription_container"] 
region = configuration["speech_api"]["region"]

BaseURL ="https://" + region + ".cris.ai/api/speechtotext/v2.1/transcriptions/"
EndpointURL = "https://" + region + ".cris.ai/api/speechtotext/v2.1/endpoints/"
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

##############################################################
# 
# Get the full path link to the JSON transcription and return the 
# JSON content from that.
#
##############################################################



def getTranscription(df):
    row=df.iloc[0,]
    headers = {"Ocp-Apim-Subscription-Key": SUBSCRIPTION_KEY}
    r = requests.get(row["resultUrl"], headers=headers)
    js = json.loads(r.text)
    return(js)


##############################################################
# 
# Given a transcription id obtain the blob file name (fileName) and the resulting
# Url to the transcription (resultUrl)
#
##############################################################

def getTranscriptionFiles(TID) :
    import datetime    
    df = pd.DataFrame(columns=["TID","fileName", "resultUrl"])
    headers = {"Ocp-Apim-Subscription-Key": SUBSCRIPTION_KEY}
    r = requests.get(BaseURL+TID, headers=headers)
    js = json.loads(r.text)
    for r in js["results"] :
      for rurls in r["resultUrls"] :
        df = df.append({
         "TID":                      TID,
         "fileName":                 rurls["fileName"],
         "resultUrl":                rurls["resultUrl"]
          }, ignore_index=True)
    return(df)





from azure.storage.blob import BlockBlobService
block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)

##############################################################
# 
# Delete all transcriptions that are currently in the list with status 
# either Succeeded or Failed. Those with status Started cannot be deleted.
#
##############################################################

def deleteAllTranscriptions() :
   trs=listAllTranscriptions()
   for x in trs["id"] :
      print(x)
      headers = {"Ocp-Apim-Subscription-Key": SUBSCRIPTION_KEY}
      r = requests.delete(BaseURL+x, headers=headers)
      print(r)


##############################################################
# 
# Given an model endpoint id return a custom model's id
#
##############################################################

def get_model_id(endpoint_id):
    headers = {"Ocp-Apim-Subscription-Key": SUBSCRIPTION_KEY,"Content-Type": "application/json"}
    r = requests.get(EndpointURL + endpoint_id, headers=headers)
    model_meta = json.loads(r.text)
    name = model_name
    model_id = ''
 
    for m in model_meta['models']:
        name = "base model"
        if name not in m['description']:
            model_id = m['id']
    print("MODEL ID : ", model_id)
    return model_id
 
##############################################################
# 
# Send POST request to Speech to Text API to start transcription using 
# either a baseline model or a custom trained model
#
##############################################################
 

def bulkTranscriptions(URL, blobname):
    headers = {"Ocp-Apim-Subscription-Key": SUBSCRIPTION_KEY,"Content-Type": "application/json"}
    if model_name.lower() != 'base_model':
        print("Using custom model: " + model_name)
        BD = """
        {
            "results": [],
            "recordingsUrls": [ 
                "__URL__"],
            "locale": "en-US",
            "name": "blob_name",
            "models": [{"id":" """+ str(get_model_id(endpoint_id)) + """"}],
            "properties": {
                "PunctuationMode": "Automatic",
                "ProfanityFilterMode": "Masked",
                "AddWordLevelTimestamps": "False",
                "AddSentiment": "False",
                "AddDiarization" : "True"
            }
        }
        """ 
    else:
        print("Using base model.")
        BD = """
        {
            "results": [],
            "recordingsUrls": [ 
                "__URL__"],
            "locale": "en-US",
            "name": "blob_name",
            "properties": {
                "PunctuationMode": "Automatic",
                "ProfanityFilterMode": "Masked",
                "AddWordLevelTimestamps": "False",
                "AddSentiment": "True",
                "AddDiarization" : "True"
            }
        }
        """
    
    print("Submitted: " + blobname + " for speech to text processing.")
    
    BD = BD.replace("__URL__", URL)  
    BD = BD.replace("blob_name", blobname)
    r = requests.post(BaseURL, headers=headers,data=BD)
    df = listAllTranscriptions()
    for index, row in df.iterrows():
      if row['name'] == blobname:
        return row['id']














