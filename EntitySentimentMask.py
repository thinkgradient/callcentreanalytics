import os
import json
import argparse
import pandas as pd
from azure.cognitiveservices.language.textanalytics import TextAnalyticsClient
from msrest.authentication import CognitiveServicesCredentials
import datetime

config_file_name = "config.json"


parser = argparse.ArgumentParser()
parser.add_argument("blobname")
parser.add_argument("masked_target_folder")
args = parser.parse_args()

with open(config_file_name, 'r') as f:
    configuration = json.load(f)

# Blob storage info 
account_name = configuration["storage"]["account_name"] 
account_key = configuration["storage"]["account_key"] 
transcription_container = configuration["storage"]["transcription_container"] 
region = configuration["text_api"]["region"]



SUBSCRIPTION_KEY_ENV_NAME = configuration["text_api"]["subscription_key"]
TEXTANALYTICS_LOCATION = os.environ.get(
    "TEXTANALYTICS_LOCATION", region)

##############################################################
# 
# Mask a word from a sentence given its startIndex and endIndex
#
##############################################################

def maskEntities(sentence, startIndex, endIndex):
    mask_length = endIndex-startIndex
    mask_tag = "Person"
    word_to_replace_with = None
    if mask_length > len(mask_tag):
        x = mask_length - len(mask_tag)
        word_to_replace_with = mask_tag + (x * "#")
    else:
        word_to_replace_with = mask_tag
    sentence="".join((sentence[:startIndex],word_to_replace_with, sentence[endIndex:]))
    return sentence


def maskMultipleEntities(sentence, offsets):
    speakertext = sentence
    if len(offsets) > 1:
        for i in range(0,len(offsets)):
            #print(offsets[i])
            s = maskEntities(speakertext, offsets[i][0], offsets[i][1])
            speakertext = s

    elif len(offsets) == 1:
        speakertext = maskEntities(speakertext, offsets[0][0], offsets[0][1])
    else:
        return speakertext
            
    
    return speakertext



##############################################################
# 
# Extract entities from each segment (i.e. speaker sentence)
#
##############################################################


def entity_extraction(subscription_key, docs):
    """EntityExtraction.
    Extracts the entities from sentences and prints out their properties.
    """
    datetimeFormat = '%Y-%m-%d %H:%M:%S.%f'
    t1 = datetime.datetime.now().strftime(datetimeFormat)

   
    credentials = CognitiveServicesCredentials(subscription_key)
    text_analytics_url = "https://{}.api.cognitive.microsoft.com".format(
        TEXTANALYTICS_LOCATION)
    text_analytics = TextAnalyticsClient(
        endpoint=text_analytics_url, credentials=credentials)
    
   
    df = pd.DataFrame(columns=["DocumentId","Name","Type", "Subtype","Offset","Length","Score"])
    
    try:
        documents = docs
        response = text_analytics.entities(documents=documents)  
        for document in response.documents:           
            for entity in document.entities:     
                for match in entity.matches:
                    df = df.append({"DocumentId" : document.id, "Name" : entity.name, "Type" : entity.type, "Subtype" : entity.sub_type,"Offset" : match.offset, "Length" : match.length, "Score" : "{:.2f}".format(match.entity_type_score)}, ignore_index=True)

    except Exception as err:
        print("Encountered exception. {}".format(err))

    t2 = datetime.datetime.now().strftime(datetimeFormat)
    diff = datetime.datetime.strptime(t2, datetimeFormat)\
     - datetime.datetime.strptime(t1, datetimeFormat)
    print(diff.seconds)
    return df



from azure.storage.blob import BlockBlobService
block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)

blob = block_blob_service.get_blob_to_text(transcription_container, args.blobname)



##############################################################
# 
# Go through each relevant JSON nodes in blob.content input file
# and construct the new output file containing the Display field
# with masked entities and also containing entities and sentiment 
# for each segment under the SegmentResults node 
#
##############################################################

datastore = json.loads(blob.content)

document = {}
display_text = []
number_of_segments = len(datastore['AudioFileResults'][0]['SegmentResults'])

document['AudioFileName'] = datastore['AudioFileResults'][0]['AudioFileName']
document['AudioLengthInSeconds'] = datastore['AudioFileResults'][0]['AudioLengthInSeconds']
document['SegmentResults'] = []
segment = {}

total_confidence_score = 0 
total_sentiment_score = 0

replacements = {' one':'#','two':'#','three':'#','four':'#','five':'#','six':'#','seven':'#','eight':'#','nine':'#','zero':'#','1':'#','2':'#','3':'#','4':'#','5':'#','6':'#','7':'#','8':'#','9':'#','0':'#'}


for i in range(0,number_of_segments-1):
    segment['SpeakerId'] = datastore['AudioFileResults'][0]['SegmentResults'][i]['ChannelNumber']
    segment['Confidence'] = datastore['AudioFileResults'][0]['SegmentResults'][i]['NBest'][0]['Confidence']
    speaker_display = datastore['AudioFileResults'][0]['SegmentResults'][i]['NBest'][0]['Display']

    segment['OffsetInSeconds'] = datastore['AudioFileResults'][0]['SegmentResults'][i]['OffsetInSeconds']
    segment['DurationInSeconds'] = datastore['AudioFileResults'][0]['SegmentResults'][i]['DurationInSeconds']
    for src, target in replacements.items():
        speaker_display = speaker_display.replace(src, target)
    segment['Display'] = speaker_display

    speaker_text = speaker_display.strip()
    
    entities = [{"id":"1", "language":"en","text": speaker_text}]
    
    # Get sentiment from each segment or speaker sentence
    segment['Sentiment'] = {}
    segment['Sentiment']['Negative'] = datastore['AudioFileResults'][0]['SegmentResults'][i]['NBest'][0]['Sentiment']['Negative']
    segment['Sentiment']['Positive'] = datastore['AudioFileResults'][0]['SegmentResults'][i]['NBest'][0]['Sentiment']['Positive']
    segment['Sentiment']['Neutral'] = datastore['AudioFileResults'][0]['SegmentResults'][i]['NBest'][0]['Sentiment']['Neutral']

    
    # Extract entities (Person only) from sentences that contain entities
    entities_tables = entity_extraction(SUBSCRIPTION_KEY_ENV_NAME, entities)
    #print(len(entities_tables.index))
    for index, row in entities_tables.iterrows():
        if(row['Type'].lower() == 'person'):
            
            segment["Entity" + str(index)] = [row['Type'], row['Offset'], row['Length']]
            
    document['SegmentResults'].append(segment)
    
    offsets = []
    for key, value in segment.items():
       
        if 'Entity' in key:    
            startIndex = value[1]
            endIndex = startIndex + value[2]
            offsets.append([startIndex,endIndex])
        
        
    if len(offsets) > 0:
        masked_speaker_text = maskMultipleEntities(speaker_text, offsets)
        segment['Display'] = masked_speaker_text
        display_text.append(masked_speaker_text)
    else:
        display_text.append(speaker_text)
    segment = {}
    

document['Display'] = ' '.join(display_text)


try:

    document_bytes = json.dumps(document).encode("utf-8")

    blobname = args.blobname.split("/")[-1]

    masked_file_name = args.masked_target_folder + 'Masked_' + blobname


    #Write JSON document containing masked transcriptions with entities and sentiment scores to Blob Storage

    block_blob_service.create_blob_from_bytes(transcription_container, masked_file_name, document_bytes)

except:
    print("Could not obtain sentiment from transcription: ", args.blobname)
    document_bytes = json.dumps(document).encode("utf-8")
    blobname = args.blobname.split("/")[-1]
    masked_file_name = args.masked_target_folder + 'Error_' + blobname
    block_blob_service.create_blob_from_bytes(transcription_container, masked_file_name, document_bytes)
    pass








