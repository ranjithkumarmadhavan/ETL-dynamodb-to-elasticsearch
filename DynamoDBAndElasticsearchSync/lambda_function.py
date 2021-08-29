import json
import boto3
from requests_aws4auth import AWS4Auth
import logging
import sys
from boto3.dynamodb.types import TypeDeserializer
import decimal
import os
import uuid
deserializer = TypeDeserializer()

sys.path.append('/opt/python/requests')
import requests

# Get logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

my_session = boto3.session.Session()
region = my_session.region_name
service = "es"
credentials = my_session.get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

# Get url from environment variables
host = os.environ['HOST_NAME']

headers = { "Content-Type": "application/json" }
bulkUrl = host + "/_bulk"
tableIndexName = "INDEX-NAME"
tableUniqueKey = "guid"
upsertDataList = None
deleteDataList = None
listOfIdsToDelete = None

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 == 0:
                return int(o)
            else:
                return float(o)
        return super(DecimalEncoder, self).default(o)

def lambda_handler(event, context):
    # logger.info(event)
    try:
        upsertDataList = []
        deleteDataList = []
        logger.info("No of records : {0}".format(len(event['Records'])))
        for record in event['Records']:
            if record['eventName'] == "INSERT" or record['eventName'] == "MODIFY":
                upsertDataList.append(record)
            elif record['eventName'] == "REMOVE":
                deleteDataList.append(record)
        if len(upsertDataList) > 0:
            handleUpsert(upsertDataList)
        if len(deleteDataList) > 0:
            handleDelete(deleteDataList)
    except Exception as e:
        error = str(e) +" Error in lambda_handler(). Event : {0}".format(event)
        logger.error(e)

def handleUpsert(ListOfRecords):
    logger.info("inside handleUpsert()")
    document = ""
    for record in ListOfRecords:
        newImage = record["dynamodb"]["NewImage"]
        # logger.info(newImage)
        deserialised = {k: deserializer.deserialize(v) for k, v in newImage.items()}
        indexName = None
        uniqueKey = None
        if tableIndexName in record["eventSourceARN"]:
            indexName = tableIndexName
            uniqueKey = tableUniqueKey
        else:
            error = "Table does not have index in elasticsearch"
            logger.error(error)
            raise error
        logger.info("indexName - {0} , uniqueKey - {1}".format(indexName,uniqueKey))
        if record['eventName'] == "MODIFY":
            logger.info("Update event")
            # logger.info("Old Data : " + str(record["dynamodb"]["OldImage"]))
        else:
            logger.info("Insert event")
        
        header = {
            "index": {
                "_index": indexName,
                "_id": str(deserialised[uniqueKey]).replace('"',"").replace("'","")
                }
            }
        document += json.dumps(header)+"\n"+json.dumps(deserialised,cls=DecimalEncoder)+"\n"
    # uploading data to elasticsearch 
    uploadToElasticSearch(document)


def handleDelete(ListOfRecords):
    logger.info("handling delete event")
    listOfIdsToDelete = []
    for record in ListOfRecords:
        OldImage = record["dynamodb"]["OldImage"]
        # logger.info(OldImage)
        if tableIndexName in record["eventSourceARN"]:
            deserialised = {k: deserializer.deserialize(v) for k, v in OldImage.items()}
            indexName = tableIndexName
            uniqueId = str(deserialised[tableUniqueKey]).replace('"',"").replace("'","")
            listOfIdsToDelete.append(uniqueId)
        else:
            error = "Table does not have index in elasticsearch"
            logger.error(error)
            raise error

    # delete data from elasticsearch
    if len(listOfIdsToDelete) > 0:
        deleteFromElasticSearch(tableIndexName, tableUniqueKey, listOfIdsToDelete)
        

def uploadToElasticSearch(document):
    logger.info("Inside uploadToElasticSearch()")
    try:
        response = requests.post(bulkUrl,auth=awsauth, data = document,headers = headers)
        if json.loads(response.text)['errors'] == True:
            logger.error("Error in uploading document")
            raise str(response.text)
        else:
            logger.info("Successfully uploaded data")
    except Exception as e:
        error = "Error in uploadToElasticSearch() - " + str(e)
        logger.error(error)
        
def deleteFromElasticSearch(indexName, uniqueKey,ids):
    logger.info("Inside deleteFromElasticSearch()")
    if type(ids[0]) == str:
        uniqueKey = uniqueKey + str(".keyword")
    try:
        payload = {
          "query": {
            "terms": {
              uniqueKey : ids
            }
          }
        }
        response = requests.post(host + "/" + indexName + "/_delete_by_query", auth=awsauth,data=json.dumps(payload), headers=headers)
        if 'failures' in json.loads(response.text) and len(json.loads(response.text)['failures']) > 0:
            logger.error("Error in deleting document")
            logger.error(response.text)
        else:
            logger.info("Successfully Deleted data")
    except Exception as e:
        logger.error("Error in deleteFromElasticSearch() - " + str(e))
