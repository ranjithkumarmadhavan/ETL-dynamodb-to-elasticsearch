import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
import decimal
from decimal import *
from requests_aws4auth import AWS4Auth
import logging
import sys
import datetime
import os
import uuid

sys.path.append('/opt/python/requests')
import requests

# Get logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

tableName = None
uniqueId = None
currentDate = datetime.datetime.today().strftime('%Y-%m-%d')
index_name = None
dateFieldName = None

my_session = boto3.session.Session()
region = my_session.region_name
service = "es"
credentials = my_session.get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

headers = { "Content-Type": "application/json" }
# Get url from environment variables
timeOut = int(os.environ['TIME_OUT'])
sqsUrl = os.environ['SQS_URL']
bucketName = os.environ['BUCKET_NAME']
host = os.environ['HOST_NAME']
tableName = os.environ['TABLE_NAME']
uniqueId = "guid"
index_name = os.environ['INDEX_NAME']
bulkUrl = host + "/_bulk"
logGroup = "existing-dynamodb-data-to-s3"
tableName = None
TYPE = "ES-SYNC"

table = boto3.resource('dynamodb').Table(tableName)

# for error handling
filePrefix = 0
s3 = boto3.resource('s3')

# sqsClient = session.client(service_name='sqs', endpoint_url='https://sqs.us-east-1.amazonaws.com') #if lambda inside vpc
sqsClient = boto3.client('sqs') 
session = boto3.Session()

currentDate = datetime.datetime.today().strftime('%Y-%m-%d')
filesWithError = []

def lambda_handler(event, context):
    try:
        logger.info(event)
        if ("Records" in event) and (len(event["Records"]) > 0):
            logger.info("Trigger through SQS.")
            for record in event["Records"]:
                event = eval(record["body"])
        else:
            logger.info("Triggered manually.")
        segmentNumber = int(event["segmentNumber"])
        if 'filePrefix' in event:
            filePrefix = int(event["filePrefix"])
        else:
            filePrefix = 0
        response = None
        lastEvaluatedKey = None
        fileSuffixFieldName = uniqueId
        if "lastEvaluatedKey" in event:
            response = {}
            response['LastEvaluatedKey'] = eval(json.loads(json.dumps(str(event["lastEvaluatedKey"]))))
            
        s3Folder = "es-sync/" + tableName + "/" + currentDate + "/"
        isDataPresent = True
        while isDataPresent:
            document = ""
            out_file = ""
            if not response:
                # Scan from the start.
                response = table.scan()
            else:
                # Scan from where you stopped previously.
                response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            if 'LastEvaluatedKey' in response:
                logger.info("Last evaluated key - {0}, Count - {1}".format(response['LastEvaluatedKey'],response['Count']))
                lastEvaluatedKey = response['LastEvaluatedKey']
            else:
                lastEvaluatedKey = "FINAL SET OF RECORDS"
            if len(response["Items"]) > 0:
                for item in response["Items"]:
                    header = {
                        "index": {
                            "_index": index_name,
                            "_id": str(item[uniqueId]).replace('"',"").replace("'","")
                        }
                    }
                    document += json.dumps(header)+"\n"+json.dumps(item,cls=DecimalEncoder)+"\n"
                    
                esResponse = requests.post(bulkUrl,auth=awsauth, data = document,headers = headers)
                filePrefix += 1
                filePrefixString = getPrefixString(filePrefix) #getting a four digit string
                if 'LastEvaluatedKey' in response:
                    out_file = '{0}{1}_{2}.json'.format(s3Folder, filePrefixString, response['LastEvaluatedKey'][fileSuffixFieldName])
                else:
                    out_file = '{0}{1}_{2}.json'.format(s3Folder, filePrefixString, "finalRecord")
                # adding record in s3 bucket 
                s3.Object(bucketName, out_file).put(Body = document)
                if json.loads(esResponse.text)['errors'] == True:
                    logger.error("Error in uploading document")
                    if 'LastEvaluatedKey' in response:
                        out_file = 'es-sync-error/{0}/{1}/{2}_{3}.json'.format(tableName,currentDate, filePrefixString, response['LastEvaluatedKey'][fileSuffixFieldName])
                    else:
                        out_file = 'es-sync-error/{0}/{1}/{2}_{3}.json'.format(tableName, currentDate,  filePrefixString, "finalRecord")
                    s3.Object(bucketName, out_file).put(Body = esResponse.text)
                    filesWithError.append(out_file)
                    error = "Error while uploading data to elasticsearch. Error File upload to s3 in {0}. Event - {1}".format(out_file, event)
                    logger.error(error)
                else:
                    logger.info("Successfully uploaded data")
                # break
            # get remaining processing time
            remainingTime = context.get_remaining_time_in_millis()
            logger.info("Remaining time - {0} ms ".format(remainingTime))
            if remainingTime < timeOut:
                logger.info("Lambda remaining time is less than 2 mins.So adding lastEvaluatedKey in sqs and ending this lambda.")
                segmentNumber += 1
                payload = {
                    "tableName" : tableName,
                    "lastEvaluatedKey" : lastEvaluatedKey,
                    "segmentNumber" : segmentNumber,
                    "filePrefix" : filePrefix
                }
                response = sqsClient.send_message(QueueUrl = sqsUrl, MessageBody=json.dumps(payload,cls=DecimalEncoder))
                logger.info("lastEvaluatedKey added in sqs.")
                isDataPresent = False
                break

            # Stop the loop if no additional records are
            # available.
            if 'LastEvaluatedKey' not in response:
                logger.info("No more records are present.")
                isDataPresent = False
                break
            else:
                logger.info("Last evaluated key - {0}".format(response['LastEvaluatedKey']))
                del document
                del out_file   
        if len(filesWithError) > 0:
            logger.info("File with error - {0}".format(filesWithError))
            return filesWithError
        else:
            logger.info("Successfully Uploaded without error in elasticsearch bulk upload.")
            return "Successfully Uploaded"
    except Exception as e:
        error = str(e) +" Error in lambda_handler(). Event : {0}".format(event)
        logger.error(e)
        
def getPrefixString(filePrefix):
    if filePrefix < 10:
        filePrefixString = "0000" + str(filePrefix)
    elif filePrefix < 100:
        filePrefixString = "000" + str(filePrefix)
    elif filePrefix < 1000:
        filePrefixString = "00" + str(filePrefix)
    else:
        filePrefixString = "0" + str(filePrefix)
    return filePrefixString


# to handle decimal fields from dynamodb
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 == 0:
                return int(o)
            else:
                return float(o)
        return super(DecimalEncoder, self).default(o)

