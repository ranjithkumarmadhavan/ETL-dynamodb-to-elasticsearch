import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
import decimal
from requests_aws4auth import AWS4Auth
import logging
import sys
import datetime

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

headers = { "Content-Type": "application/json" }
host = "https://YOUR_ENDPOINT.ap-south-1.es.amazonaws.com"
bulkUrl = host + "/_bulk"
fromDate = None
toDate = None
tableName = None

# for error handling
fileSuffix = 0
s3 = boto3.resource('s3')
bucketName = "BUCKET_NAME"
currentDate = datetime.datetime.today().strftime('%Y-%m-%d')
filesWithError = []

def lambda_handler(event, context):
    response = None
    fileSuffix = 0
    tableName = "DEMO-TABLE"
    uniqueId = "guid"
    index_name = "DEMO-TABLE"
    dateFieldName = "businessDate"
    table = boto3.resource('dynamodb',region_name = "ap-south-1").Table(tableName)
    # fromDate = "2020-10-01" #uncomment this line if you want to filter by date
    # toDate = "2020-11-30" #uncomment this line if you want to filter by date
    while True:
        document = ""
        if not response:
            # Scan from the start.
            if fromDate is None:
                response = table.scan()
            else:
                response = table.scan(FilterExpression=Attr(dateFieldName).gte(fromDate) & Attr(dateFieldName).lte(toDate))
        else:
            # Scan from where you stopped previously.
            if fromDate is None:
                response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            else:
                response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], FilterExpression=Attr(dateFieldName).gte(fromDate) & Attr(dateFieldName).lte(toDate))
        if len(response["Items"]) > 0:
            for item in response["Items"]:
                header = {
                    "index": {
                        "_index": index_name,
                        "_id": str(item[uniqueId]).replace('"',"").replace("'","")
                    }
                }
                document += json.dumps(header)+"\n"+json.dumps(item,cls=DecimalEncoder)+"\n"
                # break
            esResponse = requests.post(bulkUrl,auth=awsauth, data = document,headers = headers)
            # print(esResponse.text)
            if json.loads(esResponse.text)['errors'] == True:
                logger.info("Error in uploading document")
                # neglect the below lines if you want to store the error files on S3
                fileSuffix += 1
                out_file = "es-sync-error/{0}/{1}/{0}_{2}.json".format(tableName,currentDate,fileSuffix)
                s3.Object(bucketName, out_file).put(Body = esResponse.text)
                filesWithError.append(out_file)
            else:
                print("Successfully uploaded data")
            break
    
        # Stop the loop if no additional records are
        # available.
        if 'LastEvaluatedKey' not in response:
            break
    
    if len(filesWithError) > 0:
        return filesWithError
    else:
        return "Successfully Uploaded"
        
# to handle decimal fields from dynamodb
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 == 0:
                return int(o)
            else:
                return float(o)
        return super(DecimalEncoder, self).default(o)

# remove the below lines if you are going to deploy it on AWS lambda
startTime = datetime.datetime.now()
lambda_handler("","")
endTime = datetime.datetime.now()
print("Time taken - {0}".format(endTime - startTime))