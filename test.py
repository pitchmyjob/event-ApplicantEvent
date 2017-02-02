from lambda_function import *
import boto3
from boto3.dynamodb.conditions import Key, Attr


session = boto3.session.Session(region_name="eu-west-1")
dynamodb = session.resource('dynamodb')
table = dynamodb.Table('dev-EventLog')

response = table.scan(
    IndexName="type-timestamp-index"
)



for res in response['Items']:
    print(res['event'])
    event = {
        'uuid': res['uuid'],
        'setting': "dev"
    }
    lambda_handler(event, None)