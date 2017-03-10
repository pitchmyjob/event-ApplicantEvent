import boto3
import json
import os
from elasticsearch import Elasticsearch, RequestsHttpConnection
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime

class ApplicantEvent(object):
    host = ""
    push_es = True
    location = ("address", "latitude", "longitude", "street_number", "route", "cp", "locality", "administrative_area_level_1", "administrative_area_level_2", "country")

    def __init__(self, uuid):
        dynamodb = boto3.session.Session().resource('dynamodb')
        self.body = None
        self.uuid = uuid

        self.host = os.environ["NAME_ES_DOMAIN"]
        self.table = dynamodb.Table(os.environ["NAME_DYNAMODB_TABLE"])

        self.es = Elasticsearch(
            [self.host],
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )

        res = self.table.get_item(Key={'type': 'ApplicantEvent', 'uuid': self.uuid })
        self.event = res['Item']

        method = getattr(self, self.event['event'].lower(), None)
        if method:
            method()
            self.save()
            self.save_es()



    def get_index_es(self):
        try:
            index = self.es.get(index="matching", doc_type="applicant", id=self.event['id'])
            self.body = index['_source']
        except:
            self.body = {}


    def applicantwasdeleted(self):
        self.es.delete(index="matching", doc_type="applicant", id=self.event['id'])
        self.push_es = False

    def applicantwasmodified(self):
        self.get_index_es()
        del self.event['payload']['id']
        for key, item in self.event['payload'].items():
            if key in self.location :
                location = self.body['location'] if "location" in self.body else {}
                location[key] = item
                self.body["location"] = location
            else:
                self.body[key] = item

    def applicantwasadded(self):
        self.body = {
            "first_name": self.event['payload']['first_name'],
            "last_name": self.event['payload']['last_name'],
            "photo": self.event['payload']['photo'],
            "email": self.event['payload']['email'],
            "date_created" : datetime.now()
        }


    def experiencewasadded(self):
        self.added("experiences")

    def experiencewasmodified(self):
        self.modified("experiences")

    def experiencewasdeleted(self):
        self.deleted("experiences")


    def educationwasadded(self):
        self.added("educations")

    def educationwasmodified(self):
        self.modified("educations")

    def educationwasdeleted(self):
        self.deleted("educations")


    def skillwasadded(self):
        self.added("skills")

    def skillwasmodified(self):
        self.modified("skills")

    def skillwasdeleted(self):
        self.deleted("skills")


    def interestwasadded(self):
        self.added("interests")

    def interestwasmodified(self):
        self.modified("interests")

    def interestwasdeleted(self):
        self.deleted("interests")


    def languagewasadded(self):
        self.added("languages")

    def languagewasmodified(self):
        self.modified("languages")

    def languagewasdeleted(self):
        self.deleted("languages")


    def deleted(self, name):
        self.get_index_es()

        for i, exp in enumerate(self.body[name]):
            if exp['id'] == self.event['payload']['id']:
                self.body[name].pop(i)

    def modified(self, name):
        self.get_index_es()

        for exp in self.body[name]:
            if exp['id'] == self.event['payload']['id']:
                for key, item in self.event['payload'].items():
                    exp[key] = item

    def added(self, name):
        self.get_index_es()

        if name in self.body :
            self.body[name].insert(0, self.event['payload'])
        else:
            self.body[name] = [
                self.event['payload']
            ]

    def save_es(self):
        if self.push_es:
            self.body["last_modified"] = datetime.now()
            self.es.index(index="matching", doc_type="applicant", id=self.event['id'], body=self.body)

    def save(self):
        self.table.update_item(
            Key={
                'type': 'ApplicantEvent',
                'uuid': self.uuid
            },
            UpdateExpression="set is_read = :val",
            ExpressionAttributeValues={
                ':val': True
            }
        )


def lambda_handler(event, context):
    ApplicantEvent(uuid=event['uuid'])
