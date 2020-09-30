import os
import re
from dotenv import load_dotenv
load_dotenv()
import pymongo
from pymongo import MongoClient
import datetime
from datetime import datetime, timedelta
from bson.objectid import ObjectId
import json
import requests
import random
import logging
import re
from os import environ
import pika
from bson import ObjectId

mongo_url = "mongodb+srv://"+os.environ.get("SHARECHAT_DB_USERNAME")+":"+os.environ.get("SHARECHAT_DB_PASSWORD")+"@tattle-data-fkpmg.mongodb.net/test?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE"   
cli = MongoClient(mongo_url)
db = cli[os.environ.get("SHARECHAT_DB_NAME")]
coll = db[os.environ.get("SHARECHAT_DB_COLLECTION")]

credentials = pika.PlainCredentials(environ.get(
    'MQ_USERNAME'), environ.get('MQ_PASSWORD'))
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=environ.get('MQ_HOST'), credentials=credentials))
channel = connection.channel()
channel.queue_declare(queue='simple-search-report-queue', durable=True)

def callback(ch, method, properties, body):
    print("MESSAGE RECEIVED %r" % body)
    try:
        payload = json.loads(body)
        report = {}
        if payload["status"] == "indexed":
            report["status"] = payload["status"]
            report["index_timestamp"] = payload["index_timestamp"]
            report["index_id"] = payload["index_id"]
            print("Updating indexing status in Sharechat db ...")
            coll.update_one(
                {"_id": ObjectId(payload["source_id"])},
                {"$set": {"simple_search.indexer_status": report}})
            print("Success report stored in Sharechat db")
        elif payload["status"] == "failed":
            report["status"] = payload["status"]
            report["failure_timestamp"] = payload["failure_timestamp"]
            print("Updating indexing status in Sharechat db ...")
            coll.update_one(
                {"_id": ObjectId(payload["source_id"])},
                {"$set": {"simple_search.indexer_status": report}})
            print("Failure report stored in Sharechat db")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception:
        print(logging.traceback.format_exc())
    


channel.basic_consume(queue='simple-search-report-queue', on_message_callback=callback)

print(' [*] Waiting for messages. To exit press CTRL+C ')
channel.start_consuming()
