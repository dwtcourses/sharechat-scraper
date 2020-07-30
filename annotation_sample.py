import os
from dotenv import load_dotenv
load_dotenv()
import pandas as pd
import pymongo
from pymongo import MongoClient
import json
import numpy as np
import bson.json_util

# Get data from Mongo DB
mongo_url = "mongodb+srv://"+os.environ.get("SHARECHAT_DB_USERNAME")+":"+os.environ.get("SHARECHAT_DB_PASSWORD")+"@tattle-data-fkpmg.mongodb.net/test?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE"   
cli = MongoClient(mongo_url)
db = cli[os.environ.get("SHARECHAT_DB_NAME")]
coll = db["post"]


data = list(coll.aggregate([{ "$match": {"$and":[{"language":"Hindi"},{"media_type":{"$ne":"link"}}]}},
                            {"$sample":{"size": 50}},
                            ]))

annotation_dict = {'field-one': '', 'field-two': '', 'field-three':'', 'field-four':'',
                    "field-five":'', "field-six":'', "field-seven":'', "annotator_notes":''} 

fin = []

for dataobj in data: 

    temp = {}
    
    metadata = {'_id' : str(dataobj['_id']), 'media_type' : dataobj.get('media_type',''),
                'post_permalink' : dataobj.get('post_permalink',''), 'tag_name' : dataobj.get('tag_name',''),
                's3_url':dataobj.get('s3_url',''), 'timestamp':str(dataobj['timestamp']),
                'caption':dataobj.get('caption',''),'text':dataobj.get('text','')}
    temp['metadata'] = metadata
    temp['annotation'] = annotation_dict
    fin.append(temp)

with open('data.json', 'w') as f:
    f.write(bson.json_util.dumps(fin))